//! Fenced, expiring index builds. Only input references and publication bindings
//! live here; Record state and derived index contents remain immutable objects.
use super::*;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct IndexBuild {
    pub partition_id: String,
    pub build_epoch: u64,
    pub owner_epoch: u64,
    pub cut_seq: u64,
    pub base_manifest: Option<ObjectRef>,
    pub base_indexed_seq: u64,
    pub cut_wal_tip: Option<ObjectRef>,
    pub deadline_ms: i64,
}
fn pin_prefix(partition: &str) -> String {
    // Length-delimited identity avoids collisions between arbitrary partition IDs.
    format!("indexer:{}:{partition}:", partition.len())
}
fn release(connection: &Connection, partition: &str) -> rusqlite::Result<()> {
    let prefix = pin_prefix(partition);
    connection.execute(
        "DELETE FROM object_pins WHERE partition_id=?1 AND pin_kind='indexer' AND substr(CAST(pin_id AS BLOB),1,?2)=CAST(?3 AS BLOB)",
        params![partition, prefix.len(), prefix],
    )?;
    Ok(())
}
impl SqliteAuthority {
    /// Fix the exact committed input and pin its closure in the same transaction.
    /// Admission is nonblocking: at most one active build per partition.
    pub(crate) fn begin_index_build(
        &mut self, partition: &str, owner_epoch: u64, now_ms: i64, deadline_ms: i64,
    ) -> rusqlite::Result<Option<IndexBuild>> {
        if deadline_ms <= now_ms { return Err(rusqlite::Error::InvalidQuery); }
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let head = read_head(&tx, partition)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        if head.owner_epoch != owner_epoch { return Ok(None); }
        let old: Option<(i64, String, bool)> = tx.query_row(
            "SELECT build_epoch,binding_json,active FROM index_builds WHERE partition_id=?1",
            [partition], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        ).optional()?;
        if let Some((_, json, true)) = &old {
            let previous: IndexBuild = serde_json::from_str(json).map_err(|_| rusqlite::Error::InvalidQuery)?;
            if previous.deadline_ms > now_ms && previous.owner_epoch == owner_epoch { return Ok(None); }
        }
        let epoch = old.map_or(Some(1), |(epoch, _, _)| epoch.checked_add(1))
            .ok_or(rusqlite::Error::InvalidQuery)?;
        release(&tx, partition)?;
        let build = IndexBuild {
            partition_id: partition.into(), build_epoch: epoch as u64, owner_epoch,
            cut_seq: head.committed_seq, base_manifest: head.manifest.clone(),
            base_indexed_seq: head.indexed_seq, cut_wal_tip: head.wal_tip.clone(), deadline_ms,
        };
        for (slot, object) in [head.bootstrap, head.manifest, head.wal_tip, head.log_anchor].into_iter().flatten().enumerate() {
            ensure_durable_object(&tx, &object)?;
            tx.execute(
                "INSERT INTO object_pins(pin_id,partition_id,object_key,pin_kind,expires_at_ms) VALUES(?1,?2,?3,'indexer',?4)",
                params![format!("{}{epoch}:{slot}",pin_prefix(partition)),partition,object.key,deadline_ms],
            )?;
        }
        tx.execute(
            "INSERT INTO index_builds VALUES(?1,?2,?3,1) ON CONFLICT(partition_id) DO UPDATE SET build_epoch=excluded.build_epoch,binding_json=excluded.binding_json,active=1",
            params![partition,epoch,serde_json::to_string(&build).map_err(|_| rusqlite::Error::InvalidQuery)?],
        )?;
        tx.commit()?;
        Ok(Some(build))
    }

    /// Output objects are protected until publication or explicit cancellation.
    pub(crate) fn pin_index_build_output(&mut self,build:&IndexBuild,object:&ObjectRef,now_ms:i64) -> rusqlite::Result<()> {
        let tx=self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let binding:Option<String>=tx.query_row("SELECT binding_json FROM index_builds WHERE partition_id=?1 AND build_epoch=?2 AND active=1",
            params![build.partition_id,to_i64(build.build_epoch)?],|r|r.get(0)).optional()?;
        if binding.as_deref()!=Some(serde_json::to_string(build).map_err(|_|rusqlite::Error::InvalidQuery)?.as_str()) || now_ms>=build.deadline_ms {
            return Err(rusqlite::Error::InvalidQuery);
        }
        ensure_durable_object(&tx,object)?;
        tx.execute("INSERT OR IGNORE INTO object_pins(pin_id,partition_id,object_key,pin_kind,expires_at_ms) VALUES(?1,?2,?3,'indexer',?4)",
            params![format!("{}{}:output:{}",pin_prefix(&build.partition_id),build.build_epoch,object.key),build.partition_id,object.key,build.deadline_ms])?;
        tx.commit()
    }

    /// Cancel only this exact build. A late cleanup cannot cancel its successor.
    pub(crate) fn cancel_index_build(&mut self, build: &IndexBuild) -> rusqlite::Result<()> {
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute(
            "UPDATE index_builds SET active=0 WHERE partition_id=?1 AND build_epoch=?2 AND active=1",
            params![build.partition_id,to_i64(build.build_epoch)?],
        )?;
        if changed == 1 { release(&tx, &build.partition_id)?; }
        tx.commit()
    }

    /// Called only after acquiring the exclusive Record owner lock on reopen.
    /// No old worker can still use these pins. Epochs remain monotonic.
    pub(crate) fn cancel_owner_index_builds(&mut self, partition: &str, owner_epoch: u64) -> rusqlite::Result<()> {
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let head = read_head(&tx, partition)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        if head.owner_epoch != owner_epoch { return Err(rusqlite::Error::InvalidQuery); }
        tx.execute("UPDATE index_builds SET active=0 WHERE partition_id=?1", [partition])?;
        release(&tx, partition)?;
        tx.commit()
    }

    /// Publish a complete old cut while preserving every subsequently committed
    /// WAL block. Foreground head-version changes alone do not reject the build.
    pub(crate) fn publish_index_build(
        &mut self, build: &IndexBuild, manifest: ObjectRef, now_ms: i64,
    ) -> rusqlite::Result<Option<PartitionHead>> {
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let binding: Option<String> = tx.query_row(
            "SELECT binding_json FROM index_builds WHERE partition_id=?1 AND build_epoch=?2 AND active=1",
            params![build.partition_id,to_i64(build.build_epoch)?], |r| r.get(0),
        ).optional()?;
        let expected = serde_json::to_string(build).map_err(|_| rusqlite::Error::InvalidQuery)?;
        if binding.as_deref() != Some(expected.as_str()) || now_ms >= build.deadline_ms { return Ok(None); }
        let head = read_head(&tx, &build.partition_id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        if head.owner_epoch != build.owner_epoch || head.manifest != build.base_manifest
            || head.indexed_seq != build.base_indexed_seq || head.committed_seq < build.cut_seq {
            return Ok(None);
        }
        ensure_durable_object(&tx, &manifest)?;
        let has_tail = head.committed_seq > build.cut_seq;
        if has_tail && head.wal_tip.is_none() { return Err(rusqlite::Error::InvalidQuery); }
        let json = |object: &Option<ObjectRef>| -> rusqlite::Result<Option<String>> {
            object.as_ref().map(serde_json::to_string).transpose().map_err(|_| rusqlite::Error::InvalidQuery)
        };
        tx.execute(
            "UPDATE partition_heads SET head_version=head_version+1,manifest_json=?1,indexed_seq=?2,log_base_seq=?2,wal_tip_json=?3,log_anchor_json=?4 WHERE partition_id=?5",
            params![serde_json::to_string(&manifest).map_err(|_| rusqlite::Error::InvalidQuery)?,
                to_i64(build.cut_seq)?,json(&if has_tail {head.wal_tip} else {None})?,
                json(&if has_tail {build.cut_wal_tip.clone()} else {None})?,build.partition_id],
        )?;
        tx.execute("UPDATE index_builds SET active=0 WHERE partition_id=?1", [&build.partition_id])?;
        release(&tx, &build.partition_id)?;
        let published = read_head(&tx, &build.partition_id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        tx.commit()?;
        Ok(Some(published))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn database() -> (std::path::PathBuf,SqliteAuthority) {
        let path=std::env::temp_dir().join(format!("anyseek-indexer-{:032x}.sqlite",rand::random::<u128>()));
        let db=SqliteAuthority::open(&path,Duration::from_secs(1)).unwrap();
        (path,db)
    }
    fn object(db:&mut SqliteAuthority,key:&str)->ObjectRef {
        let r=ObjectRef{key:key.into(),sha256:[1;32],length:1};
        db.register_durable_object(&r,1).unwrap();r
    }
    fn commit(db:&mut SqliteAuthority,seq:u64)->PartitionHead {
        let h=db.read_head("p").unwrap().unwrap();
        let wal=object(db,&format!("wal/{seq}"));
        if let Some(previous)=&h.wal_tip {db.register_object_edge(&wal,previous).unwrap();}
        match db.publish_commit("p",h.head_version,h.owner_epoch,&format!("op/{seq}"),[seq as u8;32],seq,wal,None).unwrap() {
            PublishOutcome::Published(h)=>h,other=>panic!("{other:?}"),
        }
    }
    #[test]
    fn index_build_publication_preserves_concurrent_tail_and_receipts() {
        let (_path,mut db)=database();db.create_partition("p",1).unwrap();
        let first=commit(&mut db,1);
        let build=db.begin_index_build("p",1,10,100).unwrap().unwrap();
        assert!(db.begin_index_build("p",1,11,100).unwrap().is_none());
        let last=commit(&mut db,2);
        let manifest=object(&mut db,"manifest/cut-1");
        db.pin_index_build_output(&build,&manifest,12).unwrap();
        let selected=db.publish_index_build(&build,manifest.clone(),13).unwrap().unwrap();
        assert_eq!((selected.committed_seq,selected.indexed_seq,selected.log_base_seq),(2,1,1));
        assert_eq!(selected.wal_tip,last.wal_tip);
        assert_eq!(selected.log_anchor,first.wal_tip);
        assert_eq!(selected.manifest,Some(manifest.clone()));
        assert!(db.publish_index_build(&build,manifest,14).unwrap().is_none());
        assert!(db.resolve_operation("p","op/2",[2;32]).unwrap().is_some());
        assert_eq!(db.connection.query_row::<i64,_,_>("SELECT count(*) FROM object_pins WHERE pin_kind='indexer'",[],|r|r.get(0)).unwrap(),0);
    }
    #[test]
    fn index_build_rejects_expiry_modified_binding_old_owner_and_changed_base() {
        let (_path,mut db)=database();db.create_partition("p",1).unwrap();commit(&mut db,1);
        let build=db.begin_index_build("p",1,10,20).unwrap().unwrap();
        let manifest=object(&mut db,"manifest/cut");
        let mut forged=build.clone();forged.deadline_ms=200;
        assert!(db.publish_index_build(&forged,manifest.clone(),11).unwrap().is_none());
        assert!(db.publish_index_build(&build,manifest.clone(),20).unwrap().is_none());
        let successor=db.begin_index_build("p",1,20,100).unwrap().unwrap();
        db.cancel_index_build(&build).unwrap();
        assert!(db.publish_index_build(&build,manifest.clone(),21).unwrap().is_none());
        db.connection.execute("UPDATE partition_heads SET owner_epoch=2 WHERE partition_id='p'",[]).unwrap();
        assert!(db.publish_index_build(&successor,manifest.clone(),22).unwrap().is_none());
        let newer=db.begin_index_build("p",2,23,100).unwrap().unwrap();
        let h=db.read_head("p").unwrap().unwrap();
        db.publish_checkpoint("p",h.head_version,2,h.committed_seq,manifest.clone()).unwrap().unwrap();
        assert!(db.publish_index_build(&newer,manifest,24).unwrap().is_none());
        db.cancel_owner_index_builds("p",2).unwrap();
        assert_eq!(db.connection.query_row::<i64,_,_>("SELECT active FROM index_builds WHERE partition_id='p'",[],|r|r.get(0)).unwrap(),0);
    }
    #[test]
    fn index_build_pin_is_not_released_by_clock_or_late_cleanup() {
        let (_path,mut db)=database();db.create_partition("p",1).unwrap();
        let build=db.begin_index_build("p",1,1,10).unwrap().unwrap();
        let output=object(&mut db,"candidate");
        db.pin_index_build_output(&build,&output,2).unwrap();
        assert!(db.unreferenced_objects().unwrap().iter().all(|o|o.key!=output.key));
        assert!(!db.retire_object(&output.key,1,1000).unwrap());
        db.cancel_index_build(&build).unwrap();
        let successor=db.begin_index_build("p",1,1000,2000).unwrap().unwrap();
        db.pin_index_build_output(&successor,&output,1001).unwrap();
        db.cancel_index_build(&build).unwrap();
        assert!(db.unreferenced_objects().unwrap().iter().all(|o|o.key!=output.key));
        db.cancel_index_build(&successor).unwrap();
        assert!(db.unreferenced_objects().unwrap().iter().any(|o|o.key==output.key));
    }
}
