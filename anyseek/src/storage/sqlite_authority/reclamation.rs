//! Deleted service incarnation retirement. The caller has detached its local
//! root under the exclusive service owner, proving that old workers have ended.
use super::*;

impl SqliteAuthority {
    /// Detach a deleted partition's roots only after all explicit pins release.
    /// Keep a small identity tombstone so create/late publish cannot revive it.
    pub(crate) fn retire_partition(&mut self, partition: &str) -> rusqlite::Result<bool> {
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let pins = {
            let mut query = tx.prepare("SELECT partition_id,object_key FROM object_pins")?;
            query.query_map([], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        if pins.iter().any(|(id,key)| id == partition || super::super::object_keys::owns(partition,key)) {
            return Ok(false);
        }
        tx.execute("UPDATE partition_heads SET retired=1,wal_tip_json=NULL,manifest_json=NULL,bootstrap_json=NULL,log_anchor_json=NULL WHERE partition_id=?1", [partition])?;
        tx.execute("DELETE FROM operation_receipts WHERE partition_id=?1", [partition])?;
        tx.execute("DELETE FROM index_builds WHERE partition_id=?1", [partition])?;
        tx.execute("DELETE FROM restore_receipts WHERE partition_id=?1", [partition])?;
        // Even a never-activated creation is permanently fenced.
        tx.execute("INSERT OR IGNORE INTO partition_heads(partition_id,retired,head_version,owner_epoch,committed_seq,indexed_seq,log_base_seq) VALUES(?1,1,0,0,0,0,0)", [partition])?;
        tx.commit()?;
        Ok(true)
    }

    /// Work that still needs its first successful DELETE. Ambiguous PUT
    /// tombstones (state 5) are retried by the service-wide deletion sweep.
    pub(crate) fn reclamation_objects(&self, partition: &str, limit: usize) -> rusqlite::Result<Vec<ObjectRef>> {
        let mut query = self.connection.prepare("SELECT object_key,sha256,byte_length FROM object_uploads WHERE state IN (0,1,2,4) ORDER BY object_key")?;
        let mut result = Vec::new();
        for row in query.query_map([],registry_reference)? {
            let row = row?;
            if super::super::object_keys::owns(partition,&row.key) {
                result.push(row);
                if result.len() == limit { break; }
            }
        }
        Ok(result)
    }

    /// Deleted parents can never be pinned/published again. Their dependency
    /// edges no longer protect anything; retain only the immutable-key fences.
    pub(crate) fn prune_reclaimed_edges(&mut self, partition: &str, limit: usize) -> rusqlite::Result<bool> {
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let retired: bool = tx.query_row("SELECT retired FROM partition_heads WHERE partition_id=?1",[partition],|r|r.get(0))?;
        if !retired { return Err(rusqlite::Error::InvalidQuery); }
        let mut query = tx.prepare("SELECT DISTINCT e.parent_key FROM object_edges e JOIN object_uploads o ON o.object_key=e.parent_key WHERE o.state IN (3,5) ORDER BY e.parent_key")?;
        let mut parents = Vec::new();
        for row in query.query_map([],|r|r.get::<_,String>(0))? {
            let key = row?;
            if super::super::object_keys::owns(partition,&key) { parents.push(key); if parents.len()==limit {break;} }
        }
        drop(query);
        for parent in &parents { tx.execute("DELETE FROM object_edges WHERE parent_key=?1",[parent])?; }
        let complete = parents.len() < limit;
        tx.commit()?;
        Ok(complete)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retirement_waits_for_pins_fences_old_publish_and_survives_backup() {
        let temporary = tempfile::tempdir().unwrap();
        let mut db = SqliteAuthority::open(&temporary.path().join("source.sqlite"),Duration::from_secs(1)).unwrap();
        let partition = "tenant/docs/0000000000000001";
        let h = db.create_partition(partition,1).unwrap();
        let object = ObjectRef {key:super::super::super::object_keys::key(partition,"wal","one"),sha256:[1;32],length:1};
        db.register_durable_object(&object,0).unwrap();
        db.publish_commit(partition,h.head_version,1,"op",[2;32],1,object.clone(),None).unwrap();
        let child = ObjectRef {key:super::super::super::object_keys::key(partition,"records","child"),sha256:[9;32],length:1};
        db.register_durable_object(&child,0).unwrap();
        db.register_object_edge(&object,&child).unwrap();
        db.pin_object("reader",partition,&object.key,"reader",None).unwrap();
        assert!(!db.retire_partition(partition).unwrap());
        assert!(db.read_head(partition).unwrap().is_some());
        db.unpin_object("reader").unwrap();
        assert!(db.retire_partition(partition).unwrap());
        assert!(db.read_head(partition).unwrap().is_none());
        assert!(db.partition_ids().unwrap().is_empty());
        assert!(db.create_partition(partition,2).is_err());
        assert!(db.publish_commit(partition,1,1,"late",[3;32],2,object.clone(),None).is_err());
        assert_eq!(db.reclamation_objects(partition,8).unwrap().len(),2);
        for item in [&object,&child] {
            assert!(db.retire_object(&item.key,0,1).unwrap());
            assert!(db.mark_object_deleted(item).unwrap());
        }
        assert!(db.prune_reclaimed_edges(partition,8).unwrap());
        assert_eq!(db.connection.query_row("SELECT count(*) FROM object_edges",[],|r|r.get::<_,usize>(0)).unwrap(),0);
        assert!(db.reclamation_objects(partition,1).unwrap().is_empty());
        let next = "tenant/docs/0000000000000002";
        let head = db.create_partition(next,1).unwrap();
        let second = ObjectRef {key:super::super::super::object_keys::key(next,"wal","two"),sha256:[2;32],length:1};
        db.register_durable_object(&second,2).unwrap();
        db.publish_commit(next,head.head_version,1,"new",[4;32],1,second.clone(),None).unwrap();
        let bytes = db.prepare_backup_snapshot("retired",&[second],2,1024*1024).unwrap();
        let mut target = SqliteAuthority::open(&temporary.path().join("target.sqlite"),Duration::from_secs(1)).unwrap();
        let restored = target.restore_backup_snapshot(&bytes,1024*1024).unwrap();
        assert_eq!(restored.retired_partitions,vec![partition]);
        assert!(target.create_partition(partition,3).is_err());
        assert!(target.read_head(partition).unwrap().is_none());
        assert_eq!(db.partition_ids().unwrap().len(),1);
    }
}
