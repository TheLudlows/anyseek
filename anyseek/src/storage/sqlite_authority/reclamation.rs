//! Deleted service incarnation retirement. The caller has detached its local
//! root under the exclusive service owner, proving that old workers have ended.
use super::*;

impl SqliteAuthority {
    /// Detach a deleted partition's roots only after all explicit pins release.
    /// Keep a small identity tombstone so create/late publish cannot revive it.
    pub(crate) fn retire_partition(&mut self, partition: &str) -> rusqlite::Result<bool> {
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (pattern, generation, start) = object_selector(partition)?;
        let pinned: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM object_pins WHERE partition_id=?4 OR
                 (object_key GLOB ?1 AND (?2='' OR
                  substr(object_key,?3+instr(substr(object_key,?3),'/'),length(?2))=?2)))",
            params![pattern,generation,start,partition], |r| r.get(0),
        )?;
        if pinned {
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
        let (pattern, generation, start) = object_selector(partition)?;
        let mut query = self.connection.prepare(
            "SELECT object_key,sha256,byte_length FROM object_uploads
             WHERE state IN (0,1,2,4) AND object_key GLOB ?1 AND (?2='' OR
                 substr(object_key,?3+instr(substr(object_key,?3),'/'),length(?2))=?2)
             ORDER BY object_key LIMIT ?4")?;
        query.query_map(params![pattern,generation,start,to_i64(limit as u64)?],registry_reference)?.collect()
    }

    /// Deleted parents can never be pinned/published again. Their dependency
    /// edges no longer protect anything; retain only the immutable-key fences.
    pub(crate) fn prune_reclaimed_edges(&mut self, partition: &str, limit: usize) -> rusqlite::Result<bool> {
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let retired: bool = tx.query_row("SELECT retired FROM partition_heads WHERE partition_id=?1",[partition],|r|r.get(0))?;
        if !retired { return Err(rusqlite::Error::InvalidQuery); }
        let (pattern, generation, start) = object_selector(partition)?;
        let mut query = tx.prepare(
            "SELECT e.parent_key,e.child_key FROM object_edges e JOIN object_uploads o ON o.object_key=e.parent_key
             WHERE o.state IN (3,5) AND e.parent_key GLOB ?1 AND (?2='' OR
                 substr(e.parent_key,?3+instr(substr(e.parent_key,?3),'/'),length(?2))=?2)
             ORDER BY e.parent_key,e.child_key LIMIT ?4")?;
        let edges = query.query_map(params![pattern,generation,start,to_i64(limit as u64)?],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(query);
        for (parent,child) in &edges { tx.execute("DELETE FROM object_edges WHERE parent_key=?1 AND child_key=?2",params![parent,child])?; }
        let complete = edges.len() < limit;
        tx.commit()?;
        Ok(complete)
    }

    /// A bounded keyset page of candidates, not authorization to DELETE. Every
    /// candidate must still pass collectible_object and the completion recheck.
    pub(crate) fn retired_objects_page(&self, after: &str, limit: usize) -> rusqlite::Result<Vec<ObjectRef>> {
        let mut query = self.connection.prepare(
            "SELECT object_key,sha256,byte_length FROM object_uploads
             WHERE state IN (2,4,5) AND object_key>?1 ORDER BY object_key LIMIT ?2")?;
        query.query_map(params![after,to_i64(limit as u64)?],registry_reference)?.collect()
    }

    pub(crate) fn collectible_object(&self, key: &str) -> rusqlite::Result<Option<ObjectRef>> {
        let tx = self.connection.unchecked_transaction()?;
        let mut object = tx.query_row(
            "SELECT object_key,sha256,byte_length FROM object_uploads WHERE object_key=?1 AND state IN (2,4,5)",
            [key],registry_reference).optional()?;
        if object.is_some() && object_is_reachable(&tx,key)? { object = None; }
        tx.commit()?;
        Ok(object)
    }
}

// Match exactly object_keys::owns: category is the first path component after
// the collection prefix. GLOB narrows the indexed key range; the substring check
// prevents a nested category from impersonating a generation. IDs contain no
// GLOB metacharacters because valid_partition permits only ASCII name characters.
fn object_selector(partition: &str) -> rusqlite::Result<(String,String,usize)> {
    if !super::super::object_keys::valid_partition(partition) { return Err(rusqlite::Error::InvalidQuery); }
    Ok(match partition.rsplit_once('/') {
        Some((collection,generation)) => (
            format!("anyseek/{collection}/*/{generation}-*"),format!("{generation}-"),"anyseek/".len()+collection.len()+2,
        ),
        None => (format!("anyseek/{partition}/*"),String::new(),0),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reclamation_queries_are_scoped_paged_and_preserve_global_backup_pins() {
        let temporary = tempfile::tempdir().unwrap();
        let mut db = SqliteAuthority::open(&temporary.path().join("authority.sqlite"),Duration::from_secs(1)).unwrap();
        let old = "tenant_a/docs_b/0000000000000001";
        let next = "tenant_a/docs_b/0000000000000002";
        let other = "tenantXa/docs_b/0000000000000001";
        let mut objects = Vec::new();
        for partition in [old,next,other] {
            db.create_partition(partition,1).unwrap();
            for i in 0..20 {
                let object = ObjectRef {key:super::super::super::object_keys::key(partition,"records",&format!("{i:03}")),sha256:[1;32],length:1};
                db.register_durable_object(&object,0).unwrap(); objects.push(object);
            }
        }
        // GLOB alone would also match a generation inside a nested category.
        let nested = ObjectRef {key:"anyseek/tenant_a/docs_b/records/nested/0000000000000001-bad".into(),sha256:[1;32],length:1};
        db.register_durable_object(&nested,0).unwrap();
        assert!(db.reclamation_objects(old,0).unwrap().is_empty());
        assert_eq!(db.reclamation_objects(old,7).unwrap(),objects[..7]);
        assert_eq!(db.reclamation_objects(old,100).unwrap(),objects[..20]);
        db.pin_backup_object("kept",&objects[0].key).unwrap();
        assert!(!db.retire_partition(old).unwrap());
        db.release_backup_pins("kept").unwrap();
        db.pin_backup_object("other",&objects[40].key).unwrap();
        assert!(db.retire_partition(old).unwrap(),"another namespace must not block this incarnation");
        for object in &objects[..20] { assert!(db.retire_object(&object.key,0,1).unwrap()); }
        let mut after = String::new(); let mut seen = Vec::new();
        loop {
            let page = db.retired_objects_page(&after,7).unwrap();
            if page.is_empty() { break; }
            assert!(page.len() <= 7); after = page.last().unwrap().key.clone(); seen.extend(page);
        }
        assert_eq!(seen,objects[..20]);
        assert!(db.retired_objects_page("",0).unwrap().is_empty());
        // Edges of deleted parents are pruned in batches without touching a
        // nested lookalike or the replacement incarnation's dependency graph.
        for i in 0..3 {
            db.connection.execute("INSERT INTO object_edges VALUES(?1,?2)",params![objects[i].key,objects[i+3].key]).unwrap();
            assert!(db.mark_object_deleted(&objects[i]).unwrap());
        }
        db.register_object_edge(&nested,&objects[20]).unwrap();
        db.connection.execute("UPDATE object_uploads SET state=3 WHERE object_key=?1",[&nested.key]).unwrap();
        for object in &objects[6..10] {
            db.connection.execute("INSERT INTO object_edges VALUES(?1,?2)",params![objects[0].key,object.key]).unwrap();
        }
        assert!(!db.prune_reclaimed_edges(old,2).unwrap());
        assert_eq!(db.connection.query_row("SELECT count(*) FROM object_edges",[],|r|r.get::<_,usize>(0)).unwrap(),6,
            "the batch bounds edges, even when one parent has many children");
        assert!(!db.prune_reclaimed_edges(old,2).unwrap());
        assert!(!db.prune_reclaimed_edges(old,2).unwrap());
        assert!(db.prune_reclaimed_edges(old,2).unwrap());
        assert_eq!(db.connection.query_row("SELECT count(*) FROM object_edges",[],|r|r.get::<_,usize>(0)).unwrap(),1);
        assert_eq!(db.reclamation_objects(next,100).unwrap(),objects[20..40]);
    }

    #[test]
    fn targeted_gc_checks_keep_transitive_roots_pins_and_cycles_protected() {
        let temporary = tempfile::tempdir().unwrap();
        let mut db = SqliteAuthority::open(&temporary.path().join("authority.sqlite"),Duration::from_secs(1)).unwrap();
        db.create_partition("p",1).unwrap();
        let objects = ["a","b","c"].map(|key|ObjectRef {key:key.into(),sha256:[1;32],length:1});
        for object in &objects { db.register_durable_object(object,0).unwrap(); }
        db.register_object_edge(&objects[0],&objects[1]).unwrap();
        db.register_object_edge(&objects[1],&objects[0]).unwrap();
        db.register_object_edge(&objects[1],&objects[2]).unwrap();
        for column in ["wal_tip_json","manifest_json","bootstrap_json","log_anchor_json"] {
            db.connection.execute(&format!("UPDATE partition_heads SET {column}=?1 WHERE partition_id='p'"),[serde_json::to_string(&objects[0]).unwrap()]).unwrap();
            assert!(object_is_reachable(&db.connection,"c").unwrap());
            assert!(reachable_keys(&db.connection).unwrap().contains("c"));
            assert!(!db.retire_object("c",0,1).unwrap());
            db.connection.execute(&format!("UPDATE partition_heads SET {column}=NULL WHERE partition_id='p'"),[]).unwrap();
        }
        db.pin_object("pin","p","a","reader",None).unwrap();
        assert!(!db.retire_object("c",0,1).unwrap());
        db.unpin_object("pin").unwrap();
        assert!(db.retire_object("c",0,1).unwrap());
        assert_eq!(db.collectible_object("c").unwrap(),Some(objects[2].clone()));
        // Even a directly injected inconsistent root must fail closed before
        // remote DELETE and again before recording completion.
        db.connection.execute("UPDATE partition_heads SET bootstrap_json=?1 WHERE partition_id='p'",[serde_json::to_string(&objects[0]).unwrap()]).unwrap();
        assert!(db.collectible_object("c").unwrap().is_none());
        assert!(!db.mark_object_deleted(&objects[2]).unwrap());
        db.connection.execute("UPDATE partition_heads SET bootstrap_json=?1 WHERE partition_id='p'",[r#"{"key":"bad-missing-digest"}"#]).unwrap();
        assert!(db.collectible_object("c").is_err());
        assert!(db.mark_object_deleted(&objects[2]).is_err());
        assert!(db.retire_object("a",0,1).is_err());
        db.connection.execute("UPDATE partition_heads SET bootstrap_json=NULL WHERE partition_id='p'",[]).unwrap();
        assert!(db.mark_object_deleted(&objects[2]).unwrap());
        assert!(db.collectible_object("c").unwrap().is_none());
    }

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
