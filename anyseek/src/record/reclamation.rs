//! Physical retirement after a durable Catalog reclamation intent. This is not
//! a Record mutation and must only run under the owning service's lifecycle gate.
use super::*;

impl RecordPartition {
    /// Detach an explicitly deleted incarnation and remove at most `limit` local
    /// entries. Parent service ownership prevents new opens during retirement.
    pub async fn reclaim_deleted_files(path: &Path, limit: usize) -> Result<bool, RecordError> {
        let path = path.to_owned();
        crate::resources::spawn(async move {
            let scope = crate::resources::Scope::current();
            tokio::task::spawn_blocking(move || {
                let _io = scope.as_ref().map(|scope| scope.io()).transpose().map_err(|_| RecordError::Busy)?;
                let work = || {
                    if !path.parent().is_some_and(Path::exists) { return Ok(true); }
                    package_pins::retire_tree_if_unpinned(&path, || {
                        PartitionJournal::retire_snapshot_step(&path, limit).map_err(|error| match error {
                            CreateError::NotPublished(error) if error.kind() == std::io::ErrorKind::WouldBlock => RecordError::Busy,
                            error => creation_error(error),
                        })
                    })
                };
                match scope { Some(scope) => scope.run_sync(work), None => work() }
            }).await.map_err(storage)?
        }).await.map_err(storage)?
    }

    /// Release a deleted S3 incarnation after its local owner/root was retired.
    /// The caller must retain the Catalog lifecycle and service owner guards.
    #[cfg(feature = "s3-object-store")]
    pub async fn reclaim_deleted_s3(config: S3CompatibleRecordConfig, limit: usize) -> Result<bool, RecordError> {
        crate::resources::spawn(async move {
            if limit == 0 || limit > 64 { return Err(RecordError::Rejected("invalid reclamation batch".into())); }
            let (store, mut authority) = remote(&config)?;
            if let Some(head) = authority.read_head(&config.partition_id).map_err(storage)? {
                authority.cancel_owner_index_builds(&config.partition_id, head.owner_epoch).map_err(storage)?;
            }
            if !authority.retire_partition(&config.partition_id).map_err(storage)? { return Err(RecordError::Busy); }
            let objects = authority.reclamation_objects(&config.partition_id, limit).map_err(storage)?;
            for object in objects {
                authority.retire_object(&object.key, 0, unix_time_ms_i64()).map_err(storage)?;
                store.delete_retired(&mut authority, &object.key, unix_time_ms_i64()).await.map_err(storage)?;
            }
            if !authority.reclamation_objects(&config.partition_id, 1).map_err(storage)?.is_empty() { return Ok(false); }
            authority.prune_reclaimed_edges(&config.partition_id, limit).map_err(storage)
        }).await.map_err(storage)?
    }

    /// Retry monotonic object tombstones across all service incarnations. This
    /// includes ambiguous old PUTs after the Catalog incarnation row is gone.
    #[cfg(feature = "s3-object-store")]
    pub async fn retry_s3_deletions(config: S3CompatibleRecordConfig, after: String) -> Result<String, RecordError> {
        crate::resources::spawn(async move {
            let (store, mut authority) = remote(&config)?;
            let objects = authority.collectible_objects(unix_time_ms_i64()).map_err(storage)?;
            let mut next = String::new();
            for object in objects.into_iter().filter(|object| object.key > after).take(16) {
                store.delete_retired(&mut authority, &object.key, unix_time_ms_i64()).await.map_err(storage)?;
                next = object.key;
            }
            Ok(next)
        }).await.map_err(storage)?
    }
}

#[cfg(feature = "s3-object-store")]
fn remote(config: &S3CompatibleRecordConfig) -> Result<(
    crate::storage::s3_compatible::S3CompatibleStore,
    crate::storage::sqlite_authority::SqliteAuthority,
), RecordError> {
    let store = crate::storage::s3_compatible::S3CompatibleStore::connect(
        &config.endpoint, &config.bucket, &config.region, &config.access_key_id,
        &config.secret_access_key, config.allow_http, config.max_object_bytes,
    ).map_err(storage)?.background();
    let authority = crate::storage::sqlite_authority::SqliteAuthority::open(
        &config.authority_path, std::time::Duration::from_secs(5),
    ).map_err(storage)?;
    Ok((store, authority))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::tests::{config, write};

    #[tokio::test]
    async fn bounded_retirement_waits_for_owner_and_child_pin_then_resumes() {
        let temporary = tempfile::tempdir().unwrap();
        let path = temporary.path().join("old");
        let p = RecordPartition::create(&path, config()).await.unwrap();
        p.write(write("a", "a", "alpha", ExpectedVersion::Absent)).await.unwrap();
        assert_eq!(RecordPartition::reclaim_deleted_files(&path, 0).await, Err(RecordError::Busy));
        drop(p);
        let scope = Arc::new(crate::resources::Governor::default()).admit("reclaim-test".into(),crate::resources::Lane::Background,0).unwrap();
        let held = crate::resources::hold_io_for_test(&scope).await;
        assert_eq!(scope.clone().run(RecordPartition::reclaim_deleted_files(&path,0)).await,Err(RecordError::Busy));
        assert!(path.exists(), "I/O admission must precede root detachment");
        drop(held);
        let pin = package_pins::Pin::acquire(&[path.join("owner")]).unwrap();
        assert_eq!(RecordPartition::reclaim_deleted_files(&path, 0).await, Err(RecordError::Busy));
        drop(pin);
        for i in 0..100 { std::fs::write(path.join(format!("extra-{i}")), b"x").unwrap(); }
        assert!(!RecordPartition::reclaim_deleted_files(&path, 1).await.unwrap());
        assert!(!path.exists());
        let mut passes = 1;
        while !RecordPartition::reclaim_deleted_files(&path, 8).await.unwrap() { passes += 1; assert!(passes < 100); }
        assert!(passes > 10, "each cleanup must be bounded");
        assert!(RecordPartition::reclaim_deleted_files(&path, 8).await.unwrap());
        assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 0);
    }
}
