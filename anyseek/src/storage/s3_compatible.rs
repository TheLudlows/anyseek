//! Immutable object-store adapter for S3-compatible services.
//!
//! Object creation is conditional (never overwrite), then read back and verify the
//! bytes before returning an ObjectRef. Callers may register that reference with
//! SQLite only after this method succeeds. This module deliberately exposes no
//! delete method; deletion requires the authority/backup/pin reachability protocol.
use super::sqlite_authority::ObjectRef;
use object_store::{
    aws::AmazonS3Builder,
    path::Path as ObjectPath,
    ObjectStore, PutMode, PutOptions, PutPayload,
};
use sha2::{Digest, Sha256};
use serde::{Deserialize, Serialize};
use std::{fmt, sync::Arc};
use super::sqlite_authority::{AuthorityBackupSnapshot, SqliteAuthority};

/// Error from the object-first persistence stage.
#[derive(Debug)]
pub(crate) enum UploadRegisterError {
    ObjectStore(object_store::Error),
    Metadata(rusqlite::Error),
}
impl fmt::Display for UploadRegisterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ObjectStore(error) => write!(f, "object-store operation failed: {error}"),
            Self::Metadata(error) => write!(f, "SQLite object registration failed: {error}"),
        }
    }
}
impl std::error::Error for UploadRegisterError {}



/// Immutable descriptor for a joint metadata/object backup.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct BackupDescriptor {
    pub format_version: u32,
    pub backup_id: String,
    pub created_at_ms: i64,
    pub metadata_snapshot: ObjectRef,
    pub object_closure: Vec<ObjectRef>,
}

/// A created backup and the immutable reference that must be retained by the caller.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct BackupBundle {
    pub descriptor_ref: ObjectRef,
    pub descriptor: BackupDescriptor,
}

/// Backup/restore errors are explicit; a failed restore must not serve a partial database.
#[derive(Debug)]
pub(crate) enum BackupError {
    ObjectStore(object_store::Error),
    Metadata(rusqlite::Error),
    Decode(serde_json::Error),
    Invalid(&'static str),
}
impl fmt::Display for BackupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ObjectStore(e) => write!(f, "backup object-store operation failed: {e}"),
            Self::Metadata(e) => write!(f, "backup SQLite operation failed: {e}"),
            Self::Decode(e) => write!(f, "invalid backup descriptor or metadata: {e}"),
            Self::Invalid(e) => write!(f, "invalid backup bundle: {e}"),
        }
    }
}
impl std::error::Error for BackupError {}

/// S3-compatible immutable object store with write-after-read verification.
pub(crate) struct S3CompatibleStore {
    store: Arc<dyn ObjectStore>,
    max_object_bytes: usize,
}

impl S3CompatibleStore {
    /// Build an S3-compatible client. Endpoint, bucket and credentials are supplied
    /// by deployment configuration; credentials must never be persisted in SQLite.
    pub(crate) fn connect(
        endpoint: &str,
        bucket: &str,
        region: &str,
        access_key_id: &str,
        secret_access_key: &str,
        allow_http: bool,
        max_object_bytes: usize,
    ) -> object_store::Result<Self> {
        let store = AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_region(region)
            .with_access_key_id(access_key_id)
            .with_secret_access_key(secret_access_key)
            .with_allow_http(allow_http)
            .build()?;
        Ok(Self { store: Arc::new(store), max_object_bytes })
    }

    fn object_path(key: &str) -> object_store::Result<ObjectPath> {
        if key.is_empty() || key.starts_with('/') || key.contains('\\') {
            return Err(object_store::Error::InvalidPath {
                source: "object key must be a non-empty relative slash-separated key".into(),
            });
        }
        ObjectPath::parse(key)
    }

    /// Conditionally create a new object, then read it back and verify exact bytes.
    /// A pre-existing key is an error even if the caller believes the bytes match;
    /// callers must resolve retries by deterministic key plus metadata, not overwrite.
    pub(crate) async fn put_verified(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> object_store::Result<ObjectRef> {
        if bytes.len() > self.max_object_bytes {
            return Err(object_store::Error::Generic {
                store: "s3-compatible",
                source: format!("object size {} exceeds configured limit {}", bytes.len(), self.max_object_bytes).into(),
            });
        }
        let location = Self::object_path(key)?;
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        let put_result = self.store.put_opts(
            &location,
            PutPayload::from(bytes.to_vec()),
            PutOptions { mode: PutMode::Create, ..Default::default() },
        ).await;

        // If the response was lost after the service committed the PUT, a retry
        // may observe an existing key. Resolve that ambiguity only by reading
        // the existing bytes and proving they are byte-identical.
        if let Err(put_error) = put_result {
            match self.store.get(&location).await {
                Ok(result) => {
                    let existing = result.bytes().await?;
                    let existing_digest: [u8; 32] = Sha256::digest(&existing).into();
                    if existing.len() == bytes.len()
                        && existing_digest == digest
                        && existing.as_ref() == bytes {
                        return Ok(ObjectRef { key: key.to_owned(), sha256: digest, length: bytes.len() as u64 });
                    }
                    return Err(put_error);
                }
                Err(_) => return Err(put_error),
            }
        }

        // Successful PUT alone is not enough: verify visibility and exact content
        // through the read path used by later readers before publishing metadata.
        let downloaded = self.store.get(&location).await?.bytes().await?;
        let actual: [u8; 32] = Sha256::digest(&downloaded).into();
        if downloaded.len() != bytes.len() || actual != digest || downloaded.as_ref() != bytes {
            return Err(object_store::Error::Generic {
                store: "s3-compatible",
                source: "read-after-write verification failed".into(),
            });
        }
        Ok(ObjectRef { key: key.to_owned(), sha256: digest, length: bytes.len() as u64 })
    }

    /// Execute the object-first stage and register the verified reference in SQLite.
    /// This intentionally does not publish a committed sequence; the caller must
    /// publish the reference and operation receipt in a separate SQLite transaction.
    pub(crate) async fn put_and_register(
        &self,
        authority: &mut SqliteAuthority,
        key: &str,
        bytes: &[u8],
        created_at_ms: i64,
    ) -> Result<ObjectRef, UploadRegisterError> {
        let reference = self.put_verified(key, bytes).await.map_err(UploadRegisterError::ObjectStore)?;
        authority.register_durable_object(&reference, created_at_ms)
            .map_err(UploadRegisterError::Metadata)?;
        Ok(reference)
    }

    /// Fetch and verify an object against an authority-published reference.
    pub(crate) async fn get_verified(
        &self,
        reference: &ObjectRef,
    ) -> object_store::Result<Vec<u8>> {
        if reference.length > self.max_object_bytes as u64 {
            return Err(object_store::Error::Generic {
                store: "s3-compatible",
                source: "object reference exceeds configured size limit".into(),
            });
        }
        let location = Self::object_path(&reference.key)?;
        let bytes = self.store.get(&location).await?.bytes().await?;
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        if bytes.len() as u64 != reference.length || digest != reference.sha256 {
            return Err(object_store::Error::Generic {
                store: "s3-compatible",
                source: "object length or SHA-256 does not match committed reference".into(),
            });
        }
        Ok(bytes.to_vec())
    }

    /// Create a joint backup: atomically pin the complete live object closure and
    /// export a logical SQLite authority snapshot, upload that metadata snapshot,
    /// then publish an immutable descriptor referencing both. The caller must retain
    /// descriptor_ref; it is the restore entry point. Pins intentionally remain until
    /// the backup is explicitly retired. An ambiguous failure must not release pins.
    pub(crate) async fn create_backup_bundle(
        &self,
        authority: &mut SqliteAuthority,
        backup_id: &str,
        object_closure: &[ObjectRef],
        created_at_ms: i64,
        max_snapshot_bytes: usize,
    ) -> Result<BackupBundle, BackupError> {
        if backup_id.is_empty() || !backup_id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') {
            return Err(BackupError::Invalid("backup ID must contain only ASCII letters, digits, '-' or '_'"));
        }
        let snapshot_bytes = authority.prepare_backup_snapshot(
            backup_id, object_closure, created_at_ms, max_snapshot_bytes
        ).map_err(BackupError::Metadata)?;
        let snapshot_key = format!("backups/{backup_id}/authority.json");
        let metadata_snapshot = self.put_verified(&snapshot_key, &snapshot_bytes).await
            .map_err(BackupError::ObjectStore)?;
        let mut closure = object_closure.to_vec();
        closure.sort_by(|a,b| a.key.cmp(&b.key));
        let descriptor = BackupDescriptor {
            format_version: 1,
            backup_id: backup_id.to_owned(),
            created_at_ms,
            metadata_snapshot,
            object_closure: closure,
        };
        let descriptor_bytes = serde_json::to_vec(&descriptor).map_err(BackupError::Decode)?;
        let descriptor_key = format!("backups/{backup_id}/descriptor.json");
        let descriptor_ref = self.put_verified(&descriptor_key, &descriptor_bytes).await
            .map_err(BackupError::ObjectStore)?;
        Ok(BackupBundle { descriptor_ref, descriptor })
    }

    /// Restore into an empty SQLite authority only after validating the descriptor,
    /// metadata snapshot and every referenced payload object. Any missing/corrupt
    /// object aborts before the target database is modified.
    pub(crate) async fn restore_backup_bundle(
        &self,
        authority: &mut SqliteAuthority,
        descriptor_ref: &ObjectRef,
        max_snapshot_bytes: usize,
        max_objects: usize,
        max_total_object_bytes: usize,
    ) -> Result<AuthorityBackupSnapshot, BackupError> {
        let descriptor_bytes = self.get_verified(descriptor_ref).await
            .map_err(BackupError::ObjectStore)?;
        let descriptor: BackupDescriptor = serde_json::from_slice(&descriptor_bytes)
            .map_err(BackupError::Decode)?;
        if descriptor.format_version != 1 || descriptor.backup_id.is_empty()
            || descriptor.object_closure.len() > max_objects {
            return Err(BackupError::Invalid("unsupported descriptor or object-count budget exceeded"));
        }
        let snapshot_bytes = self.get_verified(&descriptor.metadata_snapshot).await
            .map_err(BackupError::ObjectStore)?;
        if snapshot_bytes.len() > max_snapshot_bytes {
            return Err(BackupError::Invalid("metadata snapshot exceeds configured limit"));
        }
        let snapshot: AuthorityBackupSnapshot = serde_json::from_slice(&snapshot_bytes)
            .map_err(BackupError::Decode)?;
        if snapshot.format_version != 1 || snapshot.backup_id != descriptor.backup_id
            || snapshot.object_closure != descriptor.object_closure {
            return Err(BackupError::Invalid("descriptor and metadata snapshot disagree"));
        }
        let mut total_bytes = 0usize;
        for reference in &descriptor.object_closure {
            let bytes = self.get_verified(reference).await.map_err(BackupError::ObjectStore)?;
            total_bytes = total_bytes.checked_add(bytes.len())
                .ok_or(BackupError::Invalid("object closure byte count overflow"))?;
            if total_bytes > max_total_object_bytes {
                return Err(BackupError::Invalid("object closure exceeds configured byte budget"));
            }
        }
        authority.restore_backup_snapshot(&snapshot_bytes, max_snapshot_bytes)
            .map_err(BackupError::Metadata)
    }

    /// Delete only an object the SQLite authority has monotonically retired and
    /// currently reports as unreferenced and unpinned. Repeated deletion is allowed.
    pub(crate) async fn delete_retired(
        &self,
        authority: &SqliteAuthority,
        key: &str,
        now_ms: i64,
    ) -> Result<bool, UploadRegisterError> {
        let collectible = authority.collectible_objects(now_ms)
            .map_err(UploadRegisterError::Metadata)?;
        let Some(reference) = collectible.iter().find(|reference| reference.key == key) else {
            return Ok(false);
        };
        let location = Self::object_path(&reference.key)
            .map_err(UploadRegisterError::ObjectStore)?;
        self.store.delete(&location).await
            .map_err(UploadRegisterError::ObjectStore)?;
        // Keep the SQLite retired-key tombstone permanently so a delayed writer
        // cannot reuse this key after the remote delete has completed.
        Ok(true)
    }

    #[cfg(test)]
    fn from_store(store: Arc<dyn ObjectStore>, max_object_bytes: usize) -> Self {
        Self { store, max_object_bytes }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn create_then_read_back_and_verify() {
        let store = S3CompatibleStore::from_store(Arc::new(InMemory::new()), 1024);
        let body = b"durable record bytes";
        let reference = store.put_verified("partition/p0/log/0001", body).await.unwrap();
        assert_eq!(reference.length, body.len() as u64);
        assert_eq!(store.get_verified(&reference).await.unwrap(), body);
    }

    #[tokio::test]
    async fn object_is_verified_before_sqlite_registration() {
        use std::time::{Duration, SystemTime};
        let stamp = SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!("anyseek-s3-register-{stamp}-{}.sqlite", std::process::id()));
        let mut authority = SqliteAuthority::open(&path, Duration::from_secs(1)).unwrap();
        authority.create_partition("p0", 1).unwrap();
        let store = S3CompatibleStore::from_store(Arc::new(InMemory::new()), 1024);
        let reference = store.put_and_register(&mut authority, "p0/log/1", b"record bytes", 100).await.unwrap();
        assert_eq!(store.get_verified(&reference).await.unwrap(), b"record bytes");
        let head = authority.read_head("p0").unwrap().unwrap();
        assert_eq!(head.committed_seq, 0, "upload/registration must not commit the record");
        let outcome = authority.publish_commit("p0", head.head_version, 1, "op-1", [4;32], 1, reference, None).unwrap();
        assert!(matches!(outcome, super::super::sqlite_authority::PublishOutcome::Published(_)));
        assert_eq!(authority.read_head("p0").unwrap().unwrap().committed_seq, 1);
        drop(authority);
        let _=std::fs::remove_file(&path);
        let _=std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _=std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[tokio::test]
    async fn deletion_requires_retirement_and_respects_pins() {
        use std::time::{Duration, SystemTime};
        let stamp = SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!("anyseek-s3-gc-{stamp}-{}.sqlite", std::process::id()));
        let mut authority = SqliteAuthority::open(&path, Duration::from_secs(1)).unwrap();
        authority.create_partition("p0", 1).unwrap();
        let store = S3CompatibleStore::from_store(Arc::new(InMemory::new()), 1024);
        let reference = store.put_and_register(&mut authority, "p0/staging/1", b"staging bytes", 1).await.unwrap();
        authority.pin_object("backup-pin", "p0", &reference.key, "backup", None).unwrap();
        assert!(!authority.retire_object(&reference.key, 1, 2).unwrap());
        assert!(!store.delete_retired(&authority, &reference.key, 2).await.unwrap());
        assert!(authority.unpin_object("backup-pin").unwrap());
        assert!(authority.retire_object(&reference.key, 1, 3).unwrap());
        assert!(store.delete_retired(&authority, &reference.key, 3).await.unwrap());
        assert!(!store.delete_retired(&authority, &reference.key, 3).await.unwrap());
        assert!(store.get_verified(&reference).await.is_err());
        assert!(authority.register_durable_object(&reference, 4).is_err());
        drop(authority);
        let _=std::fs::remove_file(&path);
        let _=std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _=std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[tokio::test]
    async fn immutable_key_and_size_limit_are_enforced() {
        let store = S3CompatibleStore::from_store(Arc::new(InMemory::new()), 8);
        let reference = store.put_verified("segments/s1", b"12345678").await.unwrap();
        assert_eq!(store.get_verified(&reference).await.unwrap(), b"12345678");
        assert!(store.put_verified("segments/s1", b"abcdefgh").await.is_err());
        assert_eq!(store.put_verified("segments/s1", b"12345678").await.unwrap(), reference);
        assert!(store.put_verified("segments/too-large", b"123456789").await.is_err());
        let wrong = ObjectRef { sha256: [0; 32], ..reference };
        assert!(store.get_verified(&wrong).await.is_err());
    }
}
