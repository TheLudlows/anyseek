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
use futures::StreamExt;
use super::verified_cache::VerifiedCache;
use super::sqlite_authority::{AuthorityBackupSnapshot, SqliteAuthority};
use crate::resources::{self, Buffer, Lane, Lease, Scope};

/// Error from the object-first persistence stage.
#[derive(Debug)]
pub(crate) enum UploadRegisterError {
    /// Admission failed before inventory mutation or any object-store request.
    Admission(crate::resources::Busy),
    ObjectStore(object_store::Error),
    Metadata(rusqlite::Error),
}
impl fmt::Display for UploadRegisterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(error) => write!(f, "upload admission denied: {error}"),
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
#[derive(Clone)]
pub(crate) struct S3CompatibleStore {
    store: Arc<dyn ObjectStore>,
    max_object_bytes: usize,
    cache: Option<VerifiedCache>,
    resources: Scope,
    background: bool,
}
struct UploadBudget { _io: Lease, _payload: Lease, readback: Lease }

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
        Ok(Self { store: Arc::new(store), max_object_bytes, cache: None, resources: resources::isolated_scope(), background: false })
    }

    pub(crate) fn with_cache(mut self, cache: VerifiedCache) -> Self {
        self.cache = Some(cache);
        self
    }
    pub(crate) fn background(mut self) -> Self { self.background = true; self }
    fn scope(&self, fallback: Lane) -> Scope {
        let scope = Scope::current().unwrap_or_else(|| self.resources.clone().in_lane(fallback));
        if self.background { scope.in_lane(Lane::Background) } else { scope }
    }
    pub(crate) fn retain_bytes(&self, bytes: u64) -> object_store::Result<Lease> {
        self.scope(Lane::Recovery).bytes(bytes).map_err(resource_error)
    }

    fn object_path(key: &str) -> object_store::Result<ObjectPath> {
        if key.is_empty() || key.starts_with('/') || key.contains('\\') {
            return Err(object_store::Error::Generic {
                store: "s3-compatible",
                source: std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "object key must be a non-empty relative slash-separated key",
                ).into(),
            });
        }
        Ok(ObjectPath::parse(key).map_err(|error| object_store::Error::Generic {
            store: "s3-compatible",
            source: error.into(),
        })?)
    }

    /// Conditionally create a new object, then read it back and verify exact bytes.
    /// A pre-existing key is an error even if the caller believes the bytes match;
    /// callers must resolve retries by deterministic key plus metadata, not overwrite.
    #[cfg(test)]
    pub(crate) async fn put_verified(
        &self,
        key: &str,
        bytes: &[u8],
    ) -> object_store::Result<ObjectRef> {
        let budget = self.upload_budget(key, bytes.len()).map_err(|error| match error {
            UploadRegisterError::Admission(error) => resource_error(error),
            UploadRegisterError::ObjectStore(error) => error,
            UploadRegisterError::Metadata(error) => object_store::Error::Generic { store: "s3-compatible", source: Box::new(error) },
        })?;
        self.put_budgeted(key, bytes, budget).await
    }

    fn upload_budget(&self, key: &str, length: usize) -> Result<UploadBudget, UploadRegisterError> {
        Self::object_path(key).map_err(UploadRegisterError::ObjectStore)?;
        if length > self.max_object_bytes {
            return Err(UploadRegisterError::ObjectStore(object_store::Error::Generic {
                store: "s3-compatible",
                source: format!("object size {length} exceeds configured limit {}", self.max_object_bytes).into(),
            }));
        }
        let scope = self.scope(Lane::Foreground);
        let _io = scope.io().map_err(UploadRegisterError::Admission)?;
        let _payload = scope.bytes(length as u64).map_err(UploadRegisterError::Admission)?;
        let readback = scope.bytes((length as u64).checked_mul(2).ok_or_else(||UploadRegisterError::ObjectStore(object_error("object byte budget overflow")))?).map_err(UploadRegisterError::Admission)?;
        Ok(UploadBudget { _io, _payload, readback })
    }

    async fn put_budgeted(&self, key: &str, bytes: &[u8], budget: UploadBudget) -> object_store::Result<ObjectRef> {
        // Prepay payload copy AND verification before inventory mutation or PUT.
        // The original input remains charged to its producer.
        let UploadBudget { _io, _payload, readback } = budget;
        let location = Self::object_path(key)?;
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        let reference = ObjectRef { key: key.to_owned(), sha256: digest, length: bytes.len() as u64 };
        let put_result = self.store.put_opts(
            &location,
            PutPayload::from(bytes.to_vec()),
            PutOptions { mode: PutMode::Create, ..Default::default() },
        ).await;

        // If the response was lost after the service committed the PUT, a retry
        // may observe an existing key. Resolve that ambiguity only by reading
        // the existing bytes and proving they are byte-identical.
        if let Err(put_error) = put_result {
            match self.download_verified(&reference, readback).await {
                Ok(existing) => {
                    if existing == bytes {
                        return Ok(reference);
                    }
                    return Err(put_error);
                }
                Err(_) => return Err(put_error),
            }
        }

        // Successful PUT alone is not enough: verify visibility and exact content
        // through the read path used by later readers before publishing metadata.
        // Always read the backend here. An older cached copy must never certify
        // that this PUT is durable/readable at the remote service.
        let downloaded = self.download_verified(&reference, readback).await?;
        if downloaded != bytes {
            return Err(object_store::Error::Generic {
                store: "s3-compatible",
                source: "read-after-write verification failed".into(),
            });
        }
        Ok(reference)
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
        let budget = self.upload_budget(key, bytes.len())?;
        self.register_budgeted(authority, key, bytes, created_at_ms, budget).await
    }
    async fn register_budgeted(&self, authority: &mut SqliteAuthority, key: &str, bytes: &[u8], created_at_ms: i64, budget: UploadBudget) -> Result<ObjectRef, UploadRegisterError> {
        let planned=ObjectRef{key:key.to_owned(),sha256:Sha256::digest(bytes).into(),length:bytes.len() as u64};
        authority.reserve_upload(&planned,created_at_ms).map_err(UploadRegisterError::Metadata)?;
        // Reuse registered immutable bytes without issuing another PUT. Read the
        // backend, never the cache: publication retains its corruption detection
        // point and a missing object must not be silently repaired here.
        if authority.is_durable_object(&planned).map_err(UploadRegisterError::Metadata)? {
            let UploadBudget { _io, _payload, readback } = budget;
            let existing = self.download_verified(&planned, readback).await
                .map_err(UploadRegisterError::ObjectStore)?;
            if existing != bytes {
                return Err(UploadRegisterError::ObjectStore(object_error("immutable object content mismatch")));
            }
            // Recheck registry state after I/O: a concurrent authority connection
            // may have retired the key. Retirement can never be reversed.
            authority.register_durable_object(&planned,created_at_ms).map_err(UploadRegisterError::Metadata)?;
            return Ok(planned);
        }
        let reference = self.put_budgeted(key, bytes, budget).await.map_err(UploadRegisterError::ObjectStore)?;
        authority.register_durable_object(&reference, created_at_ms)
            .map_err(UploadRegisterError::Metadata)?;
        Ok(reference)
    }

    async fn put_backup_object(&self,authority:&mut SqliteAuthority,backup_id:&str,key:&str,bytes:&[u8],at:i64)
        ->Result<ObjectRef,UploadRegisterError> {
        let budget = self.upload_budget(key, bytes.len())?;
        let planned=ObjectRef{key:key.to_owned(),sha256:Sha256::digest(bytes).into(),length:bytes.len() as u64};
        authority.reserve_backup_upload(&planned,at,Some(backup_id)).map_err(UploadRegisterError::Metadata)?;
        #[cfg(feature="fault-injection")]
        if std::env::var("ANYSEEK_REMOTE_BACKUP_FAULT").as_deref()==Ok("after_inventory") {std::process::exit(92);}
        self.register_budgeted(authority,key,bytes,at,budget).await
    }

    /// Fetch and verify an object against an authority-published reference.
    pub(crate) fn uncached(&self) -> Self {
        let mut store = self.clone();
        store.cache = None;
        store
    }

    pub(crate) async fn get_verified(
        &self,
        reference: &ObjectRef,
    ) -> object_store::Result<Buffer> {
        // Validate the reference even on a hit. Cache keys bind endpoint/bucket,
        // full object key (including partition namespace), hash and exact length.
        Self::object_path(&reference.key)?;
        if reference.length > self.max_object_bytes as u64 {
            return Err(object_error("object reference exceeds configured size limit"));
        }
        let scope = self.scope(Lane::Recovery);
        let _io = scope.io().map_err(resource_error)?;
        let lease = scope.bytes(reference.length.checked_mul(2).ok_or_else(|| object_error("object byte budget overflow"))?).map_err(resource_error)?;
        if let Some(bytes) = self.cache.as_ref().and_then(|cache| cache.get(reference)) {
            return Ok(Buffer::new(bytes, lease));
        }
        let bytes = self.download_verified(reference, lease).await?;
        if let Some(cache) = &self.cache { cache.insert(reference, &bytes); }
        Ok(bytes)
    }

    async fn download_verified(&self, reference: &ObjectRef, lease: Lease) -> object_store::Result<Buffer> {
        if reference.length > self.max_object_bytes as u64 {
            return Err(object_store::Error::Generic {
                store: "s3-compatible",
                source: "object reference exceeds configured size limit".into(),
            });
        }
        let location = Self::object_path(&reference.key)?;
        // Covers the accumulating Vec and one in-flight stream chunk. The lease
        // follows the returned buffer, rather than ending when GET completes.
        let result = self.store.get(&location).await?;
        if result.meta.size as u64 != reference.length {
            return Err(object_error("object length does not match committed reference"));
        }
        let mut stream = result.into_stream();
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(usize::try_from(reference.length).map_err(|_| object_error("object length overflow"))?)
            .map_err(|_| object_error("object buffer allocation failed"))?;
        let mut hash = Sha256::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if chunk.len() as u64 > reference.length.saturating_sub(bytes.len() as u64) {
                return Err(object_error("object stream exceeds committed length"));
            }
            hash.update(&chunk);
            bytes.extend_from_slice(&chunk);
        }
        let digest: [u8; 32] = hash.finalize().into();
        if bytes.len() as u64 != reference.length || digest != reference.sha256 {
            return Err(object_error("object length or SHA-256 does not match committed reference"));
        }
        Ok(Buffer::new(bytes, lease))
    }

    /// Persist and publish the immutable initial checkpoint/configuration for an
    /// object-backed Record partition. This establishes the recovery root before the
    /// first data operation; it does not advance the data commit watermark.
    pub(crate) async fn publish_bootstrap(
        &self,
        authority: &mut SqliteAuthority,
        partition_id: &str,
        owner_epoch: u64,
        checkpoint_bytes: &[u8],
        created_at_ms: i64,
    ) -> Result<super::sqlite_authority::PartitionHead, UploadRegisterError> {
        let head = authority.read_head(partition_id)
            .map_err(UploadRegisterError::Metadata)?
            .ok_or_else(|| UploadRegisterError::Metadata(rusqlite::Error::QueryReturnedNoRows))?;
        let key = super::object_keys::key(partition_id, "manifests", "initial.json");
        let reference = self.put_and_register(authority, &key, checkpoint_bytes, created_at_ms).await?;
        authority.set_bootstrap_reference(
            partition_id, head.head_version, owner_epoch, reference,
        ).map_err(UploadRegisterError::Metadata)?
            .ok_or_else(|| UploadRegisterError::Metadata(rusqlite::Error::InvalidQuery))
    }

    /// Create a joint backup: atomically pin the complete live object closure and
    /// export a logical SQLite authority snapshot, upload that metadata snapshot,
    /// then publish an immutable descriptor referencing both. The caller must retain
    /// descriptor_ref; it is the restore entry point. Pins intentionally remain until
    /// the backup is explicitly retired. An ambiguous failure must not release pins.
    pub(crate) async fn create_backup_bundle(
        &self,
        authority: &mut SqliteAuthority,
        partition: Option<&str>,
        backup_id: &str,
        object_closure: &[ObjectRef],
        created_at_ms: i64,
        max_snapshot_bytes: usize,
    ) -> Result<BackupBundle, BackupError> {
        if backup_id.is_empty() || !backup_id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_') {
            return Err(BackupError::Invalid("backup ID must contain only ASCII letters, digits, '-' or '_'"));
        }
        let snapshot_bytes = authority.prepare_scoped_backup_snapshot(
            partition, backup_id, object_closure, created_at_ms, max_snapshot_bytes
        ).map_err(BackupError::Metadata)?;
        #[cfg(test)]
        if authority.database_path().with_extension("fail-backup-after-snapshot").exists() {
            return Err(BackupError::Invalid("injected after backup snapshot"));
        }
        #[cfg(feature="fault-injection")]
        if std::env::var("ANYSEEK_REMOTE_BACKUP_FAULT").as_deref()==Ok("after_snapshot") {std::process::exit(92);}
        let snapshot_key = super::object_keys::key(partition.unwrap_or("backup"), "manifests", &format!("backup-{backup_id}/authority.json"));
        let snapshot:AuthorityBackupSnapshot=serde_json::from_slice(&snapshot_bytes).map_err(BackupError::Decode)?;
        let metadata_snapshot = self.put_backup_object(authority,backup_id,&snapshot_key,&snapshot_bytes,snapshot.created_at_ms).await
            .map_err(backup_upload_error)?;
        #[cfg(feature="fault-injection")]
        if std::env::var("ANYSEEK_REMOTE_BACKUP_FAULT").as_deref()==Ok("after_upload") {std::process::exit(92);}
        let mut closure = snapshot.object_closure.clone();
        closure.sort_by(|a,b| a.key.cmp(&b.key));
        let descriptor = BackupDescriptor {
            format_version: 1,
            backup_id: backup_id.to_owned(),
            created_at_ms:snapshot.created_at_ms,
            metadata_snapshot,
            object_closure: closure,
        };
        let descriptor_bytes = serde_json::to_vec(&descriptor).map_err(BackupError::Decode)?;
        let descriptor_key = super::object_keys::key(partition.unwrap_or("backup"), "manifests", &format!("backup-{backup_id}/descriptor.json"));
        let descriptor_ref = self.put_backup_object(authority,backup_id,&descriptor_key,&descriptor_bytes,snapshot.created_at_ms).await
            .map_err(backup_upload_error)?;
        authority.register_object_edge(&descriptor_ref, &descriptor.metadata_snapshot)
            .map_err(BackupError::Metadata)?;
        for reference in &descriptor.object_closure {
            authority.register_object_edge(&descriptor_ref, reference)
                .map_err(BackupError::Metadata)?;
        }
        // Keep both control objects alive for the full retention period, not merely
        // the payload closure captured before the descriptor itself existed.
        authority.pin_backup_object(backup_id, &descriptor.metadata_snapshot.key)
            .map_err(BackupError::Metadata)?;
        authority.pin_backup_object(backup_id, &descriptor_ref.key)
            .map_err(BackupError::Metadata)?;
        #[cfg(feature="fault-injection")]
        if std::env::var("ANYSEEK_REMOTE_BACKUP_FAULT").as_deref()==Ok("after_descriptor") {std::process::exit(92);}
        Ok(BackupBundle { descriptor_ref, descriptor })
    }

    /// Create a backup from the authority's current transitive object graph. Every
    /// manifest/WAL/segment object must have its child dependencies registered via
    /// register_object_edge before this call; missing registry entries fail closed.
    #[cfg(test)]
    pub(crate) async fn create_authoritative_backup_bundle(
        &self,
        authority: &mut SqliteAuthority,
        backup_id: &str,
        created_at_ms: i64,
        max_snapshot_bytes: usize,
    ) -> Result<BackupBundle, BackupError> {
        let closure=match authority.saved_backup_closure(backup_id).map_err(BackupError::Metadata)? {
            Some(closure)=>closure,None=>authority.reachable_objects().map_err(BackupError::Metadata)?,
        };
        if closure.is_empty() {
            return Err(BackupError::Invalid("authority has no reachable objects to back up"));
        }
        self.create_backup_bundle(
            authority, None, backup_id, &closure, created_at_ms, max_snapshot_bytes
        ).await
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
        if snapshot.format_version != 2 || snapshot.backup_id != descriptor.backup_id
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
            .map_err(BackupError::Metadata)?;
        #[cfg(test)]
        if authority.database_path().with_extension("fail-restore-after-import").exists() {
            return Err(BackupError::Invalid("injected after restore import"));
        }
        #[cfg(feature="fault-injection")]
        if std::env::var("ANYSEEK_REMOTE_RESTORE_FAULT").as_deref()==Ok("after_import") {std::process::exit(93);}
        // The descriptor and metadata snapshot are backup control roots created after
        // the logical snapshot was captured. Re-register and pin them on the restored
        // authority so normal GC cannot remove the backup's own restore entry point.
        authority.register_durable_object(&descriptor.metadata_snapshot, descriptor.created_at_ms)
            .map_err(BackupError::Metadata)?;
        authority.register_durable_object(descriptor_ref, descriptor.created_at_ms)
            .map_err(BackupError::Metadata)?;
        authority.register_object_edge(descriptor_ref, &descriptor.metadata_snapshot)
            .map_err(BackupError::Metadata)?;
        for reference in &descriptor.object_closure {
            authority.register_object_edge(descriptor_ref, reference)
                .map_err(BackupError::Metadata)?;
        }
        authority.pin_backup_object(&descriptor.backup_id, &descriptor.metadata_snapshot.key)
            .map_err(BackupError::Metadata)?;
        authority.pin_backup_object(&descriptor.backup_id, &descriptor_ref.key)
            .map_err(BackupError::Metadata)?;
        Ok(snapshot)
    }

    /// Retire and delete only the two backup-control objects after backup pins
    /// have been released. Payload closure objects remain subject to normal head reachability.
    pub(crate) async fn retire_backup_metadata(
        &self,
        authority: &mut SqliteAuthority,
        bundle: &BackupBundle,
        now_ms: i64,
    ) -> Result<(), BackupError> {
        let refs = [&bundle.descriptor_ref, &bundle.descriptor.metadata_snapshot];
        for reference in refs {
            let _ = authority.retire_object(
                &reference.key, now_ms.max(0) as u64, now_ms,
            ).map_err(BackupError::Metadata)?;
        }
        for reference in refs {
            let _ = self.delete_retired(authority, &reference.key, now_ms).await
                .map_err(|error| match error {
                    UploadRegisterError::Admission(e) => BackupError::ObjectStore(resource_error(e)),
                    UploadRegisterError::ObjectStore(e) => BackupError::ObjectStore(e),
                    UploadRegisterError::Metadata(e) => BackupError::Metadata(e),
                })?;
        }
        Ok(())
    }

    /// Delete only an object the SQLite authority has monotonically retired and
    /// currently reports as unreferenced and unpinned. Repeated deletion is allowed.
    pub(crate) async fn delete_retired(
        &self,
        authority: &mut SqliteAuthority,
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
        let _io = self.scope(Lane::Background).io().map_err(UploadRegisterError::Admission)?;
        match self.store.delete(&location).await {
            Ok(())|Err(object_store::Error::NotFound{..})=>{},
            Err(error)=>return Err(UploadRegisterError::ObjectStore(error)),
        }
        // Record completion only after a fresh atomic reachability check. If this
        // loses a race with a pin/reference, leave the tombstone in state=2 and
        // retry conservatively rather than claiming physical deletion is complete.
        authority.mark_object_deleted(reference)
            .map_err(UploadRegisterError::Metadata)?;
        Ok(true)
    }

    #[cfg(test)]
    pub(crate) fn from_store(store: Arc<dyn ObjectStore>, max_object_bytes: usize) -> Self {
        Self { store, max_object_bytes, cache: None, resources: resources::isolated_scope(), background: false }
    }
}

fn resource_error(error: resources::Busy) -> object_store::Error {
    object_store::Error::Generic { store: "resource-budget", source: Box::new(error) }
}
pub(crate) fn is_resource_error(error: &object_store::Error) -> bool {
    matches!(error, object_store::Error::Generic { source, .. } if source.downcast_ref::<resources::Busy>().is_some())
}

fn object_error(message: &'static str) -> object_store::Error {
    object_store::Error::Generic { store: "s3-compatible", source: message.into() }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn resource_refusal_precedes_put_and_verified_buffer_owns_charge() {
        let store = S3CompatibleStore::from_store(Arc::new(object_store::memory::InMemory::new()), 1024);
        let governor = Arc::new(resources::Governor::default());
        let cap = 512 * 1024 * 1024;
        let scope = governor.admit("a".into(), Lane::Foreground, 0).unwrap();
        scope.clone().run(async {
            let full = scope.bytes(cap).unwrap();
            let error = store.put_verified("first", b"1234").await.unwrap_err();
            assert!(is_resource_error(&error));
            assert!(matches!(store.store.get(&ObjectPath::from("first")).await, Err(object_store::Error::NotFound { .. })));
            drop(full);
            let reference = store.put_verified("first", b"1234").await.unwrap();
            let buffer = store.get_verified(&reference).await.unwrap();
            let rest = scope.bytes(cap - 8).unwrap();
            assert!(scope.bytes(1).is_err());
            drop(buffer);
            assert!(scope.bytes(8).is_ok());
            drop(rest);
            let first = scope.io().unwrap(); let second = scope.io().unwrap();
            assert!(is_resource_error(&store.get_verified(&reference).await.unwrap_err()));
            drop(first); drop(second);
            assert_eq!(store.get_verified(&reference).await.unwrap(),b"1234");
        }).await;
    }
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn verified_cache_hit_corruption_loss_and_put_verification() {
        let root = std::env::temp_dir().join(format!("anyseek-object-cache-{:032x}", rand::random::<u128>()));
        let backend = Arc::new(InMemory::new());
        let store = S3CompatibleStore::from_store(backend.clone(), 32)
            .with_cache(VerifiedCache::new(root.clone(), &["endpoint", "bucket"], 32));
        let reference = store.put_verified("partitions/a/test", b"alpha").await.unwrap();
        assert_eq!(store.get_verified(&reference).await.unwrap(), b"alpha");
        let location = ObjectPath::from("partitions/a/test");
        backend.delete(&location).await.unwrap();
        // A proven immutable copy remains usable without another GET; the
        // SQLite caller must still select this exact reference at its current cut.
        assert_eq!(store.get_verified(&reference).await.unwrap(), b"alpha");
        // A cached copy must NOT resolve a failed/conflicting remote PUT.
        backend.put(&location, PutPayload::from(b"WRONG".to_vec())).await.unwrap();
        assert!(store.put_verified(&reference.key, b"alpha").await.is_err());
        for row in std::fs::read_dir(&root).unwrap() {
            let row = row.unwrap();
            if row.file_name().to_string_lossy().starts_with("block-") {
                std::fs::write(row.path(), b"other").unwrap();
            }
        }
        assert!(store.get_verified(&reference).await.is_err());
        backend.put(&location, PutPayload::from(b"alpha".to_vec())).await.unwrap();
        assert_eq!(store.get_verified(&reference).await.unwrap(), b"alpha");
        std::fs::remove_dir_all(&root).unwrap();
        backend.delete(&location).await.unwrap();
        assert!(store.get_verified(&reference).await.is_err());
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn verified_cache_cannot_cross_backend_namespace_or_reference_budget() {
        let root = std::env::temp_dir().join(format!("anyseek-object-cache-{:032x}", rand::random::<u128>()));
        let a = S3CompatibleStore::from_store(Arc::new(InMemory::new()), 32)
            .with_cache(VerifiedCache::new(root.clone(), &["endpoint", "bucket-a"], 32));
        let reference = a.put_verified("same-key", b"alpha").await.unwrap();
        a.get_verified(&reference).await.unwrap();
        let b = S3CompatibleStore::from_store(Arc::new(InMemory::new()), 32)
            .with_cache(VerifiedCache::new(root.clone(), &["endpoint", "bucket-b"], 32));
        assert!(b.get_verified(&reference).await.is_err());
        let mut restricted = a.clone(); restricted.max_object_bytes = 4;
        assert!(restricted.get_verified(&reference).await.is_err());
        let mut forged = reference.clone(); forged.length = 4;
        assert!(a.get_verified(&forged).await.is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Explicit test-bucket contract; the same test runs against the disposable
    /// HTTP fixture in CI. Only a real endpoint run is provider evidence.
    #[tokio::test]
    #[ignore = "requires a dedicated S3-compatible test endpoint and bucket"]
    async fn real_s3_compatible_contract() {
        use std::{env, panic::AssertUnwindSafe, time::{Duration, SystemTime}};
        use futures::FutureExt;
        use super::super::{object_log, sqlite_authority::PublishOutcome};
        let required = |name: &str| env::var(name).unwrap_or_else(|_| panic!("set {name} for this ignored integration test"));
        let endpoint=required("ANYSEEK_S3_TEST_ENDPOINT");
        let bucket=required("ANYSEEK_S3_TEST_BUCKET");
        let region=required("ANYSEEK_S3_TEST_REGION");
        let access=required("ANYSEEK_S3_TEST_ACCESS_KEY_ID");
        let secret=required("ANYSEEK_S3_TEST_SECRET_ACCESS_KEY");
        let allow_http=env::var("ANYSEEK_S3_TEST_ALLOW_HTTP").map(|v|v=="1").unwrap_or(false);
        let stamp=SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let id=format!("contract-{}-{stamp}",std::process::id());
        let prefix=format!("anyseek-contract-tests/{id}/");
        let partition_prefix=format!("anyseek/{id}/");
        let backup_prefix=format!("anyseek/backup/manifests/backup-{id}/");
        let store=S3CompatibleStore::connect(&endpoint,&bucket,&region,&access,&secret,allow_http,1024*1024).unwrap();
        let source_path=env::temp_dir().join(format!("anyseek-{id}-source.sqlite"));
        let target_path=env::temp_dir().join(format!("anyseek-{id}-target.sqlite"));
        let mut keys=std::collections::BTreeSet::new();
        // Clean only this run's explicit keys, including uncertain uploads. No
        // bucket LIST, prefix-wide deletion, production SQLite or foreign roots.
        let outcome=AssertUnwindSafe(async {
            let key=format!("{prefix}immutable");keys.insert(key.clone());
            let reference=store.put_verified(&key,b"canonical bytes").await.unwrap();
            assert_eq!(store.get_verified(&reference).await.unwrap(),b"canonical bytes");
            assert_eq!(store.store.head(&ObjectPath::from(key.clone())).await.unwrap().size,reference.length);
            assert_eq!(store.put_verified(&key,b"canonical bytes").await.unwrap(),reference);
            assert!(store.put_verified(&key,b"different bytes").await.is_err());
            assert_eq!(store.get_verified(&reference).await.unwrap(),b"canonical bytes");
            let mut bad=reference.clone();bad.sha256[0]^=0xff;
            assert!(store.get_verified(&bad).await.is_err());
            bad=reference.clone();bad.length+=1;
            assert!(store.get_verified(&bad).await.is_err());
            let missing=ObjectPath::from(format!("{prefix}missing"));
            assert!(store.store.get(&missing).await.is_err());

            let race=format!("{prefix}create-race");keys.insert(race.clone());
            let (left,right)=tokio::join!(store.put_verified(&race,b"left"),store.put_verified(&race,b"right"));
            assert_eq!(usize::from(left.is_ok())+usize::from(right.is_ok()),1,"conditional create must have one winner");
            let winner=left.or(right).unwrap();
            let winner_bytes=store.get_verified(&winner).await.unwrap();
            assert!(winner_bytes==b"left"||winner_bytes==b"right");
            let identical=format!("{prefix}identical-race");keys.insert(identical.clone());
            let (a,b)=tokio::join!(store.put_verified(&identical,b"same"),store.put_verified(&identical,b"same"));
            assert_eq!(a.unwrap(),b.unwrap(),"response-lost retries resolve exact immutable bytes");
            let oversize=format!("{prefix}oversize");keys.insert(oversize.clone());
            assert!(store.put_verified(&oversize,&vec![0;1024*1024+1]).await.is_err());
            assert!(store.store.head(&ObjectPath::from(oversize)).await.is_err());

            let mut source=SqliteAuthority::open(&source_path,Duration::from_secs(2)).unwrap();
            source.create_partition(&id,7).unwrap();
            keys.insert(format!("{partition_prefix}manifests/initial.json"));
            let initial=store.publish_bootstrap(&mut source,&id,7,b"initial authority",10).await.unwrap();
            let bootstrap=initial.bootstrap.unwrap();
            let body=b"operation one";let digest:[u8;32]=Sha256::digest(body).into();
            keys.insert(format!("{partition_prefix}wal/{:020}-{}.json",1,digest.iter().map(|b|format!("{b:02x}")).collect::<String>()));
            assert!(matches!(object_log::append_and_commit(&store,&mut source,&id,7,"op-1",digest,body.to_vec(),20,1024*1024).await.unwrap(),object_log::AppendCommitOutcome::Published{..}));
            let first=source.read_head(&id).unwrap().unwrap();
            let closure=source.reachable_objects().unwrap();
            keys.insert(format!("{backup_prefix}authority.json"));keys.insert(format!("{backup_prefix}descriptor.json"));
            let backup=store.create_backup_bundle(&mut source,None,&id,&closure,30,1024*1024).await.unwrap();
            let body=b"operation two";let digest:[u8;32]=Sha256::digest(body).into();
            keys.insert(format!("{partition_prefix}wal/{:020}-{}.json",2,digest.iter().map(|b|format!("{b:02x}")).collect::<String>()));
            assert!(matches!(object_log::append_and_commit(&store,&mut source,&id,7,"op-2",digest,body.to_vec(),40,1024*1024).await.unwrap(),object_log::AppendCommitOutcome::Published{..}));
            let current=source.read_head(&id).unwrap().unwrap();
            let orphan=format!("{prefix}unpublished");keys.insert(orphan.clone());
            let orphan=store.put_and_register(&mut source,&orphan,b"never committed",50).await.unwrap();
            assert!(matches!(source.publish_commit(&id,current.head_version,6,"stale-owner",[9;32],3,orphan.clone(),None).unwrap(),PublishOutcome::Conflict));
            assert_eq!(source.read_head(&id).unwrap().unwrap(),current);
            let current_closure=source.reachable_objects().unwrap();
            let retry=store.create_backup_bundle(&mut source,None,&id,&current_closure,60,1024*1024).await.unwrap();
            assert_eq!(retry,backup,"backup retry preserves the original metadata cut and descriptor");
            let mut target=SqliteAuthority::open(&target_path,Duration::from_secs(2)).unwrap();
            let snapshot=store.restore_backup_bundle(&mut target,&backup.descriptor_ref,1024*1024,100,16*1024*1024).await.unwrap();
            assert_eq!(snapshot.heads[0],first);
            let restored=target.read_head(&id).unwrap().unwrap();
            let tail=object_log::load_committed_entries_after(&store,&restored,0,100,16*1024*1024).await.unwrap();
            assert_eq!(tail.len(),1);assert_eq!(tail[0].payload,b"operation one");
            assert!(!source.retire_object(&bootstrap.key,100,100).unwrap(),"live roots cannot be retired");
            assert!(!store.delete_retired(&mut source,&bootstrap.key,100).await.unwrap());
            assert!(source.retire_object(&orphan.key,100,100).unwrap());
            assert!(store.delete_retired(&mut source,&orphan.key,100).await.unwrap());
            assert!(!store.delete_retired(&mut source,&orphan.key,101).await.unwrap());
            assert!(store.store.get(&ObjectPath::from(orphan.key)).await.is_err());
            source.release_backup_pins(&id).unwrap();
            store.retire_backup_metadata(&mut source,&backup,110).await.unwrap();
            assert!(store.get_verified(&backup.descriptor_ref).await.is_err());
            assert_eq!(store.get_verified(&bootstrap).await.unwrap(),b"initial authority");
        }).catch_unwind().await;
        let mut cleanup_errors=Vec::new();
        for key in keys {
            assert!(key.starts_with(&prefix)||key.starts_with(&partition_prefix)||key.starts_with(&backup_prefix));
            match store.store.delete(&ObjectPath::from(key.clone())).await {
                Ok(())|Err(object_store::Error::NotFound{..})=>{},
                Err(error)=>cleanup_errors.push(format!("{key}: {error}")),
            }
        }
        for path in [&source_path,&target_path] {
            let _=std::fs::remove_file(path);
            let _=std::fs::remove_file(path.with_extension("sqlite-wal"));
            let _=std::fs::remove_file(path.with_extension("sqlite-shm"));
        }
        if let Err(panic)=outcome {std::panic::resume_unwind(panic);}
        assert!(cleanup_errors.is_empty(),"test-owned object cleanup failed: {cleanup_errors:?}");
    }

    #[tokio::test]
    async fn create_then_read_back_and_verify() {
        let store = S3CompatibleStore::from_store(Arc::new(InMemory::new()), 1024);
        let body = b"durable record bytes";
        let reference = store.put_verified("partition/p0/log/0001", body).await.unwrap();
        assert_eq!(reference.length, body.len() as u64);
        assert_eq!(store.get_verified(&reference).await.unwrap(), body);
    }

    #[tokio::test]
    async fn bootstrap_checkpoint_is_published_before_any_data_commit() {
        use std::time::{Duration, SystemTime};
        let stamp=SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let path=std::env::temp_dir().join(format!("anyseek-bootstrap-{stamp}-{}.sqlite",std::process::id()));
        let mut authority=SqliteAuthority::open(&path,Duration::from_secs(1)).unwrap();
        authority.create_partition("p0",9).unwrap();
        let store=S3CompatibleStore::from_store(Arc::new(InMemory::new()),4096);
        let head=store.publish_bootstrap(&mut authority,"p0",9,b"initial checkpoint bytes",1).await.unwrap();
        assert_eq!(head.committed_seq,0);
        assert_eq!(head.indexed_seq,0);
        let bootstrap=head.bootstrap.unwrap();
        assert_eq!(store.get_verified(&bootstrap).await.unwrap(),b"initial checkpoint bytes");
        drop(authority);
        let _=std::fs::remove_file(&path);
        let _=std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _=std::fs::remove_file(path.with_extension("sqlite-shm"));
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
    async fn durable_reuse_reads_backend_and_never_recreates_missing_objects() {
        let root = tempfile::tempdir().unwrap();
        let mut authority = SqliteAuthority::open(&root.path().join("authority.sqlite"), std::time::Duration::from_secs(1)).unwrap();
        authority.create_partition("p0", 1).unwrap();
        let backend = Arc::new(InMemory::new());
        let store = S3CompatibleStore::from_store(backend.clone(), 1024);
        let key = "p0/immutable";
        let reference = store.put_and_register(&mut authority,key,b"original",1).await.unwrap();
        assert_eq!(store.put_and_register(&mut authority,key,b"original",2).await.unwrap(),reference);
        let path = S3CompatibleStore::object_path(key).unwrap();
        backend.put(&path, PutPayload::from_static(b"tampered")).await.unwrap();
        assert!(store.put_and_register(&mut authority,key,b"original",3).await.is_err());
        backend.delete(&path).await.unwrap();
        assert!(store.put_and_register(&mut authority,key,b"original",4).await.is_err());
        assert!(backend.get(&path).await.is_err(),"reuse must not repair externally removed evidence");
        assert_eq!(authority.read_head("p0").unwrap().unwrap().committed_seq,0);
        assert!(store.put_and_register(&mut authority,key,b"different",5).await.is_err());
    }

    #[tokio::test]
    async fn backup_bundle_restores_authority_only_after_closure_verification() {
        use std::time::{Duration, SystemTime};
        let stamp = SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        let source_path = std::env::temp_dir().join(format!("anyseek-backup-source-{stamp}-{}.sqlite", std::process::id()));
        let target_path = std::env::temp_dir().join(format!("anyseek-backup-target-{stamp}-{}.sqlite", std::process::id()));
        let mut source = SqliteAuthority::open(&source_path, Duration::from_secs(1)).unwrap();
        let head = source.create_partition("p0", 5).unwrap();
        let store = S3CompatibleStore::from_store(Arc::new(InMemory::new()), 1024 * 1024);
        let reference = store.put_and_register(&mut source, "p0/log/1", b"committed operation", 10).await.unwrap();
        let result = source.publish_commit("p0", head.head_version, 5, "op-1", [7;32], 1, reference.clone(), None).unwrap();
        assert!(matches!(result, super::super::sqlite_authority::PublishOutcome::Published(_)));
        let bundle = store.create_backup_bundle(&mut source,None, "backup-001", &[reference], 20, 1024 * 1024).await.unwrap();

        let mut target = SqliteAuthority::open(&target_path, Duration::from_secs(1)).unwrap();
        let restored = store.restore_backup_bundle(&mut target, &bundle.descriptor_ref, 1024 * 1024, 10, 1024 * 1024).await.unwrap();
        assert_eq!(restored.backup_id, "backup-001");
        let restored_head = target.read_head("p0").unwrap().unwrap();
        assert_eq!(restored_head.committed_seq, 1);
        assert_eq!(target.resolve_operation("p0", "op-1", [7;32]).unwrap().unwrap().committed_seq, 1);
        source.release_backup_pins(&bundle.descriptor.backup_id).unwrap();
        store.retire_backup_metadata(&mut source, &bundle, 30).await.unwrap();
        assert!(store.get_verified(&bundle.descriptor_ref).await.is_err());
        assert!(store.get_verified(&bundle.descriptor.metadata_snapshot).await.is_err());
        drop(source);
        drop(target);
        for path in [&source_path, &target_path] {
            let _=std::fs::remove_file(path);
            let _=std::fs::remove_file(path.with_extension("sqlite-wal"));
            let _=std::fs::remove_file(path.with_extension("sqlite-shm"));
        }
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
        authority.pin_object("backup-pin", "p0", &reference.key, "backup", Some(1)).unwrap();
        assert!(!authority.retire_object(&reference.key, 1, 2).unwrap());
        assert!(!store.delete_retired(&mut authority, &reference.key, 2).await.unwrap());
        assert!(authority.unpin_object("backup-pin").unwrap());
        assert!(authority.retire_object(&reference.key, 1, 3).unwrap());
        assert!(store.delete_retired(&mut authority, &reference.key, 3).await.unwrap());
        assert!(!store.delete_retired(&mut authority, &reference.key, 3).await.unwrap());
        assert!(store.get_verified(&reference).await.is_err());
        assert!(authority.register_durable_object(&reference, 4).is_err());
        assert!(!authority.collectible_objects(4).unwrap().iter().any(|item| item.key == reference.key));
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

fn backup_upload_error(error:UploadRegisterError)->BackupError {match error {
    UploadRegisterError::Admission(e)=>BackupError::ObjectStore(resource_error(e)),
    UploadRegisterError::Metadata(e)=>BackupError::Metadata(e),UploadRegisterError::ObjectStore(e)=>BackupError::ObjectStore(e),
}}

#[cfg(test)]
mod upload_retirement_tests {
    use super::*;
    #[tokio::test]
    async fn backup_resumes_with_unverified_control_inventory_pinned(){
        use object_store::memory::InMemory;
        let db=std::env::temp_dir().join(format!("anyseek-backup-inventory-{:032x}.sqlite",rand::random::<u128>()));
        let mut authority=SqliteAuthority::open(&db,std::time::Duration::from_secs(1)).unwrap();
        authority.create_partition("p",1).unwrap();
        let store=S3CompatibleStore::from_store(Arc::new(InMemory::new()),1024*1024);
        store.publish_bootstrap(&mut authority,"p",1,b"initial",1).await.unwrap();
        let closure=authority.reachable_objects().unwrap();
        let bytes=authority.prepare_backup_snapshot("interrupted",&closure,10,1024*1024).unwrap();
        let reference=ObjectRef{key:"anyseek/backup/manifests/backup-interrupted/authority.json".into(),length:bytes.len() as u64,
            sha256:Sha256::digest(&bytes).into()};
        authority.reserve_backup_upload(&reference,10,Some("interrupted")).unwrap();
        assert!(authority.reachable_objects().is_err());
        let bundle=store.create_authoritative_backup_bundle(&mut authority,"interrupted",999,1024*1024).await.unwrap();
        assert_eq!(bundle.descriptor.created_at_ms,10);
        assert_eq!(bundle.descriptor.metadata_snapshot,reference);
        assert_eq!(bundle.descriptor.object_closure,closure);
    }
    #[tokio::test]
    async fn unknown_put_inventory_prevents_republication_and_deletes_late_arrival(){
        use object_store::memory::InMemory;
        let db=std::env::temp_dir().join(format!("anyseek-late-put-{:032x}.sqlite",rand::random::<u128>()));
        let mut authority=SqliteAuthority::open(&db,std::time::Duration::from_secs(1)).unwrap();
        authority.create_partition("p",1).unwrap();
        let raw=Arc::new(InMemory::new());let store=S3CompatibleStore::from_store(raw.clone(),1024);
        let bytes=b"possibly still uploading";
        let reference=ObjectRef{key:"p/late".into(),length:bytes.len() as u64,sha256:Sha256::digest(bytes).into()};
        authority.reserve_upload(&reference,1).unwrap();
        assert!(authority.retire_object(&reference.key,1,2).unwrap());
        assert!(store.delete_retired(&mut authority,&reference.key,3).await.unwrap());
        raw.put(&ObjectPath::from(reference.key.as_str()),bytes.to_vec().into()).await.unwrap();
        assert!(authority.register_durable_object(&reference,4).is_err());
        assert!(authority.reserve_upload(&reference,4).is_err());
        assert!(store.delete_retired(&mut authority,&reference.key,5).await.unwrap());
        assert!(store.get_verified(&reference).await.is_err());
    }
}

#[cfg(test)]
#[path = "s3_admission_tests.rs"]
mod admission_tests;
