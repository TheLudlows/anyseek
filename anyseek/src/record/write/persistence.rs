//! Local WAL and SQLite/S3 publication, behind one write persistence phase.
use super::*;

pub(super) enum Prepared {
    Local,
    Remote(UploadedBatch),
}
impl Prepared {
    pub(super) async fn commit(self, inner: &mut Inner, payloads: &[&[u8]], sequence: u64) -> Result<(), RecordError> {
        match (self, inner.remote.as_mut()) {
            (Self::Local, None) => {},
            (Self::Remote(uploaded), Some(remote)) => uploaded.publish(remote).await?,
            _ => return Err(RecordError::RecoveryRequired),
        }
        // Local is authoritative here; S3 uses the same journal as a rebuildable
        // cache after the remote authority has selected the immutable object.
        inner.append_operations(payloads, sequence).await
    }
}

impl RecordPartition {
    /// Preserve the Inner guard for the executor's shared finish/abort path.
    /// The writer remains owned throughout upload, cancellation and unwind.
    pub(super) async fn persist_prepared<'a>(
        &'a self,
        mut inner: tokio::sync::MutexGuard<'a, Inner>,
        prepared: PreparedBatch<'_>,
        scope: &mut AttemptScope,
    ) -> (tokio::sync::MutexGuard<'a, Inner>, Result<BatchResult, RecordError>) {
        let persistence = match inner.prepare_remote_batch(&prepared).await {
            Ok(Some(upload)) => {
                drop(inner);
                let uploaded = upload.upload(&prepared).await;
                inner = self.inner.lock().await;
                match uploaded {
                    Ok(uploaded) => Prepared::Remote(uploaded),
                    Err(error) => return (inner, Err(error)),
                }
            }
            Ok(None) => Prepared::Local,
            Err(error) => return (inner, Err(error)),
        };
        let result = self.commit_batch(&mut inner, prepared, scope, persistence).await;
        (inner, result)
    }
}

pub(in crate::record) struct RemoteUpload {
    store: crate::storage::s3_compatible::S3CompatibleStore,
    pub(in crate::record) path: std::path::PathBuf,
    head: crate::storage::authority::PartitionHead,
    max_bytes: usize,
    #[cfg(test)]
    root: std::path::PathBuf,
}
pub(in crate::record) struct UploadedBatch {
    head: crate::storage::authority::PartitionHead,
    block: crate::storage::object_log::ObjectLogBlock,
    reference: crate::storage::authority::ObjectRef,
}
impl RemoteUpload {
    async fn upload(self, prepared: &PreparedBatch<'_>) -> Result<UploadedBatch, RecordError> {
        use sha2::{Digest, Sha256};
        // The writer keeps the decisions fixed. Encoding and copying remote
        // payloads need neither Inner nor the shared SQLite connection.
        let entries: Vec<_> = prepared.plans.iter().map(|plan| match plan {
            mutation::PreparedWrite::Append { applied, bytes, .. } => {
                crate::storage::object_log::ObjectLogEntry {
                    seq: applied.receipt.seq,
                    operation_id: format!("{}:{}", applied.request.operation_id.epoch, applied.request.operation_id.nonce),
                    request_sha256: Sha256::digest(bytes).into(),
                    payload: bytes.clone(),
                }
            }
            _ => unreachable!(),
        }).collect();
        self.upload_entries(entries).await
    }
    pub(in crate::record) async fn upload_entries(self, entries: Vec<crate::storage::object_log::ObjectLogEntry>) -> Result<UploadedBatch, RecordError> {
        let block = crate::storage::object_log::ObjectLogBlock {
            partition_id: self.head.partition_id.clone(),
            first_seq: entries.first().ok_or(RecordError::RecoveryRequired)?.seq,
            last_seq: entries.last().ok_or(RecordError::RecoveryRequired)?.seq,
            previous: self.head.wal_tip.clone(),
            entries,
        };
        #[cfg(test)]
        if self.root.join(".test-write-upload-panic").exists() {
            panic!("injected upload unwind");
        }
        #[cfg(test)]
        if self.root.join(".test-write-upload-pause").exists() {
            std::fs::write(
                self.root.join(".test-write-upload-started"),
                b"outside state lock",
            )
            .map_err(storage)?;
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            while self.root.join(".test-write-upload-pause").exists() {
                if std::time::Instant::now() >= deadline {
                    return Err(RecordError::Busy);
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }
        let digest = checkpoint_codec::json_digest(&block)?;
        let key = crate::storage::object_keys::key(&self.head.partition_id, "wal", &format!(
            "{:020}-{}",
            block.first_seq,
            digest
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ));
        // Independent short SQLite transactions avoid holding the shared authority
        // connection while awaiting network I/O. Retired objects cannot resurrect.
        let mut authority = crate::storage::sqlite_authority::SqliteAuthority::open_existing(
            &self.path,
            std::time::Duration::from_secs(5),
        )
        .map_err(storage)?;
        let reference = crate::storage::object_log::upload_log_block(
            &self.store,
            &mut authority,
            &key,
            &block,
            unix_time_ms_i64(),
            self.max_bytes,
        )
        .await
        .map_err(|error| match RemoteCommitError::from(error) {
            RemoteCommitError::Busy => RecordError::Busy,
            RemoteCommitError::Unknown(error) => RecordError::OutcomeUnknown(error),
        })?;
        Ok(UploadedBatch {
            head: self.head,
            block,
            reference,
        })
    }
}
impl UploadedBatch {
    async fn publish(self, remote: &mut RemotePersistence) -> Result<(), RecordError> {
        self.publish_checked(remote, || Ok(())).await
    }
    pub(in crate::record) async fn publish_checked(self, remote: &mut RemotePersistence, check: impl FnOnce() -> Result<(), RecordError>) -> Result<(), RecordError> {
        if remote.partition_id != self.head.partition_id
            || remote.owner_epoch != self.head.owner_epoch
        {
            return Err(RecordError::RecoveryRequired);
        }
        let operations: Vec<_> = self
            .block
            .entries
            .iter()
            .map(|entry| (entry.operation_id.as_str(), entry.request_sha256, entry.seq))
            .collect();
        let mut authority = remote.authority.lock().await;
        // Evaluate time-sensitive admission after the last asynchronous lock wait.
        check()?;
        let result = authority.publish_batch(
                &remote.partition_id,
                self.head.head_version,
                remote.owner_epoch,
                &operations,
                self.reference,
                None,
            )
            .map_err(|error| RecordError::OutcomeUnknown(error.to_string()))?;
        match result {
            crate::storage::authority::PublishOutcome::Published(head)
            | crate::storage::authority::PublishOutcome::AlreadyPublished(head)
                if head.committed_seq == self.block.last_seq =>
            {
                Ok(())
            }
            _ => Err(RecordError::OutcomeUnknown(
                "remote batch head changed before publication".into(),
            )),
        }
    }
}
impl Inner {
    async fn prepare_remote_batch(
        &self,
        prepared: &PreparedBatch<'_>,
    ) -> Result<Option<RemoteUpload>, RecordError> {
        let Some(remote) = self.remote.as_ref() else {
            return Ok(None);
        };
        if prepared.plans.is_empty() {
            return Ok(None);
        }
        remote.prepare_upload(prepared.base_seq, self.journal.root()).await.map(Some)
    }
}
impl RemotePersistence {
    pub(in crate::record) async fn prepare_upload(&self, base_seq:u64, _root:&Path) -> Result<RemoteUpload,RecordError> {
        let (head,path) = {
            let authority = self.authority.lock().await;
            let head = authority.read_head(&self.partition_id)
                .map_err(storage)?.ok_or(RecordError::RecoveryRequired)?;
            (head,authority.database_path().to_path_buf())
        };
        if head.owner_epoch != self.owner_epoch || head.committed_seq != base_seq {
            return Err(RecordError::RecoveryRequired);
        }
        Ok(RemoteUpload {
            store: self.store.clone(),
            path,
            head,
            max_bytes: self.max_object_bytes,
            #[cfg(test)]
            root: _root.to_path_buf(),
        })
    }
}
