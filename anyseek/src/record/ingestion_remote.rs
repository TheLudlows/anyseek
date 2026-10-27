//! Cloud intake authority. SQLite contains references/receipts, never commands.
use super::*;
use crate::storage::{
    object_log, s3_compatible::S3CompatibleStore, sqlite_authority::SqliteAuthority,
};
use sha2::{Digest, Sha256};

pub(super) struct Log {
    store: S3CompatibleStore,
    authority: SqliteAuthority,
    partition: String,
    epoch: u64,
    events: u64,
    bytes: u64,
    max_events: u64,
    max_bytes: u64,
    max_object_bytes: usize,
    failed: bool,
    _owner: std::fs::File,
}
impl Log {
    pub(super) async fn open(
        source: (S3CompatibleStore, PathBuf, String, u64, usize),
        identity: &str,
        max_operations: usize,
        max_bytes: u64,
    ) -> Result<(Self, Vec<Event>), RecordError> {
        let (store, path, partition, epoch, max_object_bytes) = source;
        let database = path.with_extension("intake.sqlite");
        let owner = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path.with_extension("intake.owner"))
            .map_err(storage)?;
        owner.try_lock().map_err(storage)?;
        let mut authority =
            SqliteAuthority::open(&database, std::time::Duration::from_secs(5)).map_err(storage)?;
        let partition = format!("{partition}-intake");
        let mut head = match authority.read_head(&partition).map_err(storage)? {
            Some(head) => head,
            None => authority
                .create_partition(&partition, epoch)
                .map_err(storage)?,
        };
        if head.owner_epoch != epoch {
            return Err(RecordError::RecoveryRequired);
        }
        let marker = serde_json::to_vec(&("anyseek-intake-v1", identity)).map_err(storage)?;
        if let Some(reference) = &head.bootstrap {
            if store.get_verified(reference).await.map_err(storage)? != marker {
                return Err(storage("remote intake identity/configuration mismatch"));
            }
        } else {
            head = store
                .publish_bootstrap(
                    &mut authority,
                    &partition,
                    epoch,
                    &marker,
                    unix_time_ms_i64(),
                )
                .await
                .map_err(storage)?;
        }
        let max_events = (max_operations as u64)
            .checked_mul(2)
            .and_then(|n| n.checked_add(64))
            .ok_or_else(|| storage("intake event budget overflow"))?;
        let max_bytes = max_bytes
            .checked_mul(8)
            .ok_or_else(|| storage("remote intake budget overflow"))?;
        let entries = object_log::load_committed_entries_after(
            &store,
            &head,
            0,
            max_events as usize,
            max_bytes as usize,
        )
        .await
        .map_err(storage)?;
        let mut events = Vec::new();
        for entry in entries {
            events.push(serde_json::from_slice(&entry.payload).map_err(storage)?);
        }
        let (_, bytes) = authority.retained_tail_size(&head).map_err(storage)?;
        Ok((
            Self {
                store,
                authority,
                partition,
                epoch,
                events: head.committed_seq,
                bytes,
                max_events,
                max_bytes,
                max_object_bytes,
                failed: false,
                _owner: owner,
            },
            events,
        ))
    }
    pub(super) fn bytes(&self) -> u64 {
        self.bytes
    }
    pub(super) fn available(&self) -> u64 {
        self.max_bytes.saturating_sub(self.bytes)
    }
    pub(super) async fn append(&mut self, event: &Event) -> Result<(), RecordError> {
        if self.failed {
            return Err(RecordError::RecoveryRequired);
        }
        let payload = serde_json::to_vec(event).map_err(storage)?;
        let estimate = (payload.len() as u64)
            .checked_mul(5)
            .and_then(|n| n.checked_add(4096))
            .ok_or(RecordError::Busy)?;
        if self.events >= self.max_events
            || self
                .bytes
                .checked_add(estimate)
                .is_none_or(|n| n > self.max_bytes)
        {
            return Err(RecordError::Busy);
        }
        if estimate > self.max_object_bytes as u64 {
            return Err(RecordError::Rejected(
                "intake batch exceeds configured S3 object budget".into(),
            ));
        }
        let digest: [u8; 32] = Sha256::digest(&payload).into();
        let operation = format!("intake-event-{}", self.events + 1);
        self.failed = true;
        let outcome = object_log::append_and_commit(
            &self.store,
            &mut self.authority,
            &self.partition,
            self.epoch,
            &operation,
            digest,
            payload,
            unix_time_ms_i64(),
            self.max_object_bytes,
        )
        .await
        .map_err(|e| RecordError::OutcomeUnknown(e.to_string()))?;
        let head = match outcome {
            object_log::AppendCommitOutcome::Published { head, .. }
            | object_log::AppendCommitOutcome::AlreadyPublished(head) => head,
            object_log::AppendCommitOutcome::Conflict => return Err(RecordError::RecoveryRequired),
        };
        if head.committed_seq != self.events + 1 {
            return Err(RecordError::RecoveryRequired);
        }
        self.events = head.committed_seq;
        self.bytes = self.authority.retained_tail_size(&head).map_err(storage)?.1;
        self.failed = false;
        Ok(())
    }
}
