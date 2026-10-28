//! Cloud intake authority. SQLite contains references/receipts, never commands.
use super::*;
use crate::storage::{
    object_log, s3_compatible::{S3CompatibleStore, UploadRegisterError}, sqlite_authority::{SqliteAuthority, ObjectRef},
};
use sha2::{Digest, Sha256};

#[derive(Serialize, Deserialize)]
struct Snapshot {
    format: u32,
    identity: String,
    seq: u64,
    chunks: Vec<ObjectRef>,
}
pub(super) struct Log {
    store: S3CompatibleStore,
    authority: SqliteAuthority,
    partition: String,
    epoch: u64,
    identity: String,
    base: u64,
    events: u64,
    bytes: u64,
    snapshot_bytes: u64,
    max_events: u64,
    max_objects: u64,
    max_bytes: u64,
    max_object_bytes: usize,
    failed: bool,
    spare_objects: u64,
    _owner: std::fs::File,
}
pub(super) struct Cut {
    store: S3CompatibleStore, database: PathBuf, partition: String, epoch: u64,
    identity: String, head: crate::storage::sqlite_authority::PartitionHead,
    events: u64, base: u64, max_bytes: u64, max_object_bytes: usize,
    _owner: std::fs::File,
}
pub(super) struct Prepared {
    cut: Cut, build: String, objects: Vec<(ObjectRef, Vec<u8>)>, manifest: ObjectRef,
    retained: u64,
}
impl Prepared {
    pub(super) fn encode(cut: Cut, events: Vec<Vec<u8>>) -> Result<Self, RecordError> {
        let build = format!("{:032x}", rand::random::<u128>());
        let prefix = format!("partitions/{}/snapshot/{:020}-{build}", cut.partition,
            cut.head.head_version.checked_add(1).ok_or(RecordError::RecoveryRequired)?);
        let mut objects = Vec::new();
        let mut buffer = vec![b'['];
        let mut retained = 0u64;
        let mut add = |key: String, bytes: Vec<u8>| -> Result<ObjectRef, RecordError> {
            if bytes.len() > cut.max_object_bytes { return Err(RecordError::Busy); }
            retained = retained.checked_add(bytes.len() as u64).filter(|n| *n <= cut.max_bytes).ok_or(RecordError::Busy)?;
            let reference = ObjectRef { key, sha256: Sha256::digest(&bytes).into(), length: bytes.len() as u64 };
            objects.push((reference.clone(), bytes)); Ok(reference)
        };
        let mut chunks = Vec::new();
        for event in events {
            if event.len().checked_add(2).is_none_or(|n| n > cut.max_object_bytes) { return Err(RecordError::Busy); }
            if buffer.len() + event.len() + 1 + usize::from(buffer.len() > 1) > cut.max_object_bytes {
                buffer.push(b']'); chunks.push(add(format!("{prefix}/{}", chunks.len()), buffer)?);
                buffer = vec![b'['];
            }
            if buffer.len() > 1 { buffer.push(b','); }
            buffer.extend(event);
        }
        buffer.push(b']'); chunks.push(add(format!("{prefix}/{}", chunks.len()), buffer)?);
        let snapshot = Snapshot { format: 1, identity: cut.identity.clone(), seq: cut.events, chunks };
        let manifest = add(format!("{prefix}/manifest"), serde_json::to_vec(&snapshot).map_err(storage)?)?;
        Ok(Self { cut, build, objects, manifest, retained })
    }
    fn authority(&self) -> Result<SqliteAuthority, RecordError> {
        SqliteAuthority::open(&self.cut.database, std::time::Duration::from_secs(5)).map_err(storage)
    }
    pub(super) async fn upload(&self) -> Result<(), RecordError> {
        let _phase = maintenance::Phase::new("intake_remote_upload_unlocked", self.cut.events);
        let mut authority = self.authority()?;
        #[cfg(test)] self.pause("upload").await;
        let started = std::time::Instant::now();
        let deadline = std::time::Duration::from_secs(300);
        #[cfg(test)] let deadline = if self.cut.database.with_extension("fail-intake-admission-deadline").exists() { std::time::Duration::ZERO } else { deadline };
        for (reference, bytes) in &self.objects {
            #[cfg(test)] if authority.database_path().with_extension("fail-intake-unknown-put").exists() {
                return Err(RecordError::OutcomeUnknown("injected snapshot PUT response loss".into()));
            }
            loop {
                match self.cut.store.put_and_register(&mut authority, &reference.key, bytes, unix_time_ms_i64()).await {
                    Ok(_) => break,
                    Err(UploadRegisterError::Admission(_)) if started.elapsed() < deadline => {
                        // Preserve this build and its staging pins; never allocate a new
                        // candidate for a transient refusal or repeat a verified prefix.
                        #[cfg(test)] std::fs::write(self.cut.database.with_extension("entered-intake-admission"), b"").unwrap();
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    Err(UploadRegisterError::Admission(error)) => return Err(storage(format!("snapshot upload admission deadline: {error}; reopen required"))),
                    Err(error) => return Err(upload_error(error)),
                }
            }
            #[cfg(test)] if reference == &self.objects[0].0 { self.pause("upload-prefix").await; }
        }
        for (child, _) in &self.objects[..self.objects.len()-1] {
            authority.register_object_edge(&self.manifest, child).map_err(storage)?;
        }
        #[cfg(test)] if authority.database_path().with_extension("fail-intake-before-select").exists() {
            return Err(storage("injected failure after verified snapshot upload"));
        }
        Ok(())
    }
    #[cfg(test)]
    async fn pause(&self, phase: &str) {
        let gate = self.cut.database.with_extension(format!("pause-intake-{phase}"));
        if gate.exists() {
            std::fs::write(self.cut.database.with_extension(format!("entered-intake-{phase}")), b"").unwrap();
            while gate.exists() { tokio::time::sleep(std::time::Duration::from_millis(5)).await; }
        }
    }
    pub(super) async fn release(&self, garbage: Vec<ObjectRef>) -> Result<(), RecordError> {
        let _phase = maintenance::Phase::new("intake_remote_release_unlocked", self.cut.events);
        let mut authority = self.authority()?;
        authority.release_intake_snapshot_uploads(&self.cut.partition, self.cut.epoch, &self.build).map_err(storage)?;
        let head = authority.read_head(&self.cut.partition).map_err(storage)?.ok_or(RecordError::RecoveryRequired)?;
        if head.log_base_seq > 0 {
            authority.prune_intake_receipts(&self.cut.partition, self.cut.epoch, head.log_base_seq).map_err(storage)?;
        }
        let now = unix_time_ms_i64();
        for object in garbage.iter().chain(self.objects.iter().map(|(r, _)| r)) {
            authority.retire_object(&object.key, self.cut.events, now).map_err(storage)?;
        }
        #[cfg(test)] self.pause("delete").await;
        for object in authority.collectible_objects(now).map_err(storage)? {
            if object.key.starts_with(&format!("partitions/{}/", self.cut.partition)) {
                self.cut.store.delete_retired(&mut authority, &object.key, now).await.map_err(storage)?;
            }
        }
        authority.prune_intake_deleted_objects(&self.cut.partition, self.cut.epoch).map_err(storage)?;
        Ok(())
    }
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
        authority.enable_intake_gc(&partition, epoch).map_err(storage)?;
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
            head.log_base_seq,
            max_events as usize,
            max_bytes as usize,
        )
        .await
        .map_err(storage)?;
        let mut events = Vec::new();
        let mut snapshot_bytes = 0u64;
        if let Some(reference) = &head.manifest {
            let snapshot: Snapshot = serde_json::from_slice(&store.get_verified(reference).await.map_err(storage)?).map_err(storage)?;
            if snapshot.format != 1 || snapshot.identity != identity || snapshot.seq != head.log_base_seq
                || snapshot.chunks.len() > max_operations.saturating_add(1) {
                return Err(storage("invalid remote intake snapshot"));
            }
            let mut total = reference.length;
            for chunk in &snapshot.chunks {
                total = total.checked_add(chunk.length).filter(|n| *n <= max_bytes).ok_or(RecordError::Busy)?;
                let bytes = store.get_verified(chunk).await.map_err(storage)?;
                events.extend(serde_json::from_slice::<Vec<Event>>(&bytes).map_err(storage)?);
                if events.len() > max_operations.saturating_add(1) { return Err(storage("oversized intake snapshot")); }
            }
            snapshot_bytes = total;
        } else if head.log_base_seq != 0 { return Err(storage("missing remote intake snapshot")); }

        for entry in entries {
            events.push(serde_json::from_slice(&entry.payload).map_err(storage)?);
        }
        let (_, bytes) = authority.retained_tail_size(&head).map_err(storage)?;
        let mut log = Self {
                store,
                authority,
                partition,
                epoch,
                identity: identity.into(),
                base: head.log_base_seq,
                events: head.committed_seq,
                bytes: bytes.checked_add(snapshot_bytes).filter(|n| *n <= max_bytes).ok_or(RecordError::Busy)?,
                snapshot_bytes,
                max_events,
                max_objects: max_events.checked_mul(4).ok_or_else(|| storage("intake inventory budget overflow"))?,
                max_bytes,
                max_object_bytes,
                failed: false,
                spare_objects: 0,
                _owner: owner,
            };
        log.authority.cancel_intake_snapshot_uploads(&log.partition, log.epoch).map_err(storage)?;
        let _ = log.cleanup().await;
        Ok((log, events))
    }
    pub(super) fn cut(&self) -> Result<Cut, RecordError> {
        if self.failed { return Err(RecordError::RecoveryRequired); }
        let head = self.authority.read_head(&self.partition).map_err(storage)?.ok_or(RecordError::RecoveryRequired)?;
        if head.committed_seq != self.events || head.owner_epoch != self.epoch { return Err(RecordError::RecoveryRequired); }
        Ok(Cut { store: self.store.clone(), database: self.authority.database_path().to_path_buf(),
            partition: self.partition.clone(), epoch: self.epoch, identity: self.identity.clone(), head,
            events: self.events, base: self.base, max_bytes: self.max_bytes, max_object_bytes: self.max_object_bytes,
            _owner: self._owner.try_clone().map_err(storage)? })
    }
    pub(super) fn reserve(&mut self, prepared: &Prepared, slots: u64, bytes: u64) -> Result<bool, RecordError> {
        if self.failed { return Err(RecordError::RecoveryRequired); }
        if self.events != prepared.cut.events || self.base != prepared.cut.base { return Ok(false); }
        if prepared.retained.checked_add(bytes).is_none_or(|n| n > self.max_bytes) { return Err(RecordError::Busy); }
        let objects = prepared.objects.iter().map(|(r, _)| r.clone()).collect::<Vec<_>>();
        self.authority.reserve_intake_snapshot_uploads(&self.partition, self.epoch,
            prepared.cut.head.head_version, &prepared.build, &objects, unix_time_ms_i64(), self.max_objects, slots).map_err(storage)
    }
    pub(super) fn reserve_with_spare(&mut self, prepared: &Prepared, slots: u64, bytes: u64, spare: u64) -> Result<bool, RecordError> {
        let reserved = self.reserve(prepared, slots.checked_add(spare).ok_or(RecordError::Busy)?, bytes)?;
        if reserved { self.spare_objects = spare; }
        Ok(reserved)
    }
    #[cfg(test)]
    pub(super) fn set_inventory_limit(&mut self, limit: u64) { self.max_objects = limit; }
    #[cfg(test)]
    pub(super) fn set_spare_for_test(&mut self, spare: u64) { self.spare_objects = spare; }
    pub(super) fn clear_spare(&mut self) { self.spare_objects = 0; }
    pub(super) fn select(&mut self, prepared: &Prepared, bytes: u64) -> Result<bool, RecordError> {
        if self.failed { return Err(RecordError::RecoveryRequired); }
        if self.events != prepared.cut.events || self.base != prepared.cut.base { return Ok(false); }
        if prepared.retained.checked_add(bytes).is_none_or(|n| n > self.max_bytes) { return Err(RecordError::Busy); }
        self.failed = true;
        let selected = self.authority.publish_checkpoint(&self.partition, prepared.cut.head.head_version,
            self.epoch, self.events, prepared.manifest.clone()).map_err(|e| RecordError::OutcomeUnknown(e.to_string()))?;
        if selected.is_none() { self.failed = false; return Ok(false); }
        #[cfg(test)] if self.authority.database_path().with_extension("fail-intake-after-select").exists() {
            return Err(RecordError::OutcomeUnknown("injected remote intake response loss".into()));
        }
        self.base = self.events; self.snapshot_bytes = prepared.retained; self.bytes = prepared.retained; self.failed = false;
        Ok(true)
    }
    pub(super) fn garbage(&self) -> Result<Vec<ObjectRef>, RecordError> {
        let prefix = format!("partitions/{}/", self.partition);
        Ok(self.authority.unreferenced_objects().map_err(storage)?.into_iter().filter(|o| o.key.starts_with(&prefix)).collect())
    }
    #[cfg(test)]
    pub(super) async fn compact(&mut self, events: Vec<Vec<u8>>, completion_slots: u64, completion_bytes: u64) -> Result<(), RecordError> {
        if self.failed { return Err(RecordError::RecoveryRequired); }
        let head = self.authority.read_head(&self.partition).map_err(storage)?.ok_or(RecordError::RecoveryRequired)?;
        if head.committed_seq != self.events || head.owner_epoch != self.epoch { return Err(RecordError::RecoveryRequired); }
        self.cleanup().await?;
        // Stable intake ownership excludes another publisher. Reserve all possible
        // candidate uploads and one completion object per pending command before
        // the first inventory row/PUT; interrupted candidates consume this budget.
        let mut objects = 2u64; // final chunk and manifest
        let mut size = 1usize;
        for event in &events {
            if event.len().checked_add(2).is_none_or(|n| n > self.max_object_bytes) { return Err(RecordError::Busy); }
            if size + event.len() + 2 > self.max_object_bytes {
                objects = objects.checked_add(1).ok_or(RecordError::Busy)?;
                size = 1;
            }
            size += event.len() + usize::from(size > 1);
        }
        self.admit_objects(objects.checked_add(completion_slots).ok_or(RecordError::Busy)?)?;
        let total = events.iter().try_fold(0u64, |sum, e| sum.checked_add(e.len() as u64 + 2))
            .filter(|n| *n <= self.max_bytes).ok_or(RecordError::Busy)?;
        let _memory = crate::storage::build_budget::reserve_memory(total.checked_mul(2).ok_or(RecordError::Busy)?).map_err(storage)?;
        let prefix = format!("partitions/{}/snapshot/{:020}-{:032x}", self.partition, head.head_version.checked_add(1).ok_or(RecordError::RecoveryRequired)?, rand::random::<u128>());
        let mut chunks = Vec::new();
        let mut buffer = vec![b'['];
        for event in events {
            if event.len() + 2 > self.max_object_bytes { return Err(RecordError::Busy); }
            if buffer.len() + event.len() + 2 > self.max_object_bytes {
                buffer.push(b']');
                let key = format!("{prefix}/{}", chunks.len());
                chunks.push(self.upload_snapshot(&key, &buffer).await?);
                buffer = vec![b'['];
            }
            if buffer.len() > 1 { buffer.push(b','); }
            buffer.extend(event);
        }
        buffer.push(b']');
        let key = format!("{prefix}/{}", chunks.len());
        chunks.push(self.upload_snapshot(&key, &buffer).await?);
        let snapshot = Snapshot { format: 1, identity: self.identity.clone(), seq: self.events, chunks };
        let bytes = serde_json::to_vec(&snapshot).map_err(storage)?;
        if bytes.len() > self.max_object_bytes { return Err(RecordError::Busy); }
        let manifest = self.upload_snapshot(&format!("{prefix}/manifest"), &bytes).await?;
        for child in &snapshot.chunks { self.authority.register_object_edge(&manifest, child).map_err(storage)?; }
        let retained = snapshot.chunks.iter().try_fold(bytes.len() as u64, |n, chunk| n.checked_add(chunk.length))
            .filter(|n| *n <= self.max_bytes).ok_or(RecordError::Busy)?;
        // Object-slot reservation is not a substitute for the byte headroom
        // already promised to accepted, pending commands.
        if retained.checked_add(completion_bytes).is_none_or(|n| n > self.max_bytes) {
            return Err(RecordError::Busy);
        }
        #[cfg(test)] if self.authority.database_path().with_extension("fail-intake-before-select").exists() {
            return Err(storage("injected failure after verified snapshot upload"));
        }
        self.failed = true;
        self.authority.publish_checkpoint(&self.partition, head.head_version, self.epoch, self.events, manifest)
            .map_err(|e| RecordError::OutcomeUnknown(e.to_string()))?.ok_or(RecordError::RecoveryRequired)?;
        #[cfg(test)] if self.authority.database_path().with_extension("fail-intake-after-select").exists() {
            return Err(RecordError::OutcomeUnknown("injected remote intake response loss".into()));
        }
        self.base = self.events;
        self.snapshot_bytes = retained;
        self.bytes = self.snapshot_bytes;
        self.failed = false;
        // Selected snapshots are authoritative before removing any old root. GC is
        // monotonic; a deletion failure leaks space and can be retried on reopen.
        let _ = self.cleanup().await;
        Ok(())
    }
    #[cfg(test)]
    async fn upload_snapshot(&mut self, key: &str, bytes: &[u8]) -> Result<ObjectRef, RecordError> {
        #[cfg(test)] if self.authority.database_path().with_extension("fail-intake-unknown-put").exists() {
            let reference = ObjectRef { key: key.into(), sha256: Sha256::digest(bytes).into(), length: bytes.len() as u64 };
            self.authority.reserve_upload(&reference, unix_time_ms_i64()).map_err(storage)?;
            return Err(RecordError::OutcomeUnknown("injected snapshot PUT response loss".into()));
        }
        self.store.put_and_register(&mut self.authority, key, bytes, unix_time_ms_i64()).await.map_err(upload_error)
    }
    pub(super) fn admit_objects(&self, additional: u64) -> Result<(), RecordError> {
        let retained = self.authority.intake_inventory_count(&self.partition, self.epoch).map_err(storage)?;
        if retained.checked_add(additional).and_then(|n| n.checked_add(self.spare_objects)).is_none_or(|n| n > self.max_objects) { return Err(RecordError::Busy); }
        Ok(())
    }
    async fn cleanup(&mut self) -> Result<(), RecordError> {
        let _phase = maintenance::Phase::new("intake_remote_cleanup", self.events);
        if self.base > 0 { self.authority.prune_intake_receipts(&self.partition, self.epoch, self.base).map_err(storage)?; }
        let now = unix_time_ms_i64();
        let prefix = format!("partitions/{}/", self.partition);
        for object in self.authority.unreferenced_objects().map_err(storage)? {
            if object.key.starts_with(&prefix) {
                self.authority.retire_object(&object.key, self.events, now).map_err(storage)?;
            }
        }
        for object in self.authority.collectible_objects(now).map_err(storage)? {
            if object.key.starts_with(&prefix) {
                self.store.delete_retired(&mut self.authority, &object.key, now).await.map_err(storage)?;
            }
        }
        self.authority.prune_intake_deleted_objects(&self.partition, self.epoch).map_err(storage)?;
        Ok(())
    }
    pub(super) fn bytes(&self) -> u64 {
        self.bytes
    }
    pub(super) fn available(&self) -> u64 {
        self.max_bytes.saturating_sub(self.bytes)
    }
    #[cfg(test)]
    pub(super) fn inventory_for_test(&self) -> u64 {
        self.authority.intake_inventory_count(&self.partition, self.epoch).unwrap()
    }
    pub(super) async fn append(&mut self, event: &Event) -> Result<(), RecordError> {
        if self.failed {
            return Err(RecordError::RecoveryRequired);
        }
        self.admit_objects(1)?;
        let payload = serde_json::to_vec(event).map_err(storage)?;
        let estimate = (payload.len() as u64)
            .checked_mul(5)
            .and_then(|n| n.checked_add(4096))
            .ok_or(RecordError::Busy)?;
        if self.events.saturating_sub(self.base) >= self.max_events
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
        .map_err(|e| match e {
            object_log::TailError::Admission(_) => {
                // No inventory mutation or PUT occurred: this exact event is safe to retry.
                self.failed = false;
                RecordError::Busy
            }
            other => RecordError::OutcomeUnknown(other.to_string()),
        })?;
        let head = match outcome {
            object_log::AppendCommitOutcome::Published { head, .. }
            | object_log::AppendCommitOutcome::AlreadyPublished(head) => head,
            object_log::AppendCommitOutcome::Conflict => return Err(RecordError::RecoveryRequired),
        };
        if head.committed_seq != self.events + 1 {
            return Err(RecordError::RecoveryRequired);
        }
        self.events = head.committed_seq;
        self.bytes = self.snapshot_bytes + self.authority.retained_tail_size(&head).map_err(storage)?.1;
        self.failed = false;
        Ok(())
    }
}

fn upload_error(error: UploadRegisterError) -> RecordError {
    match error {
        UploadRegisterError::Admission(_) => RecordError::Busy,
        other => storage(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn remote_append_admission_is_retryable_and_snapshot_keeps_staging() {
        let temporary = tempfile::tempdir().unwrap();
        let source = (S3CompatibleStore::from_store(Arc::new(object_store::memory::InMemory::new()), 16384),
            temporary.path().join("authority.sqlite"), "source".to_owned(), 1, 16384);
        let (mut log, _) = Log::open(source.clone(), "config", 128, 16384).await.unwrap();
        let request = super::super::super::tests::write("admission", "a", "alpha", ExpectedVersion::Absent);
        let accept = Event::Accept { first: 1, requests: vec![request] };
        let inventory = log.authority.intake_inventory_count(&log.partition, log.epoch).unwrap();
        let scope = Arc::new(crate::resources::Governor::default()).admit("remote-admission".into(), crate::resources::Lane::Background, 0).unwrap();
        scope.clone().run(async {
            let held = scope.io().unwrap();
            assert!(matches!(log.append(&accept).await, Err(RecordError::Busy)));
            assert!(!log.failed);
            assert_eq!(log.events, 0);
            assert_eq!(log.authority.intake_inventory_count(&log.partition, log.epoch).unwrap(), inventory);
            drop(held);
            log.append(&accept).await.unwrap();
            let complete = Event::Complete { first: 1, results: vec![Err(IngestFailure { code: "conflict".into(), message: "done".into() })] };
            let held = scope.io().unwrap();
            assert!(matches!(log.append(&complete).await, Err(RecordError::Busy)));
            assert!(!log.failed); assert_eq!(log.events, 1);
            drop(held); log.append(&complete).await.unwrap();
            let candidate = Prepared::encode(log.cut().unwrap(), vec![serde_json::to_vec(&accept).unwrap(), serde_json::to_vec(&complete).unwrap()]).unwrap();
            assert!(log.reserve(&candidate, 0, 0).unwrap());
            let held = scope.io().unwrap();
            let upload = candidate.upload();
            tokio::pin!(upload);
            assert!(tokio::time::timeout(std::time::Duration::from_millis(30), &mut upload).await.is_err());
            for (reference, _) in &candidate.objects {
                assert!(!log.authority.retire_object(&reference.key, 0, i64::MAX).unwrap());
            }
            drop(held); upload.await.unwrap();
            assert!(log.select(&candidate, 0).unwrap());
            candidate.release(log.garbage().unwrap()).await.unwrap();
        }).await;
        drop(log);
        let (_, events) = Log::open(source, "config", 128, 16384).await.unwrap();
        assert!(matches!(&events[..], [Event::Accept { .. }, Event::Complete { .. }]));
    }

    #[tokio::test]
    async fn unlocked_snapshot_exact_chunk_boundary_never_emits_empty_extra_chunk() {
        let temporary = tempfile::tempdir().unwrap();
        let source = (S3CompatibleStore::from_store(Arc::new(object_store::memory::InMemory::new()), 4096),
            temporary.path().join("authority.sqlite"), "source".to_owned(), 1, 4096);
        let (log, _) = Log::open(source, "config", 128, 4096).await.unwrap();
        let event = vec![b' '; 4094]; // packing boundary independent of Event decoding
        let candidate = Prepared::encode(log.cut().unwrap(), vec![event.clone(), event]).unwrap();
        assert_eq!(candidate.objects.len(), 3); // two chunks and one manifest
        assert_eq!(candidate.objects[0].0.length, 4096);
        assert_eq!(candidate.objects[1].0.length, 4096);
    }

    #[tokio::test]
    async fn unlocked_snapshot_spare_reservation_preserves_pending_completion() {
        let temporary = tempfile::tempdir().unwrap();
        let source = (S3CompatibleStore::from_store(Arc::new(object_store::memory::InMemory::new()), 16384),
            temporary.path().join("authority.sqlite"), "source".to_owned(), 1, 16384);
        let (mut log, _) = Log::open(source, "config", 128, 16384).await.unwrap();
        let request = super::super::super::tests::write("pending", "a", "alpha", ExpectedVersion::Absent);
        log.append(&Event::Accept { first: 1, requests: vec![request] }).await.unwrap();
        let candidate = Prepared::encode(log.cut().unwrap(), vec![serde_json::to_vec(&Event::Reset { base: 0, epoch_floor: 0, materialized_seq: 0 }).unwrap()]).unwrap();
        log.max_objects = 7;
        assert!(log.reserve_with_spare(&candidate, 1, 0, 2).unwrap());
        assert!(matches!(log.admit_objects(2), Err(RecordError::Busy)));
        log.append(&Event::Complete { first: 1, results: vec![Err(IngestFailure { code: "conflict".into(), message: "done".into() })] }).await.unwrap();
        assert_eq!(log.spare_objects, 2);
        let stable = Prepared::encode(log.cut().unwrap(), vec![serde_json::to_vec(&Event::Reset { base: 0, epoch_floor: 0, materialized_seq: 0 }).unwrap()]).unwrap();
        assert!(log.reserve_with_spare(&stable, 0, 0, 0).unwrap());
        assert_eq!(log.spare_objects, 0);
        assert_eq!(log.authority.intake_inventory_count(&log.partition, log.epoch).unwrap(), 7);
    }

    #[tokio::test]
    async fn unlocked_snapshot_stale_cut_pins_unknown_and_preserves_append_suffix() {
        let temporary = tempfile::tempdir().unwrap();
        let source = (S3CompatibleStore::from_store(Arc::new(object_store::memory::InMemory::new()), 16384),
            temporary.path().join("authority.sqlite"), "source".to_owned(), 1, 16384);
        let (mut log, _) = Log::open(source.clone(), "config", 128, 16384).await.unwrap();
        let events = || vec![serde_json::to_vec(&Event::Reset { base: 0, epoch_floor: 0, materialized_seq: 0 }).unwrap()];
        let candidate = Prepared::encode(log.cut().unwrap(), events()).unwrap();
        assert!(log.reserve(&candidate, 0, 0).unwrap());
        for (reference, _) in &candidate.objects {
            assert!(!log.authority.retire_object(&reference.key, 0, i64::MAX).unwrap());
        }
        let request = super::super::super::tests::write("concurrent", "a", "alpha", ExpectedVersion::Absent);
        log.append(&Event::Accept { first: 1, requests: vec![request.clone()] }).await.unwrap();
        log.append(&Event::Complete { first: 1, results: vec![Err(IngestFailure { code: "conflict".into(), message: "terminal".into() })] }).await.unwrap();
        candidate.upload().await.unwrap();
        assert!(!log.select(&candidate, 0).unwrap());
        let garbage = log.garbage().unwrap();
        candidate.release(garbage).await.unwrap();
        assert!(!log.failed);
        drop(log);
        // In-flight maintenance retains stable ownership until it finishes.
        assert!(Log::open(source.clone(), "config", 128, 16384).await.is_err());
        drop(candidate);
        let (mut log, recovered) = Log::open(source.clone(), "config", 128, 16384).await.unwrap();
        assert!(matches!(&recovered[..], [Event::Accept { .. }, Event::Complete { .. }]));
        let candidate = Prepared::encode(log.cut().unwrap(), events()).unwrap();
        assert!(log.reserve(&candidate, 1, 0).unwrap());
        let fault = log.authority.database_path().with_extension("fail-intake-unknown-put");
        std::fs::write(&fault, b"").unwrap();
        assert!(matches!(candidate.upload().await, Err(RecordError::OutcomeUnknown(_))));
        candidate.release(Vec::new()).await.unwrap();
        for (reference, _) in &candidate.objects {
            assert!(log.authority.collectible_objects(0).unwrap().contains(reference));
            assert!(log.authority.mark_object_deleted(reference).unwrap());
        }
        std::fs::remove_file(fault).unwrap();
    }

    #[tokio::test]
    async fn unlocked_snapshot_selected_release_preserves_manifest_closure() {
        let temporary = tempfile::tempdir().unwrap();
        let source = (S3CompatibleStore::from_store(Arc::new(object_store::memory::InMemory::new()), 16384),
            temporary.path().join("authority.sqlite"), "source".to_owned(), 1, 16384);
        let (mut log, _) = Log::open(source.clone(), "config", 128, 16384).await.unwrap();
        let candidate = Prepared::encode(log.cut().unwrap(), vec![serde_json::to_vec(&Event::Reset {
            base: 0, epoch_floor: 0, materialized_seq: 0 }).unwrap()]).unwrap();
        assert!(log.reserve(&candidate, 0, 0).unwrap());
        candidate.upload().await.unwrap();
        assert!(log.select(&candidate, 0).unwrap());
        candidate.release(log.garbage().unwrap()).await.unwrap();
        for (reference, _) in &candidate.objects {
            assert!(!log.authority.retire_object(&reference.key, 0, i64::MAX).unwrap());
            assert!(log.store.get_verified(reference).await.is_ok());
        }
        assert_eq!(log.authority.release_intake_snapshot_uploads(&log.partition, 1, &candidate.build).unwrap(), 0);
        drop(candidate); drop(log);
        let (_, events) = Log::open(source, "config", 128, 16384).await.unwrap();
        assert!(matches!(&events[..], [Event::Reset { .. }]));
    }

    #[tokio::test]
    async fn unlocked_snapshot_publication_unknown_keeps_pins_until_owner_recovery() {
        let temporary = tempfile::tempdir().unwrap();
        let source = (S3CompatibleStore::from_store(Arc::new(object_store::memory::InMemory::new()), 16384),
            temporary.path().join("authority.sqlite"), "source".to_owned(), 1, 16384);
        let (mut log, _) = Log::open(source.clone(), "config", 128, 16384).await.unwrap();
        let candidate = Prepared::encode(log.cut().unwrap(), vec![serde_json::to_vec(&Event::Reset {
            base: 0, epoch_floor: 0, materialized_seq: 0 }).unwrap()]).unwrap();
        assert!(log.reserve(&candidate, 0, 0).unwrap());
        let before = log.authority.database_path().with_extension("fail-intake-before-select");
        std::fs::write(&before, b"").unwrap();
        assert!(candidate.upload().await.is_err());
        assert_eq!(log.authority.read_head(&log.partition).unwrap().unwrap(), candidate.cut.head);
        std::fs::remove_file(before).unwrap();
        candidate.upload().await.unwrap();
        let after = log.authority.database_path().with_extension("fail-intake-after-select");
        std::fs::write(&after, b"").unwrap();
        assert!(matches!(log.select(&candidate, 0), Err(RecordError::OutcomeUnknown(_))));
        assert!(matches!(log.append(&Event::Reset { base: 0, epoch_floor: 0, materialized_seq: 0 }).await, Err(RecordError::RecoveryRequired)));
        assert_eq!(log.authority.release_intake_snapshot_uploads(&log.partition, 2, &candidate.build).is_err(), true);
        let manifest = candidate.manifest.clone();
        drop(candidate); drop(log); std::fs::remove_file(after).unwrap();
        let (mut reopened, events) = Log::open(source, "config", 128, 16384).await.unwrap();
        assert!(matches!(&events[..], [Event::Reset { .. }]));
        assert_eq!(reopened.authority.read_head(&reopened.partition).unwrap().unwrap().manifest, Some(manifest.clone()));
        assert_eq!(reopened.authority.cancel_intake_snapshot_uploads(&reopened.partition, 1).unwrap(), 0);
        assert!(!reopened.authority.retire_object(&manifest.key, 0, i64::MAX).unwrap());
    }

    #[tokio::test]
    async fn remote_compaction_preserves_pending_completion_byte_headroom() {
        let temporary = tempfile::tempdir().unwrap();
        let source = (S3CompatibleStore::from_store(Arc::new(object_store::memory::InMemory::new()), 128 * 1024),
            temporary.path().join("authority.sqlite"), "source".to_owned(), 1, 128 * 1024);
        // The identity is duplicated in the snapshot descriptor, so a complete
        // selected snapshot can occupy more bytes than the original short tail.
        let identity = "configuration".repeat(5000);
        let (mut log, _) = Log::open(source.clone(), &identity, 128, 16384).await.unwrap();
        let request = super::super::super::tests::write("pending-reserve", "a", "alpha", ExpectedVersion::Absent);
        log.append(&Event::Accept { first: 1, requests: vec![request.clone()] }).await.unwrap();
        let head = log.authority.read_head(&log.partition).unwrap().unwrap();
        let completion_bytes = completion_headroom(checkpoint_codec::json_length(&request).unwrap() as u64 + 16384, 5).unwrap();
        assert!(log.bytes() + completion_bytes <= log.max_bytes);
        let snapshot = vec![
            serde_json::to_vec(&Event::Reset { base: 0, epoch_floor: 0, materialized_seq: 0 }).unwrap(),
            serde_json::to_vec(&Event::Accept { first: 1, requests: vec![request.clone()] }).unwrap(),
        ];
        assert!(matches!(log.compact(snapshot, 1, completion_bytes).await, Err(RecordError::Busy)));
        assert_eq!(log.authority.read_head(&log.partition).unwrap().unwrap(), head);
        log.append(&Event::Complete { first: 1, results: vec![Err(IngestFailure { code: "conflict".into(), message: "terminal".into() })] }).await.unwrap();
        drop(log);
        let (_, events) = Log::open(source, &identity, 128, 16384).await.unwrap();
        assert!(matches!(&events[..], [Event::Accept { first: 1, requests }, Event::Complete { first: 1, results }]
            if requests[0].operation_id == request.operation_id && results[0].is_err()));
    }
    #[tokio::test]
    async fn remote_unknown_upload_inventory_is_bounded_and_preserves_pending() {
        use object_store::ObjectStore;
        let temporary = tempfile::tempdir().unwrap();
        let database = temporary.path().join("authority.sqlite");
        let objects = Arc::new(object_store::memory::InMemory::new());
        let source = (S3CompatibleStore::from_store(objects.clone(), 16384), database, "source".to_owned(), 1, 16384);
        let (mut log, _) = Log::open(source.clone(), "config", 128, 16384).await.unwrap();
        log.max_objects = 8;
        let request = super::super::super::tests::write("pending-budget", "a", "alpha", ExpectedVersion::Absent);
        log.append(&Event::Accept { first: 1, requests: vec![request.clone()] }).await.unwrap();
        let selected = log.authority.read_head(&log.partition).unwrap().unwrap();
        let fault = log.authority.database_path().with_extension("fail-intake-unknown-put");
        std::fs::write(&fault, b"").unwrap();
        let snapshot = || vec![
            serde_json::to_vec(&Event::Reset { base: 0, epoch_floor: 0, materialized_seq: 0 }).unwrap(),
            serde_json::to_vec(&Event::Accept { first: 1, requests: vec![request.clone()] }).unwrap(),
        ];
        let mut failures = 0;
        loop {
            match log.compact(snapshot(), 1, 0).await {
                Err(RecordError::OutcomeUnknown(_)) => failures += 1,
                Err(RecordError::Busy) => break,
                other => panic!("unexpected compaction result: {other:?}"),
            }
            assert!(failures < 8);
            drop(log);
            let (reopened, events) = Log::open(source.clone(), "config", 128, 16384).await.unwrap();
            assert!(matches!(&events[..], [Event::Accept { first: 1, requests }] if requests[0].operation_id == request.operation_id));
            log = reopened;
            log.max_objects = 8;
        }
        assert_eq!(failures, 4); // bootstrap + original WAL + four unknown PUTs
        let count = log.authority.intake_inventory_count(&log.partition, log.epoch).unwrap();
        assert_eq!(count, 6);
        assert_eq!(log.authority.read_head(&log.partition).unwrap().unwrap(), selected);
        for _ in 0..3 {
            assert!(matches!(log.compact(snapshot(), 1, 0).await, Err(RecordError::Busy)));
            assert_eq!(log.authority.intake_inventory_count(&log.partition, log.epoch).unwrap(), count);
        }
        // Simulate a PUT arriving after DELETE: the state-5 identity stays known
        // and repeated cleanup removes the late object without forgetting it.
        let late = log.authority.collectible_objects(unix_time_ms_i64()).unwrap().pop().unwrap();
        let path = object_store::path::Path::from(late.key);
        objects.put(&path, b"late PUT".to_vec().into()).await.unwrap();
        log.cleanup().await.unwrap();
        assert!(objects.head(&path).await.is_err());
        assert_eq!(log.authority.intake_inventory_count(&log.partition, log.epoch).unwrap(), count);
        // One new acceptance plus two possible completions is refused, but the
        // already accepted command retains its reserved completion slot.
        assert!(matches!(log.admit_objects(3), Err(RecordError::Busy)));
        log.append(&Event::Complete { first: 1, results: vec![Err(IngestFailure { code: "conflict".into(), message: "test terminal".into() })] }).await.unwrap();
        std::fs::remove_file(fault).unwrap();
        drop(log);
        let (log, events) = Log::open(source, "config", 128, 16384).await.unwrap();
        assert!(matches!(&events[..], [Event::Accept { first: 1, requests }, Event::Complete { first: 1, results }]
            if requests[0].operation_id == request.operation_id && results[0].is_err()));
        assert_eq!(log.authority.intake_inventory_count(&log.partition, log.epoch).unwrap(), 7);
    }
    #[tokio::test]
    async fn remote_unselected_verified_candidates_also_consume_inventory_budget() {
        let temporary = tempfile::tempdir().unwrap();
        let source = (S3CompatibleStore::from_store(Arc::new(object_store::memory::InMemory::new()), 16384),
            temporary.path().join("authority.sqlite"), "source".to_owned(), 1, 16384);
        let (mut log, _) = Log::open(source.clone(), "config", 128, 16384).await.unwrap();
        log.max_objects = 8;
        let selected = log.authority.read_head(&log.partition).unwrap().unwrap();
        std::fs::write(log.authority.database_path().with_extension("fail-intake-before-select"), b"").unwrap();
        let snapshot = || vec![serde_json::to_vec(&Event::Reset { base: 0, epoch_floor: 0, materialized_seq: 0 }).unwrap()];
        for _ in 0..3 { assert!(matches!(log.compact(snapshot(), 0, 0).await, Err(RecordError::Storage(_)))); }
        for _ in 0..3 { assert!(matches!(log.compact(snapshot(), 0, 0).await, Err(RecordError::Busy))); }
        assert_eq!(log.authority.intake_inventory_count(&log.partition, log.epoch).unwrap(), 7);
        assert_eq!(log.authority.read_head(&log.partition).unwrap().unwrap(), selected);
        drop(log);
        let (log, events) = Log::open(source, "config", 128, 16384).await.unwrap();
        assert!(events.is_empty());
        assert_eq!(log.authority.intake_inventory_count(&log.partition, log.epoch).unwrap(), 7);
    }
    #[tokio::test]
    async fn remote_successful_compactions_recycle_metadata_within_budget() {
        let temporary = tempfile::tempdir().unwrap();
        let source = (S3CompatibleStore::from_store(Arc::new(object_store::memory::InMemory::new()), 16384),
            temporary.path().join("authority.sqlite"), "source".to_owned(), 1, 16384);
        let (mut log, _) = Log::open(source, "config", 128, 16384).await.unwrap();
        log.max_objects = 8;
        for _ in 0..20 {
            log.compact(vec![serde_json::to_vec(&Event::Reset { base: 0, epoch_floor: 0, materialized_seq: 0 }).unwrap()], 0, 0).await.unwrap();
            assert_eq!(log.authority.intake_inventory_count(&log.partition, log.epoch).unwrap(), 3);
        }
    }
    #[tokio::test]
    async fn remote_snapshot_reclaims_payloads_and_recovers_response_lost_selection() {
        let temporary = tempfile::tempdir().unwrap();
        let database = temporary.path().join("authority.sqlite");
        let objects = Arc::new(object_store::memory::InMemory::new());
        let store = S3CompatibleStore::from_store(objects.clone(), 16384);
        let source = (store.clone(), database, "source".to_owned(), 1, 16384);
        let (mut log, events) = Log::open(source.clone(), "config", 128, 16384).await.unwrap();
        assert!(events.is_empty());
        let request = super::super::super::tests::write("one", "a", "alpha", ExpectedVersion::Absent);
        log.append(&Event::Accept { first: 1, requests: vec![request.clone()] }).await.unwrap();
        let old_tip = log.authority.read_head(&log.partition).unwrap().unwrap().wal_tip.unwrap();
        let status = IngestStatus { accepted: IngestReceipt { durable_seq: 1, operation_id: request.operation_id.clone() },
            receipt: None, error: Some(IngestFailure { code: "rejected".into(), message: "test".into() }) };
        let events = vec![
            serde_json::to_vec(&Event::Reset { base: 0, epoch_floor: 0, materialized_seq: 0 }).unwrap(),
            serde_json::to_vec(&Event::Retain { digest: request_digest(&request).unwrap(), status }).unwrap(),
        ];
        let fault = log.authority.database_path().with_extension("fail-intake-after-select");
        std::fs::write(&fault, b"").unwrap();
        assert!(matches!(log.compact(events, 0, 0).await, Err(RecordError::OutcomeUnknown(_))));
        assert!(matches!(log.append(&Event::Reset { base: 0, epoch_floor: 0, materialized_seq: 0 }).await,
            Err(RecordError::RecoveryRequired)));
        drop(log);
        std::fs::remove_file(fault).unwrap();
        let (mut log, events) = Log::open(source.clone(), "config", 128, 16384).await.unwrap();
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[1], Event::Retain { status, .. } if status.accepted.operation_id == request.operation_id));
        assert!(log.bytes() > 0);
        assert!(store.get_verified(&old_tip).await.is_err());
        assert!(log.authority.resolve_operation(&log.partition, "intake-event-1", [0; 32]).unwrap().is_none());
        log.append(&Event::Accept { first: 2, requests: vec![request] }).await.unwrap();
        drop(log);
        let (log, events) = Log::open(source.clone(), "config", 128, 16384).await.unwrap();
        assert_eq!(events.len(), 3);
        let manifest = log.authority.read_head(&log.partition).unwrap().unwrap().manifest.unwrap();
        let path = object_store::path::Path::from(manifest.key);
        use object_store::ObjectStore;
        objects.delete(&path).await.unwrap();
        drop(log);
        // The missing selected snapshot cannot silently fall back to the old chain.
        let source = (store.uncached(), source.1, source.2, source.3, source.4);
        assert!(Log::open(source, "config", 128, 16384).await.is_err());
    }
}
