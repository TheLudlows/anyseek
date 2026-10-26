//! Bounded immutable object-log blocks and fail-closed committed-tail replay.
//!
//! The SQLite head decides which tip and sequence range is committed. Objects not
//! reachable from that head are never replayed. This module provides the tail
//! primitive; RecordPartition applies returned entries in the same read view
//! as its selected manifest before serving search results.
use super::{
    s3_compatible::{S3CompatibleStore, UploadRegisterError},
    sqlite_authority::{ObjectRef, PartitionHead, PublishOutcome, SqliteAuthority},
};
use serde::{Deserialize, Serialize};
use std::fmt;

/// One durable application operation in the partition commit log.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObjectLogEntry {
    pub seq: u64,
    pub operation_id: String,
    pub request_sha256: [u8; 32],
    /// Canonical Record operation envelope, not a search index or model output.
    pub payload: Vec<u8>,
}

/// Immutable log block. The previous reference chains blocks; the SQLite head alone
/// selects the latest committed tip and its committed sequence.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObjectLogBlock {
    pub partition_id: String,
    pub first_seq: u64,
    pub last_seq: u64,
    pub previous: Option<ObjectRef>,
    pub entries: Vec<ObjectLogEntry>,
}

/// Tail replay failure. Any missing block, gap, digest error or exhausted budget
/// fails the read; callers must never return a partial tail as a complete result.
#[derive(Debug)]
pub(crate) enum TailError {
    ObjectStore(object_store::Error),
    Metadata(rusqlite::Error),
    Invalid(&'static str),
    Decode(serde_json::Error),
}
impl fmt::Display for TailError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ObjectStore(e) => write!(f, "object-store tail read failed: {e}"),
            Self::Metadata(e) => write!(f, "SQLite head read failed: {e}"),
            Self::Invalid(e) => write!(f, "invalid committed object log: {e}"),
            Self::Decode(e) => write!(f, "invalid committed log encoding: {e}"),
        }
    }
}
impl std::error::Error for TailError {}
impl From<UploadRegisterError> for TailError {
    fn from(value: UploadRegisterError) -> Self {
        match value {
            UploadRegisterError::ObjectStore(e) => Self::ObjectStore(e),
            UploadRegisterError::Metadata(e) => Self::Metadata(e),
        }
    }
}

/// Decoded recovery inputs retain their aggregate charge after individual GET
/// buffers are released, including while the caller consumes the iterator.
pub(crate) struct CommittedEntries {
    entries: Vec<ObjectLogEntry>,
    leases: std::sync::Arc<Vec<crate::resources::Lease>>,
}
impl std::ops::Deref for CommittedEntries {
    type Target = [ObjectLogEntry];
    fn deref(&self) -> &[ObjectLogEntry] { &self.entries }
}
impl std::fmt::Debug for CommittedEntries {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.entries.fmt(f) }
}
impl PartialEq<Vec<ObjectLogEntry>> for CommittedEntries {
    fn eq(&self, other: &Vec<ObjectLogEntry>) -> bool { &self.entries == other }
}
pub(crate) struct CommittedIter {
    entries: std::vec::IntoIter<ObjectLogEntry>,
    leases: std::sync::Arc<Vec<crate::resources::Lease>>,
}
pub(crate) struct CommittedEntry {
    entry: ObjectLogEntry,
    _leases: std::sync::Arc<Vec<crate::resources::Lease>>,
}
impl std::ops::Deref for CommittedEntry {
    type Target = ObjectLogEntry;
    fn deref(&self) -> &ObjectLogEntry { &self.entry }
}
impl Iterator for CommittedIter {
    type Item = CommittedEntry;
    fn next(&mut self) -> Option<Self::Item> { self.entries.next().map(|entry| CommittedEntry {entry, _leases:self.leases.clone()}) }
}
impl IntoIterator for CommittedEntries {
    type Item = CommittedEntry;
    type IntoIter = CommittedIter;
    fn into_iter(self) -> Self::IntoIter { CommittedIter { entries: self.entries.into_iter(), leases: self.leases } }
}

/// Serialize and upload a block, verify it through the read path, and register it
/// as durable. This does not advance the commit watermark.
pub(crate) async fn upload_log_block(
    store: &S3CompatibleStore,
    authority: &mut SqliteAuthority,
    key: &str,
    block: &ObjectLogBlock,
    created_at_ms: i64,
    max_block_bytes: usize,
) -> Result<ObjectRef, TailError> {
    validate_block(block)?;
    let bytes = serde_json::to_vec(block).map_err(TailError::Decode)?;
    if bytes.len() > max_block_bytes {
        return Err(TailError::Invalid("log block exceeds configured byte budget"));
    }
    let reference = store.put_and_register(authority, key, &bytes, created_at_ms).await
        .map_err(TailError::from)?;
    if let Some(previous) = block.previous.as_ref() {
        // The immutable block embeds a previous-object reference; record the same
        // edge in SQLite so backup closure discovery and GC traverse the full chain.
        authority.register_object_edge(&reference, previous).map_err(TailError::Metadata)?;
    }
    Ok(reference)
}

/// Result of the object-first append and SQLite publication protocol.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AppendCommitOutcome {
    Published { head: PartitionHead, object: ObjectRef },
    AlreadyPublished(PartitionHead),
    Conflict,
}

/// Persist one operation as an immutable log object, then atomically publish its
/// reference and committed watermark in SQLite. A retry first resolves the original
/// operation receipt; it never allocates a new sequence or rewrites a committed key.
///
/// This storage primitive does not apply Record state. RecordPartition owns
/// canonical mutation validation and publication of its complete retrieval view.
pub(crate) async fn append_and_commit(
    store: &S3CompatibleStore,
    authority: &mut SqliteAuthority,
    partition_id: &str,
    owner_epoch: u64,
    operation_id: &str,
    request_sha256: [u8;32],
    payload: Vec<u8>,
    created_at_ms: i64,
    max_block_bytes: usize,
) -> Result<AppendCommitOutcome, TailError> {
    if let Some(receipt) = authority.resolve_operation(partition_id, operation_id, request_sha256)
        .map_err(TailError::Metadata)? {
        return Ok(AppendCommitOutcome::AlreadyPublished(receipt));
    }
    let head = authority.read_head(partition_id).map_err(TailError::Metadata)?
        .ok_or(TailError::Invalid("partition authority does not exist"))?;
    if head.owner_epoch != owner_epoch {
        return Ok(AppendCommitOutcome::Conflict);
    }
    let seq = head.committed_seq.checked_add(1)
        .ok_or(TailError::Invalid("committed sequence exhausted"))?;
    let entry = ObjectLogEntry {
        seq,
        operation_id: operation_id.to_owned(),
        request_sha256,
        payload,
    };
    let block = ObjectLogBlock {
        partition_id: partition_id.to_owned(),
        first_seq: seq,
        last_seq: seq,
        previous: head.wal_tip.clone(),
        entries: vec![entry],
    };
    let digest_hex: String = request_sha256.iter().map(|byte| format!("{byte:02x}")).collect();
    let key = format!("partitions/{partition_id}/wal/{seq:020}-{digest_hex}.json");
    let object = upload_log_block(store, authority, &key, &block, created_at_ms, max_block_bytes).await?;
    match authority.publish_commit(
        partition_id, head.head_version, owner_epoch, operation_id, request_sha256,
        seq, object.clone(), None,
    ).map_err(TailError::Metadata)? {
        PublishOutcome::Published(head) => Ok(AppendCommitOutcome::Published { head, object }),
        PublishOutcome::AlreadyPublished(head) => Ok(AppendCommitOutcome::AlreadyPublished(head)),
        PublishOutcome::Conflict => Ok(AppendCommitOutcome::Conflict),
    }
}

/// Read exactly the committed, not-yet-indexed tail. Blocks are fetched backward
/// from the authoritative tip, bounded by both block count and total bytes, then
/// verified for partition identity, digest, contiguous sequence and chain linkage.
pub(crate) async fn load_committed_tail(
    store: &S3CompatibleStore,
    head: &PartitionHead,
    max_blocks: usize,
    max_total_bytes: usize,
) -> Result<CommittedEntries, TailError> {
    if head.indexed_seq > head.committed_seq {
        return Err(TailError::Invalid("invalid index watermark"));
    }
    load_committed_entries_after(store, head, head.indexed_seq, max_blocks, max_total_bytes).await
}

/// Read the committed log suffix strictly after a caller-verified local state
/// sequence. This avoids rereading old entries when the remote search-index
/// watermark intentionally remains unchanged for a rebuildable local index.
pub(crate) async fn load_committed_entries_after(
    store: &S3CompatibleStore,
    head: &PartitionHead,
    after_seq: u64,
    max_blocks: usize,
    max_total_bytes: usize,
) -> Result<CommittedEntries, TailError> {
    if head.log_base_seq > head.committed_seq || after_seq > head.committed_seq {
        return Err(TailError::Invalid("invalid log watermarks"));
    }
    if after_seq < head.log_base_seq {
        return Err(TailError::Invalid("requested sequence is behind retained log anchor"));
    }
    if after_seq == head.committed_seq {
        return Ok(CommittedEntries { entries: Vec::new(), leases: std::sync::Arc::new(Vec::new()) });
    }
    if max_blocks == 0 || max_total_bytes == 0 {
        return Err(TailError::Invalid("empty committed-tail budget"));
    }
    let mut expected_last = head.committed_seq;
    let mut current = head.wal_tip.clone().ok_or(TailError::Invalid("committed tail has no WAL tip"))?;
    let mut blocks_read = 0usize;
    let mut total_bytes = 0usize;
    let mut reverse_blocks: Vec<Vec<ObjectLogEntry>> = Vec::new();
    let mut leases = Vec::new();

    // A recovery baseline may lag the separately published index watermark.
    // Honor the caller's authenticated starting cut, not an index-only hint.
    while expected_last > after_seq {
        if blocks_read >= max_blocks {
            return Err(TailError::Invalid("committed tail exceeds block-count budget"));
        }
        let bytes = store.get_verified(&current).await.map_err(TailError::ObjectStore)?;
        total_bytes = total_bytes.checked_add(bytes.len()).ok_or(TailError::Invalid("tail byte count overflow"))?;
        if total_bytes > max_total_bytes {
            return Err(TailError::Invalid("committed tail exceeds byte budget"));
        }
        // Reserve before deserializing or retaining decoded payloads. Charge
        // both decoded entries and the temporary flattening/address workspace.
        let lease = store.retain_bytes((bytes.len() as u64).checked_mul(2)
            .ok_or(TailError::Invalid("tail reservation overflow"))?).map_err(TailError::ObjectStore)?;
        let block: ObjectLogBlock = serde_json::from_slice(&bytes).map_err(TailError::Decode)?;
        validate_block(&block)?;
        if block.partition_id != head.partition_id || block.last_seq != expected_last {
            return Err(TailError::Invalid("partition or log chain sequence mismatch"));
        }
        let reaches_cut = block.first_seq <= after_seq.checked_add(1)
            .ok_or(TailError::Invalid("sequence overflow"))?;
        if reaches_cut && after_seq == head.log_base_seq {
            let bound = if block.first_seq == after_seq + 1 {
                block.previous.as_ref() == head.log_anchor.as_ref()
            } else {
                Some(&current) == head.log_anchor.as_ref()
            };
            if !bound { return Err(TailError::Invalid("recovery log anchor mismatch")); }
        }
        let filtered: Vec<_> = block.entries.into_iter()
            .filter(|entry| entry.seq > after_seq)
            .collect();
        reverse_blocks.push(filtered);
        leases.push(lease);
        blocks_read += 1;

        if block.first_seq <= after_seq.checked_add(1).ok_or(TailError::Invalid("sequence overflow"))? {
            break;
        }
        expected_last = block.first_seq.checked_sub(1).ok_or(TailError::Invalid("invalid block start"))?;
        current = block.previous.ok_or(TailError::Invalid("gap before indexed watermark"))?;
    }

    reverse_blocks.reverse();
    let entries: Vec<_> = reverse_blocks.into_iter().flatten().collect();
    let expected_count = head.committed_seq - after_seq;
    if entries.len() as u64 != expected_count {
        return Err(TailError::Invalid("tail does not cover every committed sequence"));
    }
    for (offset, entry) in entries.iter().enumerate() {
        let expected = after_seq + 1 + offset as u64;
        if entry.seq != expected {
            return Err(TailError::Invalid("non-contiguous operation sequence"));
        }
    }
    Ok(CommittedEntries { entries, leases: std::sync::Arc::new(leases) })
}

fn validate_block(block: &ObjectLogBlock) -> Result<(), TailError> {
    if block.partition_id.is_empty() || block.entries.is_empty()
        || block.first_seq == 0 || block.last_seq < block.first_seq
        || block.entries.len() as u64 != block.last_seq - block.first_seq + 1 {
        return Err(TailError::Invalid("invalid log block range"));
    }
    for (offset, entry) in block.entries.iter().enumerate() {
        if entry.seq != block.first_seq + offset as u64 || entry.operation_id.is_empty() {
            return Err(TailError::Invalid("invalid entry sequence or operation ID"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::s3_compatible::S3CompatibleStore;
    use object_store::memory::InMemory;
    use std::{sync::Arc, time::{Duration, SystemTime}};

    fn entry(seq: u64) -> ObjectLogEntry {
        ObjectLogEntry { seq, operation_id: format!("op-{seq}"), request_sha256: [seq as u8;32], payload: format!("payload-{seq}").into_bytes() }
    }
    fn temp_db() -> std::path::PathBuf {
        let stamp = SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        std::env::temp_dir().join(format!("anyseek-tail-{stamp}-{}.sqlite",std::process::id()))
    }

    #[tokio::test]
    async fn resource_tail_charge_survives_get_and_partial_iterator_consumption() {
        use crate::resources::{Governor, Lane};
        let path = temp_db();
        let mut authority = SqliteAuthority::open(&path,Duration::from_secs(1)).unwrap();
        authority.create_partition("p0",1).unwrap();
        let store = S3CompatibleStore::from_store(Arc::new(InMemory::new()),4096);
        let outcome = append_and_commit(&store,&mut authority,"p0",1,"op-1",[1;32],b"payload".to_vec(),1,2048).await.unwrap();
        let AppendCommitOutcome::Published { head, object } = outcome else { panic!("expected publication") };
        let governor = Arc::new(Governor::default());
        let scope = governor.admit("a".into(),Lane::Recovery,0).unwrap();
        let tail = scope.clone().run(load_committed_tail(&store,&head,4,4096)).await.unwrap();
        let rest = scope.bytes(256*1024*1024-object.length*2).unwrap();
        assert!(scope.bytes(1).is_err());
        let mut iterator = tail.into_iter();
        let entry = iterator.next().unwrap();
        assert_eq!(entry.payload,b"payload");
        assert!(scope.bytes(1).is_err());
        drop(iterator);
        assert!(scope.bytes(1).is_err());
        drop(entry);
        assert!(scope.bytes(object.length*2).is_ok());
        drop(rest);
        let full = scope.bytes(256*1024*1024).unwrap();
        assert!(scope.clone().run(load_committed_tail(&store,&head,4,4096)).await.is_err());
        drop(full);drop(authority);let _=std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn append_protocol_uploads_before_commit_and_resolves_same_operation_retry() {
        let path=temp_db();
        let mut authority=SqliteAuthority::open(&path,Duration::from_secs(1)).unwrap();
        authority.create_partition("p0",8).unwrap();
        let store=S3CompatibleStore::from_store(Arc::new(InMemory::new()),4096);
        let outcome=append_and_commit(
            &store,&mut authority,"p0",8,"op-1",[11;32],b"canonical mutation".to_vec(),1,2048
        ).await.unwrap();
        let head=match outcome {
            AppendCommitOutcome::Published{head,object}=>{
                assert_eq!(head.committed_seq,1);
                assert_eq!(head.indexed_seq,0);
                assert_eq!(store.get_verified(&object).await.unwrap().len()>0,true);
                head
            },
            other=>panic!("{other:?}"),
        };
        let retry=append_and_commit(
            &store,&mut authority,"p0",8,"op-1",[11;32],b"canonical mutation".to_vec(),2,2048
        ).await.unwrap();
        assert!(matches!(retry,AppendCommitOutcome::AlreadyPublished(original) if original.committed_seq==1));
        let tail=load_committed_tail(&store,&head,4,4096).await.unwrap();
        assert_eq!(tail.len(),1);
        assert_eq!(tail[0].payload,b"canonical mutation");
        assert!(matches!(
            append_and_commit(&store,&mut authority,"p0",7,"op-2",[12;32],b"next".to_vec(),3,2048).await.unwrap(),
            AppendCommitOutcome::Conflict
        ));
        drop(authority);
        let _=std::fs::remove_file(&path);
        let _=std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _=std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[tokio::test]
    async fn committed_tail_replays_only_entries_after_index_watermark() {
        let path=temp_db();
        let mut authority=SqliteAuthority::open(&path,Duration::from_secs(1)).unwrap();
        let mut head=authority.create_partition("p0",4).unwrap();
        let store=S3CompatibleStore::from_store(Arc::new(InMemory::new()),4096);
        let b1=ObjectLogBlock{partition_id:"p0".into(),first_seq:1,last_seq:1,previous:None,entries:vec![entry(1)]};
        let r1=upload_log_block(&store,&mut authority,"p0/log/1",&b1,1,2048).await.unwrap();
        let out=authority.publish_commit("p0",head.head_version,4,"op-1",[1;32],1,r1.clone(),None).unwrap();
        head=match out {super::super::sqlite_authority::PublishOutcome::Published(h)=>h,other=>panic!("{other:?}")};
        let b2=ObjectLogBlock{partition_id:"p0".into(),first_seq:2,last_seq:2,previous:Some(r1),entries:vec![entry(2)]};
        let r2=upload_log_block(&store,&mut authority,"p0/log/2",&b2,2,2048).await.unwrap();
        let out=authority.publish_commit("p0",head.head_version,4,"op-2",[2;32],2,r2,None).unwrap();
        head=match out {super::super::sqlite_authority::PublishOutcome::Published(h)=>h,other=>panic!("{other:?}")};
        assert_eq!(load_committed_tail(&store,&head,4,4096).await.unwrap(),vec![entry(1),entry(2)]);
        assert_eq!(load_committed_entries_after(&store,&head,1,4,4096).await.unwrap(),vec![entry(2)]);
        assert_eq!(load_committed_entries_after(&store,&head,2,4,4096).await.unwrap(),Vec::<ObjectLogEntry>::new());
        let reachable=authority.reachable_objects().unwrap();
        assert!(reachable.iter().any(|object| object.key=="p0/log/1"));
        assert!(reachable.iter().any(|object| object.key=="p0/log/2"));
        let manifest=ObjectLogBlock{partition_id:"p0".into(),first_seq:1,last_seq:1,previous:None,entries:vec![entry(1)]};
        let manifest_ref=upload_log_block(&store,&mut authority,"p0/manifest/1",&manifest,3,2048).await.unwrap();
        head=authority.publish_index("p0",head.head_version,4,1,manifest_ref).unwrap().unwrap();
        assert_eq!(load_committed_tail(&store,&head,4,4096).await.unwrap(),vec![entry(2)]);
        assert_eq!(head.log_base_seq,0);
        assert_eq!(load_committed_entries_after(&store,&head,0,4,4096).await.unwrap(),vec![entry(1),entry(2)],
            "State recovery must cover C-to-I as well as I-to-D");
        assert!(load_committed_entries_after(&store,&head,0,1,4096).await.is_err(),
            "advancing I cannot bypass the retained C-to-D block budget");
        let mut wrong_anchor=head.clone();wrong_anchor.log_anchor=head.wal_tip.clone();
        assert!(load_committed_entries_after(&store,&wrong_anchor,0,4,4096).await.is_err(),
            "the complete recovery suffix must bind its selected base anchor");
        assert!(load_committed_tail(&store,&head,1,1).await.is_err());
        drop(authority);
        let _=std::fs::remove_file(&path);
        let _=std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _=std::fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[tokio::test]
    async fn missing_or_corrupt_chain_fails_closed() {
        let path=temp_db();
        let mut authority=SqliteAuthority::open(&path,Duration::from_secs(1)).unwrap();
        let head=authority.create_partition("p0",1).unwrap();
        let store=S3CompatibleStore::from_store(Arc::new(InMemory::new()),4096);
        let bad=ObjectLogBlock{partition_id:"other".into(),first_seq:1,last_seq:1,previous:None,entries:vec![entry(1)]};
        let reference=upload_log_block(&store,&mut authority,"p0/log/bad",&bad,1,2048).await.unwrap();
        let published=authority.publish_commit("p0",head.head_version,1,"op-1",[1;32],1,reference,None).unwrap();
        let head=match published {super::super::sqlite_authority::PublishOutcome::Published(h)=>h,other=>panic!("{other:?}")};
        // A nonempty unindexed range whose object claims another partition is never returned.
        assert!(load_committed_tail(&store,&head,4,4096).await.is_err());
        drop(authority);
        let _=std::fs::remove_file(&path);
        let _=std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _=std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
