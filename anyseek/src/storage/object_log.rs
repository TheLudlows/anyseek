//! Bounded immutable object-log blocks and fail-closed committed-tail replay.
//!
//! The SQLite head decides which tip and sequence range is committed. Objects not
//! reachable from that head are never replayed. This module provides the tail
//! primitive; RecordService must apply returned entries inside the same read view
//! as its manifest before advertising end-to-end search consistency.
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
/// This is the storage protocol primitive. The existing RecordPartition API must still
/// translate its canonical mutation envelope and apply the resulting tail in its read
/// view before this can be considered end-to-end RecordService integration.
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
) -> Result<Vec<ObjectLogEntry>, TailError> {
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
) -> Result<Vec<ObjectLogEntry>, TailError> {
    if head.log_base_seq > head.committed_seq || after_seq > head.committed_seq {
        return Err(TailError::Invalid("invalid log watermarks"));
    }
    if after_seq < head.log_base_seq {
        return Err(TailError::Invalid("requested sequence is behind retained log anchor"));
    }
    if after_seq == head.committed_seq {
        return Ok(Vec::new());
    }
    if max_blocks == 0 || max_total_bytes == 0 {
        return Err(TailError::Invalid("empty committed-tail budget"));
    }
    let mut expected_last = head.committed_seq;
    let mut current = head.wal_tip.clone().ok_or(TailError::Invalid("committed tail has no WAL tip"))?;
    let mut blocks_read = 0usize;
    let mut total_bytes = 0usize;
    let mut reverse_blocks: Vec<Vec<ObjectLogEntry>> = Vec::new();

    while expected_last > head.indexed_seq {
        if blocks_read >= max_blocks {
            return Err(TailError::Invalid("committed tail exceeds block-count budget"));
        }
        let bytes = store.get_verified(&current).await.map_err(TailError::ObjectStore)?;
        total_bytes = total_bytes.checked_add(bytes.len()).ok_or(TailError::Invalid("tail byte count overflow"))?;
        if total_bytes > max_total_bytes {
            return Err(TailError::Invalid("committed tail exceeds byte budget"));
        }
        let block: ObjectLogBlock = serde_json::from_slice(&bytes).map_err(TailError::Decode)?;
        validate_block(&block)?;
        if block.partition_id != head.partition_id || block.last_seq != expected_last {
            return Err(TailError::Invalid("partition or log chain sequence mismatch"));
        }
        let filtered: Vec<_> = block.entries.iter()
            .filter(|entry| entry.seq > after_seq)
            .cloned()
            .collect();
        reverse_blocks.push(filtered);
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
    Ok(entries)
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
