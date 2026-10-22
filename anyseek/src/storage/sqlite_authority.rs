//! SQLite metadata authority for the object-store persistence path.
//!
//! This database stores coordination metadata only. Record bodies, vectors, WAL
//! blocks and immutable index segments remain in external immutable objects.
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{path::{Path, PathBuf}, time::Duration};

const SCHEMA_VERSION: i64 = 1;

/// Immutable object identity; payload bytes are never stored in SQLite.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObjectRef {
    pub key: String,
    pub sha256: [u8; 32],
    pub length: u64,
}

/// Authoritative partition watermarks and immutable object references.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PartitionHead {
    pub partition_id: String,
    pub head_version: u64,
    pub owner_epoch: u64,
    pub committed_seq: u64,
    pub wal_tip: Option<ObjectRef>,
    pub manifest: Option<ObjectRef>,
    /// Immutable Record configuration + initial empty checkpoint for object-backed recovery.
    pub bootstrap: Option<ObjectRef>,
    pub indexed_seq: u64,
    pub log_base_seq: u64,
    pub log_anchor: Option<ObjectRef>,
}


/// Logical snapshot of the SQLite authority and the caller-verified object closure.
/// The payload bytes live in object storage; this snapshot contains metadata only.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AuthorityBackupSnapshot {
    pub format_version: u32,
    pub backup_id: String,
    pub created_at_ms: i64,
    pub heads: Vec<PartitionHead>,
    pub receipts: Vec<ReceiptSnapshot>,
    pub objects: Vec<ObjectRegistrySnapshot>,
    pub pins: Vec<PinSnapshot>,
    pub retired: Vec<RetiredSnapshot>,
    pub edges: Vec<ObjectEdgeSnapshot>,
    pub object_closure: Vec<ObjectRef>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReceiptSnapshot {
    pub partition_id: String,
    pub operation_id: String,
    pub request_sha256: [u8;32],
    pub committed_seq: u64,
    pub result_json: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObjectRegistrySnapshot {
    pub reference: ObjectRef,
    pub state: i64,
    pub created_at_ms: i64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PinSnapshot {
    pub pin_id: String,
    pub partition_id: String,
    pub object_key: String,
    pub pin_kind: String,
    pub expires_at_ms: Option<i64>,
    pub backup_id: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObjectEdgeSnapshot {
    pub parent_key: String,
    pub child_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetiredSnapshot {
    pub object_key: String,
    pub retired_generation: u64,
    pub retired_at_ms: i64,
}

/// Publication result. Uncertain I/O errors must be resolved with the same operation ID.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PublishOutcome {
    Published(PartitionHead),
    AlreadyPublished(PartitionHead),
    Conflict,
}

/// One local SQLite authority. Never share its database/WAL files between hosts.
pub(crate) struct SqliteAuthority {
    path: PathBuf,
    connection: Connection,
}

impl SqliteAuthority {
    /// Open a durable SQLite database and enforce WAL plus FULL synchronization.
    pub(crate) fn open(path: &Path, busy_timeout: Duration) -> rusqlite::Result<Self> {
        let connection = Connection::open(path)?;
        connection.busy_timeout(busy_timeout)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        let synchronous: i64 = connection.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
        if synchronous != 2 {
            return Err(rusqlite::Error::InvalidQuery);
        }
        let authority = Self { path: path.to_path_buf(), connection };
        authority.initialize()?;
        Ok(authority)
    }

    fn initialize(&self) -> rusqlite::Result<()> {
        self.connection.execute_batch(
            "BEGIN IMMEDIATE;
             CREATE TABLE IF NOT EXISTS authority_schema (
                 singleton INTEGER PRIMARY KEY CHECK(singleton=1), version INTEGER NOT NULL
             );
             INSERT OR IGNORE INTO authority_schema(singleton,version) VALUES(1,1);
             CREATE TABLE IF NOT EXISTS partition_heads (
                 partition_id TEXT PRIMARY KEY,
                 head_version INTEGER NOT NULL CHECK(head_version>=0),
                 owner_epoch INTEGER NOT NULL CHECK(owner_epoch>=0),
                 committed_seq INTEGER NOT NULL CHECK(committed_seq>=0),
                 wal_tip_json TEXT,
                 manifest_json TEXT,
                 bootstrap_json TEXT,
                 indexed_seq INTEGER NOT NULL CHECK(indexed_seq>=0 AND indexed_seq<=committed_seq),
                 log_base_seq INTEGER NOT NULL CHECK(log_base_seq>=0 AND log_base_seq<=committed_seq),
                 log_anchor_json TEXT
             );
             CREATE TABLE IF NOT EXISTS operation_receipts (
                 partition_id TEXT NOT NULL,
                 operation_id TEXT NOT NULL,
                 request_sha256 BLOB NOT NULL CHECK(length(request_sha256)=32),
                 committed_seq INTEGER NOT NULL CHECK(committed_seq>=0),
                 result_json TEXT NOT NULL,
                 PRIMARY KEY(partition_id,operation_id)
             );
             CREATE TABLE IF NOT EXISTS object_uploads (
                 object_key TEXT PRIMARY KEY,
                 sha256 BLOB NOT NULL CHECK(length(sha256)=32),
                 byte_length INTEGER NOT NULL CHECK(byte_length>=0),
                 state INTEGER NOT NULL CHECK(state IN (0,1,2,3,4,5)),
                 created_at_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS object_edges (
                 parent_key TEXT NOT NULL,
                 child_key TEXT NOT NULL,
                 PRIMARY KEY(parent_key,child_key),
                 CHECK(parent_key <> child_key)
             );
             CREATE INDEX IF NOT EXISTS object_edges_child_idx ON object_edges(child_key);
             CREATE TABLE IF NOT EXISTS object_pins (
                 pin_id TEXT PRIMARY KEY,
                 partition_id TEXT NOT NULL,
                 object_key TEXT NOT NULL,
                 pin_kind TEXT NOT NULL CHECK(pin_kind IN ('reader','indexer','backup','staging')),
                 expires_at_ms INTEGER,
                 backup_id TEXT
             );
             CREATE TABLE IF NOT EXISTS backup_exports (
                 backup_id TEXT PRIMARY KEY, snapshot BLOB NOT NULL
             );
             CREATE TABLE IF NOT EXISTS restore_receipts (
                 singleton INTEGER PRIMARY KEY CHECK(singleton=1), snapshot_sha256 BLOB NOT NULL
             );
             CREATE TABLE IF NOT EXISTS retired_objects (
                 object_key TEXT PRIMARY KEY,
                 retired_generation INTEGER NOT NULL CHECK(retired_generation>=0),
                 retired_at_ms INTEGER NOT NULL
             );
             COMMIT;"
        )?;
        let version: i64 = self.connection.query_row(
            "SELECT version FROM authority_schema WHERE singleton=1", [], |row| row.get(0)
        )?;
        if version != SCHEMA_VERSION {
            return Err(rusqlite::Error::InvalidQuery);
        }
        Ok(())
    }

    pub(crate) fn database_path(&self) -> &Path { &self.path }

    /// Create a partition's initial authority record without overwriting an existing one.
    pub(crate) fn create_partition(&mut self, partition_id: &str, owner_epoch: u64)
        -> rusqlite::Result<PartitionHead>
    {
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "INSERT INTO partition_heads(partition_id,head_version,owner_epoch,committed_seq,indexed_seq,log_base_seq)
             VALUES(?1,0,?2,0,0,0)",
            params![partition_id, to_i64(owner_epoch)?],
        )?;
        let head = read_head(&tx, partition_id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        tx.commit()?;
        Ok(head)
    }

    pub(crate) fn read_head(&self, partition_id: &str) -> rusqlite::Result<Option<PartitionHead>> {
        read_head(&self.connection, partition_id)
    }

    pub(crate) fn single_partition_id(&self)->rusqlite::Result<Option<String>> {
        let mut statement=self.connection.prepare("SELECT partition_id FROM partition_heads ORDER BY partition_id LIMIT 2")?;
        let ids=statement.query_map([],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
        if ids.len()>1 {return Err(rusqlite::Error::InvalidQuery);}
        Ok(ids.into_iter().next())
    }

    /// Publish an immutable initial Record checkpoint/configuration before any
    /// operation sequence exists. This is metadata-root publication, not a data commit.
    pub(crate) fn set_bootstrap_reference(
        &mut self,
        partition_id: &str,
        expected_head_version: u64,
        owner_epoch: u64,
        bootstrap: ObjectRef,
    ) -> rusqlite::Result<Option<PartitionHead>> {
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        ensure_durable_object(&tx, &bootstrap)?;
        let current = read_head(&tx, partition_id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        if current.head_version != expected_head_version || current.owner_epoch != owner_epoch
            || current.committed_seq != 0 || current.bootstrap.is_some() {
            tx.commit()?;
            return Ok(None);
        }
        let json = serde_json::to_string(&bootstrap).map_err(|_| rusqlite::Error::InvalidQuery)?;
        let changed = tx.execute(
            "UPDATE partition_heads SET bootstrap_json=?1,head_version=head_version+1
             WHERE partition_id=?2 AND head_version=?3 AND owner_epoch=?4 AND committed_seq=0",
            params![json,partition_id,to_i64(expected_head_version)?,to_i64(owner_epoch)?],
        )?;
        if changed != 1 {
            tx.commit()?;
            return Ok(None);
        }
        let head = read_head(&tx, partition_id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        tx.commit()?;
        Ok(Some(head))
    }

    /// Return every active object reachable from the current committed heads,
    /// following immutable manifest/WAL dependency edges transitively.
    pub(crate) fn reachable_objects(&self) -> rusqlite::Result<Vec<ObjectRef>> {
        let keys = reachable_keys(&self.connection)?;
        let mut objects = Vec::with_capacity(keys.len());
        for key in keys {
            let row: Option<(Vec<u8>,i64,i64)> = self.connection.query_row(
                "SELECT sha256,byte_length,state FROM object_uploads WHERE object_key=?1",
                [&key], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
            ).optional()?;
            let (digest,length,state) = row.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
            if state != 1 || length < 0 {
                return Err(rusqlite::Error::InvalidQuery);
            }
            let sha256: [u8;32] = digest.try_into().map_err(|_| rusqlite::Error::InvalidQuery)?;
            objects.push(ObjectRef {
                key,
                sha256,
                length: u64::try_from(length).map_err(|_| rusqlite::Error::InvalidQuery)?,
            });
        }
        Ok(objects)
    }

    // object_uploads states: 0 inventory, 1 verified, 2 retired verified,
    // 3 deleted verified, 4 retired unknown PUT, 5 deleted unknown PUT (retry DELETE).
    /// Register an object only after the object-store adapter has completed the durable
    /// upload and independently verified its byte length and SHA-256. A key is immutable:
    /// registering different bytes under an existing key is rejected.
    pub(crate) fn register_durable_object(
        &mut self,
        object: &ObjectRef,
        created_at_ms: i64,
    ) -> rusqlite::Result<()> {
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<(Vec<u8>, i64)> = tx.query_row(
            "SELECT sha256,byte_length FROM object_uploads WHERE object_key=?1",
            [&object.key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional()?;
        match existing {
            Some((digest, length)) => {
                if digest.as_slice() != &object.sha256[..] || length < 0 || length as u64 != object.length {
                    return Err(rusqlite::Error::InvalidParameterName(
                        "immutable object key reused with different bytes".into()
                    ));
                }
                let state: i64 = tx.query_row(
                    "SELECT state FROM object_uploads WHERE object_key=?1",
                    [&object.key],
                    |row| row.get(0),
                )?;
                if !matches!(state,0|1) {
                    return Err(rusqlite::Error::InvalidParameterName(
                        "retired immutable object key cannot be reactivated".into()
                    ));
                }
                tx.execute(
                    "UPDATE object_uploads SET state=1 WHERE object_key=?1",
                    [&object.key],
                )?;
            }
            None => {
                tx.execute(
                    "INSERT INTO object_uploads(object_key,sha256,byte_length,state,created_at_ms)
                     VALUES(?1,?2,?3,1,?4)",
                    params![object.key, object.sha256.as_slice(), to_i64(object.length)?, created_at_ms],
                )?;
            }
        }
        tx.commit()
    }

    /// Publish only references to objects already uploaded and verified as durable.
    /// This transaction is the commit point; object upload alone never commits data.
    /// If a manifest is supplied, the caller asserts it completely indexes through
    /// this commit's sequence; otherwise the committed tail remains unindexed.
    pub(crate) fn publish_commit(
        &mut self,
        partition_id: &str,
        expected_head_version: u64,
        owner_epoch: u64,
        operation_id: &str,
        request_sha256: [u8; 32],
        committed_seq: u64,
        wal_tip: ObjectRef,
        manifest: Option<ObjectRef>,
    ) -> rusqlite::Result<PublishOutcome> {
        if operation_id.is_empty() || partition_id.is_empty() {
            return Err(rusqlite::Error::InvalidParameterName("empty identity".into()));
        }
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some((digest, result_json)) = read_receipt(&tx, partition_id, operation_id)? {
            if digest != request_sha256 {
                return Err(rusqlite::Error::InvalidParameterName(
                    "operation ID reused with a different request digest".into()
                ));
            }
            let original_head: PartitionHead = serde_json::from_str(&result_json)
                .map_err(|_| rusqlite::Error::InvalidQuery)?;
            tx.commit()?;
            return Ok(PublishOutcome::AlreadyPublished(original_head));
        }
        ensure_durable_object(&tx, &wal_tip)?;
        if let Some(reference) = manifest.as_ref() {
            ensure_durable_object(&tx, reference)?;
        }
        let current = read_head(&tx, partition_id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        if current.head_version != expected_head_version || current.owner_epoch != owner_epoch
            || committed_seq != current.committed_seq.checked_add(1).ok_or(rusqlite::Error::InvalidQuery)?
        {
            tx.commit()?;
            return Ok(PublishOutcome::Conflict);
        }
        let next_version = current.head_version.checked_add(1).ok_or(rusqlite::Error::InvalidQuery)?;
        let wal_json = serde_json::to_string(&wal_tip).map_err(|_| rusqlite::Error::InvalidQuery)?;
        let manifest_json = manifest.as_ref().map(serde_json::to_string).transpose()
            .map_err(|_| rusqlite::Error::InvalidQuery)?;
        let changed = tx.execute(
            "UPDATE partition_heads SET head_version=?1,committed_seq=?2,wal_tip_json=?3,
             manifest_json=COALESCE(?4,manifest_json),
             indexed_seq=CASE WHEN ?4 IS NOT NULL THEN ?2 ELSE indexed_seq END
             WHERE partition_id=?5 AND head_version=?6 AND owner_epoch=?7 AND committed_seq=?8",
            params![to_i64(next_version)?,to_i64(committed_seq)?,wal_json,manifest_json,
                partition_id,to_i64(expected_head_version)?,to_i64(owner_epoch)?,to_i64(current.committed_seq)?],
        )?;
        if changed != 1 {
            tx.commit()?;
            return Ok(PublishOutcome::Conflict);
        }
        if let Some(previous) = current.wal_tip.as_ref() {
            tx.execute(
                "INSERT OR IGNORE INTO object_edges(parent_key,child_key) VALUES(?1,?2)",
                params![wal_tip.key, previous.key],
            )?;
        }
        let head = read_head(&tx, partition_id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        let result_json = serde_json::to_string(&head).map_err(|_| rusqlite::Error::InvalidQuery)?;
        tx.execute(
            "INSERT INTO operation_receipts(partition_id,operation_id,request_sha256,committed_seq,result_json)
             VALUES(?1,?2,?3,?4,?5)",
            params![partition_id,operation_id,request_sha256.as_slice(),to_i64(committed_seq)?,result_json],
        )?;
        tx.commit()?;
        Ok(PublishOutcome::Published(head))
    }

    /// Atomically publish a complete index manifest, never advancing beyond committed data.
    pub(crate) fn publish_index(
        &mut self, partition_id: &str, expected_head_version: u64,
        owner_epoch: u64, indexed_seq: u64, manifest: ObjectRef,
    ) -> rusqlite::Result<Option<PartitionHead>> {
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = read_head(&tx, partition_id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        if current.head_version != expected_head_version || current.owner_epoch != owner_epoch
            || indexed_seq < current.indexed_seq || indexed_seq > current.committed_seq {
            tx.commit()?;
            return Ok(None);
        }
        ensure_durable_object(&tx, &manifest)?;
        let manifest_json = serde_json::to_string(&manifest).map_err(|_| rusqlite::Error::InvalidQuery)?;
        let changed = tx.execute(
            "UPDATE partition_heads SET head_version=head_version+1,manifest_json=?1,indexed_seq=?2
             WHERE partition_id=?3 AND head_version=?4 AND owner_epoch=?5 AND committed_seq=?6",
            params![manifest_json,to_i64(indexed_seq)?,partition_id,to_i64(expected_head_version)?,
                to_i64(owner_epoch)?,to_i64(current.committed_seq)?],
        )?;
        if changed != 1 {
            tx.commit()?;
            return Ok(None);
        }
        let head = read_head(&tx, partition_id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        tx.commit()?;
        Ok(Some(head))
    }

    /// A complete checkpoint captures every logical component (including feed,
    /// receipts, tombstones and staging) at the current commit. Detaching the old
    /// WAL tip only changes live roots; immutable predecessor edges stay intact
    /// for existing reader/build/backup pins. No sequence is consumed.
    pub(crate) fn publish_checkpoint(
        &mut self, partition_id: &str, expected_head_version: u64,
        owner_epoch: u64, seq: u64, manifest: ObjectRef,
    ) -> rusqlite::Result<Option<PartitionHead>> {
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = read_head(&tx, partition_id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        if current.head_version != expected_head_version || current.owner_epoch != owner_epoch
            || current.committed_seq != seq {
            return Ok(None);
        }
        ensure_durable_object(&tx, &manifest)?;
        let json = serde_json::to_string(&manifest).map_err(|_| rusqlite::Error::InvalidQuery)?;
        tx.execute(
            "UPDATE partition_heads SET head_version=head_version+1,manifest_json=?1,
             indexed_seq=?2,log_base_seq=?2,wal_tip_json=NULL,log_anchor_json=NULL
             WHERE partition_id=?3",
            params![json,to_i64(seq)?,partition_id],
        )?;
        let head = read_head(&tx, partition_id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
        tx.commit()?;
        Ok(Some(head))
    }

    pub(crate) fn bind_restore(&mut self,digest:[u8;32])->rusqlite::Result<()> {
        let tx=self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let old:Option<Vec<u8>>=tx.query_row("SELECT snapshot_sha256 FROM restore_receipts WHERE singleton=1",[],|r|r.get(0)).optional()?;
        match old {
            Some(old) if old.as_slice()!=digest=>return Err(rusqlite::Error::InvalidQuery),
            None=>{tx.execute("INSERT INTO restore_receipts VALUES(1,?1)",[digest.as_slice()])?;},
            _=>{},
        }
        tx.commit()
    }

    /// Seed a NEW isolated restore namespace; never rewind an existing live head.
    pub(crate) fn publish_restored_checkpoint(
        &mut self,partition_id:&str,expected_head_version:u64,owner_epoch:u64,seq:u64,manifest:ObjectRef,
    )->rusqlite::Result<Option<PartitionHead>> {
        let tx=self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let h=read_head(&tx,partition_id)?.ok_or(rusqlite::Error::InvalidQuery)?;
        let bound:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM restore_receipts)",[],|r|r.get(0))?;
        if !bound || h.head_version!=expected_head_version || h.owner_epoch!=owner_epoch
            || h.committed_seq!=0 || h.manifest.is_some() || h.bootstrap.is_none() {return Ok(None);}
        ensure_durable_object(&tx,&manifest)?;
        let json=serde_json::to_string(&manifest).map_err(|_|rusqlite::Error::InvalidQuery)?;
        tx.execute("UPDATE partition_heads SET head_version=head_version+1,committed_seq=?1,indexed_seq=?1,
            log_base_seq=?1,manifest_json=?2,wal_tip_json=NULL,log_anchor_json=NULL WHERE partition_id=?3",
            params![to_i64(seq)?,json,partition_id])?;
        let head=read_head(&tx,partition_id)?.ok_or(rusqlite::Error::InvalidQuery)?;
        tx.commit()?;Ok(Some(head))
    }

    /// Inventory before PUT: even a response-lost or cancelled upload has a
    /// durable identity for later collection. Registration never publishes it.
    pub(crate) fn reserve_upload(&mut self,object:&ObjectRef,at:i64)->rusqlite::Result<()> {
        self.reserve_backup_upload(object,at,None)
    }
    /// Pin inventory before PUT, including backup-control objects which do not
    /// exist in the snapshot they describe. Reservation and pin are atomic.
    pub(crate) fn reserve_backup_upload(&mut self, object:&ObjectRef,at:i64,backup_id:Option<&str>)->rusqlite::Result<()> {
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let row: Option<(Vec<u8>,i64,i64)> = tx.query_row(
            "SELECT sha256,byte_length,state FROM object_uploads WHERE object_key=?1",
            [&object.key], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?)),
        ).optional()?;
        if let Some((digest,length,state)) = row {
            if digest.as_slice()!=object.sha256 || length!=to_i64(object.length)? || !matches!(state,0|1) {
                return Err(rusqlite::Error::InvalidQuery);
            }
        } else {
            tx.execute("INSERT INTO object_uploads VALUES(?1,?2,?3,0,?4)",
                params![object.key,object.sha256.as_slice(),to_i64(object.length)?,at])?;
        }
        if let Some(id)=backup_id {
            tx.execute("INSERT OR IGNORE INTO object_pins(pin_id,partition_id,object_key,pin_kind,expires_at_ms,backup_id)
                VALUES(?1,'*',?2,'backup',NULL,?3)",params![backup_pin_id(id,&object.key),object.key,id])?;
        }
        tx.commit()
    }

    pub(crate) fn unreferenced_objects(&self) -> rusqlite::Result<Vec<ObjectRef>> {
        let live = reachable_keys(&self.connection)?;
        let mut statement = self.connection.prepare(
            "SELECT object_key,sha256,byte_length FROM object_uploads WHERE state IN (0,1) ORDER BY object_key"
        )?;
        let objects = statement.query_map([], registry_reference)?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(objects.into_iter().filter(|o|!live.contains(&o.key)).collect())
    }

    /// Resume the original backup without first traversing currently pinned
    /// inventory: a crashed PUT may have left its control object unverified.
    pub(crate) fn saved_backup_closure(&self,id:&str)->rusqlite::Result<Option<Vec<ObjectRef>>> {
        let bytes:Option<Vec<u8>>=self.connection.query_row(
            "SELECT snapshot FROM backup_exports WHERE backup_id=?1",[id],|r|r.get(0)).optional()?;
        bytes.map(|bytes|serde_json::from_slice::<AuthorityBackupSnapshot>(&bytes)
            .map(|s|s.object_closure).map_err(|_|rusqlite::Error::InvalidQuery)).transpose()
    }

    /// Create a point-in-time logical metadata snapshot and pin its complete object
    /// closure in the same SQLite transaction. The caller must enumerate and validate
    /// all objects reachable through the active manifest and WAL chain before calling.
    /// Snapshot bytes contain metadata only, never record/vector/index payload bytes.
    pub(crate) fn prepare_backup_snapshot(
        &mut self,
        backup_id: &str,
        object_closure: &[ObjectRef],
        created_at_ms: i64,
        max_snapshot_bytes: usize,
    ) -> rusqlite::Result<Vec<u8>> {
        if backup_id.is_empty() || object_closure.is_empty() {
            return Err(rusqlite::Error::InvalidParameterName(
                "backup ID and complete object closure are required".into()
            ));
        }
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let saved: Option<Vec<u8>> = tx.query_row(
            "SELECT snapshot FROM backup_exports WHERE backup_id=?1", [backup_id], |row|row.get(0)
        ).optional()?;
        if let Some(bytes) = saved {
            if bytes.len()>max_snapshot_bytes { return Err(rusqlite::Error::InvalidQuery); }
            return Ok(bytes);
        }
        let prior_backup: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM object_pins WHERE backup_id=?1)",
            [backup_id], |row| row.get(0),
        )?;
        if prior_backup {
            return Err(rusqlite::Error::InvalidParameterName(
                "backup ID already has pins; use a new ID or explicitly release the failed backup".into()
            ));
        }
        let mut closure = object_closure.to_vec();
        closure.sort_by(|a,b| a.key.cmp(&b.key));
        if closure.windows(2).any(|pair| pair[0].key == pair[1].key) {
            return Err(rusqlite::Error::InvalidParameterName("duplicate object key in backup closure".into()));
        }
        for object in &closure {
            ensure_durable_object(&tx, object)?;
        }
        let reachable = reachable_keys(&tx)?;
        if reachable.iter().any(|key| !closure.iter().any(|object| &object.key == key)) {
            return Err(rusqlite::Error::InvalidParameterName(
                "backup closure omits a transitively reachable committed object".into()
            ));
        }
        let head_ids: Vec<String> = {
            let mut statement = tx.prepare("SELECT partition_id FROM partition_heads ORDER BY partition_id")?;
            let rows = statement.query_map([], |row| row.get(0))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let mut heads = Vec::with_capacity(head_ids.len());
        for id in head_ids {
            let head = read_head(&tx, &id)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
            for required in [&head.wal_tip, &head.manifest, &head.bootstrap, &head.log_anchor].into_iter().flatten() {
                if !closure.iter().any(|candidate| candidate == required) {
                    return Err(rusqlite::Error::InvalidParameterName(
                        "backup closure omits an object referenced by the current partition head".into()
                    ));
                }
            }
            heads.push(head);
        }
        for object in &closure {
            let pin_id = backup_pin_id(backup_id, &object.key);
            tx.execute(
                "INSERT INTO object_pins(pin_id,partition_id,object_key,pin_kind,expires_at_ms,backup_id)
                 VALUES(?1,'*',?2,'backup',NULL,?3)",
                params![pin_id,object.key,backup_id],
            )?;
        }

        let receipts = {
            let mut statement = tx.prepare(
                "SELECT partition_id,operation_id,request_sha256,committed_seq,result_json
                 FROM operation_receipts ORDER BY partition_id,operation_id"
            )?;
            let rows = statement.query_map([], |row| {
                let digest: Vec<u8> = row.get(2)?;
                let request_sha256: [u8;32] = digest.try_into().map_err(|_| rusqlite::Error::InvalidQuery)?;
                let committed_seq: i64 = row.get(3)?;
                Ok(ReceiptSnapshot {
                    partition_id: row.get(0)?,
                    operation_id: row.get(1)?,
                    request_sha256,
                    committed_seq: u64::try_from(committed_seq).map_err(|_| rusqlite::Error::InvalidQuery)?,
                    result_json: row.get(4)?,
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let objects = {
            let mut statement = tx.prepare(
                "SELECT object_key,sha256,byte_length,state,created_at_ms FROM object_uploads ORDER BY object_key"
            )?;
            let rows = statement.query_map([], |row| {
                let key: String = row.get(0)?;
                let digest: Vec<u8> = row.get(1)?;
                let sha256: [u8;32] = digest.try_into().map_err(|_| rusqlite::Error::InvalidQuery)?;
                let length: i64 = row.get(2)?;
                let state: i64 = row.get(3)?;
                Ok(ObjectRegistrySnapshot {
                    reference: ObjectRef {
                        key, sha256,
                        length: u64::try_from(length).map_err(|_| rusqlite::Error::InvalidQuery)?,
                    },
                    state,
                    created_at_ms: row.get(4)?,
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let pins = {
            let mut statement = tx.prepare(
                "SELECT pin_id,partition_id,object_key,pin_kind,expires_at_ms,backup_id
                 FROM object_pins ORDER BY pin_id"
            )?;
            let rows = statement.query_map([], |row| Ok(PinSnapshot {
                pin_id: row.get(0)?, partition_id: row.get(1)?, object_key: row.get(2)?,
                pin_kind: row.get(3)?, expires_at_ms: row.get(4)?, backup_id: row.get(5)?,
            }))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let edges = {
            let mut statement = tx.prepare(
                "SELECT parent_key,child_key FROM object_edges ORDER BY parent_key,child_key"
            )?;
            let rows = statement.query_map([], |row| Ok(ObjectEdgeSnapshot {
                parent_key: row.get(0)?,
                child_key: row.get(1)?,
            }))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let retired = {
            let mut statement = tx.prepare(
                "SELECT object_key,retired_generation,retired_at_ms FROM retired_objects ORDER BY object_key"
            )?;
            let rows = statement.query_map([], |row| {
                let generation: i64 = row.get(1)?;
                Ok(RetiredSnapshot {
                    object_key: row.get(0)?,
                    retired_generation: u64::try_from(generation).map_err(|_| rusqlite::Error::InvalidQuery)?,
                    retired_at_ms: row.get(2)?,
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let snapshot = AuthorityBackupSnapshot {
            format_version: 1,
            backup_id: backup_id.to_owned(),
            created_at_ms,
            heads,
            receipts,
            objects,
            pins,
            retired,
            edges,
            object_closure: closure,
        };
        let bytes = serde_json::to_vec(&snapshot).map_err(|_| rusqlite::Error::InvalidQuery)?;
        if bytes.len() > max_snapshot_bytes {
            return Err(rusqlite::Error::InvalidParameterName("backup metadata exceeds configured limit".into()));
        }
        tx.execute("INSERT INTO backup_exports(backup_id,snapshot) VALUES(?1,?2)",
            params![backup_id,bytes])?;
        tx.commit()?;
        Ok(bytes)
    }

    /// Restore a logical authority snapshot into a fresh, empty SQLite database.
    /// The caller must verify every object in the snapshot's closure before serving reads.
    pub(crate) fn restore_backup_snapshot(
        &mut self,
        bytes: &[u8],
        max_snapshot_bytes: usize,
    ) -> rusqlite::Result<AuthorityBackupSnapshot> {
        if bytes.len() > max_snapshot_bytes {
            return Err(rusqlite::Error::InvalidParameterName("backup metadata exceeds configured limit".into()));
        }
        let snapshot: AuthorityBackupSnapshot =
            serde_json::from_slice(bytes).map_err(|_| rusqlite::Error::InvalidQuery)?;
        if snapshot.format_version != 1 || snapshot.backup_id.is_empty() {
            return Err(rusqlite::Error::InvalidQuery);
        }
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let nonempty: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM partition_heads)
                 OR EXISTS(SELECT 1 FROM operation_receipts)
                 OR EXISTS(SELECT 1 FROM object_uploads)",
            [], |row| row.get(0),
        )?;
        let digest: [u8;32] = Sha256::digest(bytes).into();
        if nonempty {
            let prior: Option<Vec<u8>> = tx.query_row(
                "SELECT snapshot_sha256 FROM restore_receipts WHERE singleton=1", [], |r|r.get(0)
            ).optional()?;
            if prior.as_deref()==Some(digest.as_slice()) {
                // Retry completes post-import root registration, never rewinds a head.
                return Ok(snapshot);
            }
            return Err(rusqlite::Error::InvalidParameterName(
                "restore target must be empty or a retry of the identical snapshot".into()
            ));
        }
        let mut closure = snapshot.object_closure.clone();
        closure.sort_by(|a,b| a.key.cmp(&b.key));
        if closure.windows(2).any(|pair| pair[0].key == pair[1].key) {
            return Err(rusqlite::Error::InvalidQuery);
        }
        for object in &snapshot.objects {
            if object.reference.key.is_empty() || !matches!(object.state, 0 | 1 | 2 | 3 | 4 | 5) {
                return Err(rusqlite::Error::InvalidQuery);
            }
            tx.execute(
                "INSERT INTO object_uploads(object_key,sha256,byte_length,state,created_at_ms)
                 VALUES(?1,?2,?3,?4,?5)",
                params![object.reference.key,object.reference.sha256.as_slice(),
                    to_i64(object.reference.length)?,object.state,object.created_at_ms],
            )?;
        }
        for retired in &snapshot.retired {
            tx.execute(
                "INSERT INTO retired_objects(object_key,retired_generation,retired_at_ms) VALUES(?1,?2,?3)",
                params![retired.object_key,to_i64(retired.retired_generation)?,retired.retired_at_ms],
            )?;
        }
        for edge in &snapshot.edges {
            if edge.parent_key.is_empty() || edge.child_key.is_empty() || edge.parent_key == edge.child_key {
                return Err(rusqlite::Error::InvalidQuery);
            }
            tx.execute(
                "INSERT INTO object_edges(parent_key,child_key) VALUES(?1,?2)",
                params![edge.parent_key,edge.child_key],
            )?;
        }
        for pin in &snapshot.pins {
            if !matches!(pin.pin_kind.as_str(),"reader"|"indexer"|"backup"|"staging") {
                return Err(rusqlite::Error::InvalidQuery);
            }
            tx.execute(
                "INSERT INTO object_pins(pin_id,partition_id,object_key,pin_kind,expires_at_ms,backup_id)
                 VALUES(?1,?2,?3,?4,?5,?6)",
                params![pin.pin_id,pin.partition_id,pin.object_key,pin.pin_kind,pin.expires_at_ms,pin.backup_id],
            )?;
        }
        for head in &snapshot.heads {
            tx.execute(
                "INSERT INTO partition_heads(partition_id,head_version,owner_epoch,committed_seq,wal_tip_json,
                 manifest_json,bootstrap_json,indexed_seq,log_base_seq,log_anchor_json)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
                params![head.partition_id,to_i64(head.head_version)?,to_i64(head.owner_epoch)?,
                    to_i64(head.committed_seq)?,
                    head.wal_tip.as_ref().map(serde_json::to_string).transpose().map_err(|_| rusqlite::Error::InvalidQuery)?,
                    head.manifest.as_ref().map(serde_json::to_string).transpose().map_err(|_| rusqlite::Error::InvalidQuery)?,
                    head.bootstrap.as_ref().map(serde_json::to_string).transpose().map_err(|_| rusqlite::Error::InvalidQuery)?,
                    to_i64(head.indexed_seq)?,to_i64(head.log_base_seq)?,
                    head.log_anchor.as_ref().map(serde_json::to_string).transpose().map_err(|_| rusqlite::Error::InvalidQuery)?],
            )?;
        }
        for receipt in &snapshot.receipts {
            let _: PartitionHead = serde_json::from_str(&receipt.result_json)
                .map_err(|_| rusqlite::Error::InvalidQuery)?;
            tx.execute(
                "INSERT INTO operation_receipts(partition_id,operation_id,request_sha256,committed_seq,result_json)
                 VALUES(?1,?2,?3,?4,?5)",
                params![receipt.partition_id,receipt.operation_id,receipt.request_sha256.as_slice(),
                    to_i64(receipt.committed_seq)?,receipt.result_json],
            )?;
        }
        for head in &snapshot.heads {
            for reference in [&head.wal_tip,&head.manifest,&head.bootstrap,&head.log_anchor].into_iter().flatten() {
                ensure_durable_object(&tx, reference)?;
            }
        }
        for reference in &closure {
            ensure_durable_object(&tx, reference)?;
        }
        let required = reachable_keys(&tx)?;
        if required.iter().any(|key|!closure.iter().any(|r|&r.key==key)) {
            return Err(rusqlite::Error::InvalidQuery);
        }
        tx.execute("INSERT INTO restore_receipts VALUES(1,?1)",[digest.as_slice()])?;
        tx.commit()?;
        Ok(snapshot)
    }

    /// Release pins after a failed backup attempt, or after an explicit backup-retention
    /// deletion has itself been completed. Never call this while the backup is retained.
    pub(crate) fn release_backup_pins(&mut self, backup_id: &str) -> rusqlite::Result<usize> {
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute(
            "DELETE FROM object_pins WHERE backup_id=?1 AND pin_kind='backup'",
            [backup_id],
        )?;
        tx.execute("DELETE FROM backup_exports WHERE backup_id=?1",[backup_id])?;
        tx.commit()?;
        Ok(changed)
    }

    /// Register an immutable dependency from a manifest/segment object to its child.
    /// Both endpoints must already be verified and active in the object registry.
    pub(crate) fn register_object_edge(
        &mut self,
        parent: &ObjectRef,
        child: &ObjectRef,
    ) -> rusqlite::Result<()> {
        if parent.key == child.key {
            return Err(rusqlite::Error::InvalidParameterName("self-referential object edge".into()));
        }
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        ensure_durable_object(&tx, parent)?;
        ensure_durable_object(&tx, child)?;
        tx.execute(
            "INSERT OR IGNORE INTO object_edges(parent_key,child_key) VALUES(?1,?2)",
            params![parent.key,child.key],
        )?;
        tx.commit()
    }

    /// Pin a verified object before opening it or starting a background build.
    /// Expiry is advisory only: GC treats a pin as protective until an explicit unpin
    /// after the consumer has been fenced/cancelled. A clock deadline alone is not proof.
    pub(crate) fn pin_object(
        &mut self,
        pin_id: &str,
        partition_id: &str,
        object_key: &str,
        pin_kind: &str,
        expires_at_ms: Option<i64>,
    ) -> rusqlite::Result<()> {
        if pin_id.is_empty() || partition_id.is_empty()
            || !matches!(pin_kind, "reader" | "indexer" | "backup" | "staging") {
            return Err(rusqlite::Error::InvalidParameterName("invalid object pin".into()));
        }
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state: Option<i64> = tx.query_row(
            "SELECT state FROM object_uploads WHERE object_key=?1",
            [object_key], |row| row.get(0),
        ).optional()?;
        if state != Some(1) {
            return Err(rusqlite::Error::InvalidParameterName(
                "cannot pin an object that is not active and verified".into()
            ));
        }
        tx.execute(
            "INSERT INTO object_pins(pin_id,partition_id,object_key,pin_kind,expires_at_ms)
             VALUES(?1,?2,?3,?4,?5)",
            params![pin_id,partition_id,object_key,pin_kind,expires_at_ms],
        )?;
        tx.commit()
    }

    /// Pin a backup root under its stable backup ID so retention and GC can
    /// release all objects belonging to that backup as one explicit lifecycle action.
    pub(crate) fn pin_backup_object(
        &mut self,
        backup_id: &str,
        object_key: &str,
    ) -> rusqlite::Result<()> {
        if backup_id.is_empty() {
            return Err(rusqlite::Error::InvalidParameterName("empty backup ID".into()));
        }
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state: Option<i64> = tx.query_row(
            "SELECT state FROM object_uploads WHERE object_key=?1",
            [object_key], |row| row.get(0),
        ).optional()?;
        if state != Some(1) {
            return Err(rusqlite::Error::InvalidParameterName(
                "backup root must be registered and active".into()
            ));
        }
        tx.execute(
            "INSERT OR IGNORE INTO object_pins(pin_id,partition_id,object_key,pin_kind,expires_at_ms,backup_id)
             VALUES(?1,'*',?2,'backup',NULL,?3)",
            params![backup_pin_id(backup_id, object_key), object_key, backup_id],
        )?;
        tx.commit()
    }

    /// Release a reader/build/backup/staging pin after its consumer is finished.
    pub(crate) fn unpin_object(&mut self, pin_id: &str) -> rusqlite::Result<bool> {
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = tx.execute("DELETE FROM object_pins WHERE pin_id=?1", [pin_id])?;
        tx.commit()?;
        Ok(changed == 1)
    }

    /// Retire an object only if no current head references it and no live pin protects it.
    /// Retirement is monotonic: a retired key can never be registered or pinned again.
    pub(crate) fn retire_object(
        &mut self,
        object_key: &str,
        retired_generation: u64,
        now_ms: i64,
    ) -> rusqlite::Result<bool> {
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let state: Option<i64> = tx.query_row(
            "SELECT state FROM object_uploads WHERE object_key=?1",
            [object_key], |row| row.get(0),
        ).optional()?;
        if !matches!(state,Some(0|1)) {
            tx.commit()?;
            return Ok(false);
        }
        let referenced = reachable_keys(&tx)?.contains(object_key);
        // Pins are roots of the same transitive closure.
        if referenced {
            tx.commit()?;
            return Ok(false);
        }
        tx.execute("UPDATE object_uploads SET state=CASE WHEN state=0 THEN 4 ELSE 2 END WHERE object_key=?1", [object_key])?;
        tx.execute(
            "INSERT INTO retired_objects(object_key,retired_generation,retired_at_ms)
             VALUES(?1,?2,?3)",
            params![object_key,to_i64(retired_generation)?,now_ms],
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// Record successful (or already-absent) remote deletion after a fresh authority
    /// reachability check. State 3 is a durable tombstone and is never reactivated.
    pub(crate) fn mark_object_deleted(&mut self, reference: &ObjectRef) -> rusqlite::Result<bool> {
        let tx = self.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let matches: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM object_uploads WHERE object_key=?1 AND sha256=?2 AND byte_length=?3 AND state IN (2,4,5))",
            params![reference.key,reference.sha256.as_slice(),to_i64(reference.length)?], |r|r.get(0),
        )?;
        let collectible = matches && !reachable_keys(&tx)?.contains(&reference.key);
        if !collectible {
            tx.commit()?;
            return Ok(false);
        }
        let changed = tx.execute(
            "UPDATE object_uploads SET state=CASE WHEN state=2 THEN 3 ELSE 5 END WHERE object_key=?1 AND state IN (2,4,5)",
            [&reference.key],
        )?;
        tx.commit()?;
        Ok(changed == 1)
    }

    /// Return only monotonically retired objects that are still unreferenced and unpinned.
    /// The returned list is safe for idempotent remote deletion: state=retired prevents
    /// any future publication or pin from making the object live again.
    pub(crate) fn collectible_objects(&self, _now_ms: i64) -> rusqlite::Result<Vec<ObjectRef>> {
        let live = reachable_keys(&self.connection)?;
        let mut statement = self.connection.prepare(
            "SELECT object_key,sha256,byte_length FROM object_uploads WHERE state IN (2,4,5) ORDER BY object_key"
        )?;
        let objects = statement.query_map([],registry_reference)?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(objects.into_iter().filter(|o|!live.contains(&o.key)).collect())
    }

    /// Resolve a response-lost retry using the original operation ID and request digest.
    pub(crate) fn resolve_operation(
        &self, partition_id: &str, operation_id: &str, request_sha256: [u8; 32],
    ) -> rusqlite::Result<Option<PartitionHead>> {
        match read_receipt(&self.connection, partition_id, operation_id)? {
            Some((digest, _)) if digest != request_sha256 =>
                Err(rusqlite::Error::InvalidParameterName(
                    "operation ID reused with a different request digest".into()
                )),
            Some((_, result_json)) => serde_json::from_str(&result_json)
                .map(Some).map_err(|_| rusqlite::Error::InvalidQuery),
            None => Ok(None),
        }
    }
}

fn registry_reference(row: &rusqlite::Row<'_>) -> rusqlite::Result<ObjectRef> {
    let digest: Vec<u8> = row.get(1)?;
    let length: i64 = row.get(2)?;
    Ok(ObjectRef {key:row.get(0)?,sha256:digest.try_into().map_err(|_|rusqlite::Error::InvalidQuery)?,
        length:u64::try_from(length).map_err(|_|rusqlite::Error::InvalidQuery)?})
}

/// Walk immutable dependency metadata. All callers making lifecycle decisions
/// execute this under their publication transaction; pins are roots, not clocks.
fn reachable_keys(connection: &Connection) -> rusqlite::Result<std::collections::BTreeSet<String>> {
    let mut result = std::collections::BTreeSet::new();
    let mut heads = connection.prepare("SELECT partition_id FROM partition_heads")?;
    let ids = heads.query_map([],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
    for id in ids {
        let h=read_head(connection,&id)?.ok_or(rusqlite::Error::InvalidQuery)?;
        for r in [&h.wal_tip,&h.manifest,&h.bootstrap,&h.log_anchor].into_iter().flatten() {
            result.insert(r.key.clone());
        }
    }
    let mut pins=connection.prepare("SELECT object_key FROM object_pins")?;
    for key in pins.query_map([],|r|r.get::<_,String>(0))? {result.insert(key?);}
    let mut queue=result.iter().cloned().collect::<Vec<_>>();
    let mut children=connection.prepare("SELECT child_key FROM object_edges WHERE parent_key=?1")?;
    while let Some(key)=queue.pop() {
        for child in children.query_map([key],|r|r.get::<_,String>(0))? {
            let child=child?;if result.insert(child.clone()) {queue.push(child);}
        }
    }
    Ok(result)
}

fn backup_pin_id(backup_id: &str, object_key: &str) -> String {
    let digest = Sha256::digest([backup_id.as_bytes(), b"\\0", object_key.as_bytes()].concat());
    let suffix: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("backup:{suffix}")
}

fn to_i64(value: u64) -> rusqlite::Result<i64> {
    i64::try_from(value).map_err(|_| rusqlite::Error::InvalidQuery)
}
fn decode_object(value: Option<String>) -> rusqlite::Result<Option<ObjectRef>> {
    value.map(|s| serde_json::from_str(&s).map_err(|_| rusqlite::Error::InvalidQuery)).transpose()
}
fn ensure_durable_object(tx: &rusqlite::Transaction<'_>, object: &ObjectRef) -> rusqlite::Result<()> {
    let row: Option<(Vec<u8>, i64, i64)> = tx.query_row(
        "SELECT sha256,byte_length,state FROM object_uploads WHERE object_key=?1",
        [&object.key],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).optional()?;
    match row {
        Some((digest, length, 1))
            if digest.as_slice() == &object.sha256[..] && length >= 0 && length as u64 == object.length => Ok(()),
        _ => Err(rusqlite::Error::InvalidParameterName(
            "object reference is not registered as verified and durable".into()
        )),
    }
}
fn read_head(connection: &Connection, partition_id: &str) -> rusqlite::Result<Option<PartitionHead>> {
    connection.query_row(
        "SELECT partition_id,head_version,owner_epoch,committed_seq,wal_tip_json,manifest_json,bootstrap_json,indexed_seq,log_base_seq,log_anchor_json
         FROM partition_heads WHERE partition_id=?1",
        [partition_id],
        |row| Ok((
            row.get::<_,String>(0)?, row.get::<_,i64>(1)?, row.get::<_,i64>(2)?,
            row.get::<_,i64>(3)?, row.get::<_,Option<String>>(4)?, row.get::<_,Option<String>>(5)?,
            row.get::<_,Option<String>>(6)?, row.get::<_,i64>(7)?, row.get::<_,i64>(8)?, row.get::<_,Option<String>>(9)?,
        )),
    ).optional()?.map(|(partition_id,hv,epoch,committed,wal,manifest,bootstrap,indexed,base,anchor)| {
        Ok(PartitionHead {
            partition_id,
            head_version: u64::try_from(hv).map_err(|_| rusqlite::Error::InvalidQuery)?,
            owner_epoch: u64::try_from(epoch).map_err(|_| rusqlite::Error::InvalidQuery)?,
            committed_seq: u64::try_from(committed).map_err(|_| rusqlite::Error::InvalidQuery)?,
            wal_tip: decode_object(wal)?,
            manifest: decode_object(manifest)?,
            bootstrap: decode_object(bootstrap)?,
            indexed_seq: u64::try_from(indexed).map_err(|_| rusqlite::Error::InvalidQuery)?,
            log_base_seq: u64::try_from(base).map_err(|_| rusqlite::Error::InvalidQuery)?,
            log_anchor: decode_object(anchor)?,
        })
    }).transpose()
}
fn read_receipt(
    connection: &Connection, partition_id: &str, operation_id: &str,
) -> rusqlite::Result<Option<([u8;32], String)>> {
    connection.query_row(
        "SELECT request_sha256,result_json FROM operation_receipts WHERE partition_id=?1 AND operation_id=?2",
        params![partition_id,operation_id],
        |row| {
            let bytes: Vec<u8> = row.get(0)?;
            let digest: [u8;32] = bytes.try_into().map_err(|_| rusqlite::Error::InvalidQuery)?;
            Ok((digest,row.get(1)?))
        },
    ).optional()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, time::SystemTime};
    fn temp_db() -> PathBuf {
        let stamp = SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        std::env::temp_dir().join(format!("anyseek-authority-{stamp}-{}.sqlite",std::process::id()))
    }
    fn object(key: &str, byte: u8) -> ObjectRef {
        ObjectRef { key:key.to_owned(),sha256:[byte;32],length:128 }
    }
    #[test]
    fn complete_manifest_commit_advances_indexed_watermark_atomically() {
        let path=temp_db();
        let mut db=SqliteAuthority::open(&path,Duration::from_secs(1)).unwrap();
        let head=db.create_partition("p0",2).unwrap();
        let wal=object("wal/1",1);
        let manifest=object("manifest/1",2);
        db.register_durable_object(&wal,1).unwrap();
        db.register_durable_object(&manifest,2).unwrap();
        let published=db.publish_commit("p0",head.head_version,2,"op-1",[3;32],1,wal,Some(manifest)).unwrap();
        let head=match published {PublishOutcome::Published(head)=>head,other=>panic!("{other:?}")};
        assert_eq!(head.committed_seq,1);
        assert_eq!(head.indexed_seq,1);
        assert!(head.manifest.is_some());
        drop(db);
        let _=fs::remove_file(&path);
        let _=fs::remove_file(path.with_extension("sqlite-wal"));
        let _=fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn commit_rejects_unverified_objects_and_immutable_key_reuse() {
        let path=temp_db();
        let mut db=SqliteAuthority::open(&path,Duration::from_secs(1)).unwrap();
        let head=db.create_partition("p0",3).unwrap();
        let unverified=object("wal/unverified",1);
        assert!(db.publish_commit("p0",head.head_version,3,"op-x",[1;32],1,unverified.clone(),None).is_err());
        assert_eq!(db.read_head("p0").unwrap().unwrap().committed_seq,0);
        db.register_durable_object(&unverified,10).unwrap();
        assert!(db.register_durable_object(&object("wal/unverified",2),11).is_err());
        db.connection.execute(
            "UPDATE object_uploads SET state=2 WHERE object_key=?1",
            ["wal/unverified"],
        ).unwrap();
        assert!(db.register_durable_object(&unverified,12).is_err());
        assert!(db.publish_commit("p0",head.head_version,3,"op-x",[1;32],1,unverified,None).is_err());
        assert_eq!(db.read_head("p0").unwrap().unwrap().committed_seq, 0);
        drop(db);
        let _=fs::remove_file(&path);
        let _=fs::remove_file(path.with_extension("sqlite-wal"));
        let _=fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn wal_dependency_edges_are_preserved_in_backup() {
        let path=temp_db();
        let mut db=SqliteAuthority::open(&path,Duration::from_secs(1)).unwrap();
        let head=db.create_partition("p0",9).unwrap();
        let first=object("wal/1",1);
        let second=object("wal/2",2);
        db.register_durable_object(&first,1).unwrap();
        db.register_durable_object(&second,2).unwrap();
        let head=match db.publish_commit("p0",head.head_version,9,"op-1",[1;32],1,first.clone(),None).unwrap() {
            PublishOutcome::Published(head)=>head, other=>panic!("{other:?}")
        };
        let head=match db.publish_commit("p0",head.head_version,9,"op-2",[2;32],2,second.clone(),None).unwrap() {
            PublishOutcome::Published(head)=>head, other=>panic!("{other:?}")
        };
        assert!(!db.retire_object(&first.key,1,3).unwrap());
        assert!(db.prepare_backup_snapshot("backup-missing",&[second.clone()],4,1024*1024).is_err());
        let snapshot=db.prepare_backup_snapshot("backup-full",&[first,second],5,1024*1024).unwrap();
        let parsed: AuthorityBackupSnapshot=serde_json::from_slice(&snapshot).unwrap();
        assert_eq!(parsed.edges.len(),1);
        assert_eq!(parsed.edges[0].parent_key,"wal/2");
        assert_eq!(parsed.edges[0].child_key,"wal/1");
        drop(db);
        let _=fs::remove_file(&path);
        let _=fs::remove_file(path.with_extension("sqlite-wal"));
        let _=fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn pinned_parent_protects_transitive_child_from_gc() {
        let path=temp_db();
        let mut db=SqliteAuthority::open(&path,Duration::from_secs(1)).unwrap();
        db.create_partition("p0",1).unwrap();
        let parent=object("backup/descriptor",1);
        let child=object("backup/segment",2);
        db.register_durable_object(&parent,1).unwrap();
        db.register_durable_object(&child,1).unwrap();
        db.register_object_edge(&parent,&child).unwrap();
        db.pin_object("backup-root-pin","p0",&parent.key,"backup",None).unwrap();
        assert!(!db.retire_object(&child.key,1,2).unwrap());
        assert!(db.unpin_object("backup-root-pin").unwrap());
        assert!(db.retire_object(&child.key,1,3).unwrap());
        drop(db);
        let _=fs::remove_file(&path);
        let _=fs::remove_file(path.with_extension("sqlite-wal"));
        let _=fs::remove_file(path.with_extension("sqlite-shm"));
    }

    #[test]
    fn commit_retry_index_watermark_and_reopen() {
        let path=temp_db();
        let mut db=SqliteAuthority::open(&path,Duration::from_secs(1)).unwrap();
        let head=db.create_partition("p0",7).unwrap();
        db.register_durable_object(&object("wal/1",1), 1).unwrap();
        db.register_durable_object(&object("manifest/1",3), 2).unwrap();
        db.register_durable_object(&object("manifest/bad",2), 3).unwrap();
        let first=db.publish_commit("p0",head.head_version,7,"op-1",[9;32],1,object("wal/1",1),None).unwrap();
        let head=match first {PublishOutcome::Published(head)=>head,other=>panic!("{other:?}")};
        assert_eq!(head.committed_seq,1);
        assert_eq!(head.indexed_seq,0);
        assert!(matches!(db.publish_commit("p0",0,7,"op-1",[9;32],1,object("wal/1",1),None).unwrap(),
            PublishOutcome::AlreadyPublished(_)));
        assert!(db.resolve_operation("p0","op-1",[9;32]).unwrap().is_some());
        assert!(db.publish_index("p0",head.head_version,7,2,object("manifest/bad",2)).unwrap().is_none());
        let indexed=db.publish_index("p0",head.head_version,7,1,object("manifest/1",3)).unwrap().unwrap();
        assert_eq!(indexed.indexed_seq,1);
        drop(db);
        let db=SqliteAuthority::open(&path,Duration::from_secs(1)).unwrap();
        assert_eq!(db.read_head("p0").unwrap().unwrap().indexed_seq,1);
        drop(db);
        let _=fs::remove_file(&path);
        let _=fs::remove_file(path.with_extension("sqlite-wal"));
        let _=fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
