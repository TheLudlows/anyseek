//! SQLite metadata authority for the object-store persistence path.
//!
//! This database stores coordination metadata only. Record bodies, vectors, WAL
//! blocks and immutable index segments remain in external immutable objects.
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
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
    pub indexed_seq: u64,
    pub log_base_seq: u64,
    pub log_anchor: Option<ObjectRef>,
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
                 state INTEGER NOT NULL CHECK(state IN (0,1,2)),
                 created_at_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS object_pins (
                 pin_id TEXT PRIMARY KEY,
                 partition_id TEXT NOT NULL,
                 object_key TEXT NOT NULL,
                 pin_kind TEXT NOT NULL CHECK(pin_kind IN ('reader','indexer','backup','staging')),
                 expires_at_ms INTEGER
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
                if state == 2 {
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

    /// Pin a verified object before opening it or starting a background build.
    /// Pins are metadata leases; callers must cancel work before allowing its lease to expire.
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
        if state != Some(1) {
            tx.commit()?;
            return Ok(false);
        }
        let referenced: bool = tx.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM partition_heads
                WHERE json_extract(wal_tip_json,'$.key')=?1
                   OR json_extract(manifest_json,'$.key')=?1
                   OR json_extract(log_anchor_json,'$.key')=?1
             )",
            [object_key], |row| row.get(0),
        )?;
        let pinned: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM object_pins
             WHERE object_key=?1 AND (expires_at_ms IS NULL OR expires_at_ms>?2))",
            params![object_key,now_ms], |row| row.get(0),
        )?;
        if referenced || pinned {
            tx.commit()?;
            return Ok(false);
        }
        tx.execute("UPDATE object_uploads SET state=2 WHERE object_key=?1", [object_key])?;
        tx.execute(
            "INSERT INTO retired_objects(object_key,retired_generation,retired_at_ms)
             VALUES(?1,?2,?3)",
            params![object_key,to_i64(retired_generation)?,now_ms],
        )?;
        tx.commit()?;
        Ok(true)
    }

    /// Return only monotonically retired objects that are still unreferenced and unpinned.
    /// The returned list is safe for idempotent remote deletion: state=retired prevents
    /// any future publication or pin from making the object live again.
    pub(crate) fn collectible_objects(&self, now_ms: i64) -> rusqlite::Result<Vec<ObjectRef>> {
        let mut statement = self.connection.prepare(
            "SELECT u.object_key,u.sha256,u.byte_length
             FROM object_uploads u
             WHERE u.state=2
               AND NOT EXISTS(
                 SELECT 1 FROM partition_heads h
                 WHERE json_extract(h.wal_tip_json,'$.key')=u.object_key
                    OR json_extract(h.manifest_json,'$.key')=u.object_key
                    OR json_extract(h.log_anchor_json,'$.key')=u.object_key
               )
               AND NOT EXISTS(
                 SELECT 1 FROM object_pins p
                 WHERE p.object_key=u.object_key
                   AND (p.expires_at_ms IS NULL OR p.expires_at_ms>?1)
               )
             ORDER BY u.object_key"
        )?;
        let rows = statement.query_map([now_ms], |row| {
            let key: String = row.get(0)?;
            let digest: Vec<u8> = row.get(1)?;
            let sha256: [u8;32] = digest.try_into().map_err(|_| rusqlite::Error::InvalidQuery)?;
            let length: i64 = row.get(2)?;
            let length = u64::try_from(length).map_err(|_| rusqlite::Error::InvalidQuery)?;
            Ok(ObjectRef { key, sha256, length })
        })?;
        rows.collect()
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
        "SELECT partition_id,head_version,owner_epoch,committed_seq,wal_tip_json,manifest_json,indexed_seq,log_base_seq,log_anchor_json
         FROM partition_heads WHERE partition_id=?1",
        [partition_id],
        |row| Ok((
            row.get::<_,String>(0)?, row.get::<_,i64>(1)?, row.get::<_,i64>(2)?,
            row.get::<_,i64>(3)?, row.get::<_,Option<String>>(4)?, row.get::<_,Option<String>>(5)?,
            row.get::<_,i64>(6)?, row.get::<_,i64>(7)?, row.get::<_,Option<String>>(8)?,
        )),
    ).optional()?.map(|(partition_id,hv,epoch,committed,wal,manifest,indexed,base,anchor)| {
        Ok(PartitionHead {
            partition_id,
            head_version: u64::try_from(hv).map_err(|_| rusqlite::Error::InvalidQuery)?,
            owner_epoch: u64::try_from(epoch).map_err(|_| rusqlite::Error::InvalidQuery)?,
            committed_seq: u64::try_from(committed).map_err(|_| rusqlite::Error::InvalidQuery)?,
            wal_tip: decode_object(wal)?,
            manifest: decode_object(manifest)?,
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
        assert!(matches!(
            db.publish_commit("p0",head.head_version,3,"op-x",[1;32],1,unverified,None).unwrap(),
            PublishOutcome::Published(_)
        ));
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
