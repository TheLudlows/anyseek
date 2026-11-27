//! Runs the normal GET/search entry points against an explicitly selected test
//! checkpoint. Live authority never selects this fixture.
use super::*;
use crate::storage::{
    build_budget::{File, reserve_memory},
    disk_records::{Checkpoint, CommittedChange, Limits, Root, View, primary, rows},
    segment,
};

pub(super) struct Pinned {
    view: Arc<View>,
    _owner: Arc<std::fs::File>,
    evaluation: u64,
}

impl Pinned {
    pub(super) fn capture(inner: &Inner) -> Result<Option<Self>, RecordError> {
        let Some(view) = &inner.disk_read_fixture else {
            return Ok(None);
        };
        if view.stamp().2 != inner.state.seq {
            return Err(RecordError::Busy);
        }
        Ok(Some(Self {
            view: view.clone(),
            _owner: inner.journal.owner_guard(),
            evaluation: inner.evaluation_time_ms,
        }))
    }

    pub(super) fn validate(&self, inner: &mut Inner) -> Result<(), RecordError> {
        inner.observe_time()?;
        if !inner.healthy {
            return Err(RecordError::RecoveryRequired);
        }
        if inner.state.seq != self.view.stamp().2
            || !inner
                .disk_read_fixture
                .as_ref()
                .is_some_and(|selected| Arc::ptr_eq(selected, &self.view))
        {
            return Err(RecordError::Busy);
        }
        Ok(())
    }

    pub(super) fn read(
        &self,
        id: &str,
        candidate: Option<(u64, [u8; 32])>,
    ) -> Result<Option<Record>, RecordError> {
        let Some(row) = self.view.get(id, self.evaluation).map_err(storage)? else {
            return Ok(None);
        };
        if candidate.is_some_and(|(version, fingerprint)| {
            version != row.metadata.version || Some(fingerprint) != row.metadata.fingerprint
        }) {
            return Ok(None);
        }
        row.with_body(|bytes| {
            let size = (bytes.len() as u64)
                .checked_mul(32)
                .ok_or(RecordError::Busy)?;
            let _decode = reserve_memory(size).map_err(memory_error)?;
            let document = serde_json::from_slice(bytes).map_err(storage)?;
            let current = crate::active::fingerprint(&temporal::indexed(
                &document,
                &row.metadata.temporal,
                row.metadata.ingested_at_ms,
            ))
            .map_err(storage)?;
            if Some(current) != row.metadata.fingerprint {
                return Err(storage(
                    "disk row fingerprint differs from original payload",
                ));
            }
            Ok(Record {
                record_id: id.to_owned(),
                version: row.metadata.version,
                document: Some(document),
                temporal: row.metadata.temporal.clone(),
                ingested_at_ms: row.metadata.ingested_at_ms,
                expired_at_ms: row.metadata.expired_at_ms,
            })
        })
        .map_err(storage)?
        .transpose()
    }
}

pub(super) fn prepare_group(
    inner: &Inner,
    seq: u64,
    records: &[state_memory::Shared<Record>],
) -> Result<Option<Arc<View>>, RecordError> {
    let Some(view) = &inner.disk_read_fixture else {
        return Ok(None);
    };
    if view.stamp().2 != inner.state.seq {
        return Err(RecordError::Busy);
    }
    let mut changes = Vec::with_capacity(records.len());
    for record in records {
        let (body, fingerprint) = match record.document.as_ref() {
            Some(document) => (
                Some(serde_json::to_vec(document).map_err(storage)?),
                Some(
                    crate::active::fingerprint(&temporal::indexed(
                        document,
                        &record.temporal,
                        record.ingested_at_ms,
                    ))
                    .map_err(storage)?,
                ),
            ),
            None => (None, None),
        };
        changes.push(CommittedChange {
            id: record.record_id.clone(),
            metadata: rows::Metadata {
                version: record.version,
                ingested_at_ms: record.ingested_at_ms,
                temporal: record.temporal.clone(),
                expired_at_ms: record.expired_at_ms,
                fingerprint,
            },
            body,
        });
    }
    let successor = view.prepare_successor(seq, changes).map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            RecordError::Busy
        } else {
            memory_error(error)
        }
    })?;
    Ok(Some(Arc::new(successor)))
}

const OBJECT: [u8; 16] = [9; 16];

async fn install(
    partition: &RecordPartition,
    path: &Path,
    recent_budget: usize,
) -> std::path::PathBuf {
    let inner = partition.inner.lock().await;
    let logical = inner.state.try_clone().unwrap();
    let body_limit = inner.config.max_record_bytes;
    drop(inner);

    let name = format!("{:032x}", u128::from_be_bytes(OBJECT));
    let payload = path.join(format!("payload-{name}"));
    let mut primary = primary::Writer::create(path, body_limit).unwrap();
    let mut segment_rows = rows::SegmentWriter::create(path, 1).unwrap();
    let mut writer = segment::Writer::create(&payload, 1024 * 1024, logical.records.len()).unwrap();
    let mut prepared = Vec::new();
    logical.records.scan(None, |id, row| {
        let record = row.load(id)?;
        let doc = record.document.as_ref().unwrap();
        let fingerprint = crate::active::fingerprint(&temporal::indexed(
            doc,
            &record.temporal,
            record.ingested_at_ms,
        ))
        .unwrap();
        let position = writer.intern(&serde_json::to_vec(doc).unwrap()).unwrap();
        prepared.push((id.to_string(), record, fingerprint, position));
        Ok(true)
    }).unwrap();
    let sealed = writer.finish().unwrap();
    for (id, record, fingerprint, position) in prepared {
        let row = segment_rows
            .push(
                &id,
                &rows::Row {
                    metadata: rows::Metadata {
                        version: record.version,
                        ingested_at_ms: record.ingested_at_ms,
                        temporal: record.temporal.clone(),
                        expired_at_ms: record.expired_at_ms,
                        fingerprint: Some(fingerprint),
                    },
                    payload: Some(rows::PayloadAddress {
                        object: OBJECT,
                        range: sealed.payloads[position].clone(),
                    }),
                },
            )
            .unwrap();
        primary.push(&id, row).unwrap();
    }
    let checkpoint = Checkpoint::open(
        path,
        path,
        Root {
            generation: [3; 16],
            committed_cut: logical.seq,
            primary: primary.finish().unwrap(),
            segments: BTreeMap::from([(1, segment_rows.finish().unwrap())]),
            payloads: BTreeMap::from([(name, sealed.bytes)]),
        },
        Limits {
            max_record_bytes: body_limit,
            max_files: 4,
            page_cache_bytes: 16 * 1024,
        },
        File::read_exact_at,
    )
    .unwrap();
    let mut inner = partition.inner.lock().await;
    assert_eq!(inner.state.seq, logical.seq);
    inner.disk_read_fixture = Some(Arc::new(View::new(checkpoint, recent_budget)));
    payload
}

fn search_request() -> RecordQuery {
    RecordQuery {
        temporal: None,
        text: "alpha".into(),
        vector: None,
        mode: SearchMode::Lexical,
        query_type: QueryType::Union,
        offset: 0,
        length: 8,
        result_type: ResultType::Topk,
        fields: vec![],
        filters: vec![],
        sort: vec![],
    }
}

#[tokio::test]
async fn get_and_search_use_selected_disk_body_and_fail_closed_on_corruption() {
    use super::tests::{config, write};
    use std::io::{Seek, Write};
    let tmp = tempfile::tempdir().unwrap();
    let partition = RecordPartition::create(&tmp.path().join("live"), config())
        .await
        .unwrap();
    partition
        .write(write("a", "a", "alpha disk text", ExpectedVersion::Absent))
        .await
        .unwrap();
    let dir = tmp.path().join("disk");
    std::fs::create_dir(&dir).unwrap();
    let payload = install(&partition, &dir, 8192).await;

    let get = partition.get("a").await.unwrap().unwrap();
    let search = partition.query(search_request()).await.unwrap();
    assert_eq!(search.hits.len(), 1);
    assert_eq!(get.record_id, "a");
    assert_eq!(search.hits[0].record.document, get.document);
    assert_eq!(get.document.unwrap()["body"], "alpha disk text");

    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(payload)
        .unwrap();
    file.seek(std::io::SeekFrom::Start(16)).unwrap();
    file.write_all(b"X").unwrap();
    file.sync_all().unwrap();
    drop(file);
    // The in-memory body is still present, so these failures demonstrate
    // that the tested read path does not fall back to it.
    assert!(partition.get("a").await.is_err());
    assert!(partition.query(search_request()).await.is_err());
    partition.finish_service_shutdown().await.unwrap();
}

#[tokio::test]
async fn search_rejects_publication_after_disk_readback() {
    use super::tests::{config, write};
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("live");
    let partition = RecordPartition::create(&root, config()).await.unwrap();
    partition
        .write(write("a", "a", "alpha old", ExpectedVersion::Absent))
        .await
        .unwrap();
    let dir = tmp.path().join("disk");
    std::fs::create_dir(&dir).unwrap();
    install(&partition, &dir, 8192).await;
    let pause = root.join(".test-disk-readback-pause");
    std::fs::write(&pause, b"pause").unwrap();
    let reader = partition.clone();
    let query = tokio::spawn(async move { reader.query(search_request()).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !root.join(".test-disk-readback-pause-entered").exists() {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    partition
        .write(write("b", "a", "alpha new", ExpectedVersion::Match(1)))
        .await
        .unwrap();
    std::fs::remove_file(pause).unwrap();
    assert!(matches!(query.await.unwrap(), Err(RecordError::Busy)));
    assert!(matches!(partition.get("a").await, Err(RecordError::Busy)));
    partition.finish_service_shutdown().await.unwrap();
}

#[tokio::test]
async fn group_disk_tail_admits_before_wal_and_publishes_as_one_view() {
    use super::tests::{config, write};
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("live");
    let cfg = config();
    let partition = RecordPartition::create(&root, cfg.clone()).await.unwrap();
    let a = partition
        .write(write("seed", "seed", "alpha base", ExpectedVersion::Absent))
        .await
        .unwrap();
    let dir = tmp.path().join("disk");
    std::fs::create_dir(&dir).unwrap();
    install(&partition, &dir, 2048).await;
    let original = {
        let inner = partition.inner.lock().await;
        inner.disk_read_fixture.clone().unwrap()
    };
    let group = |nonce: &str, body: &str| GroupReplace {
        operation_id: OperationId::new(nonce.into()),
        group_id: "turn".into(),
        expected_group_version: 0,
        records: ["body", "projection"]
            .into_iter()
            .map(|id| GroupRecord {
                record_id: id.into(),
                document: write("unused", id, body, ExpectedVersion::Absent)
                    .document
                    .unwrap(),
                temporal: None,
            })
            .collect(),
    };
    let too_large = group("too-large", &"x".repeat(1400));
    assert!(matches!(
        partition.replace_group(too_large).await,
        Err(RecordError::Busy)
    ));
    assert_eq!(
        partition.committed_progress().await.committed_seq,
        Some(a.seq)
    );
    assert_eq!(partition.group_version("turn").await.unwrap(), None);
    assert!(partition.get("body").await.unwrap().is_none());
    let b = partition
        .replace_group(group("small", "alpha paired"))
        .await
        .unwrap();
    assert_eq!(b.seq, a.seq + 1);
    assert!(original.get("body", 0).unwrap().is_none());
    assert!(original.get("projection", 0).unwrap().is_none());
    let found = partition.query(search_request()).await.unwrap();
    let ids: std::collections::BTreeSet<_> = found
        .hits
        .iter()
        .map(|hit| hit.record.record_id.as_str())
        .collect();
    assert_eq!(ids, ["body", "projection", "seed"].into_iter().collect());
    partition.inner.lock().await.fail_after_durable = true;
    let withdraw = GroupReplace {
        operation_id: OperationId::new("withdraw".into()),
        group_id: "turn".into(),
        expected_group_version: b.group_version,
        records: vec![],
    };
    assert_eq!(
        partition.replace_group(withdraw.clone()).await,
        Err(RecordError::DurablePending(b.seq + 1))
    );
    assert!(matches!(
        partition.get("body").await,
        Err(RecordError::RecoveryRequired)
    ));
    {
        let inner = partition.inner.lock().await;
        let selected = inner.disk_read_fixture.as_ref().unwrap();
        assert_eq!(selected.stamp().2, b.seq);
        assert!(selected.get("body", 0).unwrap().is_some());
    }
    drop(original);
    partition.finish_service_shutdown().await.unwrap();
    drop(partition);

    let reopened = RecordPartition::open(&root, cfg.max_journal_bytes)
        .await
        .unwrap();
    assert_eq!(
        reopened.replace_group(withdraw).await.unwrap().seq,
        b.seq + 1
    );
    assert!(reopened.get("body").await.unwrap().is_none());
    assert!(reopened.get("projection").await.unwrap().is_none());
    reopened.finish_service_shutdown().await.unwrap();
}
