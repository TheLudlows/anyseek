use super::*;
use crate::{record::TemporalMetadata, storage::segment};
use rows::{Row, SegmentWriter};
use std::{
    cell::Cell,
    fs,
    io::{Read, Seek, SeekFrom},
    sync::Arc,
};

thread_local! {
    static READS: Cell<(usize, usize)> = const { Cell::new((0, 0)) };
    static MAX_READ: Cell<usize> = const { Cell::new(usize::MAX) };
}

// An ordinary-file adapter for serial fixtures until #225 provides the
// shared platform implementation. Reads still pass through the budgeted File.
fn test_read(file: &File, offset: u64, out: &mut [u8]) -> io::Result<()> {
    READS.with(|count| {
        let (reads, bytes) = count.get();
        count.set((reads + 1, bytes + out.len()));
    });
    if out.len() > MAX_READ.get() {
        return Err(io::Error::other(
            "fixture deliberately disallows large payload reads",
        ));
    }
    let mut handle = &**file;
    handle.seek(SeekFrom::Start(offset))?;
    let mut file = file;
    file.read_exact(out)
}

const GENERATION: [u8; 16] = [1; 16];
const OBJECT: [u8; 16] = [2; 16];
const LIMITS: Limits = Limits {
    page_cache_bytes: 9 * 1024,
    max_record_bytes: 1024 * 1024,
    max_files: 12,
};
const BASE_BODY: &[u8] = br#"{"title":"original","vector":[1.0,0.0]}"#;

fn body_metadata(version: u64, ttl: Option<u64>) -> Metadata {
    let document = serde_json::from_slice(BASE_BODY).unwrap();
    Metadata {
        version,
        ingested_at_ms: 2,
        temporal: TemporalMetadata {
            event_time_ms: Some(-100),
            valid_from_ms: Some(0),
            valid_to_ms: Some(100),
            expire_at_ms: ttl,
        },
        expired_at_ms: None,
        fingerprint: Some(crate::active::fingerprint(&document).unwrap()),
    }
}
fn tombstone(version: u64, expires: Option<u64>, expired: Option<u64>) -> Metadata {
    Metadata {
        fingerprint: None,
        expired_at_ms: expired,
        ..body_metadata(version, expires)
    }
}

fn payload(dir: &Path, body: &[u8], count: usize) -> (String, segment::Descriptor) {
    let id = format!("{:032x}", u128::from_be_bytes(OBJECT));
    let mut writer = segment::Writer::create(
        &dir.join(format!("payload-{id}")),
        (body.len() * count + 16) as u64,
        count,
    )
    .unwrap();
    let mut bytes = body.to_vec();
    for index in 0..count {
        bytes[0] = body[0] ^ u8::try_from(index).unwrap();
        assert_eq!(writer.intern(&bytes).unwrap(), index);
    }
    (id, writer.finish().unwrap())
}

// The caller supplies the root's ordered ID stream. Two segments deliberately
// interleave addresses so a record ordinal cannot stand in for a logical ID.
fn build(
    dir: &Path,
    cut: u64,
    first_segment: u64,
    records: impl IntoIterator<Item = (String, Metadata)>,
    body: &[u8],
) -> Root {
    build_with_payloads(dir, cut, first_segment, records, body, 1)
}

fn build_with_payloads(
    dir: &Path,
    cut: u64,
    first_segment: u64,
    records: impl IntoIterator<Item = (String, Metadata)>,
    body: &[u8],
    payload_count: usize,
) -> Root {
    let (object_id, payload) = payload(dir, body, payload_count);
    let records = records.into_iter().enumerate().map(|(i, (id, metadata))| {
        let payload = metadata.is_live().then(|| PayloadAddress {
            object: OBJECT,
            range: payload.payloads[i % payload_count].clone(),
        });
        (id, Row { metadata, payload })
    });
    seal(
        dir,
        cut,
        first_segment,
        records,
        BTreeMap::from([(object_id, payload.bytes)]),
    )
}

fn seal(
    dir: &Path,
    cut: u64,
    first_segment: u64,
    records: impl IntoIterator<Item = (String, Row)>,
    payloads: BTreeMap<String, u64>,
) -> Root {
    let mut primary = primary::Writer::create(dir, LIMITS.max_record_bytes).unwrap();
    let mut first = SegmentWriter::create(dir, first_segment).unwrap();
    let mut second = SegmentWriter::create(dir, first_segment + 1).unwrap();
    for (i, (id, row)) in records.into_iter().enumerate() {
        let writer = if i % 2 == 0 { &mut first } else { &mut second };
        let reference = writer.push(&id, &row).unwrap();
        primary.push(&id, reference).unwrap();
    }
    Root {
        generation: GENERATION,
        committed_cut: cut,
        primary: primary.finish().unwrap(),
        segments: BTreeMap::from([
            (first_segment, first.finish().unwrap()),
            (first_segment + 1, second.finish().unwrap()),
        ]),
        payloads,
    }
}

fn read_view(dir: &Path, root: Root, recent_budget: usize) -> View {
    // Exercise the descriptor's actual representation, including the payload
    // object map. Sealed references survive a same-version reopen.
    let encoded = serde_json::to_vec(&root).unwrap();
    let selected = serde_json::from_slice(&encoded).unwrap();
    let checkpoint = Checkpoint::open(dir, dir, selected, LIMITS, test_read).unwrap();
    View::new(checkpoint, recent_budget)
}

fn change(id: &str, metadata: Metadata, body: Option<&[u8]>) -> CommittedChange {
    CommittedChange {
        id: id.into(),
        metadata,
        body: body.map(<[u8]>::to_vec),
    }
}

fn overwrite(path: &Path, position: u64) {
    let mut file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(position)).unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 0xff;
    file.seek(SeekFrom::Start(position)).unwrap();
    file.write_all(&byte).unwrap();
    file.sync_all().unwrap();
}

fn page_ids<'a>(page: &'a Page<'_>) -> Vec<&'a str> {
    page.entries.iter().map(|(id, _)| id.as_str()).collect()
}

#[test]
fn sealed_and_recent_records_share_order_version_and_expiration() {
    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    // This common prefix splits a UTF-8 code point across the page/suffix.
    let prefix = format!("{}界", "k".repeat(31));
    let long_a = format!("{prefix}A");
    let long_x = format!("{prefix}X{}", "界".repeat(1000));
    let records = [
        ("a".into(), body_metadata(1, None)),
        ("b".into(), body_metadata(3, Some(20))),
        ("c".into(), tombstone(2, None, None)),
        ("d".into(), body_metadata(5, None)),
        (long_a.clone(), body_metadata(6, None)),
        (long_x.clone(), body_metadata(7, None)),
    ];
    let mut view = read_view(dir, build(dir, 8, 10, records, BASE_BODY), 4096);
    assert_eq!(view.stamp(), (GENERATION, 8, 8));
    let a = view.get("a", 19).unwrap().unwrap();
    assert_eq!(
        a.address,
        Some(Address {
            segment_id: 10,
            doc_id: 0
        })
    );
    assert_eq!(a.metadata, body_metadata(1, None));
    assert!(a.with_body(|bytes| bytes == BASE_BODY).unwrap().unwrap());
    assert_eq!(
        view.get("b", 19).unwrap().unwrap().address,
        Some(Address {
            segment_id: 11,
            doc_id: 0
        })
    );
    assert!(view.get("b", 20).unwrap().is_none());
    assert!(view.matches("b", &ExpectedVersion::Absent, 20).unwrap());
    assert!(view.matches("b", &ExpectedVersion::Match(3), 20).unwrap());
    assert!(view.matches("c", &ExpectedVersion::Match(2), 20).unwrap());
    assert!(view.matches("c", &ExpectedVersion::Absent, 20).unwrap());
    assert!(!view.matches("a", &ExpectedVersion::Absent, 20).unwrap());
    assert!(
        !view
            .matches("missing", &ExpectedVersion::Match(1), 20)
            .unwrap()
    );
    assert!(view.matches("missing", &ExpectedVersion::Any, 20).unwrap());
    assert_eq!(view.get(&long_x, 20).unwrap().unwrap().metadata.version, 7);
    assert!(view.get(&format!("{prefix}missing"), 20).unwrap().is_none());

    let page = view.page("", 2, 20).unwrap();
    assert_eq!(page_ids(&page), ["a", "d"]);
    assert!(page.has_more);
    let page = view.page("d", 2, 20).unwrap();
    assert_eq!(page_ids(&page), [&long_a, &long_x]);
    assert!(!page.has_more);

    let rejected = view.install_committed(10, vec![]);
    assert_eq!(rejected.unwrap_err().kind(), io::ErrorKind::InvalidData);
    let oversized = vec![b'X'; 4096];
    assert!(
        view.install_committed(
            9,
            vec![
                change("a", tombstone(2, None, None), None),
                change("z", body_metadata(1, None), Some(&oversized)),
            ]
        )
        .is_err()
    );
    assert_eq!(view.stamp(), (GENERATION, 8, 8));
    assert_eq!(view.get("a", 21).unwrap().unwrap().metadata.version, 1);
    view.install_committed(
        9,
        vec![
            change("a", tombstone(2, None, None), None),
            change("b", tombstone(4, Some(20), Some(21)), None),
            change("c", body_metadata(3, Some(30)), Some(BASE_BODY)),
            change("d", body_metadata(6, None), Some(BASE_BODY)),
        ],
    )
    .unwrap();
    assert_eq!(view.stamp(), (GENERATION, 8, 9));
    assert!(view.get("a", 21).unwrap().is_none());
    assert!(view.matches("b", &ExpectedVersion::Match(4), 21).unwrap());
    assert!(view.get("c", 30).unwrap().is_none());
    assert_eq!(view.get("d", 21).unwrap().unwrap().address, None);
    assert_eq!(
        page_ids(&view.page("", 3, 21).unwrap()),
        ["c", "d", &long_a]
    );
    assert_eq!(
        page_ids(&view.page("c", 5, 21).unwrap()),
        ["d", &long_a, &long_x]
    );
    assert!(view.recent_bytes <= 4096);

    let successor = view
        .prepare_successor(
            10,
            vec![
                change("c", tombstone(4, None, None), None),
                change("d", body_metadata(7, None), Some(BASE_BODY)),
            ],
        )
        .unwrap();
    assert_eq!(view.get("c", 21).unwrap().unwrap().metadata.version, 3);
    assert_eq!(view.get("d", 21).unwrap().unwrap().metadata.version, 6);
    assert_eq!(successor.get("d", 21).unwrap().unwrap().metadata.version, 7);
    assert!(successor.matches("c", &ExpectedVersion::Match(4), 21).unwrap());
    assert_eq!(
        page_ids(&successor.page("", 5, 21).unwrap()),
        ["d", &long_a, &long_x]
    );
}

#[test]
fn cache_and_metadata_io_stay_bounded_as_ids_and_bodies_grow() {
    for (count, body_bytes) in [(80, 128), (5_000, 128), (5_000, 256 * 1024)] {
        let dir = tempfile::tempdir().unwrap();
        let dir = dir.path();
        let body = vec![b'B'; body_bytes];
        let records = (0..count).map(|i| (format!("id-{i:06}"), body_metadata(1, None)));
        let root = build_with_payloads(dir, 1, 2, records, &body, 64);
        assert_eq!(
            root.payloads.values().next(),
            Some(&((64 * body_bytes + 16) as u64))
        );

        // Cold open must read just headers. No length-dependent body or ID scan.
        READS.set((0, 0));
        MAX_READ.set(4096);
        let view = read_view(dir, root, 0);
        assert_eq!(READS.get(), (5, 80));
        let target = format!("id-{:06}", count - 1);
        assert_eq!(view.get(&target, 1).unwrap().unwrap().metadata.version, 1);
        assert!(READS.get().0 <= 9, "point read should descend one path");
        assert!(view.located("id-999999", 1).unwrap().is_none());

        let mut after = String::new();
        let mut seen = 0;
        loop {
            let page = view.page(&after, 81, 1).unwrap();
            for (key, _) in page.entries {
                assert_eq!(key.as_str(), format!("id-{seen:06}"));
                seen += 1;
                after.clear();
                after.push_str(key.as_str());
            }
            assert!(view.checkpoint.primary.cache_bytes() <= LIMITS.page_cache_bytes);
            if !page.has_more {
                break;
            }
        }
        assert_eq!(seen, count);
        let row = view.get(&target, 1).unwrap().unwrap();
        MAX_READ.set(usize::MAX);
        READS.set((0, 0));
        assert_eq!(
            row.with_body(|body| (body.len(), body[0])).unwrap(),
            Some((body_bytes, b'B' ^ ((count - 1) % 64) as u8))
        );
        assert_eq!(READS.get(), (1, body_bytes));
    }
}

#[test]
fn selected_page_row_and_body_damage_are_storage_errors() {
    let mismatched = tempfile::tempdir().unwrap();
    let alias_dir = mismatched.path();
    let mut rows = SegmentWriter::create(alias_dir, 1).unwrap();
    let row = rows
        .push(
            "original",
            &Row {
                metadata: tombstone(1, None, None),
                payload: None,
            },
        )
        .unwrap();
    let mut index = primary::Writer::create(alias_dir, LIMITS.max_record_bytes).unwrap();
    index.push("alias", row).unwrap();
    let view = read_view(
        alias_dir,
        Root {
            generation: GENERATION,
            committed_cut: 1,
            primary: index.finish().unwrap(),
            segments: BTreeMap::from([(1, rows.finish().unwrap())]),
            payloads: BTreeMap::new(),
        },
        0,
    );
    assert_eq!(
        view.located("alias", 1).err().unwrap().kind(),
        io::ErrorKind::InvalidData
    );

    let dir = tempfile::tempdir().unwrap();
    let dir = dir.path();
    let long_id = "x".repeat(1000);
    let root = build(
        dir,
        1,
        2,
        [
            ("a".into(), body_metadata(1, None)),
            (long_id.clone(), body_metadata(2, None)),
        ],
        BASE_BODY,
    );
    let encoded = serde_json::to_vec(&root).unwrap();
    let view = read_view(dir, root, 0);
    let payload_path = dir.join(format!("payload-{:032x}", u128::from_be_bytes(OBJECT)));
    overwrite(&payload_path, 16);
    assert!(view.get("missing", 1).unwrap().is_none());
    let row = view.get("a", 1).unwrap().unwrap();
    assert_eq!(
        row.with_body(|_| ()).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );

    overwrite(&dir.join("records-0000000000000002"), 16);
    assert_eq!(
        view.get("a", 1).err().unwrap().kind(),
        io::ErrorKind::InvalidData
    );
    assert!(view.get(&long_id, 1).unwrap().is_some());
    overwrite(&dir.join("primary-keys"), 16);
    assert_eq!(
        view.get(&long_id, 1).err().unwrap().kind(),
        io::ErrorKind::InvalidData
    );
    drop(view);

    fs::remove_file(payload_path).unwrap();
    let missing = Checkpoint::open(
        dir,
        dir,
        serde_json::from_slice(&encoded).unwrap(),
        LIMITS,
        test_read,
    );
    assert_eq!(missing.err().unwrap().kind(), io::ErrorKind::NotFound);
    payload(dir, BASE_BODY, 1);
    overwrite(&dir.join("primary-pages"), 16);
    let view = read_view(dir, serde_json::from_slice(&encoded).unwrap(), 0);
    assert_eq!(
        view.get("a", 1).err().unwrap().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn compacted_checkpoint_changes_addresses_and_old_owner_stays_pinned() {
    let old = tempfile::tempdir().unwrap();
    let new = tempfile::tempdir().unwrap();
    let old_dir = old.path();
    let new_dir = new.path();
    let root = build(
        old_dir,
        5,
        4,
        [
            ("gone".into(), body_metadata(7, None)),
            ("kept".into(), body_metadata(3, Some(100))),
        ],
        BASE_BODY,
    );
    let mut old_view = read_view(old_dir, root, 1024);
    let old_pin = Arc::downgrade(&old_view.checkpoint);
    old_view
        .install_committed(6, vec![change("gone", tombstone(8, None, None), None)])
        .unwrap();
    let previous = old_view.located("kept", 50).unwrap().unwrap();
    let retained = Row {
        metadata: previous.metadata.clone(),
        payload: previous.sealed_payload().cloned(),
    };
    let next_root = seal(
        new_dir,
        6,
        20,
        [
            (
                "gone".into(),
                Row {
                    metadata: tombstone(8, None, None),
                    payload: None,
                },
            ),
            ("kept".into(), retained),
        ],
        BTreeMap::from([(
            format!("{:032x}", u128::from_be_bytes(OBJECT)),
            (16 + BASE_BODY.len()) as u64,
        )]),
    );

    let mut selected = Arc::new(old_view);
    let reader = selected.clone();
    assert_eq!(selected.stamp(), (GENERATION, 5, 6));
    let next = Checkpoint::open(new_dir, old_dir, next_root, LIMITS, test_read).unwrap();
    selected = Arc::new(View::new(next, 1024));
    assert_eq!(selected.stamp(), (GENERATION, 6, 6));
    let before = reader.get("kept", 50).unwrap().unwrap();
    let after = selected.get("kept", 50).unwrap().unwrap();
    assert_eq!(
        before.address,
        Some(Address {
            segment_id: 5,
            doc_id: 0
        })
    );
    assert_eq!(
        after.address,
        Some(Address {
            segment_id: 21,
            doc_id: 0
        })
    );
    assert_eq!(before.metadata, after.metadata);
    assert_eq!(
        before.with_body(<[u8]>::to_vec).unwrap(),
        after.with_body(<[u8]>::to_vec).unwrap()
    );
    assert!(selected.get("gone", 50).unwrap().is_none());
    assert!(
        selected
            .matches("gone", &ExpectedVersion::Match(8), 50)
            .unwrap()
    );
    assert!(old_pin.upgrade().is_some());
    drop(reader);
    assert!(old_pin.upgrade().is_none());
}
