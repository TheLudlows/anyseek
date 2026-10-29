//! Fixed-cut GC must commute with background catch-up metadata transitions.
use super::*;
use crate::record::tests::{config, write};

fn append(state: &mut State, settings: &RecordConfig, nonce: &str, epoch: u64, at: u64) {
    let mut request = write(nonce, nonce, "alpha", ExpectedVersion::Absent);
    request.operation_id.epoch = epoch;
    let mutation::PreparedWrite::Append { applied, .. } = mutation::prepare_at(
        state, settings, checkpoint_codec::length(settings, state).unwrap(), request, epoch, at,
    ).unwrap() else { panic!("new operation"); };
    apply(state, &applied).unwrap();
}

#[test]
fn captured_receipt_gc_does_not_expand_with_catchup_floor_or_epoch() {
    let settings = config();
    let mut source = State::default();
    append(&mut source, &settings, "selected", 100, 1000);
    append(&mut source, &settings, "later-floor", 100, 1001);
    append(&mut source, &settings, "later-epoch", 101, 1002);
    source.history.floor = 1;
    let intent = OperationGc::capture(&source, 102, 1).unwrap();
    let mut candidate = source.try_clone().unwrap();
    intent.apply(&mut candidate).unwrap();
    // Catch-up can advance both the history boundary and the epoch watermark after
    // the source snapshot was captured. This must not widen the selected set.
    candidate.history.floor = 3;
    candidate.epoch_watermark = 103;
    let mut expected = source.try_clone().unwrap();
    expected.history.floor = 3;
    expected.epoch_watermark = 103;
    intent.apply(&mut expected).unwrap();
    assert_eq!(expected.operations.len(), 2);
    assert_eq!(expected.operations[0].request.operation_id.nonce, "later-floor");
    assert_eq!(expected.operations[1].request.operation_id.nonce, "later-epoch");
    assert_eq!(expected.retired_operation_prefix.as_ref().unwrap().seq, 1);
    state_proof::verify(&settings, &expected, &candidate).unwrap();
    assert_eq!(state_proof::length(&expected).unwrap(), checkpoint_codec::json_length(&expected).unwrap());
    let mut altered = expected.try_clone().unwrap();
    altered.retired_operation_prefix.as_mut().unwrap().time_ms += 1;
    assert!(state_proof::verify(&settings, &expected, &altered).is_err());
}

#[tokio::test]
async fn receipt_gc_background_catchup_keeps_newly_expired_history_receipt() {
    struct Clock(std::sync::atomic::AtomicU64);
    impl temporal::Clock for Clock {
        fn now_ms(&self) -> Result<u64, RecordError> {
            Ok(self.0.load(std::sync::atomic::Ordering::SeqCst))
        }
    }
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("partition");
    let settings = config();
    let epoch = current_epoch();
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
    let day = 86_400_000;
    let bootstrap = RecordPartition::create(&temporary.path().join("bootstrap"), settings.clone()).await.unwrap();
    let mut state = bootstrap.inner.lock().await.state.try_clone().unwrap();
    drop(bootstrap);
    append(&mut state, &settings, "selected", epoch - 2, now - 8 * day);
    append(&mut state, &settings, "later-floor", epoch - 2, now - 6 * day);
    append(&mut state, &settings, "current", epoch, now);
    state.history.floor = 1;
    let bytes = encode_entry(&Entry::Checkpoint { config: settings.clone(), state }).unwrap();
    drop(PartitionJournal::create(&path, settings.max_journal_bytes, &bytes).unwrap());
    let p = RecordPartition::open(&path, settings.max_journal_bytes).await.unwrap();
    let clock = Arc::new(Clock(std::sync::atomic::AtomicU64::new(now)));
    p.inner.lock().await.clock = clock.clone();
    std::fs::write(path.join(".test-indexer-pause"), b"pause").unwrap();
    let job = p.start_index_job(std::time::Duration::from_secs(30), true).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while !path.join(".test-indexer-started").exists() { tokio::task::yield_now().await; }
    }).await.unwrap();
    clock.0.store(now + 2 * day, std::sync::atomic::Ordering::SeqCst);
    p.inner.lock().await.advance_time().await.unwrap();
    assert_eq!(p.inner.lock().await.state.history.floor, 2);
    std::fs::remove_file(path.join(".test-indexer-pause")).unwrap();
    job.finish().await.unwrap();
    let inner = p.inner.lock().await;
    assert_eq!(inner.state.operations.len(), 2);
    assert_eq!(inner.state.operations[0].request.operation_id.nonce, "later-floor");
    assert_eq!(inner.state.retired_operation_prefix.as_ref().unwrap().seq, 1);
    assert_eq!(inner.state.history.floor, 2);
    validate_restored_state(&settings, &inner.state).unwrap();
}
