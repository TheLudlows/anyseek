//! Ephemeral successful prefix, never a second canonical State. The caller
//! retains the partition writer while reading through to the live basis.
use super::*;
use state_memory::Shared;
use crate::storage::build_budget::{reserve_memory, MemoryLease};

pub(super) struct PlanningView {
    records: BTreeMap<String, Shared<Record>>,
    operations: BTreeMap<OperationId, Shared<Applied>>,
    seq: u64,
    pub epoch_watermark: u64,
    time_watermark_ms: u64,
    record_count: usize,
    operation_count: usize,
    ttl_count: usize,
    value_bytes: usize,
    lookup_bytes: usize,
    memory: MemoryLease,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::tests::{config, write};

    #[test]
    fn local_prefix_matches_full_state_counts_replays_and_wire_budget() {
        // Budget exhaustion is process-wide. Follow the existing ownership
        // tests' exact-child pattern so parallel tests retain their own pool.
        const FLAG: &str = "ANYSEEK_PLANNING_VIEW_CHILD";
        const NAME: &str = "record::write::planning::tests::local_prefix_matches_full_state_counts_replays_and_wire_budget";
        if std::env::var_os(FLAG).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([NAME, "--exact", "--nocapture"]).env(FLAG, "1").output().unwrap();
            assert!(output.status.success(), "{}\n{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
            assert!(String::from_utf8_lossy(&output.stdout).contains("test result: ok. 1 passed; 0 failed;"));
            return;
        }
        let mut config = config(); config.max_record_bytes = 16 * 1024;
        let mut basis = State::default();
        let mut seeds = Vec::new();
        for i in 0..9 {
            let mut request = write(&format!("seed-{i}"), &format!("id-{i}"), "large", ExpectedVersion::Absent);
            request.operation_id.epoch = 9;
            if i == 0 { request.document.as_mut().unwrap().insert("opaque".into(), serde_json::json!("x".repeat(8192))); }
            if i >= 7 { request.temporal = Some(TemporalMetadata { expire_at_ms: Some(if i == 8 { 100 } else { 2000 }), ..Default::default() }); }
            let mutation::PreparedWrite::Append { applied, .. } = mutation::prepare_at(&basis, &config,
                checkpoint_codec::full_length(&config, &basis).unwrap(), request.clone(), 9, 99).unwrap()
                else { panic!("seed append") };
            apply(&mut basis, &applied).unwrap(); seeds.push(request);
        }
        let mut prefix = PlanningView::new(&basis).unwrap();
        let mut oracle = basis.try_clone().unwrap();
        let mut snapshot = checkpoint_codec::full_length(&config, &basis).unwrap();
        let mut update = write("update", "id-0", "small", ExpectedVersion::Match(1));
        update.document.as_mut().unwrap().remove("vector");
        update.temporal = Some(TemporalMetadata { expire_at_ms: Some(2000), ..Default::default() });
        let mut patch = write("patch", "id-0", "", ExpectedVersion::Match(2));
        patch.document = None;
        patch.vector_patch = Some(serde_json::from_value(serde_json::json!({"vector":[2.0,0.0]})).unwrap());
        let mut delete = write("delete", "id-0", "", ExpectedVersion::Match(2)); delete.document = None;
        let reuse = write("reuse", "id-0", "reuse", ExpectedVersion::Absent);
        let mut ttl = write("ttl", "id-0", "ttl", ExpectedVersion::Match(4));
        ttl.temporal = update.temporal.clone();
        let new = write("new", "new-\"中", "new", ExpectedVersion::Absent);
        let expired_reuse = write("expired-reuse", "id-8", "reuse", ExpectedVersion::Absent);
        let mut conflict = update.clone(); conflict.document.as_mut().unwrap().insert("body".into(), serde_json::json!("conflict"));
        let requests = vec![update.clone(), update, patch, delete, reuse, ttl, new, expired_reuse, seeds[0].clone(), conflict];
        for mut request in requests {
            request.operation_id.epoch = if request.operation_id.nonce.starts_with("seed-") { 9 } else { 10 };
            let (previous, operation) = prefix.read(&basis, &request);
            let input = prefix.input(previous.as_deref(), operation.as_deref());
            assert_eq!((input.seq, input.epoch_watermark, input.time_watermark_ms),
                (oracle.seq, oracle.epoch_watermark, oracle.time_watermark_ms));
            assert_eq!((input.record_count, input.operation_count, input.ttl_count),
                (oracle.records.len(), oracle.operations.len(), oracle.records.live_ttl_count()));
            let expected = mutation::prepare_at(&oracle, &config, snapshot, request.clone(), 10, 100);
            let actual = mutation::prepare_input(&input, &config, snapshot, request, 10, 100);
            match (expected, actual) {
                (Ok(mutation::PreparedWrite::Append { bytes: expected_bytes, .. }),
                 Ok(mutation::PreparedWrite::Append { applied, bytes, snapshot_bytes, .. })) => {
                    assert_eq!(bytes, expected_bytes);
                    let prepared = state_memory::Prepared::from_previous(input.previous, &applied).unwrap();
                    if prefix.seq <= basis.seq + 1 {
                        let before = (prefix.seq, prefix.epoch_watermark, prefix.time_watermark_ms,
                            prefix.record_count, prefix.operation_count, prefix.ttl_count, prefix.value_bytes,
                            prefix.lookup_bytes, prefix.memory.bytes(), prefix.records.len(), prefix.operations.len());
                        let held = crate::storage::build_budget::fill_memory_for_test();
                        assert!(matches!(prefix.advance(&input, &applied, &prepared, snapshot_bytes),
                            Err(RecordError::Storage(message)) if message.contains("resource budget exceeded")));
                        assert_eq!((prefix.seq, prefix.epoch_watermark, prefix.time_watermark_ms,
                            prefix.record_count, prefix.operation_count, prefix.ttl_count, prefix.value_bytes,
                            prefix.lookup_bytes, prefix.memory.bytes(), prefix.records.len(), prefix.operations.len()), before);
                        drop(held);
                    }
                    let container = prefix.advance(&input, &applied, &prepared, snapshot_bytes).unwrap();
                    apply(&mut oracle, &applied).unwrap();
                    snapshot = checkpoint_codec::full_length(&config, &oracle).unwrap();
                    assert_eq!(snapshot_bytes, snapshot);
                    assert_eq!(container, oracle.container_bytes(snapshot).unwrap());
                    assert_eq!((prefix.value_bytes, prefix.lookup_bytes), (oracle.value_bytes().unwrap(), oracle.lookup_bytes));
                }
                (Ok(mutation::PreparedWrite::Replay(a)), Ok(mutation::PreparedWrite::Replay(b))) => assert_eq!(a,b),
                (Err(a), Err(b)) => assert_eq!(a,b),
                _ => panic!("local and full-state decisions differ"),
            }
        }
        assert_eq!((prefix.records.len(), prefix.operations.len()), (3,7));
        assert_eq!((basis.records.len(), basis.operations.len(), basis.seq), (9,9,9), "read-through basis stays unchanged");
        assert!(prefix.records.len() < basis.records.len());

        // Epoch may advance between the GC precheck and planning, then wall
        // time may retreat across midnight. Preserve the candidate watermark
        // when checking the fixed canonical GC prefix on the next command.
        basis.history.floor = basis.seq;
        let mut prefix = PlanningView::new(&basis).unwrap();
        let mut oracle = basis.try_clone().unwrap();
        let mut request = write("epoch-step", "id-0", "updated", ExpectedVersion::Any);
        request.operation_id.epoch = 11;
        let (previous, operation) = prefix.read(&basis, &request);
        let input = prefix.input(previous.as_deref(), operation.as_deref());
        let mutation::PreparedWrite::Append { applied, snapshot_bytes, .. } = mutation::prepare_input(
            &input, &config, checkpoint_codec::full_length(&config, &basis).unwrap(), request, 11, 101).unwrap()
            else { panic!("epoch append") };
        let prepared = state_memory::Prepared::from_previous(input.previous, &applied).unwrap();
        prefix.advance(&input, &applied, &prepared, snapshot_bytes).unwrap();
        apply(&mut oracle, &applied).unwrap();
        assert!(!maintenance::has_expired_operations(&basis, 10).unwrap());
        assert!(maintenance::has_expired_operations(&oracle, 10).unwrap());
        assert_eq!(maintenance::has_expired_operations(&basis, 10.max(prefix.epoch_watermark)).unwrap(),
            maintenance::has_expired_operations(&oracle, 10).unwrap());
    }
}
impl PlanningView {
    pub(super) fn new(basis: &State) -> Result<Self, RecordError> {
        Ok(Self {
            records: BTreeMap::new(), operations: BTreeMap::new(),
            seq: basis.seq, epoch_watermark: basis.epoch_watermark,
            time_watermark_ms: basis.time_watermark_ms,
            record_count: basis.records.len(), operation_count: basis.operations.len(),
            ttl_count: basis.records.live_ttl_count(),
            value_bytes: basis.value_bytes()?, lookup_bytes: basis.lookup_bytes,
            memory: reserve_memory(512 * 32).map_err(storage)?,
        })
    }

    // A tombstone in this prefix is a value, not a missing map entry. Reads of
    // untouched rows borrow the unchanged canonical basis only under Inner.
    pub(super) fn read(&self, basis: &State, request: &RecordWrite)
        -> (Option<Shared<Record>>, Option<Shared<Applied>>)
    {
        let record = self.records.get(&request.record_id).cloned()
            .or_else(|| basis.records.shared(&request.record_id));
        let operation = self.operations.get(&request.operation_id).cloned()
            .or_else(|| basis.operation_lookup.get(&request.operation_id).map(|&i| basis.operations.shared(i)));
        (record, operation)
    }
    pub(super) fn input<'a>(&self, previous: Option<&'a Record>, operation: Option<&'a Applied>) -> mutation::Input<'a> {
        mutation::Input { previous, operation, seq: self.seq,
            epoch_watermark: self.epoch_watermark, time_watermark_ms: self.time_watermark_ms,
            record_count: self.record_count, operation_count: self.operation_count, ttl_count: self.ttl_count }
    }

    /// Complete every fallible check/reservation before installing this prefix.
    /// Return the whole candidate's container estimate, not the local map size.
    pub(super) fn advance(&mut self, input: &mutation::Input<'_>, applied: &Applied,
        prepared: &state_memory::Prepared, snapshot_bytes: usize) -> Result<usize, RecordError>
    {
        let request = &applied.request;
        if self.seq.checked_add(1) != Some(applied.receipt.seq)
            || applied.observed_time_ms < self.time_watermark_ms
            || applied.receipt.record_id != request.record_id || input.operation.is_some()
            || applied.receipt.version != input.previous.map_or(1, |r|
                if request.vector_patch.is_some() { r.version } else { r.version.saturating_add(1) })
        {
            return Err(storage("inconsistent planning prefix"));
        }
        let old_bytes = input.previous.map(checkpoint_codec::json_length).transpose()?.unwrap_or(0);
        let values = self.value_bytes.checked_sub(old_bytes)
            .and_then(|n| n.checked_add(prepared.record.wire_bytes()))
            .and_then(|n| n.checked_add(prepared.operation.wire_bytes()))
            .ok_or_else(|| storage("planning value size overflow"))?;
        let lookup = self.lookup_bytes.checked_add(checkpoint_codec::json_length(&request.operation_id)?)
            .ok_or_else(|| storage("planning lookup size overflow"))?;
        let container = snapshot_bytes.checked_sub(values).and_then(|n| n.checked_add(lookup))
            .ok_or_else(|| storage("planning container size overflow"))?;
        let records = self.record_count.checked_add(usize::from(input.previous.is_none()))
            .ok_or_else(|| storage("planning record count overflow"))?;
        let operations = self.operation_count.checked_add(1).ok_or_else(|| storage("planning operation count overflow"))?;
        let ttl = self.ttl_count.checked_sub(usize::from(input.previous.is_some_and(temporal::live_ttl)))
            .and_then(|n| n.checked_add(usize::from(temporal::live_ttl(&prepared.record))))
            .ok_or_else(|| storage("planning TTL count overflow"))?;
        // Bound both maps' owned keys, pointers and BTree nodes before cloning
        // them. Shared values already own their separate Prepared leases.
        let record_key = if self.records.contains_key(&request.record_id) { 0 }
            else { checkpoint_codec::json_length(&request.record_id)?.checked_add(512)
                .ok_or_else(|| storage("planning record key overflow"))? };
        let growth = record_key.checked_add(checkpoint_codec::json_length(&request.operation_id)?)
            .and_then(|n| n.checked_add(512)).and_then(|n| (n as u64).checked_mul(32))
            .ok_or_else(|| storage("planning map size overflow"))?;
        self.memory.grow(growth).map_err(storage)?;
        self.records.insert(request.record_id.clone(), prepared.record.clone());
        self.operations.insert(request.operation_id.clone(), prepared.operation.clone());
        self.seq = applied.receipt.seq;
        self.epoch_watermark = self.epoch_watermark.max(applied.observed_epoch);
        self.time_watermark_ms = self.time_watermark_ms.max(applied.observed_time_ms);
        self.record_count = records; self.operation_count = operations; self.ttl_count = ttl;
        self.value_bytes = values; self.lookup_bytes = lookup;
        Ok(container)
    }
}
