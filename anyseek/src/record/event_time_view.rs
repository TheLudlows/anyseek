//! Native consumer of one certified event-time base and its canonical suffix.
//! The suffix is the existing materialized Record overlay. It already covers
//! every selected manifest delta and the committed WAL after that manifest.
use super::*;
use crate::storage::{
    build_budget::{MemoryLease, reserve_memory},
    disk_records::event_time,
};
use std::{cmp::Reverse, ops::Range, sync::atomic::Ordering};
use tokio::{
    sync::OwnedSemaphorePermit,
    time::{Instant as Deadline, timeout_at},
};

const MAX_OVERLAY_ROWS: usize = 128;
const MAX_OVERLAY_BYTES: usize = 2 * 1024 * 1024;
const MAX_SUFFIX_SEQUENCES: u64 = 4096;
const MAX_BASE_VISITS: usize = 4096;
const MAX_RESULTS: usize = 128;
const MAX_RESULT_ID_BYTES: usize = 8 * 1024 * 1024;

pub(crate) struct Window {
    pub(crate) cut: u64,
    pub(crate) evaluation_time_ms: u64,
    pub(crate) hits: Vec<Hit>,
    _memory: MemoryLease,
}

pub(crate) struct Hit {
    pub(crate) id: String,
    pub(crate) event_time_ms: i64,
    pub(crate) version: u64,
    expires_at_ms: Option<u64>,
    _id: MemoryLease,
}

struct View {
    cut: u64,
    evaluation: u64,
    anchor: String,
    base: Arc<state_rows::Base>,
    // Stable ID order from Records, used even for tombstones or moves outside
    // the requested window. Owning only the changed rows bounds the pin.
    suffix: Vec<state_memory::Shared<Record>>,
    max_base_visits: usize,
    barrier: DataOutputPermit,
    _memory: MemoryLease,
    _scope: Option<crate::resources::Lease>,
    _admission: OwnedSemaphorePermit,
}

fn unavailable(reason: &str) -> RecordError {
    RecordError::Rejected(format!("event-time view unavailable: {reason}"))
}

fn common_cut(partition: &RecordPartition, inner: &Inner) -> Result<u64, RecordError> {
    if !inner.healthy || partition.indexes.error().is_some() {
        return Err(RecordError::RecoveryRequired);
    }
    let lexical = partition.indexes.lexical.load(Ordering::Acquire);
    let vector = partition.indexes.vector.load(Ordering::Acquire);
    // There is no retained overlay for a lower cut. The cache cursors are
    // independent evidence and must both agree with the overlay we can pin.
    if lexical != vector || lexical != inner.state.seq {
        return Err(RecordError::Busy);
    }
    Ok(lexical)
}

impl RecordPartition {
    /// Select one exact common cut for this native consumer. Callers supply
    /// the existing request deadline. None of the public query paths use it.
    #[allow(
        dead_code,
        reason = "native consumer awaiting shared query integration"
    )]
    pub(crate) async fn read_event_time_window(
        &self,
        window: Range<i64>,
        limit: usize,
        deadline: Deadline,
    ) -> Result<Window, RecordError> {
        if window.is_empty() || limit == 0 || limit > MAX_RESULTS {
            return Err(RecordError::Rejected(
                "invalid event-time window or result limit".into(),
            ));
        }
        let admission = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let mut inner = timeout_at(deadline, self.inner.lock())
            .await
            .map_err(|_| RecordError::Busy)?;
        let view = View::capture(self, &mut inner, admission, limit)?;
        drop(inner);

        let task = crate::resources::spawn_blocking(move || {
            let output = view.read(window, limit, deadline)?;
            Ok::<_, RecordError>((view, output))
        });
        let (view, output) = timeout_at(deadline, task)
            .await
            .map_err(|_| RecordError::Busy)?
            .map_err(storage)??;
        #[cfg(test)]
        pause_delivery(&self.inner, deadline).await;
        let mut inner = timeout_at(deadline, self.inner.lock())
            .await
            .map_err(|_| RecordError::Busy)?;
        let evaluation = inner.observe_time()?;
        if Deadline::now() >= deadline
            || common_cut(self, &inner)? != view.cut
            || inner
                .anchor
                .as_ref()
                .is_none_or(|anchor| anchor.selected_owner() != view.anchor)
            || !inner
                .state
                .records
                .event_time_base()
                .is_some_and(|base| Arc::ptr_eq(base, &view.base))
            || output
                .hits
                .iter()
                .any(|hit| hit.expires_at_ms.is_some_and(|expiry| expiry <= evaluation))
        {
            return Err(RecordError::Busy);
        }
        view.barrier.deliver(|| output)
    }
}

impl View {
    fn capture(
        partition: &RecordPartition,
        inner: &mut Inner,
        admission: OwnedSemaphorePermit,
        limit: usize,
    ) -> Result<Self, RecordError> {
        let evaluation = inner.observe_time()?;
        let cut = common_cut(partition, inner)?;
        let records = &inner.state.records;
        let base = records
            .event_time_base()
            .ok_or_else(|| unavailable("no projection in the selected base"))?;
        let anchor = inner
            .anchor
            .as_ref()
            .ok_or_else(|| unavailable("missing certified manifest"))?;
        let certified = anchor
            .checkpoint_state()
            .map_err(|_| unavailable("missing certified cut"))?;
        // Recovery and checkpoint publication install one shared base both in
        // the authenticated State and its post-certificate overlay.
        if !certified
            .records
            .event_time_base()
            .is_some_and(|pinned| Arc::ptr_eq(pinned, base))
        {
            return Err(unavailable(
                "the current overlay is not attached to the certified base",
            ));
        }
        let base_cut = base.event_time.as_ref().expect("selected projection").cut();
        if base_cut > certified.seq || certified.seq > cut {
            return Err(unavailable(
                "base, manifest, and published cuts are inconsistent",
            ));
        }
        if cut - base_cut > MAX_SUFFIX_SEQUENCES {
            return Err(unavailable("sequence suffix exceeds its budget"));
        }
        if limit > inner.config.max_candidates {
            return Err(RecordError::Rejected(
                "event-time limit exceeds the collection candidate budget".into(),
            ));
        }
        let recent = records.recent_records();
        if recent.len() > MAX_OVERLAY_ROWS {
            return Err(unavailable("record suffix exceeds its row budget"));
        }
        let overlay_bytes = recent
            .map(|row| row.wire_bytes())
            .try_fold(0usize, |sum, n| {
                sum.checked_add(n)
                    .filter(|bytes| *bytes <= MAX_OVERLAY_BYTES)
                    .ok_or_else(|| unavailable("record suffix exceeds its byte budget"))
            })?;
        let count = records.recent_records().len();
        let buffers =
            1024 + count * (size_of::<state_memory::Shared<Record>>() + size_of::<TailHit<'_>>());
        let memory = reserve_memory(buffers as u64).map_err(memory_error)?;
        let scope = crate::resources::Scope::current()
            .map(|scope| scope.bytes((buffers + overlay_bytes) as u64))
            .transpose()
            .map_err(|_| RecordError::Busy)?;
        Ok(Self {
            cut,
            evaluation,
            anchor: anchor.selected_owner().to_owned(),
            base: base.clone(),
            suffix: records.recent_records().cloned().collect(),
            max_base_visits: MAX_BASE_VISITS.min(inner.config.max_candidates),
            barrier: partition.output_permit()?,
            _memory: memory,
            _scope: scope,
            _admission: admission,
        })
    }

    fn read(
        &self,
        window: Range<i64>,
        limit: usize,
        deadline: Deadline,
    ) -> Result<Window, RecordError> {
        let mut suffix_hits = Vec::with_capacity(self.suffix.len());
        for row in &self.suffix {
            if let Some(time) = row
                .temporal
                .event_time_ms
                .filter(|time| window.contains(time))
                && temporal::visible(row, self.evaluation)
            {
                suffix_hits.push(TailHit { time, row });
            }
        }
        suffix_hits.sort_unstable_by(|a, b| a.rank().cmp(&b.rank()));
        let memory = reserve_memory((2 * limit * size_of::<Hit>()) as u64).map_err(memory_error)?;
        let mut output = OutputIds::default();
        let mut base_hits = Vec::with_capacity(limit);
        let mut examined = 0;
        self.base
            .event_time
            .as_ref()
            .expect("selected projection")
            .visit(window, state_rows::read_error, |id, time, reference| {
                if Deadline::now() >= deadline {
                    return Err(RecordError::Busy);
                }
                if examined >= self.max_base_visits {
                    return Err(unavailable("base candidate budget exhausted"));
                }
                examined += 1;
                if self
                    .suffix
                    .binary_search_by(|row| row.record_id.as_str().cmp(id))
                    .is_ok()
                {
                    return Ok(true);
                }
                let row =
                    event_time::locate(&self.base.checkpoint, id, time, reference, self.evaluation)
                        .map_err(state_rows::read_error)?;
                if row.metadata.visible(self.evaluation) {
                    base_hits.push(output.hit(
                        id,
                        time,
                        row.metadata.version,
                        row.metadata.temporal.expire_at_ms,
                    )?);
                }
                Ok(base_hits.len() < limit)
            })?;

        let mut base = base_hits.into_iter().peekable();
        let mut tail = suffix_hits.into_iter().peekable();
        let mut hits = Vec::with_capacity(limit);
        while hits.len() < limit {
            match (base.peek(), tail.peek()) {
                (None, None) => break,
                (Some(disk), Some(recent)) if recent.rank() < disk.rank() => {
                    hits.push(tail.next().unwrap().copy(&mut output)?);
                }
                (Some(_), _) => hits.push(base.next().unwrap()),
                (None, Some(_)) => hits.push(tail.next().unwrap().copy(&mut output)?),
            }
        }
        Ok(Window {
            cut: self.cut,
            evaluation_time_ms: self.evaluation,
            hits,
            _memory: memory,
        })
    }
}

impl Hit {
    fn rank(&self) -> (Reverse<i64>, &str) {
        (Reverse(self.event_time_ms), &self.id)
    }
}

struct TailHit<'a> {
    time: i64,
    row: &'a Record,
}

impl TailHit<'_> {
    fn rank(&self) -> (Reverse<i64>, &str) {
        (Reverse(self.time), &self.row.record_id)
    }

    fn copy(self, output: &mut OutputIds) -> Result<Hit, RecordError> {
        output.hit(
            &self.row.record_id,
            self.time,
            self.row.version,
            self.row.temporal.expire_at_ms,
        )
    }
}

#[derive(Default)]
struct OutputIds {
    bytes: usize,
}

impl OutputIds {
    fn hit(
        &mut self,
        id: &str,
        time: i64,
        version: u64,
        expires_at_ms: Option<u64>,
    ) -> Result<Hit, RecordError> {
        self.bytes = self
            .bytes
            .checked_add(id.len())
            .filter(|sum| *sum <= MAX_RESULT_ID_BYTES)
            .ok_or_else(|| unavailable("result identity budget exhausted"))?;
        let memory = reserve_memory(id.len() as u64).map_err(memory_error)?;
        Ok(Hit {
            id: id.to_owned(),
            event_time_ms: time,
            version,
            expires_at_ms,
            _id: memory,
        })
    }
}

// Hold the native result after releasing its read worker so the tests can
// publish another real checkpoint at the same sequence before delivery.
#[cfg(test)]
async fn pause_delivery(inner: &Mutex<Inner>, deadline: Deadline) {
    let root = inner.lock().await.journal.root().to_owned();
    let gate = root.join(".test-event-time-delivery-pause");
    if gate.exists() {
        std::fs::write(root.join(".test-event-time-delivery-started"), b"ready").unwrap();
        while gate.exists() && Deadline::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }
}
