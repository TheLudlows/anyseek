//! Shared ordinal membership for a published pair of frozen indexes.
use super::*;
use crate::{
    active::ActiveStatistics,
    component::RetrievalSegment,
    resources::MemoryReservation,
};
use scoring::FieldLengths;
use std::sync::{Mutex, Weak};

const PAGE_WORDS: usize = 16;
const PAGE_BITS: usize = PAGE_WORDS * u64::BITS as usize;

/// A shared mask changes only when Record retires a published revision. Old
/// queries keep their membership and field statistics together.
pub(super) struct LiveView {
    pages: Vec<Arc<Page>>,
    pub(super) fields: Vec<FieldLengths>,
    len: usize,
    _memory: MemoryReservation,
}
struct Page {
    bits: [u64; PAGE_WORDS],
    _memory: MemoryReservation,
}
impl Page {
    fn new(bits: [u64; PAGE_WORDS]) -> Result<Arc<Self>, RecordError> {
        let memory = crate::record::build_budget::retained_memory(256)?;
        Ok(Arc::new(Self { bits, _memory: memory }))
    }
}
impl LiveView {
    fn admit(page_count: usize, field_count: usize) -> Result<MemoryReservation, RecordError> {
        crate::record::build_budget::retained_memory(256 + (page_count + field_count) as u64 * 32)
    }

    fn new(snapshot: &Snapshot, active: &ActiveStatistics) -> Result<Self, RecordError> {
        let page_count = active.keys.len().div_ceil(PAGE_BITS);
        let memory = Self::admit(page_count, snapshot.fields.len())?;
        let mut view = Self {
            pages: Vec::with_capacity(page_count),
            fields: vec![FieldLengths::default(); snapshot.fields.len()],
            len: 0,
            _memory: memory,
        };
        for offset in (0..active.keys.len()).step_by(PAGE_BITS) {
            let mut bits = [0; PAGE_WORDS];
            for ordinal in offset..(offset + PAGE_BITS).min(active.keys.len()) {
                if active.is_live(ordinal) {
                    let (word, bit) = word_and_bit(ordinal);
                    bits[word] |= bit;
                    view.len += 1;
                    FieldLengths::add_document(&mut view.fields, snapshot.lengths_for(ordinal));
                }
            }
            view.pages.push(Page::new(bits)?);
        }
        Ok(view)
    }

    pub(super) fn contains(&self, ordinal: usize) -> bool {
        let (word, bit) = word_and_bit(ordinal);
        self.pages.get(ordinal / PAGE_BITS).is_some_and(|page| page.bits[word] & bit != 0)
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    fn without(&self, ordinal: usize, lengths: &[u64]) -> Result<Self, RecordError> {
        let memory = Self::admit(self.pages.len(), self.fields.len())?;
        let mut pages = self.pages.clone();
        let page = &mut pages[ordinal / PAGE_BITS];
        let mut bits = page.bits;
        let (word, bit) = word_and_bit(ordinal);
        bits[word] &= !bit;
        *page = Page::new(bits)?;
        let mut fields = self.fields.clone();
        FieldLengths::remove_document(&mut fields, lengths);
        Ok(Self { pages, fields, len: self.len - 1, _memory: memory })
    }
}

fn word_and_bit(ordinal: usize) -> (usize, u64) {
    let within = ordinal % PAGE_BITS;
    (within / 64, 1 << (within % 64))
}

struct Published {
    snapshot: Weak<Snapshot>,
    live: Arc<LiveView>,
}
impl Published {
    fn matches(&self, snapshot: &Arc<Snapshot>) -> bool {
        std::ptr::eq(self.snapshot.as_ptr(), Arc::as_ptr(snapshot))
    }
}

/// Disposable, admission-accounted search metadata. A stale or different pair
/// remains searchable through the general revision join.
#[derive(Default)]
pub(crate) struct PublishedSegment {
    current: Mutex<Option<Published>>,
}

impl PublishedSegment {
    pub(crate) async fn publish(segment: &RetrievalSegment) {
        let shard = segment.vector.read().await;
        let Some(active) = shard.active.as_ref() else { return };
        let Ok(snapshot) = segment.lexical.snapshot().await else { return };
        if !snapshot.overlay.is_empty()
            || snapshot.identities.len() != active.keys.len()
            || !aligned_prefix(&snapshot, active)
        {
            return;
        }
        if let Ok(mut current) = segment.visibility.current.lock() {
            if current.as_ref().is_some_and(|view| view.matches(&snapshot)) {
                return;
            }
            *current = LiveView::new(&snapshot, active).ok().map(|live| Published {
                snapshot: Arc::downgrade(&snapshot),
                live: Arc::new(live),
            });
        }
    }

    pub(super) fn capture(&self, snapshot: &Arc<Snapshot>, vector_rows: usize) -> Option<Arc<LiveView>> {
        let current = self.current.lock().ok()?;
        let published = current.as_ref()?;
        (published.matches(snapshot) && snapshot.identities.len() == vector_rows)
            .then(|| published.live.clone())
    }

    pub(crate) fn retire(&self, ordinal: usize) {
        let Ok(mut current) = self.current.lock() else { return };
        let Some(published) = current.as_mut() else { return };
        if !published.live.contains(ordinal) {
            return;
        }
        let Some(snapshot) = published.snapshot.upgrade() else {
            *current = None;
            return;
        };
        match published.live.without(ordinal, snapshot.lengths_for(ordinal)) {
            Ok(live) => published.live = Arc::new(live),
            Err(_) => *current = None,
        }
    }

    pub(crate) fn invalidate(&self) {
        if let Ok(mut current) = self.current.lock() {
            *current = None;
        }
    }
}

pub(super) fn aligned_prefix(snapshot: &Snapshot, active: &ActiveStatistics) -> bool {
    active.keys.len() == active.revisions.len()
        && snapshot.identities.iter().zip(active.keys.iter().zip(&active.revisions))
            .all(|(identity, (key, &(version, revision)))| {
                identity.id == *key && identity.version == version && identity.revision == revision
            })
}

#[cfg(test)]
mod tests {
    use crate::{
        record::{
            Applied, ExpectedVersion, RecordPartition, RecordQuery, RecordSearch, WriteReceipt,
            apply, checkpoint_codec,
            tests::{config, write},
        },
        resources::{Governor, Lane},
        search::{FacetFilter, QueryType, ResultType, SearchMode},
    };
    use std::{path::PathBuf, sync::Arc};

    async fn seed_lexical_fixture(partition: &RecordPartition, rows: usize, matches: usize) {
        const BATCH_ROWS: usize = 1024;
        const MAX_APPLIED_WIRE_BYTES: usize = 1024;
        let mut inner = partition.inner.lock().await;
        assert_eq!(inner.state.seq, 0, "fixture requires a fresh collection");
        let observed_time_ms = inner.observe_time().unwrap();
        let mut state = inner.state.try_clone().unwrap();
        for ordinal in 0..rows {
            if ordinal % BATCH_ROWS == 0 {
                let batch_rows = (rows - ordinal).min(BATCH_ROWS);
                state.admit_replay_frame(batch_rows * MAX_APPLIED_WIRE_BYTES).unwrap();
            }
            let id = format!("r{ordinal:06}");
            let mut body = "common ".repeat(1 + ordinal % 5);
            if ordinal < matches {
                body.push_str("rareterm");
            }
            let mut request = write(&format!("seed-{ordinal}"), &id, &body, ExpectedVersion::Absent);
            request.document.as_mut().unwrap().insert("priority".into(), serde_json::json!(ordinal));
            let receipt = WriteReceipt { seq: ordinal as u64 + 1, version: 1, record_id: id };
            let applied = Applied {
                observed_epoch: request.operation_id.epoch,
                observed_time_ms,
                request,
                receipt,
            };
            assert!(checkpoint_codec::json_length(&applied).unwrap() <= MAX_APPLIED_WIRE_BYTES);
            apply(&mut state, &applied).unwrap();
        }
        assert_eq!(inner.publish_anchor_state(state, false).await.unwrap(), rows as u64);
    }

    #[tokio::test]
    #[ignore = "offline benchmark fixture; requires a fresh disposable collection"]
    async fn prepare_lexical_scale_fixture() {
        let path = PathBuf::from(std::env::var_os("ANYSEEK_LEXICAL_FIXTURE_PARTITION").unwrap());
        let rows = std::env::var("ANYSEEK_LEXICAL_FIXTURE_ROWS").unwrap().parse::<usize>().unwrap();
        assert!((100..=100_000).contains(&rows));
        let partition = RecordPartition::open(&path, 256 * 1024 * 1024).await.unwrap();
        seed_lexical_fixture(&partition, rows, 100).await;
        partition.finish_service_shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn frozen_membership_and_bm25_survive_retirement_without_query_workspace() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("partition");
        let mut config = config();
        config.max_journal_bytes = 16 * 1024 * 1024;
        config.schema.push(serde_json::from_value(serde_json::json!({
            "field":"priority", "field_type":"Int", "store":false, "facet":true, "index_lexical":false
        })).unwrap());
        let partition = RecordPartition::create(&path, config.clone()).await.unwrap();
        let rows = 1024;
        seed_lexical_fixture(&partition, rows, 8).await;
        partition.finish_service_shutdown().await.unwrap();
        drop(partition);
        let partition = RecordPartition::open(&path, config.max_journal_bytes).await.unwrap();
        partition.write(write("edit", "r000000", "common rareterm repeated", ExpectedVersion::Match(1)))
            .await.unwrap();
        let mut delete = write("delete", "r000001", "", ExpectedVersion::Match(1));
        delete.document = None;
        partition.write(delete).await.unwrap();

        let query = || RecordQuery {
            text: "rareterm".into(), vector: None, mode: SearchMode::Lexical,
            query_type: QueryType::Union, offset: 0, length: 10, result_type: ResultType::TopkCount,
            fields: vec![], filters: vec![FacetFilter::Int {
                field: "priority".into(), filter: 0..5,
            }], sort: vec![], temporal: None,
        };
        let scope = Arc::new(Governor::default()).admit(
            "frozen-view".into(), Lane::Foreground, 512 * 1024 * 1024 - 384 * 1024,
        ).unwrap();
        let fast = scope.clone().run(partition.query(query())).await.unwrap();
        assert_eq!(fast.count_total, 3);
        assert_eq!(fast.hits.len(), 3);
        assert_eq!(fast.statistics.physical_versions, rows + 1);

        let mut count = query();
        count.text = "common".into();
        count.length = 0;
        count.result_type = ResultType::Count;
        count.filters = vec![FacetFilter::Int {
            field: "priority".into(), filter: 0..rows as i64,
        }];
        let counted = scope.run(partition.query(count)).await.unwrap();
        assert_eq!(counted.count_total, rows - 2);
        assert!(counted.hits.is_empty());
        {
            let inner = partition.inner.lock().await;
            let index = inner.index.test_index().read().await;
            for segment in &index.segments {
                segment.visibility.invalidate();
            }
        }
        let reference = partition.query(query()).await.unwrap();
        let signature = |result: RecordSearch| {
            (result.count_total, result.hits.into_iter().map(|hit| {
                (hit.record.record_id.clone(), hit.score.to_bits())
            }).collect::<Vec<_>>())
        };
        assert_eq!(signature(fast), signature(reference));
        partition.finish_service_shutdown().await.unwrap();
    }
}
