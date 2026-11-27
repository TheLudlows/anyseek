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
