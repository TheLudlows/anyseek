//! Bounded service admission and explicit buffer/I/O accounting.
//!
//! These are conservative reservations, not a process allocator or an RSS cap.
//! Admission never queues tasks. Leases follow the actual task/buffer owner.
//! Already-admitted native transactions may wait for I/O on dedicated blocking
//! workers; ordinary admission, bytes, and I/O requests still refuse immediately.
use std::{collections::BTreeMap, future::Future, sync::{Arc, Condvar, LazyLock, Mutex, atomic::{AtomicBool, Ordering}}};

/// Independent resource classes. Control capacity is never lent to data work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lane {
    /// Ordinary reads and writes.
    Foreground,
    /// Creation, restore and recovery downloads.
    Recovery,
    /// Index construction and maintenance exports.
    Background,
    /// Trusted authorization and lifecycle controls.
    Control,
    /// Liveness and static assets.
    Public,
}
impl Lane {
    const ALL: [Self; 5] = [Self::Foreground, Self::Recovery, Self::Background, Self::Control, Self::Public];
    fn index(self) -> usize { self as usize }
    fn name(self) -> &'static str { match self {
        Self::Foreground => "FOREGROUND", Self::Recovery => "RECOVERY", Self::Background => "BACKGROUND",
        Self::Control => "CONTROL", Self::Public => "PUBLIC",
    } }
}
/// Limits for one class, across the governor and for each stable subject.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Accepted tasks, including detached work and undelivered output.
    pub slots: usize,
    /// Accepted tasks belonging to one collection (or the service itself).
    pub subject_slots: usize,
    /// Reserved explicit buffer bytes.
    pub bytes: u64,
    /// Reserved explicit buffer bytes for one subject.
    pub subject_bytes: u64,
    /// Concurrent object I/O operations.
    pub io: usize,
    /// Concurrent object I/O operations for one subject.
    pub subject_io: usize,
}
impl Limits {
    fn defaults(lane: Lane) -> Self {
        let mib = 1024 * 1024;
        let (slots, subject_slots, bytes, subject_bytes, io, subject_io) = match lane {
            Lane::Foreground => (32, 16, 2048*mib, 512*mib, 8, 2),
            Lane::Recovery => (2, 1, 512*mib, 256*mib, 2, 1),
            Lane::Background => (2, 1, 4096*mib, 2048*mib, 2, 1),
            Lane::Control => (8, 2, 144*mib, 36*mib, 2, 1),
            Lane::Public => (8, 8, 144*mib, 144*mib, 1, 1),
        };
        Self { slots, subject_slots, bytes, subject_bytes, io, subject_io }
    }
}
#[derive(Clone, Copy, Default, Debug)]
struct Usage { slots: usize, bytes: u64, io: usize }
impl Usage {
    fn add(self, n: Self, cap: Limits, subject: bool) -> Option<Self> {
        let sum = Self { slots: self.slots.checked_add(n.slots)?, bytes: self.bytes.checked_add(n.bytes)?, io: self.io.checked_add(n.io)? };
        (sum.slots <= if subject {cap.subject_slots} else {cap.slots}
            && sum.bytes <= if subject {cap.subject_bytes} else {cap.bytes}
            && sum.io <= if subject {cap.subject_io} else {cap.io}).then_some(sum)
    }
    fn empty(self) -> bool { self.slots == 0 && self.bytes == 0 && self.io == 0 }
    fn subtract(&mut self, n: Self) { self.slots -= n.slots; self.bytes -= n.bytes; self.io -= n.io; }
}
#[derive(Default)]
struct Ledger { total: [Usage; 5], subjects: BTreeMap<String, [Usage; 5]> }
/// Shared budgets. Use one governor for all collections served by a service.
pub struct Governor { limits: [Limits; 5], ledger: Mutex<Ledger>, io_available: Condvar }
/// A definite refusal to acquire resources, before the guarded I/O/allocation.
#[derive(Debug)]
pub struct Busy { detail: String }
impl Busy { fn new(detail: impl Into<String>) -> Self { Self { detail: detail.into() } } }
impl std::fmt::Display for Busy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "resource budget exhausted: {}", self.detail) }
}
impl std::error::Error for Busy {}
impl Default for Governor {
    fn default() -> Self { Self { limits: Lane::ALL.map(Limits::defaults), ledger: Mutex::new(Ledger::default()), io_available: Condvar::new() } }
}
impl Governor {
    /// Load optional ANYSEEK_{LANE}_{SLOTS,PER_COLLECTION,BYTES,PER_COLLECTION_BYTES,IO,PER_COLLECTION_IO}.
    /// Reject malformed/unsafe configuration, including loss of catalog control headroom.
    pub fn from_env() -> Result<Arc<Self>, String> {
        crate::storage::build_budget::validate_configuration()?;
        let mut governor = Self::default();
        for lane in Lane::ALL {
            let limits = &mut governor.limits[lane.index()];
            let read = |suffix: &str, default: u64| -> Result<u64, String> {
                let key = format!("ANYSEEK_{}_{}", lane.name(), suffix);
                match std::env::var(&key) {
                    Ok(value) => value.parse::<u64>().ok().filter(|n| *n > 0).ok_or_else(|| format!("invalid {key}")),
                    Err(std::env::VarError::NotPresent) => Ok(default),
                    Err(_) => Err(format!("invalid {key}")),
                }
            };
            let number = |suffix, default| usize::try_from(read(suffix, default as u64)?).map_err(|_| "resource limit overflow".to_owned());
            limits.slots = number("SLOTS", limits.slots)?;
            limits.subject_slots = number("PER_COLLECTION", limits.subject_slots)?;
            limits.bytes = read("BYTES", limits.bytes)?;
            limits.subject_bytes = read("PER_COLLECTION_BYTES", limits.subject_bytes)?;
            limits.io = number("IO", limits.io)?;
            limits.subject_io = number("PER_COLLECTION_IO", limits.subject_io)?;
            if limits.subject_slots > limits.slots || limits.subject_bytes > limits.bytes || limits.subject_io > limits.io
                || limits.slots > 64 || limits.io > 64 || limits.subject_bytes < 18 * 1024 * 1024 || limits.bytes > 8 * 1024 * 1024 * 1024 {
                return Err(format!("invalid {} resource hierarchy", lane.name()));
            }
        }
        if governor.limits[0].slots + governor.limits[1].slots + governor.limits[2].slots > 48
            || governor.limits[3].slots > 8 { return Err("resource limits must preserve catalog control headroom".into()); }
        Ok(Arc::new(governor))
    }
    fn reserve(self: &Arc<Self>, subject: &str, lane: Lane, amount: Usage) -> Result<Lease, Busy> {
        let mut ledger = self.ledger.lock().map_err(|_| Busy::new("class=ledger_poisoned"))?;
        let i = lane.index();
        if subject.len() > 272 || (!ledger.subjects.contains_key(subject) && ledger.subjects.len() >= 128) { log::warn!("resource subject admission exhausted: retained_subjects={}",ledger.subjects.len());return Err(Busy::new("class=subject_registry")); }
        let total = ledger.total[i].add(amount, self.limits[i], false).ok_or_else(|| {
            log::warn!("resource global admission exhausted: lane={lane:?} used={:?} requested={amount:?} limits={:?}",ledger.total[i],self.limits[i]);Busy::new(format!("class=global lane={lane:?} used={:?} requested={amount:?} limits={:?}",ledger.total[i],self.limits[i]))
        })?;
        let used=ledger.subjects.get(subject).map(|u|u[i]).unwrap_or_default();
        let local = used.add(amount, self.limits[i], true).ok_or_else(|| {
            log::warn!("resource collection admission exhausted: lane={lane:?} used={used:?} requested={amount:?} limits={:?}",self.limits[i]);Busy::new(format!("class=collection lane={lane:?} used={used:?} requested={amount:?} limits={:?}",self.limits[i]))
        })?;
        ledger.total[i] = total;
        ledger.subjects.entry(subject.into()).or_default()[i] = local;
        Ok(Lease { governor: self.clone(), subject: subject.into(), lane, amount })
    }
    // Only I/O saturation is waitable. Invalid identities, poisoned accounting,
    // and impossible capacity are permanent refusals, never retry loops.
    fn native_io_wait(self: &Arc<Self>, subject: &str, lane: Lane, cancelled: &AtomicBool) -> Result<Lease, Busy> {
        let mut ledger = self.ledger.lock().map_err(|_| Busy::new("class=ledger_poisoned"))?;
        let i = lane.index();
        let cap = self.limits[i];
        loop {
            if cancelled.load(Ordering::Acquire) { return Err(Busy::new("class=native_cancelled")); }
            if subject.len() > 272 || (!ledger.subjects.contains_key(subject) && ledger.subjects.len() >= 128) {
                return Err(Busy::new("class=subject_registry"));
            }
            if cap.io == 0 || cap.subject_io == 0 { return Err(Busy::new("class=io_capacity")); }
            let local = ledger.subjects.get(subject).map(|u| u[i]).unwrap_or_default();
            if ledger.total[i].io < cap.io && local.io < cap.subject_io {
                ledger.total[i].io += 1;
                ledger.subjects.entry(subject.into()).or_default()[i].io += 1;
                return Ok(Lease { governor: self.clone(), subject: subject.into(), lane,
                    amount: Usage { io: 1, ..Usage::default() } });
            }
            ledger = self.io_available.wait(ledger).map_err(|_| Busy::new("class=ledger_poisoned"))?;
        }
    }
    /// Admit before reading large bodies. `bytes` reserves body/output workspace.
    /// Subjects must come from trusted stable collection identities; unknown IDs share a service subject.
    pub fn admit(self: &Arc<Self>, subject: String, lane: Lane, bytes: u64) -> Result<Scope, Busy> {
        let lease = self.reserve(&subject, lane, Usage {slots: 1, bytes, io: 0})?;
        Ok(Scope { governor: self.clone(), subject, lane, _entry: Some(Arc::new(lease)) })
    }
}
/// A reservation released only when its actual owner is dropped.
pub struct Lease { governor: Arc<Governor>, subject: String, lane: Lane, amount: Usage }
impl Lease {
    /// Extend a retained byte reservation before its owner allocates more storage.
    pub(crate) fn grow_bytes(&mut self, bytes: u64) -> Result<(), Busy> {
        let next = self.amount.bytes.checked_add(bytes).ok_or_else(|| Busy::new("class=byte_overflow"))?;
        let mut added = self.governor.reserve(&self.subject, self.lane, Usage { bytes, ..Usage::default() })?;
        self.amount.bytes = next;
        added.amount.bytes = 0;
        Ok(())
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut ledger = self.governor.ledger.lock().unwrap_or_else(|e| e.into_inner());
        ledger.total[self.lane.index()].subtract(self.amount);
        if let Some(local) = ledger.subjects.get_mut(&self.subject) {
            local[self.lane.index()].subtract(self.amount);
            if local.iter().all(|u| u.empty()) { ledger.subjects.remove(&self.subject); }
        }
        self.governor.io_available.notify_all();
    }
}
/// Cloneable task context; clones share the original admission reservation.
#[derive(Clone)]
pub struct Scope { governor: Arc<Governor>, subject: String, lane: Lane, _entry: Option<Arc<Lease>> }
tokio::task_local! { static CURRENT: Scope; }
// A complete file transaction borrows one pre-admitted permit in this task.
// This context is never inherited by spawned tasks or native index workers.
tokio::task_local! { static JOURNAL_IO: Scope; }
pub(crate) struct JournalIo { scope: Scope, _lease: Lease }
impl JournalIo {
    pub(crate) fn run<T>(self, work: impl FnOnce() -> T) -> T {
        JOURNAL_IO.sync_scope(self.scope.clone(), work)
    }
    /// Keep ownership through a serial file-only async transaction. Do not
    /// enter native/S3 operations that independently acquire the same permit.
    /// Children spawned by the transaction deliberately do not inherit its loan.
    pub(crate) async fn run_async<F: Future>(self, work: F) -> F::Output {
        let result = JOURNAL_IO.scope(self.scope.clone(), work).await;
        drop(self);
        result
    }
}
impl Scope {
    /// Capture the context before spawning; task locals are not implicitly inherited.
    pub fn current() -> Option<Self> { CURRENT.try_with(Clone::clone).ok() }
    /// Retained storage uses background capacity and the same subject, without
    /// borrowing control capacity or retaining a completed request slot.
    pub(crate) fn retained_io(&self) -> Self { Self { governor:self.governor.clone(),subject:self.subject.clone(),lane:Lane::Background,_entry:None } }
    /// Run work under this context, including synchronous response serialization.
    pub async fn run<F: Future>(self, future: F) -> F::Output { CURRENT.scope(self, future).await }
    /// Propagate accounting explicitly into a synchronous blocking worker.
    pub(crate) fn run_sync<T>(self, work: impl FnOnce() -> T) -> T { CURRENT.sync_scope(self, work) }
    /// Internal native transaction exception: call only on a dedicated blocking
    /// or Tantivy worker, never an executor thread, while holding no I/O lease
    /// or shared native I/O gate. The existing admitted owner bounds waiters.
    pub(crate) fn native_io_wait(&self, cancelled: &AtomicBool) -> Result<Lease, Busy> {
        self.governor.native_io_wait(&self.subject, self.lane, cancelled)
    }
    /// Wake this owner's native waiters before synchronously joining its workers.
    /// Taking the predicate mutex prevents a cancellation/wait lost wakeup.
    pub(crate) fn cancel_native_io(&self, cancelled: &AtomicBool) {
        let _ledger = self.governor.ledger.lock().unwrap_or_else(|e| e.into_inner());
        cancelled.store(true, Ordering::Release);
        self.governor.io_available.notify_all();
    }
    /// Reserve explicit buffer bytes before allocation, retained by the buffer owner.
    pub fn bytes(&self, bytes: u64) -> Result<Lease, Busy> { self.governor.reserve(&self.subject, self.lane, Usage {bytes, ..Usage::default()}) }
    /// Acquire I/O capacity immediately; there is no hidden waiting queue.
    pub fn io(&self) -> Result<Lease, Busy> { self.governor.reserve(&self.subject, self.lane, Usage {io: 1, ..Usage::default()}) }
    /// Pre-admit one complete synchronous journal transaction. Waiting happens
    /// off the executor; the caller must hold no I/O permit while awaiting it.
    pub(crate) async fn journal_io(&self) -> Result<JournalIo, Busy> {
        let scope = self.clone();
        tokio::task::spawn_blocking(move || {
            let lease = scope.native_io_wait(&AtomicBool::new(false))?;
            Ok(JournalIo { scope, _lease: lease })
        }).await.map_err(|error| Busy::new(format!("class=io_worker {error}")))?
    }
    /// File leaves within a journal transaction share its permit, never another
    /// subject's/lane's capacity. All other file operations keep immediate admission.
    pub(crate) fn file_io(&self) -> Result<Option<Lease>, Busy> {
        if self.has_file_io_loan() { Ok(None) } else { self.io().map(Some) }
    }
    /// Check only the currently polled transaction; never return an unowned
    /// borrowed permit that could escape its owner's lifetime.
    pub(crate) fn has_file_io_loan(&self) -> bool {
        JOURNAL_IO.try_with(|scope| Arc::ptr_eq(&scope.governor, &self.governor)
            && scope.subject == self.subject && scope.lane == self.lane).unwrap_or(false)
    }
    /// Use an independent I/O class while retaining the caller's accepted task reservation.
    pub fn in_lane(mut self, lane: Lane) -> Self { self.lane = lane; self }
}
static CORE: LazyLock<Arc<Governor>> = LazyLock::new(|| Arc::new(Governor::default()));
/// Object I/O also has bounded fallback accounting for standalone core consumers.
pub(crate) fn io_scope(lane: Lane) -> Scope {
    Scope::current().unwrap_or_else(|| Scope { governor: CORE.clone(), subject: "core".into(), lane, _entry: None })
}
pub(crate) fn isolated_scope() -> Scope {
    Scope { governor: Arc::new(Governor::default()), subject: "core".into(), lane: Lane::Foreground, _entry: None }
}
/// Capture now, before a Tokio/JoinSet child is polled on another worker.
pub(crate) fn inherit<F: Future>(future: F) -> impl Future<Output = F::Output> {
    let context = Scope::current();
    async move { match context { Some(scope) => scope.run(future).await, None => future.await } }
}
/// Spawn on the index runtime, retaining request context after caller cancellation.
pub(crate) fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where F: Future + Send + 'static, F::Output: Send + 'static {
    let context = Scope::current();
    crate::INDEX_RUNTIME.spawn(async move { match context { Some(scope) => scope.run(future).await, None => future.await } })
}
/// Like `spawn`, with a distinct background I/O class.
pub(crate) fn spawn_background<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where F: Future + Send + 'static, F::Output: Send + 'static {
    let context = Scope::current().map(|scope| scope.in_lane(Lane::Background));
    crate::INDEX_RUNTIME.spawn(async move { match context { Some(scope) => scope.run(future).await, None => future.await } })
}
/// Verified bytes keep their charge until the caller has finished consuming them.
pub(crate) struct Buffer { bytes: Vec<u8>, _lease: Lease }
impl Buffer { pub(crate) fn new(bytes: Vec<u8>, lease: Lease) -> Self { Self { bytes, _lease: lease } } }
impl std::ops::Deref for Buffer { type Target = [u8]; fn deref(&self) -> &[u8] { &self.bytes } }
impl AsRef<[u8]> for Buffer { fn as_ref(&self) -> &[u8] { &self.bytes } }
impl<T: AsRef<[u8]>> PartialEq<T> for Buffer { fn eq(&self, other: &T) -> bool { self.bytes == other.as_ref() } }
impl std::fmt::Debug for Buffer { fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.debug_tuple("Buffer").field(&self.bytes.len()).finish() } }

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test(flavor = "current_thread")]
    async fn file_io_async_loan_survives_yield_without_escaping_to_children() {
        let scope = Arc::new(Governor::default()).admit("async-file".into(), Lane::Background, 0).unwrap();
        scope.clone().run(async {
            let permit = scope.journal_io().await.unwrap();
            permit.run_async(async {
                assert!(scope.has_file_io_loan());
                tokio::task::yield_now().await;
                assert!(scope.file_io().unwrap().is_none());
                let child_scope = scope.clone();
                assert!(spawn(async move {
                    !child_scope.has_file_io_loan() && child_scope.file_io().is_err()
                }).await.unwrap());
                assert!(scope.io().is_err(), "transaction still owns real capacity");
            }).await;
            assert!(!scope.has_file_io_loan());
            assert!(scope.io().is_ok());
        }).await;
    }
    #[tokio::test(flavor = "current_thread")]
    async fn journal_io_loan_is_synchronous_scoped_and_covers_both_witnesses() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("journal");
        let mut journal = crate::storage::journal::Journal::create(&path, 65536).unwrap();
        let governor = Arc::new(Governor::default());
        let scope = governor.admit("journal".into(), Lane::Background, 0).unwrap();
        let other = governor.admit("other".into(), Lane::Foreground, 0).unwrap().in_lane(Lane::Background);
        let other_held = other.io().unwrap();
        let foreground = scope.clone().in_lane(Lane::Foreground);
        let foreground_one = foreground.io().unwrap(); let foreground_two = foreground.io().unwrap();
        let independent = Arc::new(Governor::default()).admit("journal".into(), Lane::Background, 0).unwrap();
        let independent_held = independent.io().unwrap();
        scope.clone().run(async {
            let permit = scope.journal_io().await.unwrap();
            permit.run(|| {
                assert!(scope.io().is_err(), "one real permit remains charged");
                assert!(scope.file_io().unwrap().is_none());
                assert!(other.file_io().is_err());
                assert!(foreground.file_io().is_err());
                assert!(independent.file_io().is_err());
                let (sent, received) = std::sync::mpsc::channel();
                let escaped = scope.clone();
                spawn(async move { sent.send(escaped.file_io().is_err()).unwrap(); });
                assert!(received.recv_timeout(std::time::Duration::from_secs(2)).unwrap(), "spawn must not inherit a synchronous loan");
                assert_eq!(journal.append(b"one complete frame").unwrap(), 1);
            });
            assert!(scope.file_io().unwrap().is_some(), "loan ends with the synchronous transaction");
        }).await;
        drop((journal, other_held, foreground_one, foreground_two, independent_held));
        let (_, frames) = crate::storage::journal::Journal::open(&path, 65536).unwrap();
        assert_eq!(frames, vec![b"one complete frame".to_vec()]);
    }
    #[tokio::test(flavor = "current_thread")]
    async fn native_io_wait_preserves_scope_and_allows_async_holder_to_release() {
        let governor = Arc::new(Governor::default());
        let scope = governor.admit("native".into(), Lane::Background, 0).unwrap();
        let held = scope.io().unwrap();
        assert!(scope.io().is_err()); // Ordinary admission remains immediate.
        let context = scope.clone();
        let (start, started) = tokio::sync::oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || context.run_sync(|| {
            let current = Scope::current().unwrap();
            let bytes = current.bytes(7).unwrap();
            start.send(()).unwrap();
            let io = current.native_io_wait(&AtomicBool::new(false)).unwrap();
            (bytes, io)
        }));
        started.await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        assert!(!worker.is_finished());
        drop(held);
        let (bytes, io) = tokio::time::timeout(std::time::Duration::from_secs(2), worker).await.unwrap().unwrap();
        assert_eq!(governor.ledger.lock().unwrap().total[Lane::Background.index()].bytes, 7);
        assert!(scope.io().is_err());
        drop((bytes, io, scope));
        assert!(governor.ledger.lock().unwrap().subjects.is_empty());
    }
    #[tokio::test(flavor = "current_thread")]
    async fn native_io_cancellation_wakes_without_refunding_another_owner() {
        let scope = Arc::new(Governor::default()).admit("native".into(), Lane::Background, 0).unwrap();
        let held = scope.io().unwrap();
        let cancelled = Arc::new(AtomicBool::new(false));
        let context = scope.clone();
        let flag = cancelled.clone();
        let worker = tokio::task::spawn_blocking(move || context.native_io_wait(&flag).err().unwrap().detail);
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        scope.cancel_native_io(&cancelled);
        let detail = tokio::time::timeout(std::time::Duration::from_secs(2), worker).await.unwrap().unwrap();
        assert_eq!(detail, "class=native_cancelled");
        assert!(scope.io().is_err());
        drop(held);
        assert!(scope.io().is_ok());
    }
    #[test]
    fn native_io_permanent_refusals_never_wait() {
        let governor = Arc::new(Governor::default());
        let flag = AtomicBool::new(false);
        assert_eq!(governor.native_io_wait(&"x".repeat(273), Lane::Background, &flag).err().unwrap().detail, "class=subject_registry");
        let mut zero = Governor::default();
        zero.limits[Lane::Background.index()].io = 0;
        assert_eq!(Arc::new(zero).native_io_wait("x", Lane::Background, &flag).err().unwrap().detail, "class=io_capacity");
        let poisoned = governor.clone();
        let _ = std::thread::spawn(move || { let _guard = poisoned.ledger.lock().unwrap(); panic!("test poisoned ledger"); }).join();
        assert_eq!(governor.native_io_wait("x", Lane::Background, &flag).err().unwrap().detail, "class=ledger_poisoned");
    }
    #[tokio::test]
    async fn hot_subject_refuses_before_global_exhaustion_and_control_has_reserved_capacity() {
        let governor = Arc::new(Governor::default());
        let mut hot = Vec::new();
        for _ in 0..16 { hot.push(governor.admit("a".into(), Lane::Foreground, 18*1024*1024).unwrap()); }
        assert!(governor.admit("a".into(), Lane::Foreground, 1).is_err());
        let cold = governor.admit("b".into(), Lane::Foreground, 1).unwrap();
        for id in 0..15 { hot.push(governor.admit(id.to_string(), Lane::Foreground, 1).unwrap()); }
        assert!(governor.admit("other".into(), Lane::Foreground, 1).is_err());
        assert!(governor.admit("a".into(), Lane::Control, 1).is_ok());
        drop(cold); drop(hot);
        assert!(governor.ledger.lock().unwrap().subjects.is_empty());
    }
    #[test]
    fn retained_bytes_grow_atomically_and_release_the_full_reservation() {
        let governor = Arc::new(Governor::default());
        let scope = governor.admit("native-identities".into(), Lane::Background, 0).unwrap();
        let cap = governor.limits[Lane::Background.index()].subject_bytes;
        let mut bytes = scope.bytes(cap - 1).unwrap();
        assert!(bytes.grow_bytes(2).is_err());
        bytes.grow_bytes(1).unwrap();
        assert!(scope.bytes(1).is_err());
        drop(bytes);
        assert!(scope.bytes(cap).is_ok());
        drop(scope);
        assert!(governor.ledger.lock().unwrap().subjects.is_empty());
    }
    #[tokio::test]
    async fn cancellation_keeps_admission_and_buffer_bytes_until_actual_owner_finishes() {
        let governor = Arc::new(Governor::default());
        let scope = governor.admit("a".into(), Lane::Foreground, 5).unwrap();
        let (started, wait) = tokio::sync::oneshot::channel();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let handle = scope.run(async {
            spawn(async move {
                let buffer = Buffer::new(vec![0; 7], Scope::current().unwrap().bytes(7).unwrap());
                started.send(()).unwrap(); finished.await.unwrap(); buffer
            })
        }).await;
        wait.await.unwrap();
        assert_eq!(governor.ledger.lock().unwrap().total[0].bytes, 12);
        finish.send(()).unwrap();
        let buffer = handle.await.unwrap();
        assert_eq!(governor.ledger.lock().unwrap().total[0].bytes, 7);
        drop(buffer);
        assert!(governor.ledger.lock().unwrap().subjects.is_empty());
    }
    #[tokio::test]
    async fn cancelling_waiter_does_not_refund_owned_task() {
        let governor = Arc::new(Governor::default());
        let scope = governor.admit("a".into(), Lane::Foreground, 5).unwrap();
        let (start, started) = tokio::sync::oneshot::channel();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let (done, completed) = tokio::sync::oneshot::channel();
        let waiter = tokio::spawn(scope.run(async move {
            spawn(async move { start.send(()).unwrap(); finished.await.unwrap(); done.send(()).unwrap(); }).await.unwrap();
        }));
        started.await.unwrap();waiter.abort();let _ = waiter.await;
        assert_eq!(governor.ledger.lock().unwrap().total[0].slots, 1);
        finish.send(()).unwrap();completed.await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !governor.ledger.lock().unwrap().subjects.is_empty() { tokio::task::yield_now().await; }
        }).await.unwrap();
    }
    #[test]
    fn io_and_bytes_are_independently_bounded_and_classed() {
        let governor = Arc::new(Governor::default());
        let scope = governor.admit("a".into(), Lane::Foreground, 0).unwrap();
        let one = scope.io().unwrap(); let two = scope.io().unwrap();
        assert!(scope.io().is_err());
        let bytes = scope.bytes(512*1024*1024).unwrap();
        assert!(scope.bytes(1).is_err());
        assert!(scope.clone().in_lane(Lane::Recovery).io().is_ok());
        assert!(governor.admit("b".into(), Lane::Foreground, 1).is_ok());
        drop(bytes); drop(one); drop(two); drop(scope);
        assert!(governor.ledger.lock().unwrap().subjects.is_empty());
    }
}

#[cfg(test)]
pub(crate) async fn hold_io_for_test(scope: &Scope) -> Lease {
    // Native append can return while its bounded worker finishes a short leaf
    // operation. Establish contention only after this fixture owns the slot.
    let started = std::time::Instant::now();
    loop {
        match scope.io() {
            Ok(lease) => return lease,
            Err(error) => {
                assert!(started.elapsed() < std::time::Duration::from_secs(5),
                    "test I/O slot did not become available: {error}");
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }
    }
}
