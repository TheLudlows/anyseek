//! Independent lexical and vector/metadata consumption behind Record decisions.
//! Only immutable, bounded input batches cross the publication guard. The WAL
//! remains the recovery source; these cursors describe disposable index caches.
use super::*;
use futures::FutureExt;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub(super) struct Cursors {
    pub lexical: AtomicU64,
    pub vector: AtomicU64,
    error: std::sync::Mutex<Option<String>>,
}
impl Cursors {
    pub fn recovered(sequence: u64) -> Arc<Self> {
        Arc::new(Self {
            lexical: AtomicU64::new(sequence),
            vector: AtomicU64::new(sequence),
            error: Default::default(),
        })
    }
    pub fn error(&self) -> Option<String> {
        self.error
            .lock()
            .map(|error| error.clone())
            .unwrap_or(Some("index consumer poisoned; reopen required".into()))
    }
    pub fn fail(&self, error: impl std::fmt::Display) {
        if let Ok(mut failed) = self.error.lock() {
            *failed = Some(error.to_string());
        }
    }
    pub fn immediate(&self, sequence: u64) {
        self.vector.fetch_max(sequence, Ordering::Release);
        self.lexical.fetch_max(sequence, Ordering::Release);
    }
}

pub(super) struct Input {
    identity: lexical::Identity,
    ordinal: usize,
    fingerprint: Option<[u8; 32]>,
    document: Document,
    _memory: crate::storage::build_budget::MemoryLease,
    _scope: Option<crate::resources::Lease>,
}
impl Input {
    pub fn admitted(
        native: &lexical::runtime::Runtime,
        identity: lexical::Identity,
        document: &Document,
    ) -> Result<Self, RecordError> {
        // Copy only lexical fields. Vectors and canonical bodies remain owned by
        // their respective components and Record; this is transient work input.
        let bytes = build_budget::document_estimate(document, &native.config.schema)?;
        let memory = crate::storage::build_budget::reserve_memory(bytes).map_err(storage)?;
        let scope = crate::resources::Scope::current()
            .map(|scope| scope.retained_io().bytes(bytes))
            .transpose()
            .map_err(storage)?;
        let document = native
            .config
            .schema
            .iter()
            .filter(|f| f.index_lexical)
            .filter_map(|field| {
                document
                    .get(&field.field)
                    .map(|value| (field.field.clone(), value.clone()))
            })
            .collect();
        native.reserve_queued_update();
        Ok(Self {
            identity,
            ordinal: 0,
            fingerprint: None,
            document,
            _memory: memory,
            _scope: scope,
        })
    }
    pub fn vector(
        config: &RecordConfig,
        identity: lexical::Identity,
        ordinal: usize,
        document: &Document,
    ) -> Result<Self, RecordError> {
        let bytes = build_budget::document_estimate(document, &[])?;
        let memory = crate::storage::build_budget::reserve_memory(bytes).map_err(storage)?;
        let scope = crate::resources::Scope::current()
            .map(|scope| scope.retained_io().bytes(bytes))
            .transpose()
            .map_err(storage)?;
        let fingerprint = Some(crate::active::fingerprint(document).map_err(storage)?);
        let document = retrieval_schema(config)
            .into_iter()
            .filter(|f| f.index_vector || f.facet)
            .filter_map(|field| {
                document
                    .get(&field.field)
                    .map(|value| (field.field, value.clone()))
            })
            .collect();
        Ok(Self {
            identity,
            ordinal,
            fingerprint,
            document,
            _memory: memory,
            _scope: scope,
        })
    }
}
#[derive(Clone)]
enum Engine {
    Lexical(Arc<lexical::runtime::Runtime>),
    Vector {
        index: VectorIndexArc,
        _native: Arc<lexical::runtime::Runtime>,
    },
}
impl Engine {
    fn name(&self) -> &'static str {
        match self {
            Self::Lexical(_) => "lexical",
            Self::Vector { .. } => "vector",
        }
    }
    async fn append(&self, input: Input, cursors: &Cursors) -> Result<(), RecordError> {
        match self {
            Self::Lexical(native) => native.append_queued(input.identity, &input.document).await,
            Self::Vector { index, .. } => {
                let component = index.read().await.segments[0].clone();
                #[cfg(test)]
                let fail_after = Path::new(&index.read().await.index_path_string)
                    .parent()
                    .unwrap()
                    .join(".test-vector-consumer-fail-after-append")
                    .exists();
                #[cfg(test)]
                let panic_after = Path::new(&index.read().await.index_path_string)
                    .parent()
                    .unwrap()
                    .join(".test-vector-consumer-panic-after-append")
                    .exists();
                let mut shard = component.write().await;
                // Global lock order: component guard, then I/O. No index/shard
                // lock is acquired while this permit is held. Wait before identity.
                let permit = match crate::resources::Scope::current() {
                    Some(scope) if !scope.has_file_io_loan() => Some(scope.journal_io().await.map_err(storage)?),
                    _ => None,
                };
                let append = async {
                let outcome = std::panic::AssertUnwindSafe(async {
                    if shard.failed {
                        return Err(RecordError::RecoveryRequired);
                    }
                    let active = shard
                        .active
                        .as_ref()
                        .ok_or_else(|| storage("vector statistics missing"))?;
                    if active.keys.len() != input.ordinal
                        || shard.indexed_doc_count != input.ordinal
                    {
                        return Err(storage("vector consumer input order mismatch"));
                    }
                    if let Some(memory) = &mut shard.build_reservation {
                        memory
                            .grow(build_budget::identity_estimate(&input.identity.id)?)
                            .map_err(storage)?;
                    }
                    shard.active.as_mut().unwrap().append_identity(input.identity.id, input.identity.version, input.identity.revision, input.fingerprint.ok_or_else(|| storage("vector fingerprint missing"))?);
                    shard.append(input.document).await.map_err(storage)?;
                    #[cfg(test)]
                    if panic_after {
                        panic!("injected vector panic after append");
                    }
                    #[cfg(test)]
                    if fail_after {
                        return Err(storage("injected vector failure after append"));
                    }
                    Ok(())
                })
                .catch_unwind()
                .await;
                let result = match outcome {
                    Ok(result) => result,
                    Err(_) => Err(RecordError::RecoveryRequired),
                };
                if let Err(error) = &result {
                    // Readers inspect this flag under this same component guard;
                    // no failed mmap/metadata mutation escapes before fencing.
                    shard.failed = true;
                    cursors.fail(error);
                }
                result
                };
                match permit {
                    Some(permit) => permit.run_async(append).await,
                    None => append.await,
                }
            }
        }
    }
    async fn flush(&self) -> Result<(), RecordError> {
        match self {
            Self::Lexical(native) => {
                native.flush().await?;
                native.merge_small_segments().await
            },
            Self::Vector { .. } => Ok(()),
        }
    }
    fn advance(&self, cursors: &Cursors, sequence: u64) {
        match self {
            Self::Lexical(_) => &cursors.lexical,
            Self::Vector { .. } => &cursors.vector,
        }
        .fetch_max(sequence, Ordering::Release);
    }
}
struct Batch {
    inputs: Vec<Input>,
    sequence: u64,
}
#[derive(Default)]
struct Queue {
    batches: VecDeque<Batch>,
    running: bool,
    worker: Option<tokio::task::JoinHandle<()>>,
}
#[derive(Clone)]
pub(super) struct Consumer {
    engine: Engine,
    owner: std::sync::Weak<Mutex<Inner>>,
    cursors: Arc<Cursors>,
    queue: Arc<std::sync::Mutex<Queue>>,
    #[cfg(any(test, feature = "fault-injection"))]
    root: PathBuf,
}
impl Consumer {
    pub fn new_lexical(
        native: Arc<lexical::runtime::Runtime>,
        cursors: Arc<Cursors>,
        _root: PathBuf,
        owner: std::sync::Weak<Mutex<Inner>>,
    ) -> Self {
        Self {
            engine: Engine::Lexical(native),
            owner,
            cursors,
            queue: Default::default(),
            #[cfg(any(test, feature = "fault-injection"))]
            root: _root,
        }
    }
    pub fn new_vector(
        index: VectorIndexArc,
        native: Arc<lexical::runtime::Runtime>,
        cursors: Arc<Cursors>,
        _root: PathBuf,
        owner: std::sync::Weak<Mutex<Inner>>,
    ) -> Self {
        Self {
            engine: Engine::Vector {
                index,
                _native: native,
            },
            owner,
            cursors,
            queue: Default::default(),
            #[cfg(any(test, feature = "fault-injection"))]
            root: _root,
        }
    }
    pub fn submit(&self, inputs: Vec<Input>, sequence: u64) -> Result<(), RecordError> {
        if self.cursors.error().is_some() {
            return Err(RecordError::RecoveryRequired);
        }
        let mut queue = self
            .queue
            .lock()
            .map_err(|_| RecordError::RecoveryRequired)?;
        // Native updates are independently limited to 128 per mutable tail;
        // bound delete-only batches too, even when no postings change.
        if queue.batches.len() >= 128 {
            return Err(RecordError::Busy);
        }
        queue.batches.push_back(Batch { inputs, sequence });
        if !queue.running {
            queue.running = true;
            let lane = self.clone();
            let owner = self.owner.upgrade().ok_or(RecordError::RecoveryRequired)?;
            queue.worker = Some(crate::resources::spawn_background(async move {
                let result = std::panic::AssertUnwindSafe(lane.consume())
                    .catch_unwind()
                    .await;
                if result.is_err() {
                    lane.cursors.fail(format!(
                        "{} consumer panicked; reopen required",
                        lane.engine.name()
                    ));
                    if let Ok(mut queue) = lane.queue.lock() {
                        queue.running = false;
                        queue.batches.clear();
                    }
                }
                // Release cache/writer handles before the last journal owner.
                drop(lane);
                drop(owner);
            }));
        }
        Ok(())
    }
    async fn consume(&self) {
        loop {
            let batch = {
                let mut queue = match self.queue.lock() {
                    Ok(queue) => queue,
                    Err(_) => {
                        self.cursors
                            .fail(format!("{} queue poisoned", self.engine.name()));
                        return;
                    }
                };
                match queue.batches.pop_front() {
                    Some(batch) => batch,
                    None => {
                        queue.running = false;
                        return;
                    }
                }
            };
            let result = async {
                #[cfg(any(test, feature = "fault-injection"))]
                {
                    let pause = self
                        .root
                        .join(format!(".test-{}-consumer-pause", self.engine.name()));
                    if pause.exists() {
                        std::fs::write(
                            self.root
                                .join(format!(".test-{}-consumer-started", self.engine.name())),
                            b"ready",
                        )
                        .map_err(storage)?;
                        let deadline =
                            std::time::Instant::now() + std::time::Duration::from_secs(20);
                        while pause.exists() {
                            if std::time::Instant::now() > deadline {
                                return Err(storage(format!(
                                    "{} test pause timed out",
                                    self.engine.name()
                                )));
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                        }
                    }
                    if self
                        .root
                        .join(format!(".test-{}-consumer-fail", self.engine.name()))
                        .exists()
                    {
                        return Err(storage(format!(
                            "injected {} consumer failure",
                            self.engine.name()
                        )));
                    }
                }
                let changed = !batch.inputs.is_empty();
                for input in batch.inputs {
                    self.engine.append(input, &self.cursors).await?;
                }
                if changed {
                    self.engine.flush().await?;
                }
                Ok::<_, RecordError>(())
            }
            .await;
            if let Err(error) = result {
                self.cursors.fail(error);
                if let Ok(mut queue) = self.queue.lock() {
                    queue.batches.clear();
                    queue.running = false;
                }
                return;
            }
            self.engine.advance(&self.cursors, batch.sequence);
        }
    }
    /// Administrative/synchronous paths join owned work before sealing a tail
    /// or relying on immediate visibility. Normal committed execution never joins this task.
    pub async fn drain(&self) -> Result<(), RecordError> {
        let worker = self
            .queue
            .lock()
            .map_err(|_| RecordError::RecoveryRequired)?
            .worker
            .take();
        if let Some(worker) = worker {
            worker.await.map_err(storage)?;
        }
        if self.cursors.error().is_some() {
            Err(RecordError::RecoveryRequired)
        } else {
            if let Engine::Lexical(native) = &self.engine {
                native.finish_consumption();
            }
            Ok(())
        }
    }
}

#[cfg(test)]
#[path = "index_consumer_io_tests.rs"]
mod io_tests;
