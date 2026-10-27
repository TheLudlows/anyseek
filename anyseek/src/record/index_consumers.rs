//! Independent native consumption behind Record's vector/metadata materializer.
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
            document,
            _memory: memory,
            _scope: scope,
        })
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
pub(super) struct Lexical {
    native: Arc<lexical::runtime::Runtime>,
    cursors: Arc<Cursors>,
    queue: Arc<std::sync::Mutex<Queue>>,
    #[cfg(test)]
    root: PathBuf,
}
impl Lexical {
    pub fn new(
        native: Arc<lexical::runtime::Runtime>,
        cursors: Arc<Cursors>,
        _root: PathBuf,
    ) -> Self {
        Self {
            native,
            cursors,
            queue: Default::default(),
            #[cfg(test)]
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
        // bound delete/staging-only batches too, even when no postings change.
        if queue.batches.len() >= 128 {
            return Err(RecordError::Busy);
        }
        queue.batches.push_back(Batch { inputs, sequence });
        if !queue.running {
            queue.running = true;
            let lane = self.clone();
            queue.worker = Some(crate::resources::spawn_background(async move {
                let result = std::panic::AssertUnwindSafe(lane.consume())
                    .catch_unwind()
                    .await;
                if result.is_err() {
                    lane.cursors
                        .fail("lexical consumer panicked; reopen required");
                    if let Ok(mut queue) = lane.queue.lock() {
                        queue.running = false;
                        queue.batches.clear();
                    }
                }
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
                        self.cursors.fail("lexical queue poisoned");
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
                #[cfg(test)]
                {
                    let pause = self.root.join(".test-lexical-consumer-pause");
                    if pause.exists() {
                        std::fs::write(self.root.join(".test-lexical-consumer-started"), b"ready")
                            .map_err(storage)?;
                        let deadline =
                            std::time::Instant::now() + std::time::Duration::from_secs(20);
                        while pause.exists() {
                            if std::time::Instant::now() > deadline {
                                return Err(storage("lexical test pause timed out"));
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                        }
                    }
                    if self.root.join(".test-lexical-consumer-fail").exists() {
                        return Err(storage("injected lexical consumer failure"));
                    }
                }
                let changed = !batch.inputs.is_empty();
                for input in batch.inputs {
                    self.native
                        .append_queued(input.identity, &input.document)
                        .await?;
                }
                if changed {
                    self.native.flush().await?;
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
            self.cursors
                .lexical
                .fetch_max(batch.sequence, Ordering::Release);
        }
    }
    /// Administrative/synchronous paths join owned work before sealing a tail
    /// or relying on immediate visibility. Normal intake never joins this task.
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
            self.native.finish_consumption();
            Ok(())
        }
    }
}
