//! The HTTP owner first stops accepting and drops its router, then transfers the
//! service here. Cancelling a waiter never cancels this shutdown coordinator.
use super::*;

#[derive(Clone)]
pub(crate) struct Shutdown(tokio::sync::watch::Receiver<Option<Result<(), String>>>);
impl Shutdown {
    pub(crate) async fn wait(&mut self) -> Result<(), String> {
        loop {
            if let Some(result) = self.0.borrow().clone() { return result; }
            self.0.changed().await.map_err(|_| "shutdown coordinator exited without completion".to_owned())?;
        }
    }
}

impl Service {
    pub(crate) fn stop_accepting(&self) { self.lifecycle.stop(); }

    /// Transfer the transport owner after HTTP graceful shutdown. Old routers
    /// may finish dropping just after the server future; wait for those owners.
    /// Repeated calls share the same cancellation-safe completion.
    pub(crate) fn shutdown(self: Arc<Self>) -> Shutdown {
        self.stop_accepting();
        let mut slot = self.shutdown.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(shutdown) = &*slot { return shutdown.clone(); }
        let (done, received) = tokio::sync::watch::channel(None);
        let shutdown = Shutdown(received);
        *slot = Some(shutdown.clone());
        drop(slot);
        tokio::spawn(async move {
            let reclamation = self.reclamation.lock().unwrap_or_else(|e| e.into_inner()).take();
            if let Some(task) = reclamation {
                if let Err(error) = task.await {
                    retain_failed(done, self, format!("maintenance coordinator: {error}"));
                    return;
                }
            }
            self.lifecycle.drained().await;
            let mut owner = self;
            let mut service = loop {
                match Arc::try_unwrap(owner) {
                    Ok(service) => break service,
                    Err(remaining) => {
                        owner = remaining;
                        // Axum signals connection completion before the task
                        // destroys all router state. Avoid a hot spin or a
                        // false terminal failure during that small interval.
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                }
            };
            // Never wait for global work while holding writer/Inner/control.
            // Only after every producer has exited may we join retained
            // consumer handles and terminate native writers.
            for partition in service.records.get_mut().values() {
                if let Err(error) = partition.finish_service_shutdown().await {
                    retain_failed(done, service, format!("partition shutdown: {error}"));
                    return;
                }
            }
            // Keep the service root locked even if a storage destructor fails.
            // Blocking destruction includes mmap/files and native fallback joins.
            let root_owner = service._root_owner.take();
            if let Err(error) = tokio::task::spawn_blocking(move || drop(service)).await {
                retain_failed(done, root_owner, format!("resource destruction: {error}"));
                return;
            }
            drop(root_owner);
            done.send_replace(Some(Ok(())));
        });
        shutdown
    }
}

// A failed terminal join cannot establish that all owners are gone. Report the
// error but retain the root/resources until process exit; do not admit a second
// service or print a successful shutdown. Recovery uses the existing protocol.
fn retain_failed<T>(done: tokio::sync::watch::Sender<Option<Result<(), String>>>, owner: T, error: String) {
    // This terminal failure requires process recovery. Retention must survive
    // dropping the completion handle AND destroying the Tokio runtime.
    std::mem::forget(owner);
    log::error!("Shutdown incomplete: {error}");
    done.send_replace(Some(Err(error)));
}
