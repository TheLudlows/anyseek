//! Service admission and owned-work completion, independent of resource budgets.
use std::{future::Future, pin::Pin, sync::{Arc, Mutex}, task::{Context, Poll}};
use tokio::sync::Notify;

#[derive(Default)]
struct State { stopping: bool, active: usize }

/// One service's admission boundary. Executors and budgets remain shared as before.
#[derive(Default)]
pub struct Lifecycle { state: Mutex<State>, changed: Notify }

/// An accepted root operation. Children retain the same ticket until their actual
/// work and undelivered task output have been released.
#[derive(Clone)]
pub struct Activity(Arc<Entry>);
struct Entry(Arc<Lifecycle>);
impl Drop for Entry {
    fn drop(&mut self) {
        self.0.state.lock().unwrap_or_else(|e| e.into_inner()).active -= 1;
        self.0.changed.notify_waiters();
    }
}
tokio::task_local! { static CURRENT: Activity; }

impl Lifecycle {
    /// Atomically admit a root before shutdown, or refuse without starting work.
    pub fn enter(self: &Arc<Self>) -> Option<Activity> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.stopping { return None; }
        state.active += 1;
        Some(Activity(Arc::new(Entry(self.clone()))))
    }
    /// Stop new roots. Existing activities may still create necessary children.
    pub fn stop(&self) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).stopping = true;
        self.changed.notify_waiters();
    }
    /// Whether the service has stopped accepting new roots.
    pub fn is_stopping(&self) -> bool {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).stopping
    }
    /// Wait for the stop boundary, without consuming its notification.
    pub async fn stopped(&self) { self.wait_until(|s| s.stopping).await; }
    /// Wait for accepted roots and all retained descendants/output to finish.
    /// Call after stop; this is not a storage-resource destruction barrier.
    pub async fn drained(&self) { self.wait_until(|s| s.stopping && s.active == 0).await; }
    async fn wait_until(&self, ready: impl Fn(&State) -> bool) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if ready(&self.state.lock().unwrap_or_else(|e| e.into_inner())) { return; }
            changed.await;
        }
    }
    pub(crate) fn current() -> Option<Arc<Self>> { Activity::current().map(|a| a.0.0.clone()) }
}
impl Activity {
    /// Enter an accepted request's context; child tasks explicitly inherit it.
    pub async fn run<F: Future>(self, future: F) -> F::Output { CURRENT.scope(self, future).await }
    pub(crate) fn run_sync<T>(self, work: impl FnOnce() -> T) -> T { CURRENT.sync_scope(self, work) }
    pub(crate) fn current() -> Option<Self> { CURRENT.try_with(Clone::clone).ok() }
}

// Output precedes activity so a detached completed task destroys returned
// owners before releasing the last ticket. A pending caller retains its own
// activity while taking the output out of this envelope.
pub(crate) struct Output<T> { pub value: T, pub _activity: Option<Activity> }

/// A cancellation-safe owned task. Dropping the waiter leaves work running.
pub struct Task<T>(pub(crate) tokio::task::JoinHandle<Output<T>>);
#[cfg(test)]
impl<T> Task<T> {
    pub(crate) fn abort_handle(&self) -> tokio::task::AbortHandle { self.0.abort_handle() }
}
impl<T> Future for Task<T> {
    type Output = Result<T, tokio::task::JoinError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(|result| result.map(|output| output.value))
    }
}

/// Capture the current activity before a child is spawned on either runtime.
pub(crate) fn track<F: Future>(future: F) -> impl Future<Output = Output<F::Output>> {
    // Storage operations can have large futures. Keep the additional task-local
    // envelopes from multiplying their stack footprint while they are moved.
    let future = Box::pin(future);
    let activity = Activity::current();
    async move {
        let value = match activity.clone() { Some(a) => a.run(future).await, None => future.await };
        Output { value, _activity: activity }
    }
}

// Maintenance/consumer handles may remain in a partition after completion.
// Their outputs are only status/sequence values, never storage owners. Do not
// keep their completed activity alive through that administrative join handle.
pub(crate) fn track_background<F: Future>(future: F) -> impl Future<Output = Output<F::Output>> {
    let future = Box::pin(future);
    let activity = Activity::current();
    async move {
        let value = match activity.clone() { Some(a) => a.run(future).await, None => future.await };
        drop(activity);
        Output { value, _activity: None }
    }
}

// Background handles may be retained for later joins. Restrict their output to
// owner-free status values; a storage owner must use the ordinary tracked task.
pub(crate) trait BackgroundOutput {}
impl BackgroundOutput for () {}
impl BackgroundOutput for Result<(), crate::record::RecordError> {}
impl BackgroundOutput for Result<u64, crate::record::RecordError> {}

pub(crate) async fn stopping() {
    match Lifecycle::current() {
        Some(lifecycle) => lifecycle.stopped().await,
        None => std::future::pending().await,
    }
}

/// Automatic maintenance is a new root, unlike a required child of accepted work.
/// Keep its admission and stop boundary atomic even when a foreground request
/// schedules it just as shutdown starts.
pub(crate) fn automatic<T>(work: impl FnOnce() -> T) -> Option<T> {
    match Lifecycle::current() {
        Some(lifecycle) => lifecycle.enter().map(|activity| activity.run_sync(work)),
        None => Some(work()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn stopping_keeps_required_children_and_detached_output_owned() {
        let lifecycle = Arc::new(Lifecycle::default());
        let root = lifecycle.enter().unwrap();
        lifecycle.stop();
        assert!(lifecycle.enter().is_none());
        let (sent, received) = tokio::sync::oneshot::channel();
        struct Owner(Option<tokio::sync::oneshot::Sender<()>>);
        impl Drop for Owner { fn drop(&mut self) { let _ = self.0.take().unwrap().send(()); } }
        let worker = root.run(async {
            assert!(automatic(|| ()).is_none());
            super::super::spawn(async move { Owner(Some(sent)) })
        }).await;
        while !worker.0.is_finished() { tokio::task::yield_now().await; }
        assert_eq!(lifecycle.state.lock().unwrap().active, 1);
        drop(worker);
        lifecycle.drained().await;
        received.await.unwrap();

        let lifecycle = Arc::new(Lifecycle::default());
        let root = lifecycle.enter().unwrap();
        let retained = root.run(async { super::super::spawn_background(async {}) }).await;
        lifecycle.stop();
        tokio::time::timeout(std::time::Duration::from_secs(5), lifecycle.drained()).await.unwrap();
        retained.await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_waiter_does_not_release_blocking_work() {
        let lifecycle = Arc::new(Lifecycle::default());
        let root = lifecycle.enter().unwrap();
        let (started, ready) = tokio::sync::oneshot::channel();
        let (finish, wait) = std::sync::mpsc::channel();
        let caller = tokio::spawn(root.run(async move {
            super::super::spawn_blocking(move || { started.send(()).unwrap(); wait.recv().unwrap(); }).await.unwrap();
        }));
        ready.await.unwrap();
        lifecycle.stop();
        caller.abort(); let _ = caller.await;
        assert_eq!(lifecycle.state.lock().unwrap().active, 1);
        finish.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), lifecycle.drained()).await.unwrap();
    }
}
