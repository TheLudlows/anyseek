//! Numeric, attempt-local maintenance diagnostics. Never log records, queries,
//! operation IDs, object paths, credentials, or serialized State.
use std::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SealFallback {
    PrivateCleanup,
    MissingCapture,
    PackageBound,
    UnstableTail,
}

pub(super) fn seal_fallback(
    private_cleanup: bool, captured: bool, selected: usize,
    pending: usize, limit: usize, stable: bool,
) -> Option<SealFallback> {
    if private_cleanup { return Some(SealFallback::PrivateCleanup); }
    if !captured { return Some(SealFallback::MissingCapture); }
    if selected.checked_add(pending).and_then(|n|n.checked_add(1)).is_none_or(|n|n>=limit) {
        return Some(SealFallback::PackageBound);
    }
    if !stable { return Some(SealFallback::UnstableTail); }
    None
}

/// Exit timing includes errors/cancellation: it is not a success counter.
/// Nested phases overlap and must not be summed into total elapsed time.
pub(super) struct Phase {
    name: &'static str,
    seq: u64,
    started: Instant,
}
impl Phase {
    pub(super) fn new(name: &'static str, seq: u64) -> Self {
        log::debug!("record_maintenance phase={name} seq={seq} event=enter");
        Self {name,seq,started:Instant::now()}
    }
}
impl Drop for Phase {
    fn drop(&mut self) {
        log::info!("record_maintenance phase={} seq={} event=exit elapsed_us={}",
            self.name,self.seq,self.started.elapsed().as_micros());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn seal_bound_counts_successor_and_rejects_overflow() {
        assert_eq!(seal_fallback(false,true,9,5,16,true),None);
        assert_eq!(seal_fallback(false,true,9,6,16,true),Some(SealFallback::PackageBound));
        assert_eq!(seal_fallback(false,true,usize::MAX,1,16,true),Some(SealFallback::PackageBound));
    }
    #[test]
    fn seal_reason_preserves_private_and_capture_precedence() {
        assert_eq!(seal_fallback(true,false,16,0,16,false),Some(SealFallback::PrivateCleanup));
        assert_eq!(seal_fallback(false,false,16,0,16,false),Some(SealFallback::MissingCapture));
        assert_eq!(seal_fallback(false,true,1,0,16,false),Some(SealFallback::UnstableTail));
    }
}
