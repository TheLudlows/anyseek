//! Local persistence primitives. The journal owns authoritative bytes; mmap
//! indexes are consumers, never the commit witness.

pub(crate) mod journal;
mod platform;
