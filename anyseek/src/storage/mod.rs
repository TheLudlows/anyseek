//! Local persistence primitives. The journal owns authoritative bytes; mmap
//! indexes are consumers, never the commit witness.

pub(crate) mod journal;
pub(crate) mod sqlite_authority;
pub(crate) mod s3_compatible;
pub(crate) mod object_log;
mod platform;

pub(crate) mod partition;

pub(crate) mod segment;

pub(crate) mod payload_pool;

pub(crate) mod build_budget;
