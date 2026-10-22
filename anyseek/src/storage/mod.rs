//! Local journal and SQLite/object persistence primitives. Authenticated heads
//! select committed bytes; mmap indexes never act as independent commit witnesses.

pub(crate) mod journal;
pub(crate) mod sqlite_authority;
#[cfg(feature = "s3-object-store")]
pub(crate) mod s3_compatible;
#[cfg(feature = "s3-object-store")]
pub(crate) mod object_log;
mod platform;

pub(crate) mod partition;

pub(crate) mod segment;

pub(crate) mod payload_pool;

pub(crate) mod build_budget;
