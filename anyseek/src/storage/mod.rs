//! Local journal and SQLite/object persistence primitives. Authenticated heads
//! select committed bytes; mmap indexes never act as independent commit witnesses.

pub(crate) mod journal;
pub(crate) mod sqlite_authority;
pub(crate) mod s3_compatible;
pub(crate) mod verified_cache;
pub(crate) mod object_log;
mod platform;

pub(crate) mod partition;

pub(crate) mod segment;

pub(crate) mod payload_pool;

pub(crate) mod build_budget;

pub(crate) mod object_keys;
