#![crate_type = "lib"]
#![crate_name = "anyseek"]

//! Internal modules for the AnySeek record storage and search service.
//! The service exposes an HTTP API; models and document parsing run externally.
//! This crate also supports integration tests of storage and retrieval behavior.

#![warn(
    missing_docs,
    unused_import_braces,
    trivial_casts,
    trivial_numeric_casts,
    unused_qualifications
)]

use std::sync::LazyLock;

use tokio::runtime::Runtime;
/// Global Tokio runtime for index operations, initialized lazily on first use.
pub(crate) static INDEX_RUNTIME: LazyLock<Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .thread_name("anyseek-indexer")
        .enable_all()
        .build()
        .unwrap()
});

pub(crate) mod clustering;
// Private vector/metadata component checkpoint.
mod component;
/// Schema, analysis, scoring and vector configuration for RecordPartition.
pub mod index;
// Internal ranking helpers.
pub(crate) mod min_heap;
pub(crate) mod opencc_t2s;
/// Query configuration and result types. Execute public queries through RecordPartition.
pub mod search;
/// Tokenizes text into tokens (words), supports Chinese word segmentation, folds (converts) diacritics, accents, zalgo text, umlaut, bold, italic, full-width UTF-8 characters into their basic representation.
pub(crate) mod tokenizer;
/// Binary field codecs and base64 vector transport helpers.
pub mod utils;
/// Vector search by indexing vectors and searching for similar vectors based on cosine similarity, inner product, and Euclidean distance.
pub mod vector;
/// Vector quantization and similarity measure definitions for vector search.
pub mod vector_similarity;
mod metadata_filter;




mod term_hash;

mod storage;


pub mod record;
pub mod catalog;
pub mod resources;

mod active;

/// HTTP request, response and OpenAPI contracts for the service.
pub mod server;
