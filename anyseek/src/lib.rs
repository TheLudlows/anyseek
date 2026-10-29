#![crate_type = "lib"]
#![crate_name = "anyseek"]

//! # anyseek core
//! Lexical, external-vector and hybrid search.
//! Use a path or git dependency on this repository. Models and document parsing run externally.
//! Metadata fields support filtering and sorting; aggregate counts, histograms and extrema
//! are no longer exposed. See the repository's docs/library-examples.md for current examples.

#![warn(
    missing_docs,
    unused_import_braces,
    trivial_casts,
    trivial_numeric_casts,
    unused_qualifications
)]

/// Include the retained library examples in documentation.
#[cfg_attr(doctest, doc = include_str!("../../docs/library-examples.md"))]
pub struct ReadmeDoctests;

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
pub(crate) mod compatible;
/// Schema, analysis, scoring and vector configuration for RecordPartition.
pub mod index;
// Internal ranking helpers.
pub(crate) mod min_heap;
pub(crate) mod opencc_t2s;
/// Query configuration and result types. Execute public queries through RecordPartition.
pub mod search;
/// Tokenizes text into tokens (words), supports Chinese word segmentation, folds (converts) diacritics, accents, zalgo text, umlaut, bold, italic, full-width UTF-8 characters into their basic representation.
pub(crate) mod tokenizer;
/// Utils `truncate()` and `substring()`
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

/// Optional HTTP contracts; the core never depends on this module.
#[cfg(feature = "server")]
#[path = "../server/mod.rs"]
pub mod server;
