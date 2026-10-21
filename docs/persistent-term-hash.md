# Persistent term hashing: index format 8.0

[文档索引](README.md) · [当前功能与限制](../FEATURES.md) · [开发与验证](development.md)


Format 8.0 replaces the architecture-selected ahash term-key contract of 7.0.
It is an incompatible disk format change, not an index migration. Existing 7.0
indexes (AES or fallback), unknown versions and malformed/missing headers are
rejected. Never change an old header to 8.0: the posting keys would still be wrong.
Create a separate index from source data if the new format is wanted; no original
index is rebuilt, deleted or converted by this change.

## Byte-level contract

The format's algorithm identifier is `xxh64-utf8-v1` (`TERM_HASH_ALGORITHM`).
The root and every shard start with the existing four-byte, little-endian header:
major u16 = 8, minor u16 = 0, i.e. `08 00 00 00`. Exact version matching is required.
This version identifies the whole contract below; it is not a runtime algorithm
choice and does not depend on AES, CPU model, compiler Hash encodings or pointer size.

Input is the tokenizer's exact normalized UTF-8 term bytes. No Rust `Hash` trait,
length prefix, NUL terminator, or extra normalization is used. Existing n-gram
string construction is unchanged. Canonically equivalent Unicode strings can
have distinct hashes unless the configured analysis rule equates them first.
Embedded zero bytes and long terms are processed in full.

- Segment routing: standard XXH64 with seed `0x9e3779b185ebca87`, low 32 bits,
  then the existing segment mask at the call site.
- Posting term key: standard XXH64 with seed 0; clear the low 3 bits. Those bits
  remain reserved for the existing `NgramType` tag, OR-ed at the call site.
- XXH64's lane decoding is explicitly little-endian. Stored integer keys and
  index format versions retain the existing little-endian encoding.

Both entry points live in `term_hash.rs` and are routed through the existing
`index::hash32/hash64` helpers. The implementation is pinned to
`xxhash-rust = 0.8.15`, with only the `xxh64` feature and no default features.
It adds no transitive dependencies, CPU detection or native library dependency.
The standard noncryptographic XXH64 algorithm fits term lookup; a cryptographic
hash would add unnecessary cost. The low-three-bit tagging retains the existing
61-bit term-key collision model; this change does not add collision resolution.
Any future change to the byte/seed/tag contract requires a new format version.

## Audited uses and boundaries

- `tokenizer.rs`: document/query segment routing, single-term and n-gram keys,
  frequent-term membership and component identities.
- `compress_postinglist.rs`: n-gram component posting/DF lookups through the same
  two helpers.
- `realtime_search.rs`: realtime term/component posting lookup.
- `index.rs`: frequent-word key construction on creation/open.
- `commit.rs`: serializes the resulting numeric keys; loading reads them back
  without rehashing. Query lookup therefore must use the identical contract.

In-memory AHashMap/AHashSet, segmentation dictionary maps, schema/facet maps and
other ordinary HashMaps stay unchanged. Server API-key SHA-256/hashes are unrelated
and unchanged. No scoring, pruning, RRF default, tokenizer, normalization, scan8,
result extraction, or diagnostic instrumentation is included in this patch.

## Loading and rejection

Read-only preflight validates the root and every numeric shard directory before
root/shard construction opens writable files. `open_index`, `open_shard`,
`create_index_root` and `create_shard` all participate. The low-level header reads
also use `read_exact`. A new empty/nonexistent directory can be initialized;
an existing empty/truncated index.bin cannot be mistaken for a fresh index.
Missing headers in a nonempty directory are rejected rather than recreated.

The library returns an explicit error before search. Existing HTTP startup policy
prints the open error and omits an incompatible index; it does not register a
queryable index returning silent empty results. This patch does not change that
server-wide error policy. Concurrent external mutation during open remains outside
the index's existing supported lifecycle; preflight is not a filesystem transaction.

## Verification and future benchmarking

Fixed key vectors cover English, Chinese, non-ASCII/combining Unicode, empty input,
NUL, XXH64's 31/32/33-byte boundary and a long term. Constants come from the
independent C libxxhash 0.8.3 reference; the test suite needs no C library.

`persistent_term_hash` covers fresh/realtime/committed/reopened search, three-shard
root/all-shard header validation, legacy/future/minor mismatch, truncation and
missing headers. Rejection checks all file bytes/names before and after both
open and create attempts.

The ignored `cross_build_same_index` fixture is explicitly orchestrated under
`-C target-cpu=x86-64-v3 -C target-feature=-aes` and `+aes`. Each side creates
independent format-8 fixtures; the opposite side reads those exact directories
without rebuilding. One/three shards and None/OpenCC t2s analysis cover Chinese,
English, OR/AND/phrase/NOT, n-grams and frequent terms. Ordered IDs, f32 score bits,
counts, and committed file bytes must match.

Format-8 indexes cannot be compared directly with existing format-7 benchmark
indexes for a performance claim. A future benchmark needs a separately built new
index, frozen inputs/vectors, matching analysis and a new equivalent baseline.
No performance benefit is claimed for this compatibility fix. Cross-endian and
non-x86 execution are not tested here; their hash contract is fixed by XXH64 and
UTF-8/little-endian encoding, while other engine portability constraints remain.
