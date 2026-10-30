# Persistent analysis and Record packages

Applications create new databases through RecordPartition. Legacy format-8 roots and their index.bin headers are not read or migrated. Do not change old headers or overwrite deployed data.

Analysis retains the stable `xxh64-utf8-v1` term-key helper for analysis identities. Tantivy stores its native term dictionaries/postings rather than the retired hash-keyed inverted-file format. Exact Tantivy version, schema and analysis contract are recorded in the authenticated Record package. The Record checkpoint uses V15 and package manifest version 3; the private vector/metadata descriptor has format 1. These are distinct structural version tags, not independent commit authorities.

Payload alone retains canonical original text and vectors. Native lexical files do not store complete bodies. The companion component stores only derived vector/metadata data in component.json, vectors.bin, metadata.bin and schema.json. Record authenticates the complete immutable file closure before selecting it; fixed Searchers and owned pins keep selected packages alive through query/output lifetimes.

`tests/persistent_term_hash.rs` retains multilingual, N-gram, long-term and exact score-bit round trips and an opt-in opposite-AES build fixture. The latter must be executed under both builds to claim cross-build evidence; ordinary unit-test completion does not run it. Opening Record may rebuild derived cache files, so read-only checks apply to rejected legacy authority roots, not byte equality of every cache file.

Windows, opposite-AES builds, real S3 and power-loss evidence must be reported from actual execution. No portability or performance result is inferred from fixture success.
