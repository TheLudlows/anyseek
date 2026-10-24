# Field length normalization

Record configuration accepts `field_length_normalization: "Shared" | "PerField"`; Shared is the default. The setting is immutable. Rust callers use `IndexMetaObject::field_length_normalization`.

The production Tantivy scorer retains BM25F/Bm25fProximity, field boosts, k1/b, and decoded length quantization. PerField changes each field denominator to the mean decoded nonempty length of that field. Shared uses the shared mean. Empty populations use mean 1. Term DF is exact across the active Record population and across fields, including live tail and frozen components at the same cut and evaluation time. Retired, hidden and expired records do not contribute active statistics. Metadata filters restrict candidates before Top-K; they do not silently redefine the corpus statistics.

Successful Record writes are durable and visible. Checkpoint is an authenticated publication/reclamation operation, not a second visibility switch. Native commit is not business authority. No physical shard-local DF, compressed posting DF, old field-length snapshot layout, or uncommitted-query flag is exposed by the Record API. These were low-level engine details; the previous Record active-statistics override already used exact active DF.

Independent literal-formula tests in `tests/common/ngram_oracle.rs` and `tests/lexical_correctness.rs` cover boosts, unequal DF, Shared/PerField and persistence states. Physical legacy block tests have been migrated to Record behavior; they are not evidence of retaining the deleted codec. This replacement makes no performance claim and does not restart performance-budget work.
