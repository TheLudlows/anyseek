# seekpower-posting

Encoding and decoding for posting lists — the inverted-index structure that maps
each term to the documents (and positions) it appears in. This crate is the
boundary between the in-memory `PostingEntry` representation used by the indexer
and the byte format that gets persisted in SlateDB.

The on-disk layout is defined in design spec §4.1. This crate owns that
contract: `PostingCodec::serialize` produces the bytes, `PostingCodec::deserialize`
parses them, and `merge_postings` is the only legal way to combine two existing
blocks during commit.

## On-disk format

```
+--------------------------------------------------------+
| Header (fixed 16 bytes)                                |
|   posting_count:    u32                                |
|   max_score:        f32      (WAND hint)               |
|   compression_type: u8       (1=Array, 2=Bitmap, 3=RLE)|
|   block_count:      u8                                 |
|   reserved:         6 bytes                            |
+--------------------------------------------------------+
| Per-block metadata (24 bytes each)                     |
|   max_block_score: f32                                 |
|   posting_count:   u32                                 |
|   max_doc_id:      u64      (WAND jump hint)           |
|   offset:          u32                                 |
|   len:             u32                                 |
+--------------------------------------------------------+
| data section: delta+varint encoded doc_ids & positions |
+--------------------------------------------------------+
```

The current code always produces a single block per list, but the format is
already multi-block capable so adaptive selection can split lists later
without breaking on-disk compatibility.

## Compression types

| Variant | Discriminant | Status |
|---|---|---|
| `CompressionType::Array` | `1` | Used for all cases today. Sorted `u64` doc_ids, delta + varint. |
| `CompressionType::Bitmap` | `2` | Reserved (spec §4.2). For dense posting lists. |
| `CompressionType::Rle` | `3` | Reserved (spec §4.2). For runs of consecutive doc_ids. |

Adaptive selection is not yet wired up — `choose_compression` is a stub that
returns `Array`. The deserializer accepts all three values so future writers
can land without a format bump.

## varint encoding

1–9 bytes per value, 7 payload bits per byte, MSB set = continuation bit. Used
for both the `doc_id` deltas and the `position` deltas inside the `data`
section. `field_id` and `pos_count` are stored as fixed-width little-endian
(`u16`) since they have small, predictable ranges.

## Types

```rust
pub struct PostingEntry {
    pub doc_id: u64,
    pub field_id: u16,
    pub positions: Vec<u16>,
}

pub struct PostingListBlock {
    pub posting_count: u32,
    pub max_score: f32,
    pub compression_type: CompressionType,
    pub blocks: Vec<PostingBlock>,
    pub data: Vec<u8>,
}

pub struct PostingBlock {
    pub header: BlockHeader,
    pub offset: usize,
    pub len: usize,
}

pub struct BlockHeader {
    pub max_block_score: f32,
    pub posting_count: u32,
    pub max_doc_id: u64,
}

#[repr(u8)]
pub enum CompressionType { Array = 1, Bitmap = 2, Rle = 3 }
```

## `PostingCodec`

| Method | Purpose |
|---|---|
| `encode(entries: &[PostingEntry]) -> PostingListBlock` | Delta-encodes `doc_id` and `positions` with varint. `max_score` / `max_block_score` are written as `0.0` — score propagation is a TODO in the current code. |
| `decode(block: &PostingListBlock) -> Vec<PostingEntry>` | Inverse of `encode`. |
| `serialize(block: &PostingListBlock) -> Vec<u8>` | Writes the header + per-block metadata + `data` section as the on-disk bytes. |
| `deserialize(data: &[u8]) -> Result<PostingListBlock>` | Parses the on-disk bytes. Validates minimum length and `compression_type` discriminant. |

`PostingCodec` is a unit struct — all methods are static.

## `merge_postings`

```rust
pub fn merge_postings(
    existing: &PostingListBlock,
    new_entries: &[PostingEntry],
) -> PostingListBlock
```

Decodes `existing`, appends `new_entries`, sorts by `doc_id`, dedupes by
`doc_id` (keeps first occurrence), and re-encodes. Used by the commit path
when flushing a `PostingBuffer` over an existing list in SlateDB.

## WAND relevance

Two fields in the block header are load-bearing for the WAND optimizer
(spec §4.5):

- `max_block_score` — the upper bound on any score coming out of this block.
  Compared against `MinHeap::min_score()` to decide whether to skip decoding.
- `max_doc_id` — used as a jump target when the WAND pointer needs to skip
  past a block of doc ids entirely.

These are currently `0.0` / the last `doc_id` in the list because real score
propagation has not been implemented. Filling these in is the main follow-up
needed for the WAND code path to deliver the spec'd speedups.

## Tests

Unit tests in `src/lib.rs` cover varint round-trip, encode/decode symmetry,
serialize/deserialize symmetry, `merge_postings` ordering, empty and single-
entry edge cases, and a 1000-entry list for larger-scale regression.

## See also

- Design spec: `docs/superpowers/specs/2026-06-30-hybrid-search-design.md`
  (§4.1, §4.2, §4.5)
- Source: `crates/seekpower-posting/src/lib.rs`
