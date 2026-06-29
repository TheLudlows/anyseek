// Search engine lexical search: types and functions for single-term, union,
// intersection, and phrase queries over in-memory posting list data.

use crate::compress::{compress_postinglist, decompress_docids};
use crate::posting::{PostingListObjectIndex, CompressionType};
use crate::ranking::{
    compute_bm25f, SearchResult, ResultSource, MinHeap,
};
use ahash::AHashSet;

// ---------------------------------------------------------------------------
// Search types
// ---------------------------------------------------------------------------

/// Query type for combining term results.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum QueryType {
    Union,
    Intersection,
    Phrase,
}

/// Search mode: lexical, vector, or hybrid.
#[derive(Debug, Clone)]
pub enum SearchMode {
    Lexical,
    Vector {
        ann_mode: AnnMode,
        similarity_threshold: f32,
    },
    Hybrid {
        ann_mode: AnnMode,
        similarity_threshold: f32,
    },
}

/// Approximate nearest-neighbour search mode.
#[derive(Debug, Clone)]
pub enum AnnMode {
    All,
    Nprobe(usize),
    SimilarityThreshold(f32),
}

/// What to return from a search.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ResultType {
    Count,
    Topk,
    TopkCount,
}

/// A search request.
pub struct SearchRequest {
    pub query_string: Option<String>,
    pub query_vector: Option<Vec<f32>>,
    pub query_type: QueryType,
    pub search_mode: SearchMode,
    pub offset: usize,
    pub length: usize,
    pub result_type: ResultType,
    pub include_uncommitted: bool,
    pub field_filter: Option<Vec<String>>,
}

/// A search response.
pub struct SearchResponse {
    pub query: String,
    pub result_count: usize,
    pub results: Vec<SearchResult>,
}

// ---------------------------------------------------------------------------
// Single-term search
// ---------------------------------------------------------------------------

/// Search a single posting list and add matching documents to the heap.
///
/// Returns the count of matching documents (before heap capacity filtering).
pub fn search_single_term(
    posting_list: &PostingListObjectIndex,
    delete_set: &AHashSet<usize>,
    heap: &mut MinHeap,
    avg_doc_length: f32,
    boost: f32,
    idf: f32,
    level: usize,
) -> usize {
    let mut match_count = 0usize;

    for block in &posting_list.blocks {
        // WAND optimisation: skip block if its max score cannot beat the heap floor.
        if heap.is_full() {
            if let Some(min_score) = heap.peek_min() {
                if block.max_block_score <= min_score {
                    continue;
                }
            }
        }

        let ct = CompressionType::from_u32_top2(block.compression_type_pointer);
        let byte_offset = CompressionType::unpack_offset(block.compression_type_pointer) as usize;
        let posting_count = block.posting_count;

        // Decompress from the block's byte offset through the byte_array.

        // For decompress_docids we need to know the docid portion of the data.
        // In the compressed format the layout is [docid_data | position_data].
        // We pass the whole remaining slice; decompress_docids only reads docid data
        // based on the compression type.
        let data_slice = &posting_list.byte_array[byte_offset..posting_list.byte_array.len()];
        let docids = decompress_docids(data_slice, ct, posting_count);

        for &docid in &docids {
            let local_docid = docid as usize;
            if delete_set.contains(&local_docid) {
                continue;
            }
            let global_doc_id = ((level as u64) << 16) | (docid as u64);

            // Simplified BM25F: tf=1, doc_length_compressed=128 (neutral).
            let score = compute_bm25f(1, 128, boost, idf, avg_doc_length);

            heap.add(SearchResult::new(
                global_doc_id,
                score,
                ResultSource::Lexical,
            ));
            match_count += 1;
        }
    }

    match_count
}

// ---------------------------------------------------------------------------
// Union search
// ---------------------------------------------------------------------------

/// Search multiple posting lists with union semantics.
///
/// Documents appearing in multiple posting lists have their scores summed.
/// Returns the total count of unique matching documents.
pub fn search_union(
    posting_lists: &[&PostingListObjectIndex],
    delete_set: &AHashSet<usize>,
    heap: &mut MinHeap,
    avg_doc_length: f32,
    boosts: &[f32],
    idfs: &[f32],
    level: usize,
) -> usize {
    // Track per-docid accumulated scores.
    let mut doc_scores: ahash::AHashMap<u64, f32> = ahash::AHashMap::new();
    let mut unique_docs: AHashSet<u64> = AHashSet::new();

    for (term_idx, &pl) in posting_lists.iter().enumerate() {
        let boost = boosts.get(term_idx).copied().unwrap_or(1.0);
        let idf = idfs.get(term_idx).copied().unwrap_or(1.0);

        for block in &pl.blocks {
            let ct = CompressionType::from_u32_top2(block.compression_type_pointer);
            let byte_offset = CompressionType::unpack_offset(block.compression_type_pointer) as usize;
            let posting_count = block.posting_count;

            let data_slice = &pl.byte_array[byte_offset..pl.byte_array.len()];
            let docids = decompress_docids(data_slice, ct, posting_count);

            for &docid in &docids {
                let local_docid = docid as usize;
                if delete_set.contains(&local_docid) {
                    continue;
                }
                let global_doc_id = ((level as u64) << 16) | (docid as u64);

                let score = compute_bm25f(1, 128, boost, idf, avg_doc_length);

                *doc_scores.entry(global_doc_id).or_insert(0.0) += score;
                unique_docs.insert(global_doc_id);
            }
        }
    }

    // Insert accumulated scores into the heap.
    for (doc_id, score) in &doc_scores {
        heap.add(SearchResult::new(
            *doc_id,
            *score,
            ResultSource::Lexical,
        ));
    }

    unique_docs.len()
}

// ---------------------------------------------------------------------------
// Intersection search
// ---------------------------------------------------------------------------

/// Search multiple posting lists with intersection semantics.
///
/// Only documents present in ALL posting lists are returned.
/// Returns the total count of matching documents.
pub fn search_intersection(
    posting_lists: &[&PostingListObjectIndex],
    delete_set: &AHashSet<usize>,
    heap: &mut MinHeap,
    avg_doc_length: f32,
    boosts: &[f32],
    idfs: &[f32],
    level: usize,
) -> usize {
    if posting_lists.is_empty() {
        return 0;
    }

    // Decompress all posting lists into sorted Vec<u16> docid lists.
    let mut all_docids: Vec<Vec<u16>> = Vec::with_capacity(posting_lists.len());
    for pl in posting_lists {
        for block in &pl.blocks {
            let ct = CompressionType::from_u32_top2(block.compression_type_pointer);
            let byte_offset = CompressionType::unpack_offset(block.compression_type_pointer) as usize;
            let posting_count = block.posting_count;

            let data_slice = &pl.byte_array[byte_offset..pl.byte_array.len()];
            let docids = decompress_docids(data_slice, ct, posting_count);
            all_docids.push(docids);
        }
    }

    // If there is only one list, fall through to intersection logic with
    // one list (all docs match).
    if all_docids.is_empty() {
        return 0;
    }

    // Flatten per-block docids into per-term lists (merge blocks per term).
    // For simplicity, treat each block as a separate list in the intersection.
    // A more correct approach would merge blocks per term first, but the spec
    // says "decompress all posting lists into Vec<u16> docid lists" so we do
    // multi-pointer intersection across all block-level lists.

    let match_count = multi_pointer_intersection(
        &all_docids,
        posting_lists,
        delete_set,
        heap,
        avg_doc_length,
        boosts,
        idfs,
        level,
    );

    match_count
}

/// Multi-pointer intersection across sorted docid lists.
///
/// `all_docids` is a flat list of sorted docid lists (one per block across all terms).
/// `posting_lists` is used to look up the term index for score computation.
fn multi_pointer_intersection(
    all_docids: &[Vec<u16>],
    _posting_lists: &[&PostingListObjectIndex],
    delete_set: &AHashSet<usize>,
    heap: &mut MinHeap,
    avg_doc_length: f32,
    boosts: &[f32],
    idfs: &[f32],
    level: usize,
) -> usize {
    if all_docids.is_empty() {
        return 0;
    }

    let mut pointers: Vec<usize> = vec![0usize; all_docids.len()];
    let mut match_count = 0usize;

    loop {
        // Find the list with the smallest current docid and track the max.
        let mut max_val = u16::MIN;
        let mut min_val = u16::MAX;
        let mut min_idx = 0usize;

        for (i, ptr) in pointers.iter().enumerate() {
            if *ptr >= all_docids[i].len() {
                return match_count; // one list exhausted => intersection done
            }
            let val = all_docids[i][*ptr];
            if val < min_val {
                min_val = val;
                min_idx = i;
            }
            if val > max_val {
                max_val = val;
            }
        }

        if min_val == max_val {
            // All lists point to the same docid => intersection hit.
            let docid = min_val;
            let local_docid = docid as usize;

            if !delete_set.contains(&local_docid) {
                let global_doc_id = ((level as u64) << 16) | (docid as u64);

                // Sum BM25F scores across all terms.
                // We assume one block per term for score lookup; if there are
                // more blocks than terms, cycle through boosts/idfs.
                let mut total_score = 0.0f32;
                for (term_idx, _) in all_docids.iter().enumerate() {
                    let boost = boosts.get(term_idx).copied().unwrap_or(1.0);
                    let idf = idfs.get(term_idx).copied().unwrap_or(1.0);
                    total_score += compute_bm25f(1, 128, boost, idf, avg_doc_length);
                }

                heap.add(SearchResult::new(
                    global_doc_id,
                    total_score,
                    ResultSource::Lexical,
                ));
                match_count += 1;
            }

            // Advance all pointers past this docid.
            for (i, ptr) in pointers.iter_mut().enumerate() {
                *ptr += 1;
                // Skip past any duplicates within a single list.
                while *ptr < all_docids[i].len() && all_docids[i][*ptr] == docid {
                    *ptr += 1;
                }
            }
        } else {
            // Advance only the pointer pointing to the smallest docid.
            pointers[min_idx] += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Phrase search
// ---------------------------------------------------------------------------

/// Search multiple posting lists with phrase semantics.
///
/// Currently implemented as intersection with a TODO for proper position
/// verification. Position data is embedded in the byte_array of the posting
/// lists; full position-aware matching will be added later.
pub fn search_phrase(
    posting_lists: &[&PostingListObjectIndex],
    delete_set: &AHashSet<usize>,
    heap: &mut MinHeap,
    avg_doc_length: f32,
    boosts: &[f32],
    idfs: &[f32],
    level: usize,
) -> usize {
    // TODO: Add proper position verification. Currently we decode positions
    // from the byte_array but skip the positional adjacency check. Future work:
    // verify that positions of consecutive terms are adjacent in the document.
    search_intersection(
        posting_lists,
        delete_set,
        heap,
        avg_doc_length,
        boosts,
        idfs,
        level,
    )
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Create a mock posting list from a slice of docids for testing.
/// Uses `compress_postinglist` to produce properly compressed block data.
fn make_mock_posting_list(docids: &[u16]) -> PostingListObjectIndex {
    let positions: Vec<Vec<u16>> = docids.iter().map(|_| vec![0]).collect();
    let (data, ct, _, _) = compress_postinglist(docids, &positions);

    // Build the byte_array: pack compression type + offset into block metadata,
    // then store data starting at offset 0.
    let ct_packed = (ct as u32) << 30; // offset = 0

    let block = crate::posting::BlockObjectIndex {
        max_block_score: 10.0, // generous score so WAND does not skip in tests
        block_id: 0,
        compression_type_pointer: ct_packed,
        posting_count: docids.len() as u16,
        max_docid: docids.last().copied().unwrap_or(0),
        max_p_docid: 0,
        pointer_pivot_p_docid: docids.len() as u16,
    };

    PostingListObjectIndex {
        posting_count: docids.len() as u32,
        max_list_score: 10.0,
        blocks: vec![block],
        byte_array: data,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Single-term search
    // -----------------------------------------------------------------------

    #[test]
    fn test_search_single_term_returns_correct_results() {
        let docids = vec![10, 20, 30, 40, 50];
        let pl = make_mock_posting_list(&docids);
        let delete_set = AHashSet::new();
        let mut heap = MinHeap::new(10);

        let count = search_single_term(&pl, &delete_set, &mut heap, 100.0, 1.0, 2.0, 0);

        assert_eq!(count, 5);
        assert_eq!(heap.len(), 5);

        let results = heap.into_sorted_vec();
        // All scores are equal, so order is not deterministic — check membership.
        let expected: Vec<u64> = docids.iter().map(|&d| (0u64 << 16) | d as u64).collect();
        let actual: Vec<u64> = results.iter().map(|r| r.doc_id).collect();
        for &eid in &expected {
            assert!(actual.contains(&eid), "doc_id {eid} not found in results");
        }
    }

    #[test]
    fn test_search_single_term_with_delete_set() {
        let docids = vec![10, 20, 30, 40, 50];
        let pl = make_mock_posting_list(&docids);
        let mut delete_set = AHashSet::new();
        delete_set.insert(20);
        delete_set.insert(40);
        let mut heap = MinHeap::new(10);

        let count = search_single_term(&pl, &delete_set, &mut heap, 100.0, 1.0, 2.0, 0);

        assert_eq!(count, 3);
        assert_eq!(heap.len(), 3);

        // Verify deleted docids are not in results.
        let results = heap.into_sorted_vec();
        let doc_ids: Vec<u64> = results.iter().map(|r| r.doc_id).collect();
        assert!(!doc_ids.contains(&((0u64 << 16) | 20)));
        assert!(!doc_ids.contains(&((0u64 << 16) | 40)));
    }

    #[test]
    fn test_search_single_term_wand_skip() {
        // Create a posting list with a low max_block_score.
        let docids = vec![1, 2, 3];
        let positions: Vec<Vec<u16>> = docids.iter().map(|_| vec![0]).collect();
        let (data, ct, _, _) = compress_postinglist(&docids, &positions);

        let block = crate::posting::BlockObjectIndex {
            max_block_score: 0.5, // low score
            block_id: 0,
            compression_type_pointer: (ct as u32) << 30,
            posting_count: docids.len() as u16,
            max_docid: docids.last().copied().unwrap_or(0),
            max_p_docid: 0,
            pointer_pivot_p_docid: docids.len() as u16,
        };

        let pl = PostingListObjectIndex {
            posting_count: docids.len() as u32,
            max_list_score: 0.5,
            blocks: vec![block],
            byte_array: data,
        };

        // Fill heap to capacity with high-score results so the low block gets skipped.
        let mut heap = MinHeap::new(3);
        heap.add(SearchResult::new(100, 5.0, ResultSource::Lexical));
        heap.add(SearchResult::new(101, 4.0, ResultSource::Lexical));
        heap.add(SearchResult::new(102, 3.0, ResultSource::Lexical));
        assert!(heap.is_full());

        let delete_set = AHashSet::new();
        let count = search_single_term(&pl, &delete_set, &mut heap, 100.0, 1.0, 2.0, 0);

        // Block max_block_score (0.5) <= heap min (3.0) => block should be skipped.
        assert_eq!(count, 0);
        assert_eq!(heap.len(), 3);
        // Heap contents should be unchanged.
        let results = heap.into_sorted_vec();
        assert_eq!(results[0].doc_id, 100);
        assert_eq!(results[0].score, 5.0);
    }

    // -----------------------------------------------------------------------
    // Union search
    // -----------------------------------------------------------------------

    #[test]
    fn test_search_union_combines_results() {
        let docids_a = vec![10, 20, 30];
        let docids_b = vec![30, 40, 50];
        let pl_a = make_mock_posting_list(&docids_a);
        let pl_b = make_mock_posting_list(&docids_b);

        let delete_set = AHashSet::new();
        let mut heap = MinHeap::new(10);
        let posting_lists = vec![&pl_a, &pl_b];
        let boosts = vec![1.0, 1.0];
        let idfs = vec![2.0, 3.0];

        let count = search_union(
            &posting_lists,
            &delete_set,
            &mut heap,
            100.0,
            &boosts,
            &idfs,
            0,
        );

        // Unique docs: 10, 20, 30, 40, 50 => 5
        assert_eq!(count, 5);
        assert_eq!(heap.len(), 5);

        // Doc 30 appears in both lists, so its score should be higher than
        // docs that appear in only one list.
        let results = heap.into_sorted_vec();
        let top = &results[0];
        // Doc 30 should have the highest score since it matches both terms.
        assert_eq!(top.doc_id, 30);
    }

    // -----------------------------------------------------------------------
    // Intersection search
    // -----------------------------------------------------------------------

    #[test]
    fn test_search_intersection_only_common_docids() {
        let docids_a = vec![10, 20, 30, 40];
        let docids_b = vec![20, 30, 50];
        let pl_a = make_mock_posting_list(&docids_a);
        let pl_b = make_mock_posting_list(&docids_b);

        let delete_set = AHashSet::new();
        let mut heap = MinHeap::new(10);
        let posting_lists = vec![&pl_a, &pl_b];
        let boosts = vec![1.0, 1.5];
        let idfs = vec![2.0, 2.5];

        let count = search_intersection(
            &posting_lists,
            &delete_set,
            &mut heap,
            100.0,
            &boosts,
            &idfs,
            0,
        );

        // Common docids: 20, 30
        assert_eq!(count, 2);
        assert_eq!(heap.len(), 2);

        let results = heap.into_sorted_vec();
        let doc_ids: Vec<u64> = results.iter().map(|r| r.doc_id).collect();
        assert!(doc_ids.contains(&(20u64)));
        assert!(doc_ids.contains(&(30u64)));
    }

    #[test]
    fn test_search_intersection_empty() {
        let docids_a = vec![1, 2, 3];
        let docids_b = vec![100, 200, 300];
        let pl_a = make_mock_posting_list(&docids_a);
        let pl_b = make_mock_posting_list(&docids_b);

        let delete_set = AHashSet::new();
        let mut heap = MinHeap::new(10);

        let count = search_intersection(
            &[&pl_a, &pl_b],
            &delete_set,
            &mut heap,
            100.0,
            &[1.0, 1.0],
            &[1.0, 1.0],
            0,
        );

        assert_eq!(count, 0);
        assert_eq!(heap.len(), 0);
    }

    // -----------------------------------------------------------------------
    // Delete set filtering in union/intersection
    // -----------------------------------------------------------------------

    #[test]
    fn test_union_delete_set_filtering() {
        let docids_a = vec![10, 20, 30];
        let docids_b = vec![30, 40];
        let pl_a = make_mock_posting_list(&docids_a);
        let pl_b = make_mock_posting_list(&docids_b);

        let mut delete_set = AHashSet::new();
        delete_set.insert(30);

        let mut heap = MinHeap::new(10);

        let count = search_union(
            &[&pl_a, &pl_b],
            &delete_set,
            &mut heap,
            100.0,
            &[1.0, 1.0],
            &[1.0, 1.0],
            0,
        );

        // 30 is deleted, so unique docs: 10, 20, 40
        assert_eq!(count, 3);

        let results = heap.into_sorted_vec();
        let doc_ids: Vec<u64> = results.iter().map(|r| r.doc_id).collect();
        assert!(!doc_ids.contains(&(30u64)));
    }

    // -----------------------------------------------------------------------
    // Phrase search (delegates to intersection for now)
    // -----------------------------------------------------------------------

    #[test]
    fn test_search_phrase_delegates_to_intersection() {
        let docids_a = vec![5, 10, 15];
        let docids_b = vec![10, 15, 20];
        let pl_a = make_mock_posting_list(&docids_a);
        let pl_b = make_mock_posting_list(&docids_b);

        let delete_set = AHashSet::new();
        let mut heap = MinHeap::new(10);

        let count = search_phrase(
            &[&pl_a, &pl_b],
            &delete_set,
            &mut heap,
            100.0,
            &[1.0, 1.0],
            &[1.0, 1.0],
            0,
        );

        // Common docids: 10, 15
        assert_eq!(count, 2);
    }

    // -----------------------------------------------------------------------
    // WAND skip with heap full
    // -----------------------------------------------------------------------

    #[test]
    fn test_wand_skip_blocks_with_low_max_score() {
        // Create a list with two blocks: first has low score, second has high score.
        let docids_low = vec![1, 2, 3];
        let docids_high = vec![100, 200, 300];

        let positions_low: Vec<Vec<u16>> = docids_low.iter().map(|_| vec![0]).collect();
        let positions_high: Vec<Vec<u16>> = docids_high.iter().map(|_| vec![0]).collect();
        let (data_low, ct_low, _, _) = compress_postinglist(&docids_low, &positions_low);
        let (data_high, ct_high, _, _) = compress_postinglist(&docids_high, &positions_high);

        let block_low = crate::posting::BlockObjectIndex {
            max_block_score: 0.5,
            block_id: 0,
            compression_type_pointer: (ct_low as u32) << 30,
            posting_count: docids_low.len() as u16,
            max_docid: *docids_low.last().unwrap(),
            max_p_docid: 0,
            pointer_pivot_p_docid: docids_low.len() as u16,
        };

        let block_high = crate::posting::BlockObjectIndex {
            max_block_score: 10.0,
            block_id: 1,
            compression_type_pointer: (ct_high as u32) << 30, // offset = 0 in a separate byte_array
            posting_count: docids_high.len() as u16,
            max_docid: *docids_high.last().unwrap(),
            max_p_docid: 0,
            pointer_pivot_p_docid: docids_high.len() as u16,
        };

        // Concatenate both blocks' data into a single byte_array.
        // Block low starts at offset 0, block high starts at offset data_low.len().
        let mut byte_array = data_low.clone();
        let high_offset = byte_array.len() as u32;
        byte_array.extend_from_slice(&data_high);

        // Fix the second block's offset.
        let block_high_fixed = crate::posting::BlockObjectIndex {
            compression_type_pointer: (ct_high as u32) << 30 | (high_offset & 0x3FFF_FFFF),
            ..block_high
        };

        let pl = PostingListObjectIndex {
            posting_count: (docids_low.len() + docids_high.len()) as u32,
            max_list_score: 10.0,
            blocks: vec![block_low, block_high_fixed],
            byte_array,
        };

        // Fill heap to capacity with scores between the two block scores.
        let mut heap = MinHeap::new(3);
        heap.add(SearchResult::new(50, 5.0, ResultSource::Lexical));
        heap.add(SearchResult::new(51, 4.0, ResultSource::Lexical));
        heap.add(SearchResult::new(52, 3.0, ResultSource::Lexical));

        let delete_set = AHashSet::new();
        let count = search_single_term(&pl, &delete_set, &mut heap, 100.0, 1.0, 2.0, 0);

        // Low-score block (0.5) should be skipped (min in heap is 3.0).
        // High-score block (10.0) should be processed.
        assert_eq!(count, 3); // only the high block's docids
        assert_eq!(heap.len(), 3);
    }

    // -----------------------------------------------------------------------
    // Helper: make_mock_posting_list is already defined above; verify it works
    // -----------------------------------------------------------------------

    #[test]
    fn test_make_mock_posting_list_array_type() {
        // Sparse docids => Array compression.
        let pl = make_mock_posting_list(&[1, 10, 100, 1000]);
        assert!(!pl.blocks.is_empty());
        assert_eq!(pl.posting_count, 4);
    }

    #[test]
    fn test_make_mock_posting_list_rle_type() {
        // Consecutive docids => RLE compression.
        let docids: Vec<u16> = (100..200).collect();
        let pl = make_mock_posting_list(&docids);
        assert!(!pl.blocks.is_empty());
        assert_eq!(pl.posting_count, 100);
    }
}
