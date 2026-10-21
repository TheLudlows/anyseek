use tokio::sync::RwLockReadGuard;

use ahash::AHashMap;
use serde::{Deserialize, Serialize};

#[cfg(feature = "vb")]
use crate::vector::ResultSource;
use crate::{
    index::{FieldType, Shard},
    search::{ResultSortIndex, SortOrder},
    utils::{
        read_f32, read_f64, read_i8, read_i16, read_i32, read_i64, read_u16, read_u32, read_u64,
    },
};

#[derive(Clone, Debug, Copy, Default, Deserialize, Serialize)]
pub struct Result {
    pub doc_id: usize,
    pub score: f32,

    #[cfg(feature = "vb")]
    pub field_id: u32,
    #[cfg(feature = "vb")]
    pub chunk_id: u32,
    #[cfg(feature = "vb")]
    pub level_id: u32,
    #[cfg(feature = "vb")]
    pub shard_id: u32,
    #[cfg(feature = "vb")]
    pub cluster_id: u32,
    #[cfg(feature = "vb")]
    pub cluster_score: f32,
    #[cfg(feature = "vb")]
    pub vector_score: f32,
    #[cfg(feature = "vb")]
    pub lexical_score: f32,
    #[cfg(feature = "vb")]
    pub source: ResultSource,
}

/// Numeric score ordering: larger is better, signed zeros tie, and all NaNs
/// rank below numbers (including infinities). External vectors reject NaNs;
/// this fallback keeps internal comparison transitive without changing scores.
#[inline]
pub(crate) fn score_ordering(a: f32, b: f32) -> core::cmp::Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => core::cmp::Ordering::Equal,
        (true, false) => core::cmp::Ordering::Less,
        (false, true) => core::cmp::Ordering::Greater,
        (false, false) => a.partial_cmp(&b).unwrap(),
    }
}

/// Greater means a better candidate. Use the same rule before TopK truncation
/// and after shard merging; local IDs preserve global ID order within a shard.
#[inline]
pub(crate) fn rank_ordering(
    a_score: f32,
    a_id: usize,
    b_score: f32,
    b_id: usize,
) -> core::cmp::Ordering {
    score_ordering(a_score, b_score).then_with(|| b_id.cmp(&a_id))
}

/// MinHeap implements an min-heap, which is a binary heap used as priority queue.
/// Maintains a list of the top-k most relevant result candidates.
/// Better performance than a ordered list with binary search, inserts, and deletes
pub(crate) struct MinHeap<'a> {
    pub _elements: Vec<Result>,
    pub current_heap_size: usize,
    pub docid_hashset: AHashMap<usize, f32>,

    pub index: &'a Shard,
    pub empty_query: bool,
    pub result_sort: &'a Vec<ResultSortIndex>,
}

#[inline]
pub(crate) fn result_ordering_root(
    shard_vec: &[RwLockReadGuard<'_, Shard>],
    shard_number: usize,
    empty_query: bool,
    result_sort: &Vec<ResultSortIndex>,
    result1: Result,
    result2: Result,
) -> core::cmp::Ordering {
    let shard_id1 = result1.doc_id % shard_number;
    let doc_id1 = result1.doc_id / shard_number;
    let shard1 = &shard_vec[shard_id1];

    let shard_id2 = result2.doc_id % shard_number;
    let doc_id2 = result2.doc_id / shard_number;
    let shard2 = &shard_vec[shard_id2];

    for field in result_sort.iter() {
        if field.idx == usize::MAX {
            let order = if field.order == SortOrder::Descending {
                result1.doc_id.cmp(&result2.doc_id)
            } else {
                result2.doc_id.cmp(&result1.doc_id)
            };

            return order;
        }

        if field.idx == usize::MAX - 1 {
            let order = if field.order == SortOrder::Descending {
                score_ordering(result1.score, result2.score)
            } else {
                score_ordering(result2.score, result1.score)
            };

            if !order.is_eq() {
                return order;
            }
            continue;
        }

        match shard1.facets[field.idx].field_type {
            FieldType::U8 => {
                let offset = shard1.facets[field.idx].offset;

                let facet_value_1 =
                    &shard1.facets_file_mmap[(shard1.facets_size_sum * doc_id1) + offset];

                let facet_value_2 =
                    &shard2.facets_file_mmap[(shard2.facets_size_sum * doc_id2) + offset];

                let order = if field.order == SortOrder::Descending {
                    facet_value_1.cmp(facet_value_2)
                } else {
                    facet_value_2.cmp(facet_value_1)
                };

                if order != core::cmp::Ordering::Equal {
                    return order;
                };
            }

            FieldType::U16 => {
                let offset = shard1.facets[field.idx].offset;

                let facet_value_1 = read_u16(
                    &shard1.facets_file_mmap,
                    (shard1.facets_size_sum * doc_id1) + offset,
                );
                let facet_value_2 = read_u16(
                    &shard2.facets_file_mmap,
                    (shard2.facets_size_sum * doc_id2) + offset,
                );

                let order = if field.order == SortOrder::Descending {
                    facet_value_1.cmp(&facet_value_2)
                } else {
                    facet_value_2.cmp(&facet_value_1)
                };

                if order != core::cmp::Ordering::Equal {
                    return order;
                };
            }
            FieldType::U32 => {
                let offset = shard1.facets[field.idx].offset;

                let facet_value_1 = read_u32(
                    &shard1.facets_file_mmap,
                    (shard1.facets_size_sum * doc_id1) + offset,
                );
                let facet_value_2 = read_u32(
                    &shard2.facets_file_mmap,
                    (shard2.facets_size_sum * doc_id2) + offset,
                );

                let order = if field.order == SortOrder::Descending {
                    facet_value_1.cmp(&facet_value_2)
                } else {
                    facet_value_2.cmp(&facet_value_1)
                };

                if order != core::cmp::Ordering::Equal {
                    return order;
                };
            }
            FieldType::U64 => {
                let offset = shard1.facets[field.idx].offset;

                let facet_value_1 = read_u64(
                    &shard1.facets_file_mmap,
                    (shard1.facets_size_sum * doc_id1) + offset,
                );
                let facet_value_2 = read_u64(
                    &shard2.facets_file_mmap,
                    (shard2.facets_size_sum * doc_id2) + offset,
                );

                let order = if field.order == SortOrder::Descending {
                    facet_value_1.cmp(&facet_value_2)
                } else {
                    facet_value_2.cmp(&facet_value_1)
                };

                if order != core::cmp::Ordering::Equal {
                    return order;
                };
            }

            FieldType::I8 => {
                let offset = shard1.facets[field.idx].offset;

                let facet_value_1 = read_i8(
                    &shard1.facets_file_mmap,
                    (shard1.facets_size_sum * doc_id1) + offset,
                );
                let facet_value_2 = read_i8(
                    &shard2.facets_file_mmap,
                    (shard2.facets_size_sum * doc_id2) + offset,
                );

                let order = if field.order == SortOrder::Descending {
                    facet_value_1.cmp(&facet_value_2)
                } else {
                    facet_value_2.cmp(&facet_value_1)
                };

                if order != core::cmp::Ordering::Equal {
                    return order;
                };
            }

            FieldType::I16 => {
                let offset = shard1.facets[field.idx].offset;

                let facet_value_1 = read_i16(
                    &shard1.facets_file_mmap,
                    (shard1.facets_size_sum * doc_id1) + offset,
                );
                let facet_value_2 = read_i16(
                    &shard2.facets_file_mmap,
                    (shard2.facets_size_sum * doc_id2) + offset,
                );

                let order = if field.order == SortOrder::Descending {
                    facet_value_1.cmp(&facet_value_2)
                } else {
                    facet_value_2.cmp(&facet_value_1)
                };

                if order != core::cmp::Ordering::Equal {
                    return order;
                };
            }
            FieldType::I32 => {
                let offset = shard1.facets[field.idx].offset;

                let facet_value_1 = read_i32(
                    &shard1.facets_file_mmap,
                    (shard1.facets_size_sum * doc_id1) + offset,
                );
                let facet_value_2 = read_i32(
                    &shard2.facets_file_mmap,
                    (shard2.facets_size_sum * doc_id2) + offset,
                );

                let order = if field.order == SortOrder::Descending {
                    facet_value_1.cmp(&facet_value_2)
                } else {
                    facet_value_2.cmp(&facet_value_1)
                };

                if order != core::cmp::Ordering::Equal {
                    return order;
                };
            }
            FieldType::I64 => {
                let offset = shard1.facets[field.idx].offset;

                let facet_value_1 = read_i64(
                    &shard1.facets_file_mmap,
                    (shard1.facets_size_sum * doc_id1) + offset,
                );
                let facet_value_2 = read_i64(
                    &shard2.facets_file_mmap,
                    (shard2.facets_size_sum * doc_id2) + offset,
                );

                let order = if field.order == SortOrder::Descending {
                    facet_value_1.cmp(&facet_value_2)
                } else {
                    facet_value_2.cmp(&facet_value_1)
                };

                if order != core::cmp::Ordering::Equal {
                    return order;
                };
            }

            FieldType::Timestamp => {
                let offset = shard1.facets[field.idx].offset;

                let facet_value_1 = read_i64(
                    &shard1.facets_file_mmap,
                    (shard1.facets_size_sum * doc_id1) + offset,
                );
                let facet_value_2 = read_i64(
                    &shard2.facets_file_mmap,
                    (shard2.facets_size_sum * doc_id2) + offset,
                );

                let order = if field.order == SortOrder::Descending {
                    facet_value_1.cmp(&facet_value_2)
                } else {
                    facet_value_2.cmp(&facet_value_1)
                };

                if order != core::cmp::Ordering::Equal {
                    return order;
                };
            }

            FieldType::F32 => {
                let offset = shard1.facets[field.idx].offset;

                let facet_value_1 = read_f32(
                    &shard1.facets_file_mmap,
                    (shard1.facets_size_sum * doc_id1) + offset,
                );
                let facet_value_2 = read_f32(
                    &shard2.facets_file_mmap,
                    (shard2.facets_size_sum * doc_id2) + offset,
                );

                let order = if field.order == SortOrder::Descending {
                    facet_value_1
                        .partial_cmp(&facet_value_2)
                        .unwrap_or(core::cmp::Ordering::Equal)
                } else {
                    facet_value_2
                        .partial_cmp(&facet_value_1)
                        .unwrap_or(core::cmp::Ordering::Equal)
                };

                if order != core::cmp::Ordering::Equal {
                    return order;
                };
            }

            FieldType::F64 => {
                let offset = shard1.facets[field.idx].offset;

                let facet_value_1 = read_f64(
                    &shard1.facets_file_mmap,
                    (shard1.facets_size_sum * doc_id1) + offset,
                );
                let facet_value_2 = read_f64(
                    &shard2.facets_file_mmap,
                    (shard2.facets_size_sum * doc_id2) + offset,
                );

                let order = if field.order == SortOrder::Descending {
                    facet_value_1
                        .partial_cmp(&facet_value_2)
                        .unwrap_or(core::cmp::Ordering::Equal)
                } else {
                    facet_value_2
                        .partial_cmp(&facet_value_1)
                        .unwrap_or(core::cmp::Ordering::Equal)
                };

                if order != core::cmp::Ordering::Equal {
                    return order;
                };
            }

            FieldType::String16 => {
                let offset = shard1.facets[field.idx].offset;

                let facet_id_1 = read_u16(
                    &shard1.facets_file_mmap,
                    (shard1.facets_size_sum * doc_id1) + offset,
                );
                let facet_id_2 = read_u16(
                    &shard2.facets_file_mmap,
                    (shard2.facets_size_sum * doc_id2) + offset,
                );

                let facet_value_1 = shard1.facets[field.idx]
                    .values
                    .get_index((facet_id_1).into())
                    .unwrap()
                    .1[0]
                    .clone();

                let facet_value_2 = shard2.facets[field.idx]
                    .values
                    .get_index((facet_id_2).into())
                    .unwrap()
                    .1[0]
                    .clone();

                let order = if field.order == SortOrder::Descending {
                    facet_value_1.cmp(&facet_value_2)
                } else {
                    facet_value_2.cmp(&facet_value_1)
                };

                if order != core::cmp::Ordering::Equal {
                    return order;
                };
            }

            FieldType::StringSet16 => {
                let offset = shard1.facets[field.idx].offset;

                let facet_id_1 = read_u16(
                    &shard1.facets_file_mmap,
                    (shard1.facets_size_sum * doc_id1) + offset,
                );
                let facet_id_2 = read_u16(
                    &shard2.facets_file_mmap,
                    (shard2.facets_size_sum * doc_id2) + offset,
                );

                let facet_value_1 = shard1.facets[field.idx]
                    .values
                    .get_index((facet_id_1).into())
                    .unwrap()
                    .1[0]
                    .clone();

                let facet_value_2 = shard2.facets[field.idx]
                    .values
                    .get_index((facet_id_2).into())
                    .unwrap()
                    .1[0]
                    .clone();

                let order = if field.order == SortOrder::Descending {
                    facet_value_1.cmp(&facet_value_2)
                } else {
                    facet_value_2.cmp(&facet_value_1)
                };

                if order != core::cmp::Ordering::Equal {
                    return order;
                };
            }

            FieldType::String32 => {
                let offset = shard1.facets[field.idx].offset;

                let facet_id_1 = read_u32(
                    &shard1.facets_file_mmap,
                    (shard1.facets_size_sum * doc_id1) + offset,
                );
                let facet_id_2 = read_u32(
                    &shard2.facets_file_mmap,
                    (shard2.facets_size_sum * doc_id2) + offset,
                );

                let facet_value_1 = shard1.facets[field.idx]
                    .values
                    .get_index(facet_id_1 as usize)
                    .unwrap()
                    .1[0]
                    .clone();

                let facet_value_2 = shard2.facets[field.idx]
                    .values
                    .get_index(facet_id_2 as usize)
                    .unwrap()
                    .1[0]
                    .clone();

                let order = if field.order == SortOrder::Descending {
                    facet_value_1.cmp(&facet_value_2)
                } else {
                    facet_value_2.cmp(&facet_value_1)
                };

                if order != core::cmp::Ordering::Equal {
                    return order;
                };
            }

            FieldType::StringSet32 => {
                let offset = shard1.facets[field.idx].offset;

                let facet_id_1 = read_u32(
                    &shard1.facets_file_mmap,
                    (shard1.facets_size_sum * doc_id1) + offset,
                );
                let facet_id_2 = read_u32(
                    &shard2.facets_file_mmap,
                    (shard2.facets_size_sum * doc_id2) + offset,
                );

                let facet_value_1 = shard1.facets[field.idx]
                    .values
                    .get_index(facet_id_1 as usize)
                    .unwrap()
                    .1[0]
                    .clone();

                let facet_value_2 = shard2.facets[field.idx]
                    .values
                    .get_index(facet_id_2 as usize)
                    .unwrap()
                    .1[0]
                    .clone();

                let order = if field.order == SortOrder::Descending {
                    facet_value_1.cmp(&facet_value_2)
                } else {
                    facet_value_2.cmp(&facet_value_1)
                };

                if order != core::cmp::Ordering::Equal {
                    return order;
                };
            }

            _ => {}
        }
    }

    if shard_number == 1 && shard1.active.is_some() {
        return shard1.rank(result1.score, doc_id1, result2.score, doc_id2);
    }
    if empty_query && result_sort.is_empty() {
        result1.doc_id.cmp(&result2.doc_id)
    } else {
        rank_ordering(result1.score, result1.doc_id, result2.score, result2.doc_id)
    }
}

impl<'a> MinHeap<'a> {
    #[inline(always)]
    pub(crate) fn new(
        size: usize,
        index: &'a Shard,
        empty_query: bool,
        result_sort: &'a Vec<ResultSortIndex>,
    ) -> MinHeap<'a> {
        MinHeap {
            current_heap_size: 0,
            docid_hashset: AHashMap::new(),
            _elements: vec![
                Result {
                    doc_id: 0,
                    score: 0.0,

                    #[cfg(feature = "vb")]
                    source: ResultSource::Lexical,
                    ..Default::default()
                };
                size
            ],
            index,
            empty_query,
            result_sort,
        }
    }

    #[inline]
    pub(crate) fn result_ordering_shard(
        &self,
        result1: Result,
        result2: Result,
    ) -> core::cmp::Ordering {
        for field in self.result_sort.iter() {
            if field.idx == usize::MAX {
                let order = if field.order == SortOrder::Descending {
                    result1.doc_id.cmp(&result2.doc_id)
                } else {
                    result2.doc_id.cmp(&result1.doc_id)
                };

                return order;
            }

            if field.idx == usize::MAX - 1 {
                let order = if field.order == SortOrder::Descending {
                    score_ordering(result1.score, result2.score)
                } else {
                    score_ordering(result2.score, result1.score)
                };

                if !order.is_eq() {
                    return order;
                }
                continue;
            }

            match self.index.facets[field.idx].field_type {
                FieldType::U8 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = &self.index.facets_file_mmap
                        [(self.index.facets_size_sum * result1.doc_id) + offset];

                    let facet_value_2 = &self.index.facets_file_mmap
                        [(self.index.facets_size_sum * result2.doc_id) + offset];

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(facet_value_2)
                    } else {
                        facet_value_2.cmp(facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                FieldType::U16 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_u16(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_u16(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }
                FieldType::U32 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_u32(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_u32(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }
                FieldType::U64 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_u64(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_u64(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                FieldType::I8 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_i8(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_i8(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                FieldType::I16 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_i16(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_i16(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }
                FieldType::I32 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_i32(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_i32(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }
                FieldType::I64 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_i64(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_i64(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                FieldType::Timestamp => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_i64(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_i64(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                FieldType::F32 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_f32(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_f32(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1
                            .partial_cmp(&facet_value_2)
                            .unwrap_or(core::cmp::Ordering::Equal)
                    } else {
                        facet_value_2
                            .partial_cmp(&facet_value_1)
                            .unwrap_or(core::cmp::Ordering::Equal)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                FieldType::F64 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_value_1 = read_f64(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_value_2 = read_f64(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1
                            .partial_cmp(&facet_value_2)
                            .unwrap_or(core::cmp::Ordering::Equal)
                    } else {
                        facet_value_2
                            .partial_cmp(&facet_value_1)
                            .unwrap_or(core::cmp::Ordering::Equal)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                FieldType::String16 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_id_1 = read_u16(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_id_2 = read_u16(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let facet_value_1 = self.index.facets[field.idx]
                        .values
                        .get_index((facet_id_1).into())
                        .unwrap()
                        .1[0]
                        .clone();

                    let facet_value_2 = self.index.facets[field.idx]
                        .values
                        .get_index((facet_id_2).into())
                        .unwrap()
                        .1[0]
                        .clone();

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                FieldType::StringSet16 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_id_1 = read_u16(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_id_2 = read_u16(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let facet_value_1 = self.index.facets[field.idx]
                        .values
                        .get_index((facet_id_1).into())
                        .unwrap()
                        .1[0]
                        .clone();

                    let facet_value_2 = self.index.facets[field.idx]
                        .values
                        .get_index((facet_id_2).into())
                        .unwrap()
                        .1[0]
                        .clone();

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                FieldType::String32 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_id_1 = read_u32(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_id_2 = read_u32(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let facet_value_1 = self.index.facets[field.idx]
                        .values
                        .get_index(facet_id_1 as usize)
                        .unwrap()
                        .1[0]
                        .clone();

                    let facet_value_2 = self.index.facets[field.idx]
                        .values
                        .get_index(facet_id_2 as usize)
                        .unwrap()
                        .1[0]
                        .clone();

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                FieldType::StringSet32 => {
                    let offset = self.index.facets[field.idx].offset;

                    let facet_id_1 = read_u32(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result1.doc_id) + offset,
                    );
                    let facet_id_2 = read_u32(
                        &self.index.facets_file_mmap,
                        (self.index.facets_size_sum * result2.doc_id) + offset,
                    );

                    let facet_value_1 = self.index.facets[field.idx]
                        .values
                        .get_index(facet_id_1 as usize)
                        .unwrap()
                        .1[0]
                        .clone();

                    let facet_value_2 = self.index.facets[field.idx]
                        .values
                        .get_index(facet_id_2 as usize)
                        .unwrap()
                        .1[0]
                        .clone();

                    let order = if field.order == SortOrder::Descending {
                        facet_value_1.cmp(&facet_value_2)
                    } else {
                        facet_value_2.cmp(&facet_value_1)
                    };

                    if order != core::cmp::Ordering::Equal {
                        return order;
                    };
                }

                _ => {}
            }
        }

        if self.index.active.is_some() {
            return self.index.rank(result1.score, result1.doc_id, result2.score, result2.doc_id);
        }
        if self.empty_query && self.result_sort.is_empty() {
            result1.doc_id.cmp(&result2.doc_id)
        } else {
            rank_ordering(result1.score, result1.doc_id, result2.score, result2.doc_id)
        }
    }

    #[inline(always)]
    fn get_left_child_index(element_index: usize) -> usize {
        2 * element_index + 1
    }

    #[inline(always)]
    fn get_right_child_index(element_index: usize) -> usize {
        2 * element_index + 2
    }

    #[inline(always)]
    fn get_parent_index(element_index: usize) -> usize {
        (element_index - 1) / 2
    }

    #[inline(always)]
    fn has_left_child(&self, element_index: usize) -> bool {
        Self::get_left_child_index(element_index) < self.current_heap_size
    }

    #[inline(always)]
    fn has_right_child(&self, element_index: usize) -> bool {
        Self::get_right_child_index(element_index) < self.current_heap_size
    }

    #[inline(always)]
    fn is_root(element_index: usize) -> bool {
        element_index == 0
    }

    #[inline(always)]
    fn get_left_child(&self, element_index: usize) -> &Result {
        &self._elements[Self::get_left_child_index(element_index)]
    }

    #[inline(always)]
    fn get_right_child(&self, element_index: usize) -> &Result {
        &self._elements[Self::get_right_child_index(element_index)]
    }

    #[inline(always)]
    fn get_parent(&self, element_index: usize) -> &Result {
        &self._elements[Self::get_parent_index(element_index)]
    }

    #[inline(always)]
    fn swap(&mut self, first_index: usize, second_index: usize) {
        self._elements.swap(first_index, second_index);
    }

    #[inline(always)]
    fn add(&mut self, result: &Result) {
        self._elements[self.current_heap_size].score = result.score;
        self._elements[self.current_heap_size].doc_id = result.doc_id;
        self.current_heap_size += 1;

        self.heapify_up();
    }

    #[inline(always)]
    fn pop_add(&mut self, score: f32, doc_id: usize) {
        if !self.docid_hashset.is_empty() {
            self.docid_hashset.remove(&self._elements[0].doc_id);
        }

        self._elements[0].score = score;
        self._elements[0].doc_id = doc_id;
        self.heapify_down();
    }

    #[inline(always)]
    fn heapify_up(&mut self) {
        let mut index = self.current_heap_size - 1;
        while !Self::is_root(index)
            && self
                .result_ordering_shard(self._elements[index], *Self::get_parent(self, index))
                .is_lt()
        {
            let parent_index = Self::get_parent_index(index);
            self.swap(parent_index, index);
            index = parent_index;
        }
    }

    #[inline(always)]
    fn heapify_down(&mut self) {
        let mut index: usize = 0;
        while self.has_left_child(index) {
            let mut smaller_index = Self::get_left_child_index(index);
            if self.has_right_child(index)
                && self
                    .result_ordering_shard(
                        *self.get_right_child(index),
                        *self.get_left_child(index),
                    )
                    .is_lt()
            {
                smaller_index = Self::get_right_child_index(index);
            }
            if self
                .result_ordering_shard(self._elements[smaller_index], self._elements[index])
                .is_ge()
            {
                break;
            }

            self.swap(smaller_index, index);
            index = smaller_index;
        }
    }

    #[inline(always)]
    fn heapify_down_index(&mut self, index: usize) {
        let mut index: usize = index;
        while self.has_left_child(index) {
            let mut smaller_index = Self::get_left_child_index(index);
            if self.has_right_child(index)
                && self
                    .result_ordering_shard(
                        *self.get_right_child(index),
                        *self.get_left_child(index),
                    )
                    .is_lt()
            {
                smaller_index = Self::get_right_child_index(index);
            }

            if self
                .result_ordering_shard(self._elements[smaller_index], self._elements[index])
                .is_ge()
            {
                break;
            }

            self.swap(smaller_index, index);
            index = smaller_index;
        }
    }

    /// Prune only if even the best possible (score upper bound, ID lower
    /// bound) cannot improve the full heap. Bounds and IDs are shard-local.
    /// Unknown/NaN bounds, browsing and explicit user sorts must keep scanning.
    #[inline(always)]
    pub(crate) fn can_prune(&self, score_upper: f32, id_lower: usize, top_k: usize) -> bool {
        if top_k == 0
            || self.current_heap_size < top_k
            || self.empty_query
            || !self.result_sort.is_empty()
            || score_upper.is_nan()
        {
            return false;
        }
        let worst = self._elements[0];
        score_upper < worst.score || (self.index.active.is_none()
            && score_upper == worst.score && id_lower >= worst.doc_id)
    }

    /// Snapshot the complete worst rank, including ID improvements at equal score.
    #[inline(always)]
    pub(crate) fn heap_progress(&self) -> (usize, Result) {
        let size = self.current_heap_size;
        let min = if size > 0 {
            self._elements[0]
        } else {
            Result {
                doc_id: usize::MAX,
                score: f32::NEG_INFINITY,
                ..Default::default()
            }
        };
        (size, min)
    }

    /// Remember every heap entry in docid_hashset so re-scoring a document
    /// refreshes it instead of duplicating it.
    #[inline(always)]
    pub(crate) fn pin_heap(&mut self) {
        for i in 0..self.current_heap_size {
            self.docid_hashset
                .insert(self._elements[i].doc_id, self._elements[i].score);
        }
    }

    #[inline(always)]
    pub(crate) fn add_topk(&mut self, result: Result, top_k: usize) -> bool {
        if top_k == 0 {
            return false;
        }
        if self.current_heap_size >= top_k
            && self
                .result_ordering_shard(self._elements[0], result)
                .is_ge()
        {
            return false;
        }

        if !self.docid_hashset.is_empty() && self.docid_hashset.contains_key(&result.doc_id) {
            if self._elements[0].doc_id == result.doc_id {
                if self
                    .result_ordering_shard(result, self._elements[0])
                    .is_gt()
                {
                    self._elements[0].score = result.score;
                    self.heapify_down();
                    return true;
                } else {
                    return false;
                }
            } else {
                if self
                    .result_ordering_shard(
                        Result {
                            doc_id: result.doc_id,
                            score: self.docid_hashset[&result.doc_id],

                            #[cfg(feature = "vb")]
                            source: ResultSource::Lexical,
                            ..Default::default()
                        },
                        result,
                    )
                    .is_ge()
                {
                    return false;
                }

                let mut index = 0;
                while result.doc_id != self._elements[index].doc_id {
                    if index == self.current_heap_size - 1 {
                        self.pop_add(result.score, result.doc_id);
                        return true;
                    }
                    index += 1;
                }

                self._elements[index].score = result.score;
                self.heapify_down_index(index);
                return true;
            }
        }

        if self.current_heap_size < top_k {
            self.add(&result);
            true
        } else if self
            .result_ordering_shard(result, self._elements[0])
            .is_gt()
        {
            self.pop_add(result.score, result.doc_id);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod stable_ranking_tests {
    use super::*;

    #[tokio::test]
    async fn stable_heap_topk_input_order_and_duplicates() {
        use crate::index::*;
        use crate::vector::Inference;
        let path =
            std::env::temp_dir().join(format!("seekpower-stable-heap-{}", std::process::id()));
        assert!(!path.exists());
        let schema = serde_json::from_str(
            r#"[{"field":"body","field_type":"Text","store":false,"index_lexical":true}]"#,
        )
        .unwrap();
        let meta = IndexMetaObject {
            id: 0,
            name: "heap".into(),
            lexical_similarity: LexicalSimilarity::Bm25f,
            tokenizer: TokenizerType::UnicodeAlphanumeric,
        text_normalization: crate::index::TextNormalization::None,
        field_length_normalization: crate::index::FieldLengthNormalization::Shared,
            stop_words: StopwordType::None,
            frequent_words: FrequentwordType::None,
            ngram_indexing: NgramSet::SingleTerm as u8,
            document_compression: DocumentCompression::Snappy,
            access_type: AccessType::Mmap,
            clustering: Clustering::None,
            inference: Inference::None,
        };
        let index = create_index(&path, meta, &schema, 11, false, Some(1))
            .await
            .unwrap();
        {
            let root = index.read().await;
            let shard = root.shard_vec[0].read().await;
            let sorts = vec![];
            for order in [
                (0..97).collect::<Vec<_>>(),
                (0..97).rev().collect(),
                (0..97).map(|i| (i * 37) % 97).collect(),
            ] {
                let mut heap = MinHeap::new(7, &shard, false, &sorts);
                for id in order {
                    heap.add_topk(
                        Result {
                            doc_id: id,
                            score: 1.0,
                            ..Default::default()
                        },
                        7,
                    );
                }
                assert!(heap.can_prune(1.0, 6, 7));
                assert!(heap.can_prune(1.0, 1 << 16, 7));
                assert!(!heap.can_prune(1.0, 0, 7));
                assert!(!heap.can_prune(1.0, 5, 7));
                assert!(heap.can_prune(0.5, 0, 7));
                assert!(!heap.can_prune(2.0, usize::MAX, 7));
                assert!(!heap.can_prune(f32::NAN, usize::MAX, 7));
                assert!(!heap.can_prune(0.0, 100, 8));
                // A bound may be discarded only when every represented key
                // is no better than the actual root, including equal IDs.
                for upper in [0.5, 1.0, 2.0] {
                    for lower in [0, 5, 6, 65536] {
                        if heap.can_prune(upper, lower, 7) {
                            for score in [upper - 0.5, upper] {
                                for id in [lower, lower + 1] {
                                    assert!(rank_ordering(score, id, 1.0, 6).is_le());
                                }
                            }
                        }
                    }
                }
                let explicit = vec![ResultSortIndex {
                    idx: usize::MAX - 1,
                    order: SortOrder::Descending,
                }];
                heap.result_sort = &explicit;
                assert!(!heap.can_prune(0.0, usize::MAX, 7));
                heap.result_sort = &sorts;
                heap.empty_query = true;
                assert!(!heap.can_prune(0.0, usize::MAX, 7));
                heap.empty_query = false;
                heap.pin_heap();
                assert!(!heap.add_topk(
                    Result {
                        doc_id: 0,
                        score: 1.0,
                        ..Default::default()
                    },
                    7
                ));
                let mut ids: Vec<_> = heap._elements[..heap.current_heap_size]
                    .iter()
                    .map(|x| x.doc_id)
                    .collect();
                ids.sort();
                assert_eq!(ids, (0..7).collect::<Vec<_>>());
            }
        }
        index.close().await.unwrap();
        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn stable_score_order_is_total_and_zero_is_a_tie() {
        let values = [
            f32::NAN,
            f32::from_bits(0xffc00001),
            f32::NEG_INFINITY,
            -1.0,
            -0.0,
            0.0,
            1.0,
            f32::INFINITY,
        ];
        assert!(score_ordering(-0.0, 0.0).is_eq());
        assert!(rank_ordering(-0.0, 1, 0.0, 2).is_gt());
        for a in values {
            for b in values {
                for c in values {
                    assert_eq!(score_ordering(a, b), score_ordering(b, a).reverse());
                    if score_ordering(a, b).is_le() && score_ordering(b, c).is_le() {
                        assert!(score_ordering(a, c).is_le());
                    }
                }
            }
        }
    }
}
