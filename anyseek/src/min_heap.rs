use tokio::sync::RwLockReadGuard;

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

#[inline]
pub(crate) fn rank_with_shards(shards:&[RwLockReadGuard<'_,Shard>],a:f32,a_id:usize,b:f32,b_id:usize)->core::cmp::Ordering {
    if shards.iter().all(|shard|shard.active.is_some()) {
        let n=shards.len();
        let a_key=&shards[a_id%n].active.as_ref().unwrap().keys[a_id/n];
        let b_key=&shards[b_id%n].active.as_ref().unwrap().keys[b_id/n];
        score_ordering(a,b).then_with(||b_key.cmp(a_key)).then_with(||b_id.cmp(&a_id))
    } else {rank_ordering(a,a_id,b,b_id)}
}

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
            if shard_vec.iter().all(|shard|shard.active.is_some()) {
                let first=&shard1.active.as_ref().unwrap().keys[doc_id1];
                let second=&shard2.active.as_ref().unwrap().keys[doc_id2];
                return if field.order==SortOrder::Descending {first.cmp(second)}else{second.cmp(first)};
            }
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

    if shard_vec.iter().all(|shard|shard.active.is_some()) {
        return rank_with_shards(shard_vec,result1.score,result1.doc_id,result2.score,result2.doc_id);
    }
    if empty_query && result_sort.is_empty() {
        result1.doc_id.cmp(&result2.doc_id)
    } else {
        rank_ordering(result1.score, result1.doc_id, result2.score, result2.doc_id)
    }
}

#[cfg(test)]
mod stable_ranking_tests {
    use super::*;

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
