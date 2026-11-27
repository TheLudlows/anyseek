use tokio::sync::RwLockReadGuard;

use serde::{Deserialize, Serialize};

#[cfg(feature = "vb")]
use crate::vector::ResultSource;
use crate::{
    index::{FieldType, VectorSegment},
    record::{build_budget::admit_memory, RecordError},
    resources::{MemoryReservation, Scope},
    search::{ResultSortIndex, SortOrder},
    utils::{read_f64, read_i64, read_u32},
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
pub(crate) fn rank_with_segments(
    shards: &[RwLockReadGuard<'_, VectorSegment>],
    a: f32,
    a_id: usize,
    b: f32,
    b_id: usize,
) -> core::cmp::Ordering {
    if shards.iter().all(|shard| shard.active.is_some()) {
        let n = shards.len();
        let a_key = &shards[a_id % n].active.as_ref().unwrap().keys[a_id / n];
        let b_key = &shards[b_id % n].active.as_ref().unwrap().keys[b_id / n];
        score_ordering(a, b)
            .then_with(|| b_key.cmp(a_key))
            .then_with(|| b_id.cmp(&a_id))
    } else {
        rank_ordering(a, a_id, b, b_id)
    }
}

enum FacetValue<'a> {
    Integer(i64),
    Float(f64),
    String(Option<&'a str>),
    Present,
}

#[derive(Default)]
pub(crate) struct SortKey<'a> {
    fields: Vec<Option<FacetValue<'a>>>,
    _memory: Option<MemoryReservation>,
}

fn has_metadata(sorts: &[ResultSortIndex]) -> bool {
    sorts
        .iter()
        .take_while(|field| field.idx != usize::MAX)
        .any(|field| field.idx < usize::MAX - 1)
}

/// Read keys before inserting into a heap or sorting. Comparison itself is
/// infallible so an I/O failure cannot leave a partially ordered response.
pub(crate) fn prepare_sort_key<'a>(
    segments: &'a [RwLockReadGuard<'_, VectorSegment>],
    sorts: &[ResultSortIndex],
    doc_id: usize,
) -> std::result::Result<SortKey<'a>, RecordError> {
    if !has_metadata(sorts) {
        return Ok(SortKey::default());
    }
    let shard = &segments[doc_id % segments.len()];
    let ordinal = doc_id / segments.len();
    let fields_to_read = sorts.iter().take_while(|field| field.idx != usize::MAX);
    let count = fields_to_read.clone().count();
    let bytes = size_of::<Option<FacetValue>>()
        .checked_mul(count)
        .ok_or_else(|| RecordError::Rejected("sort key size overflow".into()))?;
    let memory = admit_memory(bytes as u64, Scope::current().as_ref())?;
    let mut fields = Vec::with_capacity(count);
    for sort in fields_to_read {
        let value = if sort.idx >= usize::MAX - 1 {
            None
        } else {
            let facet = &shard.facets[sort.idx];
            let bytes = shard.read_facet(ordinal, facet)?;
            let present = bytes[crate::component::field_width(&facet.field_type) - 1] != 0;
            if present {
                match facet.field_type {
                    FieldType::Int | FieldType::Timestamp => {
                        Some(FacetValue::Integer(read_i64(&bytes, 0)))
                    }
                    FieldType::Float => {
                        let value = read_f64(&bytes, 0);
                        if !value.is_finite() {
                            return Err(RecordError::Storage("invalid metadata float".into()));
                        }
                        Some(FacetValue::Float(value))
                    }
                    FieldType::String | FieldType::StringSet => {
                        let (_, values) = facet
                            .values
                            .get_index(read_u32(&bytes, 0) as usize)
                            .ok_or_else(|| {
                                RecordError::Storage("invalid metadata dictionary index".into())
                            })?;
                        Some(FacetValue::String(values.first().map(String::as_str)))
                    }
                    _ => Some(FacetValue::Present),
                }
            } else {
                None
            }
        };
        fields.push(value);
    }
    Ok(SortKey {
        fields,
        _memory: Some(memory),
    })
}

pub(crate) fn sort_results(
    segments: &[RwLockReadGuard<'_, VectorSegment>],
    empty_query: bool,
    sorts: &[ResultSortIndex],
    mut results: Vec<Result>,
) -> std::result::Result<Vec<Result>, RecordError> {
    if !has_metadata(sorts) {
        let empty = SortKey::default();
        results.sort_by(|a, b| {
            result_ordering_root(segments, empty_query, sorts, *b, &empty, *a, &empty)
        });
        return Ok(results);
    }
    let bytes = size_of::<(Result, SortKey)>()
        .checked_mul(results.len())
        .ok_or_else(|| RecordError::Rejected("sort workspace size overflow".into()))?;
    let _workspace = admit_memory(bytes as u64, Scope::current().as_ref())?;
    let mut keyed = Vec::with_capacity(results.len());
    for result in results {
        keyed.push((result, prepare_sort_key(segments, sorts, result.doc_id)?));
    }
    keyed.sort_by(|(a, a_key), (b, b_key)| {
        result_ordering_root(segments, empty_query, sorts, *b, b_key, *a, a_key)
    });
    Ok(keyed.into_iter().map(|(result, _)| result).collect())
}

pub(crate) fn result_ordering_root(
    segments: &[RwLockReadGuard<'_, VectorSegment>],
    empty_query: bool,
    result_sort: &[ResultSortIndex],
    result1: Result,
    key1: &SortKey<'_>,
    result2: Result,
    key2: &SortKey<'_>,
) -> core::cmp::Ordering {
    let segment_count = segments.len();
    let slot1 = result1.doc_id % segment_count;
    let doc_id1 = result1.doc_id / segment_count;
    let shard1 = &segments[slot1];

    let slot2 = result2.doc_id % segment_count;
    let doc_id2 = result2.doc_id / segment_count;
    let shard2 = &segments[slot2];

    for (index, field) in result_sort.iter().enumerate() {
        if field.idx == usize::MAX {
            if segments.iter().all(|shard| shard.active.is_some()) {
                let first = &shard1.active.as_ref().unwrap().keys[doc_id1];
                let second = &shard2.active.as_ref().unwrap().keys[doc_id2];
                return if field.order == SortOrder::Descending {
                    first.cmp(second)
                } else {
                    second.cmp(first)
                };
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

        use FacetValue::*;
        use core::cmp::Ordering;
        let order = match (&key1.fields[index], &key2.fields[index]) {
            // Present values rank before null/missing in either direction.
            (Some(_), None) => return Ordering::Greater,
            (None, Some(_)) => return Ordering::Less,
            (None, None) => continue,
            (Some(Integer(a)), Some(Integer(b))) => a.cmp(b),
            (Some(Float(a)), Some(Float(b))) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Some(String(a)), Some(String(b))) => a.cmp(b),
            (Some(Present), Some(Present)) => Ordering::Equal,
            _ => unreachable!("facet sort keys use the shared schema"),
        };
        if !order.is_eq() {
            return if field.order == SortOrder::Descending {
                order
            } else {
                order.reverse()
            };
        }
    }

    if segments.iter().all(|shard| shard.active.is_some()) {
        return rank_with_segments(
            segments,
            result1.score,
            result1.doc_id,
            result2.score,
            result2.doc_id,
        );
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
