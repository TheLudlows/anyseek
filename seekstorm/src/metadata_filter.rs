//! Shared schema validation and per-shard metadata predicates for all search modes.
use crate::{
    index::{FieldType, SchemaField, Shard},
    search::FacetFilter,
    utils::{
        read_f32, read_f64, read_i8, read_i16, read_i32, read_i64, read_u8, read_u16, read_u32,
        read_u64,
    },
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, ops::Range};

#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq)]
pub(crate) enum FilterSparse {
    U8(Range<u8>),
    U16(Range<u16>),
    U32(Range<u32>),
    U64(Range<u64>),
    I8(Range<i8>),
    I16(Range<i16>),
    I32(Range<i32>),
    I64(Range<i64>),
    /// Unix timestamp: the number of seconds since 1 January 1970
    Timestamp(Range<i64>),
    F32(Range<f32>),
    F64(Range<f64>),
    String16(Vec<u16>),
    String32(Vec<u32>),
    And(Vec<FilterSparse>),
    #[default]
    None,
}

pub(crate) fn validate_filters(
    schema: &HashMap<String, SchemaField>,
    filters: &[FacetFilter],
) -> Result<(), String> {
    for filter in filters {
        let (field, expected, valid_range) = match filter {
            FacetFilter::U8 { field, filter } => (field, FieldType::U8, filter.start <= filter.end),
            FacetFilter::U16 { field, filter } => {
                (field, FieldType::U16, filter.start <= filter.end)
            }
            FacetFilter::U32 { field, filter } => {
                (field, FieldType::U32, filter.start <= filter.end)
            }
            FacetFilter::U64 { field, filter } => {
                (field, FieldType::U64, filter.start <= filter.end)
            }
            FacetFilter::I8 { field, filter } => (field, FieldType::I8, filter.start <= filter.end),
            FacetFilter::I16 { field, filter } => {
                (field, FieldType::I16, filter.start <= filter.end)
            }
            FacetFilter::I32 { field, filter } => {
                (field, FieldType::I32, filter.start <= filter.end)
            }
            FacetFilter::I64 { field, filter } => {
                (field, FieldType::I64, filter.start <= filter.end)
            }
            FacetFilter::Timestamp { field, filter } => {
                (field, FieldType::Timestamp, filter.start <= filter.end)
            }
            FacetFilter::F32 { field, filter } => (
                field,
                FieldType::F32,
                filter.start <= filter.end && filter.start.is_finite() && filter.end.is_finite(),
            ),
            FacetFilter::F64 { field, filter } => (
                field,
                FieldType::F64,
                filter.start <= filter.end && filter.start.is_finite() && filter.end.is_finite(),
            ),
            FacetFilter::String16 { field, .. } => (field, FieldType::String16, true),
            FacetFilter::String32 { field, .. } => (field, FieldType::String32, true),
            FacetFilter::StringSet16 { field, .. } => (field, FieldType::StringSet16, true),
            FacetFilter::StringSet32 { field, .. } => (field, FieldType::StringSet32, true),
        };
        let definition = schema
            .get(field)
            .ok_or_else(|| format!("facet_filter field '{field}' does not exist"))?;
        if !definition.facet || definition.field_type != expected {
            return Err(format!(
                "facet_filter field '{field}' requires facet:true and type {expected:?}"
            ));
        }
        if !valid_range {
            return Err(format!(
                "facet_filter field '{field}' requires finite range bounds with start <= end"
            ));
        }
    }
    Ok(())
}

pub(crate) fn compile_filters(
    shard: &Shard,
    filters: &[FacetFilter],
) -> Result<Vec<FilterSparse>, String> {
    validate_filters(&shard.schema_map, filters)?;
    if filters.is_empty() {
        return Ok(Vec::new());
    }
    let mut compiled = vec![FilterSparse::None; shard.facets.len()];
    for filter in filters {
        let (field, predicate) = match filter {
            FacetFilter::U8 { field, filter } => (field, FilterSparse::U8(filter.clone())),
            FacetFilter::U16 { field, filter } => (field, FilterSparse::U16(filter.clone())),
            FacetFilter::U32 { field, filter } => (field, FilterSparse::U32(filter.clone())),
            FacetFilter::U64 { field, filter } => (field, FilterSparse::U64(filter.clone())),
            FacetFilter::I8 { field, filter } => (field, FilterSparse::I8(filter.clone())),
            FacetFilter::I16 { field, filter } => (field, FilterSparse::I16(filter.clone())),
            FacetFilter::I32 { field, filter } => (field, FilterSparse::I32(filter.clone())),
            FacetFilter::I64 { field, filter } => (field, FilterSparse::I64(filter.clone())),
            FacetFilter::Timestamp { field, filter } => {
                (field, FilterSparse::Timestamp(filter.clone()))
            }
            FacetFilter::F32 { field, filter } => (field, FilterSparse::F32(filter.clone())),
            FacetFilter::F64 { field, filter } => (field, FilterSparse::F64(filter.clone())),
            FacetFilter::String16 { field, filter } => {
                let idx = shard.facets_map[field];
                let mut ids = Vec::new();
                for value in filter {
                    if let Some(id) = shard.facets[idx].values.get_index_of(value) {
                        ids.push(id as u16);
                    }
                }
                ids.sort_unstable();
                ids.dedup();
                (field, FilterSparse::String16(ids))
            }
            FacetFilter::StringSet16 { field, filter } => {
                let idx = shard.facets_map[field];
                let mut ids = Vec::new();
                for value in filter {
                    if let Some(set_ids) = shard.string_set_to_single_term_id_vec[idx].get(value) {
                        ids.extend(set_ids.iter().map(|id| *id as u16));
                    }
                }
                ids.sort_unstable();
                ids.dedup();
                (field, FilterSparse::String16(ids))
            }
            FacetFilter::String32 { field, filter } => {
                let idx = shard.facets_map[field];
                let mut ids = Vec::new();
                for value in filter {
                    if let Some(id) = shard.facets[idx].values.get_index_of(value) {
                        ids.push(id as u32);
                    }
                }
                ids.sort_unstable();
                ids.dedup();
                (field, FilterSparse::String32(ids))
            }
            FacetFilter::StringSet32 { field, filter } => {
                let idx = shard.facets_map[field];
                let mut ids = Vec::new();
                for value in filter {
                    if let Some(set_ids) = shard.string_set_to_single_term_id_vec[idx].get(value) {
                        ids.extend(set_ids.iter().copied());
                    }
                }
                ids.sort_unstable();
                ids.dedup();
                (field, FilterSparse::String32(ids))
            }
        };
        let slot = &mut compiled[shard.facets_map[field]];
        match slot {
            FilterSparse::None => *slot = predicate,
            FilterSparse::And(predicates) => predicates.push(predicate),
            _ => {
                let previous = std::mem::take(slot);
                *slot = FilterSparse::And(vec![previous, predicate]);
            }
        }
    }
    Ok(compiled)
}

// True means reject this document. Empty predicates accept every document.
#[inline]
pub(crate) fn is_facet_filter(shard: &Shard, filters: &[FilterSparse], doc_id: usize) -> bool {
    filters.iter().zip(&shard.facets).any(|(filter, field)| {
        let address = shard.facets_size_sum * doc_id + field.offset;
        rejects(filter, &shard.facets_file_mmap, address)
    })
}

#[inline]
fn rejects(filter: &FilterSparse, bytes: &[u8], address: usize) -> bool {
    match filter {
        FilterSparse::U8(range) => !range.contains(&read_u8(bytes, address)),
        FilterSparse::U16(range) => !range.contains(&read_u16(bytes, address)),
        FilterSparse::U32(range) => !range.contains(&read_u32(bytes, address)),
        FilterSparse::U64(range) => !range.contains(&read_u64(bytes, address)),
        FilterSparse::I8(range) => !range.contains(&read_i8(bytes, address)),
        FilterSparse::I16(range) => !range.contains(&read_i16(bytes, address)),
        FilterSparse::I32(range) => !range.contains(&read_i32(bytes, address)),
        FilterSparse::I64(range) => !range.contains(&read_i64(bytes, address)),
        FilterSparse::Timestamp(range) => !range.contains(&read_i64(bytes, address)),
        FilterSparse::F32(range) => !range.contains(&read_f32(bytes, address)),
        FilterSparse::F64(range) => !range.contains(&read_f64(bytes, address)),
        FilterSparse::String16(ids) => ids.binary_search(&read_u16(bytes, address)).is_err(),
        FilterSparse::String32(ids) => ids.binary_search(&read_u32(bytes, address)).is_err(),
        FilterSparse::And(predicates) => predicates
            .iter()
            .any(|predicate| rejects(predicate, bytes, address)),
        FilterSparse::None => false,
    }
}
