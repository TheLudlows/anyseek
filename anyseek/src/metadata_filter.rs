//! Shared schema validation and per-shard metadata predicates for all search modes.
use crate::{
    component::FacetReader,
    index::{FieldType, SchemaField, VectorSegment},
    search::FacetFilter,
    utils::{read_f64, read_i64, read_u32},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, ops::Range};

#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq)]
pub(crate) enum FilterSparse {
    Int(Range<i64>),
    /// Unix timestamp: the number of seconds since 1 January 1970
    Timestamp(Range<i64>),
    Float(Range<f64>),
    String(Vec<u32>),
    And(Vec<FilterSparse>),
    #[default]
    None,
}

pub(crate) fn validate_filters(
    schema: &HashMap<String, SchemaField>,
    filters: &[FacetFilter],
) -> Result<(), String> {
    validate_filter_fields(filters, |name| schema.get(name))
}

pub(crate) fn validate_record_filters(schema: &[SchemaField], filters: &[FacetFilter]) -> Result<(), String> {
    validate_filter_fields(filters, |name| schema.iter().find(|field| field.field == name))
}

fn validate_filter_fields<'a>(
    filters: &[FacetFilter], lookup: impl Fn(&str) -> Option<&'a SchemaField>,
) -> Result<(), String> {
    for filter in filters {
        let (field, expected, valid_range) = match filter {
            FacetFilter::Int { field, filter } => {
                (field, FieldType::Int, filter.start <= filter.end)
            }
            FacetFilter::Timestamp { field, filter } => {
                (field, FieldType::Timestamp, filter.start <= filter.end)
            }
            FacetFilter::Float { field, filter } => (
                field,
                FieldType::Float,
                filter.start <= filter.end && filter.start.is_finite() && filter.end.is_finite(),
            ),
            FacetFilter::String { field, .. } => (field, FieldType::String, true),
            FacetFilter::StringSet { field, .. } => (field, FieldType::StringSet, true),
        };
        let definition = lookup(field)
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

/// Evaluate the same validated predicates on a canonical Record body.
/// Like the segment evaluator, missing or null values reject the record.
pub(crate) fn matches_document(
    document: &crate::index::Document,
    filters: &[FacetFilter],
) -> bool {
    filters.iter().all(|predicate| match predicate {
        FacetFilter::Int { field, filter } | FacetFilter::Timestamp { field, filter } =>
            document.get(field).and_then(serde_json::Value::as_i64).is_some_and(|value| filter.contains(&value)),
        FacetFilter::Float { field, filter } =>
            document.get(field).and_then(serde_json::Value::as_f64).is_some_and(|value| filter.contains(&value)),
        FacetFilter::String { field, filter } =>
            document.get(field).and_then(serde_json::Value::as_str).is_some_and(|value| filter.iter().any(|expected| expected == value)),
        FacetFilter::StringSet { field, filter } =>
            document.get(field).and_then(serde_json::Value::as_array).is_some_and(|values|
                values.iter().any(|value| value.as_str().is_some_and(|value| filter.iter().any(|expected| expected == value)))),
    })
}

/// Bound the temporary string-set expansion before copying IDs into a predicate.
pub(crate) fn workspace_bytes(shard: &VectorSegment, filters: &[FacetFilter]) -> Result<u64, String> {
    validate_filters(&shard.schema_map, filters)?;
    let mut ids = 0u64;
    for filter in filters {
        let count = match filter {
            FacetFilter::String { filter, .. } => filter.len() as u64,
            FacetFilter::StringSet { field, filter } => {
                let values = &shard.string_set_to_single_term_id_vec[shard.facets_map[field]];
                filter.iter().filter_map(|value| values.get(value)).try_fold(0u64, |sum, matches| {
                    sum.checked_add(matches.len() as u64)
                        .ok_or_else(|| "metadata predicate memory overflow".to_owned())
                })?
            }
            _ => 0,
        };
        ids = ids.checked_add(count).ok_or_else(|| "metadata predicate memory overflow".to_owned())?;
    }
    ids.checked_mul(16)
        .and_then(|bytes| bytes.checked_add((shard.facets.len() + filters.len()) as u64 * 256))
        .ok_or_else(|| "metadata predicate memory overflow".to_owned())
}

pub(crate) fn compile_filters(
    shard: &VectorSegment,
    filters: &[FacetFilter],
) -> Result<Vec<FilterSparse>, String> {
    validate_filters(&shard.schema_map, filters)?;
    if filters.is_empty() {
        return Ok(Vec::new());
    }
    let mut compiled = vec![FilterSparse::None; shard.facets.len()];
    for filter in filters {
        let (field, predicate) = match filter {
            FacetFilter::Int { field, filter } => (field, FilterSparse::Int(filter.clone())),
            FacetFilter::Timestamp { field, filter } => {
                (field, FilterSparse::Timestamp(filter.clone()))
            }
            FacetFilter::Float { field, filter } => (field, FilterSparse::Float(filter.clone())),
            FacetFilter::String { field, filter } => {
                let idx = shard.facets_map[field];
                let mut ids = Vec::new();
                for value in filter {
                    if let Some(id) = shard.facets[idx].values.get_index_of(value) {
                        ids.push(id as u32);
                    }
                }
                ids.sort_unstable();
                ids.dedup();
                (field, FilterSparse::String(ids))
            }
            FacetFilter::StringSet { field, filter } => {
                let idx = shard.facets_map[field];
                let mut ids = Vec::new();
                for value in filter {
                    if let Some(set_ids) = shard.string_set_to_single_term_id_vec[idx].get(value) {
                        ids.extend(set_ids.iter().copied());
                    }
                }
                ids.sort_unstable();
                ids.dedup();
                (field, FilterSparse::String(ids))
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
pub(crate) fn is_facet_filter(
    reader: &mut FacetReader<'_>,
    filters: &[FilterSparse],
    doc_id: usize,
) -> Result<bool, crate::record::RecordError> {
    for (filter, field) in filters.iter().zip(&reader.segment.facets) {
        if matches!(filter, FilterSparse::None) {
            continue;
        }
        let bytes = reader.read(doc_id, field)?;
        if bytes[crate::component::field_width(&field.field_type) - 1] == 0 {
            return Ok(true);
        }
        if matches!(field.field_type, FieldType::String | FieldType::StringSet)
            && read_u32(&bytes, 0) as usize >= field.values.len()
        {
            return Err(crate::record::RecordError::Storage(
                "invalid metadata dictionary index".into(),
            ));
        }
        if rejects(filter, &bytes, 0) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[inline]
fn rejects(filter: &FilterSparse, bytes: &[u8], address: usize) -> bool {
    match filter {
        FilterSparse::Int(range) => !range.contains(&read_i64(bytes, address)),
        FilterSparse::Timestamp(range) => !range.contains(&read_i64(bytes, address)),
        FilterSparse::Float(range) => !range.contains(&read_f64(bytes, address)),
        FilterSparse::String(ids) => ids.binary_search(&read_u32(bytes, address)).is_err(),
        FilterSparse::And(predicates) => predicates
            .iter()
            .any(|predicate| rejects(predicate, bytes, address)),
        FilterSparse::None => false,
    }
}
