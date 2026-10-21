//! Maintained physical per-field length statistics and score lookup tables.
use crate::{
    add_result::{B, K},
    index::{DOCUMENT_LENGTH_COMPRESSION, FieldLengthNormalization, Shard},
};
impl Shard {
    /// No document scan: statistics change under the existing shard write lock.
    pub(crate) fn refresh_field_length_cache(&mut self, field_id: usize) {
        let field = &mut self.indexed_field_vec[field_id];
        let average = if field.field_document_count == 0 {
            1.0
        } else {
            field.field_length_sum as f32 / field.field_document_count as f32
        };
        for (code, component) in field.length_component_cache.iter_mut().enumerate() {
            *component = K * (1.0 - B + B * (DOCUMENT_LENGTH_COMPRESSION[code] as f32 / average));
        }
    }
    #[inline(always)]
    pub(crate) fn length_component(&self, field_id: usize, code: u8) -> f32 {
        if let Some(component) = self.active_component(field_id, code) {
            return component;
        }
        match self.meta.field_length_normalization {
            FieldLengthNormalization::Shared => self.bm25_component_cache[code as usize],
            FieldLengthNormalization::PerField => {
                self.indexed_field_vec[field_id].length_component_cache[code as usize]
            }
        }
    }
    #[inline(always)]
    pub(crate) fn field_length_average(&self, field_id: usize, shared: f32) -> f32 {
        if let Some(average) = self.active_average(field_id) {
            return average;
        }
        if self.meta.field_length_normalization == FieldLengthNormalization::Shared {
            return shared;
        }
        let field = &self.indexed_field_vec[field_id];
        if field.field_document_count == 0 {
            1.0
        } else {
            field.field_length_sum as f32 / field.field_document_count as f32
        }
    }
}
