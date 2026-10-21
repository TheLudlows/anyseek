//! Published active-version statistics for authoritative record partitions.
//! Physical posting counters remain independent because they describe encoding.
use crate::{
    add_result::{B, K},
    index::{DOCUMENT_LENGTH_COMPRESSION, FieldLengthNormalization, Shard},
};
use ahash::AHashMap;

#[derive(Default, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct ActiveStatistics {
    pub keys: Vec<String>,
    footprints: AHashMap<usize, Footprint>,
    df: AHashMap<u64, u32>,
    fields: Vec<(u64, u64)>,
    shared_sum: u64,
}
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct Footprint {
    terms: Vec<u64>,
    fields: Vec<(usize, u32)>,
}
impl ActiveStatistics {
    /// Validate a persisted footprint closure without re-tokenizing payloads.
    pub(crate) fn validate(&self,field_count:usize)->Result<(),String>{
        if self.footprints.len()!=self.keys.len(){return Err("incomplete active footprints".into());}
        let mut rebuilt=Self::default();
        for (&id,footprint) in &self.footprints {
            if id>=self.keys.len() || footprint.terms.windows(2).any(|v|v[0]>=v[1])
                || footprint.fields.iter().any(|(field,_)|*field>=field_count)
                || footprint.fields.iter().map(|(field,_)|*field).collect::<std::collections::BTreeSet<_>>().len()!=footprint.fields.len(){
                return Err("invalid active footprint".into());
            }
            rebuilt.insert(id,footprint.terms.clone(),footprint.fields.clone());
        }
        if self.df!=rebuilt.df || self.fields!=rebuilt.fields || self.shared_sum!=rebuilt.shared_sum {
            return Err("inconsistent active statistics".into());
        }
        Ok(())
    }
    pub fn insert(&mut self, id: usize, mut terms: Vec<u64>, fields: Vec<(usize, u32)>) {
        terms.sort_unstable();
        terms.dedup();
        for term in &terms {
            *self.df.entry(*term).or_default() += 1;
        }
        for &(field, length) in &fields {
            if self.fields.len() <= field {
                self.fields.resize(field + 1, (0, 0));
            }
            self.fields[field].0 += length as u64;
            self.fields[field].1 += u64::from(length > 0);
            self.shared_sum += length as u64;
        }
        assert!(
            self.footprints
                .insert(id, Footprint { terms, fields })
                .is_none()
        );
    }
    pub fn retire(&mut self, id: usize) {
        if let Some(old) = self.footprints.remove(&id) {
            for term in old.terms {
                let count = self.df.get_mut(&term).expect("active term");
                *count -= 1;
                if *count == 0 {
                    self.df.remove(&term);
                }
            }
            for (field, length) in old.fields {
                self.fields[field].0 -= length as u64;
                self.fields[field].1 -= u64::from(length > 0);
                self.shared_sum -= length as u64;
            }
        }
    }
    fn average(&self, field: usize, normalization: &FieldLengthNormalization) -> f32 {
        let (sum, count) = if *normalization == FieldLengthNormalization::Shared {
            (self.shared_sum, self.footprints.len() as u64)
        } else {
            self.fields.get(field).copied().unwrap_or_default()
        };
        if count == 0 || sum == 0 {
            1.0
        } else {
            sum as f32 / count as f32
        }
    }
}
pub(crate) fn rank_with_keys(
    keys: Option<&[String]>,
    a: f32,
    a_id: usize,
    b: f32,
    b_id: usize,
) -> std::cmp::Ordering {
    crate::min_heap::score_ordering(a, b).then_with(|| match keys {
        Some(keys) => keys[b_id].cmp(&keys[a_id]).then_with(|| b_id.cmp(&a_id)),
        None => b_id.cmp(&a_id),
    })
}
impl Shard {
    pub(crate) fn scoring_count(&self) -> usize {
        self.active
            .as_ref()
            .map_or(self.indexed_doc_count, |a| a.footprints.len())
    }
    pub(crate) fn scoring_df(&self, hash: u64, physical: u32) -> u32 {
        self.active
            .as_ref()
            .map_or(physical, |a| a.df.get(&hash).copied().unwrap_or(0))
    }
    pub(crate) fn active_average(&self, field: usize) -> Option<f32> {
        self.active
            .as_ref()
            .map(|a| a.average(field, &self.meta.field_length_normalization))
    }
    pub(crate) fn active_component(&self, field: usize, code: u8) -> Option<f32> {
        self.active_average(field).map(|average| {
            K * (1.0 - B + B * (DOCUMENT_LENGTH_COMPRESSION[code as usize] as f32 / average))
        })
    }
    pub(crate) fn rank(&self, a: f32, a_id: usize, b: f32, b_id: usize) -> std::cmp::Ordering {
        rank_with_keys(
            self.active.as_ref().map(|a| a.keys.as_slice()),
            a,
            a_id,
            b,
            b_id,
        )
    }
}
