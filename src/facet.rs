// storm-db facet module: facet types, encoding/decoding, and counting

use crate::schema::{FieldType, SchemaField};
use indexmap::IndexMap;

/// Buffer for documents indexed but not yet committed.
#[derive(Default)]
pub struct PendingDocuments {
    /// (doc_id, compressed document bytes)
    pub docs: Vec<(u64, Vec<u8>)>,
    /// (doc_id, facet row bytes)
    pub facets: Vec<(u64, Vec<u8>)>,
}

impl PendingDocuments {
    pub fn push(&mut self, doc_id: u64, doc_bytes: Vec<u8>, facet_bytes: Vec<u8>) {
        self.docs.push((doc_id, doc_bytes));
        self.facets.push((doc_id, facet_bytes));
    }
}

pub struct FacetField {
    pub name: String,
    pub values: IndexMap<String, (Vec<String>, usize)>, // string_value -> (aliases, count)
    pub offset: usize,   // byte offset within a facet row
    pub field_type: FieldType,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValueType {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Bool(bool),
    String16(u16),
    String32(u32),
    Point(u64),
}

pub struct FacetFilter {
    pub field: String,
    pub values: Vec<String>,
    pub range: Option<(ValueType, ValueType)>,
}

pub struct FacetResult {
    pub field: String,
    pub values: Vec<(String, usize)>, // (value_label, count) sorted by count desc
}

/// A simple in-memory string dictionary for String16/String32 facet fields.
/// Keys are strings, values are the next available index.
#[derive(Debug, Default)]
pub struct StringDictionary {
    pub entries: IndexMap<String, usize>,
}

impl StringDictionary {
    pub fn new() -> Self {
        Self {
            entries: IndexMap::new(),
        }
    }

    /// Look up or insert a string, returning its index.
    pub fn get_or_insert(&mut self, s: &str) -> usize {
        let len = self.entries.len();
        *self.entries.entry(s.to_string()).or_insert(len)
    }

    /// Look up a string, returning its index or None.
    pub fn get(&self, s: &str) -> Option<usize> {
        self.entries.get(s).copied()
    }
}

/// Encode a facet value from a JSON value and FieldType into bytes.
pub fn encode_facet_value(
    value: &serde_json::Value,
    field_type: FieldType,
    dict: &mut StringDictionary,
) -> Vec<u8> {
    match field_type {
        FieldType::U8 => {
            let v = value.as_u64().unwrap_or(0) as u8;
            v.to_le_bytes().to_vec()
        }
        FieldType::U16 => {
            let v = value.as_u64().unwrap_or(0) as u16;
            v.to_le_bytes().to_vec()
        }
        FieldType::U32 => {
            let v = value.as_u64().unwrap_or(0) as u32;
            v.to_le_bytes().to_vec()
        }
        FieldType::U64 => {
            let v = value.as_u64().unwrap_or(0);
            v.to_le_bytes().to_vec()
        }
        FieldType::I8 => {
            let v = value.as_i64().unwrap_or(0) as i8;
            v.to_le_bytes().to_vec()
        }
        FieldType::I16 => {
            let v = value.as_i64().unwrap_or(0) as i16;
            v.to_le_bytes().to_vec()
        }
        FieldType::I32 => {
            let v = value.as_i64().unwrap_or(0) as i32;
            v.to_le_bytes().to_vec()
        }
        FieldType::I64 => {
            let v = value.as_i64().unwrap_or(0);
            v.to_le_bytes().to_vec()
        }
        FieldType::Timestamp => {
            let v = value.as_i64().unwrap_or(0);
            v.to_le_bytes().to_vec()
        }
        FieldType::F32 => {
            let v = value.as_f64().unwrap_or(0.0) as f32;
            v.to_le_bytes().to_vec()
        }
        FieldType::F64 => {
            let v = value.as_f64().unwrap_or(0.0);
            v.to_le_bytes().to_vec()
        }
        FieldType::Bool => {
            let v = value.as_bool().unwrap_or(false);
            vec![if v { 1u8 } else { 0u8 }]
        }
        FieldType::String16 => {
            let idx = dict.get_or_insert(value.as_str().unwrap_or("")) as u16;
            idx.to_le_bytes().to_vec()
        }
        FieldType::String32 => {
            let idx = dict.get_or_insert(value.as_str().unwrap_or("")) as u32;
            idx.to_le_bytes().to_vec()
        }
        FieldType::StringSet16 | FieldType::StringSet32 => {
            // Not directly supported as a single facet value; treat as empty.
            vec![]
        }
        FieldType::Point => {
            // Encode as lat:f32 + lon:f32 = 8 bytes (Morton code placeholder).
            if let Some(arr) = value.as_array() {
                if arr.len() >= 2 {
                    let lat = arr[0].as_f64().unwrap_or(0.0) as f32;
                    let lon = arr[1].as_f64().unwrap_or(0.0) as f32;
                    let mut buf = Vec::with_capacity(8);
                    buf.extend_from_slice(&lat.to_le_bytes());
                    buf.extend_from_slice(&lon.to_le_bytes());
                    return buf;
                }
            }
            vec![0u8; 8]
        }
        FieldType::Text | FieldType::Vector => {
            // Not supported as facets.
            vec![]
        }
    }
}

/// Decode a facet value from raw bytes at a given offset, returning the appropriate ValueType.
pub fn decode_facet_value(data: &[u8], field_type: FieldType, offset: usize) -> ValueType {
    match field_type {
        FieldType::U8 => {
            let v = if offset + 1 <= data.len() {
                data[offset]
            } else {
                0
            };
            ValueType::U8(v)
        }
        FieldType::U16 => {
            let v = if offset + 2 <= data.len() {
                u16::from_le_bytes([data[offset], data[offset + 1]])
            } else {
                0
            };
            ValueType::U16(v)
        }
        FieldType::U32 => {
            let mut buf = [0u8; 4];
            if offset + 4 <= data.len() {
                buf.copy_from_slice(&data[offset..offset + 4]);
            }
            ValueType::U32(u32::from_le_bytes(buf))
        }
        FieldType::U64 => {
            let mut buf = [0u8; 8];
            if offset + 8 <= data.len() {
                buf.copy_from_slice(&data[offset..offset + 8]);
            }
            ValueType::U64(u64::from_le_bytes(buf))
        }
        FieldType::I8 => {
            let v = if offset + 1 <= data.len() {
                data[offset] as i8
            } else {
                0
            };
            ValueType::I8(v)
        }
        FieldType::I16 => {
            let mut buf = [0u8; 2];
            if offset + 2 <= data.len() {
                buf.copy_from_slice(&data[offset..offset + 2]);
            }
            ValueType::I16(i16::from_le_bytes(buf))
        }
        FieldType::I32 => {
            let mut buf = [0u8; 4];
            if offset + 4 <= data.len() {
                buf.copy_from_slice(&data[offset..offset + 4]);
            }
            ValueType::I32(i32::from_le_bytes(buf))
        }
        FieldType::I64 => {
            let mut buf = [0u8; 8];
            if offset + 8 <= data.len() {
                buf.copy_from_slice(&data[offset..offset + 8]);
            }
            ValueType::I64(i64::from_le_bytes(buf))
        }
        FieldType::Timestamp => {
            let mut buf = [0u8; 8];
            if offset + 8 <= data.len() {
                buf.copy_from_slice(&data[offset..offset + 8]);
            }
            ValueType::I64(i64::from_le_bytes(buf))
        }
        FieldType::F32 => {
            let mut buf = [0u8; 4];
            if offset + 4 <= data.len() {
                buf.copy_from_slice(&data[offset..offset + 4]);
            }
            ValueType::F32(f32::from_le_bytes(buf))
        }
        FieldType::F64 => {
            let mut buf = [0u8; 8];
            if offset + 8 <= data.len() {
                buf.copy_from_slice(&data[offset..offset + 8]);
            }
            ValueType::F64(f64::from_le_bytes(buf))
        }
        FieldType::Bool => {
            let v = if offset < data.len() {
                data[offset] != 0
            } else {
                false
            };
            ValueType::Bool(v)
        }
        FieldType::String16 => {
            let mut buf = [0u8; 2];
            if offset + 2 <= data.len() {
                buf.copy_from_slice(&data[offset..offset + 2]);
            }
            ValueType::String16(u16::from_le_bytes(buf))
        }
        FieldType::String32 => {
            let mut buf = [0u8; 4];
            if offset + 4 <= data.len() {
                buf.copy_from_slice(&data[offset..offset + 4]);
            }
            ValueType::String32(u32::from_le_bytes(buf))
        }
        FieldType::StringSet16 | FieldType::StringSet32 => {
            ValueType::String16(0)
        }
        FieldType::Point => {
            let mut buf = [0u8; 8];
            if offset + 8 <= data.len() {
                buf.copy_from_slice(&data[offset..offset + 8]);
            }
            ValueType::Point(u64::from_le_bytes(buf))
        }
        FieldType::Text | FieldType::Vector => {
            ValueType::U8(0)
        }
    }
}

/// Compute the total byte size of a facet row given the schema fields.
pub fn compute_facet_row_size(fields: &[SchemaField]) -> usize {
    fields
        .iter()
        .filter(|f| f.facet)
        .map(|f| f.field_type.byte_size())
        .sum()
}

/// Serialize a facet row: concatenate all encoded field values.
/// Input: vec of (field_index, encoded_bytes). The output is the concatenation
/// in field-index order, padded to the expected size if needed.
pub fn serialize_facet_row(values: &[(usize, Vec<u8>)], row_size: usize) -> Vec<u8> {
    let mut row = vec![0u8; row_size];
    let mut offset = 0usize;
    // We assume values are ordered by field index and placed sequentially.
    for (_, encoded) in values {
        let len = encoded.len();
        if offset + len <= row_size {
            row[offset..offset + len].copy_from_slice(encoded);
        }
        offset += len;
    }
    row
}

/// Deserialize a facet row: read values at each (field_name, offset, FieldType).
pub fn deserialize_facet_row(
    data: &[u8],
    offsets: &[(String, usize, FieldType)],
) -> Vec<(String, ValueType)> {
    offsets
        .iter()
        .map(|(name, offset, field_type)| {
            let value = decode_facet_value(data, *field_type, *offset);
            (name.clone(), value)
        })
        .collect()
}

/// Count facet values across matching documents.
///
/// - `facet_data`: one encoded facet row per document (indexed by doc_id).
/// - `doc_ids`: the list of matching doc IDs.
/// - `facet_fields`: the facet field definitions (with offsets and types).
/// - `query_facets`: which facet fields to count.
pub fn count_facets(
    facet_data: &[Vec<u8>],
    doc_ids: &[u64],
    facet_fields: &[FacetField],
    query_facets: &[String],
) -> Vec<FacetResult> {
    let mut results = Vec::new();

    for facet_field in facet_fields {
        if !query_facets.contains(&facet_field.name) {
            continue;
        }

        let field_type = facet_field.field_type;
        let offset = facet_field.offset;
        let mut counts: IndexMap<String, usize> = IndexMap::new();

        for &doc_id in doc_ids {
            if (doc_id as usize) >= facet_data.len() {
                continue;
            }
            let row = &facet_data[doc_id as usize];
            let value = decode_facet_value(row, field_type, offset);

            let label = value_to_string(&value, field_type);
            *counts.entry(label).or_insert(0) += 1;
        }

        // Sort by count descending.
        let mut sorted: Vec<(String, usize)> = counts.into_iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

        results.push(FacetResult {
            field: facet_field.name.clone(),
            values: sorted,
        });
    }

    results
}

/// Convert a ValueType to a human-readable string label.
fn value_to_string(value: &ValueType, field_type: FieldType) -> String {
    match (value, field_type) {
        (ValueType::U8(v), _) => v.to_string(),
        (ValueType::U16(v), _) => v.to_string(),
        (ValueType::U32(v), _) => v.to_string(),
        (ValueType::U64(v), _) => v.to_string(),
        (ValueType::I8(v), _) => v.to_string(),
        (ValueType::I16(v), _) => v.to_string(),
        (ValueType::I32(v), _) => v.to_string(),
        (ValueType::I64(v), _) => v.to_string(),
        (ValueType::F32(v), _) => format!("{:.6}", v),
        (ValueType::F64(v), _) => format!("{:.6}", v),
        (ValueType::Bool(v), _) => v.to_string(),
        (ValueType::String16(idx), _) => format!("str:{}", idx),
        (ValueType::String32(idx), _) => format!("str:{}", idx),
        (ValueType::Point(v), _) => v.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- encode/decode roundtrip for each numeric FieldType ---

    fn roundtrip(value: &serde_json::Value, field_type: FieldType) -> ValueType {
        let mut dict = StringDictionary::new();
        let encoded = encode_facet_value(value, field_type, &mut dict);
        assert!(!encoded.is_empty(), "encoded bytes should not be empty for {:?}", field_type);
        decode_facet_value(&encoded, field_type, 0)
    }

    #[test]
    fn test_roundtrip_u8() {
        let v = serde_json::json!(42u8);
        let decoded = roundtrip(&v, FieldType::U8);
        assert_eq!(decoded, ValueType::U8(42));
    }

    #[test]
    fn test_roundtrip_u16() {
        let v = serde_json::json!(65535u16);
        let decoded = roundtrip(&v, FieldType::U16);
        assert_eq!(decoded, ValueType::U16(65535));
    }

    #[test]
    fn test_roundtrip_u32() {
        let v = serde_json::json!(4294967295u32);
        let decoded = roundtrip(&v, FieldType::U32);
        assert_eq!(decoded, ValueType::U32(4294967295));
    }

    #[test]
    fn test_roundtrip_u64() {
        let v = serde_json::json!(9223372036854775807u64);
        let decoded = roundtrip(&v, FieldType::U64);
        assert_eq!(decoded, ValueType::U64(9223372036854775807));
    }

    #[test]
    fn test_roundtrip_i8() {
        let v = serde_json::json!(-128i64);
        let decoded = roundtrip(&v, FieldType::I8);
        assert_eq!(decoded, ValueType::I8(-128));
    }

    #[test]
    fn test_roundtrip_i16() {
        let v = serde_json::json!(-32768i64);
        let decoded = roundtrip(&v, FieldType::I16);
        assert_eq!(decoded, ValueType::I16(-32768));
    }

    #[test]
    fn test_roundtrip_i32() {
        let v = serde_json::json!(-2147483648i64);
        let decoded = roundtrip(&v, FieldType::I32);
        assert_eq!(decoded, ValueType::I32(-2147483648));
    }

    #[test]
    fn test_roundtrip_i64() {
        let v = serde_json::json!(-9223372036854775808i64);
        let decoded = roundtrip(&v, FieldType::I64);
        assert_eq!(decoded, ValueType::I64(-9223372036854775808));
    }

    #[test]
    fn test_roundtrip_f32() {
        let v = serde_json::json!(3.14159);
        let decoded = roundtrip(&v, FieldType::F32);
        if let ValueType::F32(f) = decoded {
            assert!((f - 3.14159f32).abs() < 1e-5);
        } else {
            panic!("expected F32");
        }
    }

    #[test]
    fn test_roundtrip_f64() {
        let v = serde_json::json!(3.14159265358979);
        let decoded = roundtrip(&v, FieldType::F64);
        if let ValueType::F64(f) = decoded {
            assert!((f - 3.14159265358979f64).abs() < 1e-10);
        } else {
            panic!("expected F64");
        }
    }

    #[test]
    fn test_roundtrip_bool_true() {
        let v = serde_json::json!(true);
        let decoded = roundtrip(&v, FieldType::Bool);
        assert_eq!(decoded, ValueType::Bool(true));
    }

    #[test]
    fn test_roundtrip_bool_false() {
        let v = serde_json::json!(false);
        let decoded = roundtrip(&v, FieldType::Bool);
        assert_eq!(decoded, ValueType::Bool(false));
    }

    #[test]
    fn test_roundtrip_string16() {
        let v = serde_json::json!("hello");
        let mut dict = StringDictionary::new();
        let encoded = encode_facet_value(&v, FieldType::String16, &mut dict);
        assert_eq!(encoded.len(), 2);
        let decoded = decode_facet_value(&encoded, FieldType::String16, 0);
        assert_eq!(decoded, ValueType::String16(0));
        // Second insert should get index 1.
        let v2 = serde_json::json!("world");
        let encoded2 = encode_facet_value(&v2, FieldType::String16, &mut dict);
        let decoded2 = decode_facet_value(&encoded2, FieldType::String16, 0);
        assert_eq!(decoded2, ValueType::String16(1));
        // Re-insert "hello" should return index 0.
        let v3 = serde_json::json!("hello");
        let encoded3 = encode_facet_value(&v3, FieldType::String16, &mut dict);
        let decoded3 = decode_facet_value(&encoded3, FieldType::String16, 0);
        assert_eq!(decoded3, ValueType::String16(0));
    }

    #[test]
    fn test_roundtrip_string32() {
        let v = serde_json::json!("test");
        let mut dict = StringDictionary::new();
        let encoded = encode_facet_value(&v, FieldType::String32, &mut dict);
        assert_eq!(encoded.len(), 4);
        let decoded = decode_facet_value(&encoded, FieldType::String32, 0);
        assert_eq!(decoded, ValueType::String32(0));
    }

    #[test]
    fn test_encode_point() {
        let v = serde_json::json!([37.7749, -122.4194]);
        let mut dict = StringDictionary::new();
        let encoded = encode_facet_value(&v, FieldType::Point, &mut dict);
        assert_eq!(encoded.len(), 8);
        let decoded = decode_facet_value(&encoded, FieldType::Point, 0);
        // The decoded value is a u64 Morton code placeholder,
        // just verify it's not zero.
        if let ValueType::Point(v) = decoded {
            assert_ne!(v, 0);
        } else {
            panic!("expected Point");
        }
    }

    #[test]
    fn test_encode_text_returns_empty() {
        let v = serde_json::json!("hello world");
        let mut dict = StringDictionary::new();
        let encoded = encode_facet_value(&v, FieldType::Text, &mut dict);
        assert!(encoded.is_empty());
    }

    #[test]
    fn test_encode_vector_returns_empty() {
        let v = serde_json::json!([0.1, 0.2, 0.3]);
        let mut dict = StringDictionary::new();
        let encoded = encode_facet_value(&v, FieldType::Vector, &mut dict);
        assert!(encoded.is_empty());
    }

    // --- compute_facet_row_size ---

    #[test]
    fn test_compute_facet_row_size() {
        let fields = vec![
            SchemaField {
                field: "category".into(),
                store: true,
                index_lexical: false,
                index_vector: false,
                field_type: FieldType::String16,
                facet: true,
                boost: 1.0,
            },
            SchemaField {
                field: "price".into(),
                store: true,
                index_lexical: false,
                index_vector: false,
                field_type: FieldType::U32,
                facet: true,
                boost: 1.0,
            },
            SchemaField {
                field: "title".into(),
                store: true,
                index_lexical: true,
                index_vector: false,
                field_type: FieldType::Text,
                facet: false,
                boost: 1.0,
            },
        ];
        // String16 = 2 + U32 = 4, Text not facet => total = 6
        assert_eq!(compute_facet_row_size(&fields), 6);
    }

    #[test]
    fn test_compute_facet_row_size_empty() {
        let fields: Vec<SchemaField> = vec![];
        assert_eq!(compute_facet_row_size(&fields), 0);
    }

    // --- serialize/deserialize facet row roundtrip ---

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        // Build a row with U8=42, U16=1234, Bool=true, F32=3.14
        let mut dict = StringDictionary::new();
        let val1 = encode_facet_value(&serde_json::json!(42), FieldType::U8, &mut dict);
        let val2 = encode_facet_value(&serde_json::json!(1234), FieldType::U16, &mut dict);
        let val3 = encode_facet_value(&serde_json::json!(true), FieldType::Bool, &mut dict);
        let val4 = encode_facet_value(&serde_json::json!(3.14), FieldType::F32, &mut dict);

        let values = vec![
            (0, val1.clone()),
            (1, val2.clone()),
            (2, val3.clone()),
            (3, val4.clone()),
        ];
        let row_size = val1.len() + val2.len() + val3.len() + val4.len();
        let row = serialize_facet_row(&values, row_size);

        let offsets = vec![
            ("u8_val".to_string(), 0, FieldType::U8),       // U8 = 1 byte
            ("u16_val".to_string(), 1, FieldType::U16),     // U16 = 2 bytes
            ("bool_val".to_string(), 3, FieldType::Bool),   // Bool = 1 byte
            ("f32_val".to_string(), 4, FieldType::F32),     // F32 = 4 bytes
        ];

        let decoded = deserialize_facet_row(&row, &offsets);
        assert_eq!(decoded.len(), 4);

        assert_eq!(decoded[0].0, "u8_val");
        assert_eq!(decoded[0].1, ValueType::U8(42));

        assert_eq!(decoded[1].0, "u16_val");
        assert_eq!(decoded[1].1, ValueType::U16(1234));

        assert_eq!(decoded[2].0, "bool_val");
        assert_eq!(decoded[2].1, ValueType::Bool(true));

        if let ValueType::F32(f) = &decoded[3].1 {
            assert!((*f - 3.14f32).abs() < 0.01);
        } else {
            panic!("expected F32");
        }
    }

    // --- count_facets ---

    #[test]
    fn test_count_facets_simple() {
        // Build facet data for 5 docs with a U8 "category" field at offset 0.
        let mut dict = StringDictionary::new();
        let row_1 = serialize_facet_row(&[(0, encode_facet_value(&serde_json::json!(1), FieldType::U8, &mut dict))], 1);
        let row_2 = serialize_facet_row(&[(0, encode_facet_value(&serde_json::json!(2), FieldType::U8, &mut dict))], 1);
        let row_3 = serialize_facet_row(&[(0, encode_facet_value(&serde_json::json!(1), FieldType::U8, &mut dict))], 1);
        let row_4 = serialize_facet_row(&[(0, encode_facet_value(&serde_json::json!(3), FieldType::U8, &mut dict))], 1);
        let row_5 = serialize_facet_row(&[(0, encode_facet_value(&serde_json::json!(1), FieldType::U8, &mut dict))], 1);

        let facet_data = vec![row_1, row_2, row_3, row_4, row_5];
        let doc_ids = vec![0u64, 1, 2, 3, 4];

        let facet_fields = vec![FacetField {
            name: "category".into(),
            values: IndexMap::new(),
            offset: 0,
            field_type: FieldType::U8,
        }];

        let results = count_facets(&facet_data, &doc_ids, &facet_fields, &["category".into()]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].field, "category");

        // Should be sorted by count desc: 1->3, 2->1, 3->1
        assert_eq!(results[0].values[0], ("1".into(), 3));
        assert_eq!(results[0].values[1].1, 1); // either 2 or 3
        assert_eq!(results[0].values[2].1, 1);
    }

    #[test]
    fn test_count_facets_multiple_fields() {
        // Row has U8 at offset 0, Bool at offset 1.
        fn make_row(u8_val: u8, bool_val: bool) -> Vec<u8> {
            let mut d = StringDictionary::new();
            let v1 = encode_facet_value(&serde_json::json!(u8_val), FieldType::U8, &mut d);
            let v2 = encode_facet_value(&serde_json::json!(bool_val), FieldType::Bool, &mut d);
            serialize_facet_row(&[(0, v1), (1, v2)], 2)
        }

        let facet_data = vec![
            make_row(1, true),
            make_row(2, false),
            make_row(1, true),
            make_row(3, false),
        ];
        let doc_ids = vec![0u64, 1, 2, 3];

        let facet_fields = vec![
            FacetField {
                name: "status".into(),
                values: IndexMap::new(),
                offset: 0,
                field_type: FieldType::U8,
            },
            FacetField {
                name: "active".into(),
                values: IndexMap::new(),
                offset: 1,
                field_type: FieldType::Bool,
            },
        ];

        let results = count_facets(&facet_data, &doc_ids, &facet_fields, &["status".into(), "active".into()]);
        assert_eq!(results.len(), 2);

        // status: 1->2, 2->1, 3->1
        let status_result = results.iter().find(|r| r.field == "status").unwrap();
        assert_eq!(status_result.values[0], ("1".into(), 2));

        // active: true->2, false->2
        let active_result = results.iter().find(|r| r.field == "active").unwrap();
        assert_eq!(active_result.values[0].1, 2);
        assert_eq!(active_result.values[1].1, 2);
    }

    #[test]
    fn test_count_facets_subset_docs() {
        let mut dict = StringDictionary::new();
        let row_0 = serialize_facet_row(&[(0, encode_facet_value(&serde_json::json!(1), FieldType::U8, &mut dict))], 1);
        let row_1 = serialize_facet_row(&[(0, encode_facet_value(&serde_json::json!(2), FieldType::U8, &mut dict))], 1);
        let row_2 = serialize_facet_row(&[(0, encode_facet_value(&serde_json::json!(1), FieldType::U8, &mut dict))], 1);

        let facet_data = vec![row_0, row_1, row_2];

        // Only count docs 0 and 2 (skip doc 1).
        let doc_ids = vec![0u64, 2];

        let facet_fields = vec![FacetField {
            name: "category".into(),
            values: IndexMap::new(),
            offset: 0,
            field_type: FieldType::U8,
        }];

        let results = count_facets(&facet_data, &doc_ids, &facet_fields, &["category".into()]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].values[0], ("1".into(), 2));
        assert_eq!(results[0].values.len(), 1);
    }

    #[test]
    fn test_count_facets_no_matching_query() {
        let facet_data: Vec<Vec<u8>> = vec![vec![0u8; 1]];
        let facet_fields = vec![FacetField {
            name: "color".into(),
            values: IndexMap::new(),
            offset: 0,
            field_type: FieldType::U8,
        }];

        // Query for a facet field that doesn't exist in query_facets.
        let results = count_facets(&facet_data, &[0], &facet_fields, &["size".into()]);
        assert!(results.is_empty());
    }

    #[test]
    fn test_decode_out_of_bounds_returns_default() {
        let data: Vec<u8> = vec![];
        let decoded = decode_facet_value(&data, FieldType::U32, 0);
        assert_eq!(decoded, ValueType::U32(0));
    }

    #[test]
    fn test_string_dictionary_get() {
        let mut dict = StringDictionary::new();
        assert_eq!(dict.get_or_insert("apple"), 0);
        assert_eq!(dict.get_or_insert("banana"), 1);
        assert_eq!(dict.get("apple"), Some(0));
        assert_eq!(dict.get("cherry"), None);
        assert_eq!(dict.get("banana"), Some(1));
    }
}
