use serde_json::Value;

use crate::FieldType;

impl FieldType {
    /// 推断字段类型
    pub fn infer(value: &Value) -> Option<Self> {
        match value {
            // 短文本 → Keyword（< 100 字符）
            Value::String(s) if s.len() < 100 => Some(FieldType::Keyword),
            // 长文本 → Text（>= 100 字符）
            Value::String(_) => Some(FieldType::Text),
            // 纯数字数组 → Vector（2~4096 维）
            Value::Array(arr) if arr.iter().all(|v| v.is_number()) => {
                let len = arr.len() as u32;
                if (2..=4096).contains(&len) {
                    Some(FieldType::Vector(len))
                } else {
                    None
                }
            }
            // 字符串数组 → Json
            Value::Array(arr) if arr.iter().all(|v| v.is_string()) => Some(FieldType::Json),
            // 其他数组 → 不推断
            Value::Array(_) => None,
            // 数字（修正 U64 推断）
            Value::Number(n) => {
                if n.as_u64().is_some() {
                    Some(FieldType::U64)
                } else if let Some(i) = n.as_i64() {
                    if i >= 0 { Some(FieldType::U64) } else { Some(FieldType::I64) }
                } else if n.is_f64() {
                    Some(FieldType::F64)
                } else {
                    None
                }
            }
            Value::Bool(_) => Some(FieldType::Bool),
            Value::Object(_) => Some(FieldType::Json),
            Value::Null => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_infer_short_text() {
        let value = json!("short");
        let inferred = FieldType::infer(&value).unwrap();
        assert_eq!(inferred, FieldType::Keyword);
    }

    #[test]
    fn test_infer_long_text() {
        let long_text = "a".repeat(150);
        let value = json!(long_text);
        let inferred = FieldType::infer(&value).unwrap();
        assert_eq!(inferred, FieldType::Text);
    }

    #[test]
    fn test_infer_u64() {
        let value = json!(12345);
        let inferred = FieldType::infer(&value).unwrap();
        assert_eq!(inferred, FieldType::U64);
    }

    #[test]
    fn test_infer_negative_i64() {
        let value = json!(-12345);
        let inferred = FieldType::infer(&value).unwrap();
        assert_eq!(inferred, FieldType::I64);
    }

    #[test]
    fn test_infer_f64() {
        let value = json!(3.14159);
        let inferred = FieldType::infer(&value).unwrap();
        assert_eq!(inferred, FieldType::F64);
    }

    #[test]
    fn test_infer_bool() {
        let value = json!(true);
        let inferred = FieldType::infer(&value).unwrap();
        assert_eq!(inferred, FieldType::Bool);
    }

    #[test]
    fn test_infer_vector() {
        let value = json!([0.1, 0.2, 0.3]);
        let inferred = FieldType::infer(&value).unwrap();
        assert_eq!(inferred, FieldType::Vector(3));
    }

    #[test]
    fn test_infer_string_array() {
        let value = json!(["tag1", "tag2"]);
        let inferred = FieldType::infer(&value).unwrap();
        assert_eq!(inferred, FieldType::Json);
    }

    #[test]
    fn test_infer_object() {
        let value = json!({"nested": "value"});
        let inferred = FieldType::infer(&value).unwrap();
        assert_eq!(inferred, FieldType::Json);
    }

    #[test]
    fn test_infer_null_returns_none() {
        let value = json!(null);
        let inferred = FieldType::infer(&value);
        assert!(inferred.is_none());
    }

    #[test]
    fn test_infer_short_array_returns_none() {
        let value = json!([1]);
        let inferred = FieldType::infer(&value);
        assert!(inferred.is_none());
    }
}
