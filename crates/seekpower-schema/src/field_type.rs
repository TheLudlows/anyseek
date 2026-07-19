use serde::{Deserialize, Serialize};

/// 字段类型枚举
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FieldType {
    /// 文本类型（分词索引）
    #[serde(rename = "Text")]
    Text,
    /// 关键词类型（精确匹配）
    #[serde(rename = "Keyword")]
    Keyword,
    /// 64 位无符号整数
    #[serde(rename = "U64")]
    U64,
    /// 64 位有符号整数
    #[serde(rename = "I64")]
    I64,
    /// 32 位浮点数
    #[serde(rename = "F32")]
    F32,
    /// 64 位浮点数
    #[serde(rename = "F64")]
    F64,
    /// 布尔值
    #[serde(rename = "Bool")]
    Bool,
    /// 时间戳（Unix 毫秒）
    #[serde(rename = "Timestamp")]
    Timestamp,
    /// 向量（固定维度）
    #[serde(rename = "Vector")]
    Vector(u32),
    /// JSON 对象（不索引，仅存储）
    #[serde(rename = "Json")]
    Json,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fieldtype_equality() {
        assert_eq!(FieldType::Text, FieldType::Text);
        assert_eq!(FieldType::F32, FieldType::F32);
        assert_eq!(FieldType::Vector(3), FieldType::Vector(3));
        assert_ne!(FieldType::Vector(3), FieldType::Vector(4));
    }

    #[test]
    fn test_fieldtype_serialization() {
        let text = FieldType::Text;
        let json = serde_json::to_string(&text).unwrap();
        assert_eq!(json, r#""Text""#);

        let f32_type = FieldType::F32;
        let json = serde_json::to_string(&f32_type).unwrap();
        assert_eq!(json, r#""F32""#);

        let vector = FieldType::Vector(768);
        let json = serde_json::to_string(&vector).unwrap();
        assert_eq!(json, r#"{"Vector":768}"#);
    }

    #[test]
    fn test_fieldtype_deserialization() {
        let text: FieldType = serde_json::from_str(r#""Text""#).unwrap();
        assert_eq!(text, FieldType::Text);

        let f32_type: FieldType = serde_json::from_str(r#""F32""#).unwrap();
        assert_eq!(f32_type, FieldType::F32);

        let vector: FieldType = serde_json::from_str(r#"{"Vector":768}"#).unwrap();
        assert_eq!(vector, FieldType::Vector(768));
    }
}
