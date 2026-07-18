use crate::FieldType;

impl FieldType {
    /// 类型收敛：合并两个可能的类型
    pub fn converge(&self, other: &Self) -> Self {
        match (self, other) {
            (t1, t2) if t1 == t2 => t1.clone(),

            // 文本升级
            (FieldType::Keyword, FieldType::Text) |
            (FieldType::Text, FieldType::Keyword) => FieldType::Text,

            // 数值升级
            (FieldType::U64, FieldType::I64) |
            (FieldType::I64, FieldType::U64) => FieldType::I64,
            (FieldType::U64, FieldType::F64) |
            (FieldType::F64, FieldType::U64) |
            (FieldType::I64, FieldType::F64) |
            (FieldType::F64, FieldType::I64) => FieldType::F64,

            // 其他所有冲突 → Json
            (_, _) => FieldType::Json,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_converge_same_type() {
        assert_eq!(
            FieldType::Text.converge(&FieldType::Text),
            FieldType::Text
        );
    }

    #[test]
    fn test_converge_keyword_text() {
        assert_eq!(
            FieldType::Keyword.converge(&FieldType::Text),
            FieldType::Text
        );
        assert_eq!(
            FieldType::Text.converge(&FieldType::Keyword),
            FieldType::Text
        );
    }

    #[test]
    fn test_converge_u64_i64() {
        assert_eq!(
            FieldType::U64.converge(&FieldType::I64),
            FieldType::I64
        );
    }

    #[test]
    fn test_converge_i64_f64() {
        assert_eq!(
            FieldType::I64.converge(&FieldType::F64),
            FieldType::F64
        );
    }

    #[test]
    fn test_converge_u64_f64() {
        assert_eq!(
            FieldType::U64.converge(&FieldType::F64),
            FieldType::F64
        );
    }

    #[test]
    fn test_converge_incompatible_types() {
        assert_eq!(
            FieldType::Text.converge(&FieldType::I64),
            FieldType::Json
        );
        assert_eq!(
            FieldType::Bool.converge(&FieldType::F64),
            FieldType::Json
        );
    }

    #[test]
    fn test_converge_vector_dimension_mismatch() {
        assert_eq!(
            FieldType::Vector(3).converge(&FieldType::Vector(4)),
            FieldType::Json
        );
    }

    #[test]
    fn test_converge_json_with_any() {
        assert_eq!(
            FieldType::Json.converge(&FieldType::Text),
            FieldType::Json
        );
    }
}
