use ahash::AHasher;
use std::hash::{Hash, Hasher};

/// Deterministically hash an index name to a u32 ID using AHasher.
pub fn name_to_id(name: &str) -> u32 {
    let mut hasher = AHasher::default();
    name.hash(&mut hasher);
    hasher.finish() as u32
}

/// Encode a full key: data_type (u8) ‖ index_id (u32 BE) ‖ business_key.
pub fn encode_key(index_id: u32, data_type: u8, business_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + 4 + business_key.len());
    key.push(data_type);
    key.extend_from_slice(&index_id.to_be_bytes());
    key.extend_from_slice(business_key);
    key
}

/// Encode just the prefix: data_type (u8) ‖ index_id (u32 BE).
pub fn encode_prefix(index_id: u32, data_type: u8) -> Vec<u8> {
    let mut prefix = Vec::with_capacity(5);
    prefix.push(data_type);
    prefix.extend_from_slice(&index_id.to_be_bytes());
    prefix
}

/// Encode the index_id prefix (no data_type) for index-wide operations.
/// Note: With data_type first, this returns just the index_id, NOT the full prefix.
pub fn encode_index_prefix(index_id: u32) -> Vec<u8> {
    index_id.to_be_bytes().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_key_roundtrip() {
        let index_id = 42u32;
        let data_type = 1u8;
        let business_key = b"my_key";

        let key = encode_key(index_id, data_type, business_key);

        // Format: data_type (1B) ‖ index_id (4B BE) ‖ business_key
        assert_eq!(key[0], data_type);
        assert_eq!(&key[1..5], &index_id.to_be_bytes());
        assert_eq!(&key[5..], business_key);
    }

    #[test]
    fn test_encode_prefix_is_prefix_of_encode_key() {
        let index_id = 123u32;
        let data_type = 2u8;
        let business_key = b"test_key";

        let prefix = encode_prefix(index_id, data_type);
        let full_key = encode_key(index_id, data_type, business_key);

        assert!(full_key.starts_with(&prefix));
        assert_eq!(prefix.len(), 5);
    }

    #[test]
    fn test_name_to_id_deterministic() {
        let name1 = "articles";
        let name2 = "products";

        let id1a = name_to_id(name1);
        let id1b = name_to_id(name1);
        assert_eq!(id1a, id1b);

        let id2 = name_to_id(name2);
        assert_ne!(id1a, id2);
    }
}
