use std::{collections::HashMap, sync::Arc};

use base64::{Engine as _, engine::general_purpose};
use seekpower::index::IndexArc;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use std::hash::{DefaultHasher, Hash, Hasher};

pub(crate) fn calculate_hash<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

pub(crate) async fn get_apikey_hash(
    api_key_base64: String,
    apikey_list: &Arc<tokio::sync::RwLock<HashMap<u128, ApikeyObject>>>,
) -> Option<u128> {
    match general_purpose::STANDARD.decode(api_key_base64) {
        Ok(apikey) => {
            let apikey_hash = calculate_hash(&apikey) as u128;
            let apikey_list_ref = apikey_list.read().await;

            if apikey_list_ref.contains_key(&apikey_hash) {
                Some(apikey_hash)
            } else {
                None
            }
        }
        Err(_e) => None,
    }
}


/// Create a random API key. The request body must be an empty JSON object.
#[derive(Default, Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields, try_from = "serde_json::Value")]
pub(crate) struct CreateApikeyRequest {}

impl TryFrom<serde_json::Value> for CreateApikeyRequest {
    type Error = &'static str;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        if value.as_object().is_some_and(|object| object.is_empty()) {
            Ok(Self {})
        } else {
            Err("API key creation requires an empty JSON object")
        }
    }
}

/// Persisted key identity and its in-memory index scope. Legacy quota is ignored
/// during deserialization; it is never enforced or written by this version.
#[derive(Deserialize, Serialize)]
pub(crate) struct ApikeyObject {
    pub id: u64,
    pub apikey_hash: u128,
    #[serde(skip)]
    pub index_list: HashMap<u64, IndexArc>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeleteApikeyRequest {
    pub apikey_base64: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn creation_rejects_retired_options() {
        assert!(serde_json::from_value::<CreateApikeyRequest>(json!({})).is_ok());
        assert!(serde_json::from_value::<CreateApikeyRequest>(json!({"demo":true})).is_err());
        for value in [json!([]), json!(null), json!(false), json!(""), json!({"demo":false})] {
            assert!(serde_json::from_value::<CreateApikeyRequest>(value).is_err());
        }
        for field in ["indices_max", "indices_size_max", "documents_max", "operations_max", "rate_limit", "quota"] {
            assert!(serde_json::from_value::<CreateApikeyRequest>(json!({field:0})).is_err(), "{field}");
        }
    }

    #[test]
    fn legacy_key_identity_survives_quota_removal() {
        for quota in [None, Some(json!({"indices_max":0,"indices_size_max":0,"documents_max":0,
            "operations_max":0,"rate_limit":0,"demo":true}))] {
            let mut record=json!({"id":7,"apikey_hash":12345});
            if let Some(quota)=quota { record["quota"]=quota; }
            let key: ApikeyObject=serde_json::from_value(record).unwrap();
            assert_eq!(key.id,7);assert_eq!(key.apikey_hash,12345);assert!(key.index_list.is_empty());
            assert_eq!(serde_json::to_value(key).unwrap(),json!({"id":7,"apikey_hash":12345}));
        }
    }
}
