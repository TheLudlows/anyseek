use std::{collections::HashMap, sync::Arc};

use base64::{Engine as _, engine::general_purpose};
use seekpower::index::{IndexArc, open_index};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
};
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

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeleteApikeyRequest {
    pub apikey_base64: String,
}

const APIKEY_PATH: &str = "apikey.json";

/// Save file atomically
fn save_file_atomically(path: &PathBuf, content: String) {
    let mut temp_path = path.clone();
    temp_path.set_extension("bak");
    fs::write(&temp_path, content).unwrap();
    match fs::rename(temp_path, path) {
        Ok(_) => {}
        Err(e) => println!("error: {e:?}"),
    }
}

fn save_apikey_data(apikey: &ApikeyObject, index_path: &PathBuf) {
    let apikey_id: u64 = apikey.id;

    let apikey_id_path = Path::new(&index_path).join(apikey_id.to_string());
    let apikey_persistence_json = serde_json::to_string(&apikey).unwrap();
    let apikey_persistence_path = Path::new(&apikey_id_path).join(APIKEY_PATH);
    save_file_atomically(&apikey_persistence_path, apikey_persistence_json);
}

pub(crate) fn create_apikey_api<'a>(
    index_path: &'a PathBuf,
    apikey: &[u8],
    apikey_list: &'a mut HashMap<u128, ApikeyObject>,
) -> &'a mut ApikeyObject {
    let apikey_hash_u128 = calculate_hash(&apikey) as u128;
    // A repeated identity must not replace its scope or orphan indices.
    if apikey_list.contains_key(&apikey_hash_u128) {
        return apikey_list.get_mut(&apikey_hash_u128).unwrap();
    }

    let mut apikey_id: u64 = 0;
    let mut apikey_list_vec: Vec<(&u128, &ApikeyObject)> = apikey_list.iter().collect();
    apikey_list_vec.sort_by_key(|a| a.1.id);
    for value in apikey_list_vec {
        if value.1.id == apikey_id {
            apikey_id = value.1.id + 1;
        } else {
            break;
        }
    }

    let apikey_object = ApikeyObject {
        id: apikey_id,
        apikey_hash: apikey_hash_u128,
        index_list: HashMap::new(),
    };

    let apikey_id_path = Path::new(&index_path).join(apikey_id.to_string());
    fs::create_dir_all(apikey_id_path).unwrap();

    save_apikey_data(&apikey_object, index_path);

    apikey_list.insert(apikey_hash_u128, apikey_object);
    apikey_list.get_mut(&apikey_hash_u128).unwrap()
}

pub(crate) fn delete_apikey_api(
    index_path: &PathBuf,
    apikey_list: &mut HashMap<u128, ApikeyObject>,
    apikey_hash: u128,
) -> Result<u64, String> {
    if let Some(apikey_object) = apikey_list.get(&apikey_hash) {
        let apikey_id_path = Path::new(&index_path).join(apikey_object.id.to_string());
        println!("delete path {}", apikey_id_path.to_string_lossy());
        fs::remove_dir_all(&apikey_id_path).unwrap();

        apikey_list.remove(&apikey_hash);
        Ok(apikey_list.len() as u64)
    } else {
        Err("not found".to_string())
    }
}

/// Open all indices below a single apikey
async fn open_all_indices(index_path: &PathBuf, index_list: &mut HashMap<u64, IndexArc>) {
    if !Path::exists(index_path) {
        fs::create_dir_all(index_path).unwrap();
    }

    for result in fs::read_dir(index_path).unwrap() {
        let path = result.unwrap();
        if path.path().is_dir() {
            let single_index_path = path.path();

            let index_arc = match open_index(&single_index_path).await {
                Ok(index_arc) => index_arc,
                Err(err) => {
                    println!("{} {}", err, single_index_path.display());
                    continue;
                }
            };

            let index_id = index_arc.read().await.meta.id;

            index_list.insert(index_id, index_arc);
        }
    }
}

/// Open api key
async fn open_apikey(index_path: &PathBuf, apikey_list: &mut HashMap<u128, ApikeyObject>) -> bool {
    let apikey_path = Path::new(&index_path).join(APIKEY_PATH);
    match fs::read_to_string(apikey_path) {
        Ok(apikey_string) => {
            let mut apikey_object: ApikeyObject = serde_json::from_str(&apikey_string).unwrap();

            open_all_indices(index_path, &mut apikey_object.index_list).await;
            apikey_list.insert(apikey_object.apikey_hash, apikey_object);

            true
        }
        Err(_) => false,
    }
}

/// Open all apikeys in the specified path
pub(crate) async fn open_all_apikeys(
    index_path: &PathBuf,
    apikey_list: &mut HashMap<u128, ApikeyObject>,
) -> bool {
    let mut test_index_flag = false;
    if !Path::exists(index_path) {
        println!("index path not found: {} ", index_path.to_string_lossy());
        fs::create_dir_all(index_path).unwrap();
    }

    for result in fs::read_dir(index_path).unwrap() {
        let path = result.unwrap();
        if path.path().is_dir() {
            let single_index_path = path.path();
            test_index_flag |= open_apikey(&single_index_path, apikey_list).await;
        }
    }
    test_index_flag
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn creation_rejects_retired_options() {
        assert!(serde_json::from_value::<CreateApikeyRequest>(json!({})).is_ok());
        assert!(serde_json::from_value::<CreateApikeyRequest>(json!({"demo":true})).is_err());
        for value in [
            json!([]),
            json!(null),
            json!(false),
            json!(""),
            json!({"demo":false}),
        ] {
            assert!(serde_json::from_value::<CreateApikeyRequest>(value).is_err());
        }
        for field in [
            "indices_max",
            "indices_size_max",
            "documents_max",
            "operations_max",
            "rate_limit",
            "quota",
        ] {
            assert!(
                serde_json::from_value::<CreateApikeyRequest>(json!({field:0})).is_err(),
                "{field}"
            );
        }
    }

    #[test]
    fn legacy_key_identity_survives_quota_removal() {
        for quota in [
            None,
            Some(
                json!({"indices_max":0,"indices_size_max":0,"documents_max":0,
            "operations_max":0,"rate_limit":0,"demo":true}),
            ),
        ] {
            let mut record = json!({"id":7,"apikey_hash":12345});
            if let Some(quota) = quota {
                record["quota"] = quota;
            }
            let key: ApikeyObject = serde_json::from_value(record).unwrap();
            assert_eq!(key.id, 7);
            assert_eq!(key.apikey_hash, 12345);
            assert!(key.index_list.is_empty());
            assert_eq!(
                serde_json::to_value(key).unwrap(),
                json!({"id":7,"apikey_hash":12345})
            );
        }
    }
}
