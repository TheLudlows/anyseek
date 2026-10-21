//! Retired storage options fail explicitly; retained API defaults are stable.
use seekpower::index::{AccessType, Close, create_index, open_index};
use seekpower_server::api_types::CreateIndexRequest;
use serde_json::json;
use std::{fs, path::Path};

#[test]
fn api_defaults_and_retired_codecs() {
    let request = json!({"index_name":"storage_contract", "schema":[
        {"field":"body","field_type":"Text","store":true,"index_lexical":true}
    ]});
    let parsed: CreateIndexRequest = serde_json::from_value(request.clone()).unwrap();
    assert_eq!(parsed.schema.len(), 1);
    for codec in ["Lz4", "Zstd"] {
        let mut invalid = request.clone();
        invalid["document_compression"] = json!(codec);
        assert!(serde_json::from_value::<CreateIndexRequest>(invalid).is_err());
    }
    assert!(serde_json::from_value::<AccessType>(json!("Ram")).is_err());
}

#[tokio::test]
async fn retired_disk_metadata_is_not_silently_reinterpreted() {
    let path = Path::new("tests/index_retired_storage_contract");
    let _ = fs::remove_dir_all(path);
    let schema = serde_json::from_value(json!([
        {"field":"body","field_type":"Text","store":true,"index_lexical":true}
    ]))
    .unwrap();
    let meta = serde_json::from_value(json!({
        "id":0,"name":"storage_contract", "access_type":"Mmap",
        "lexical_similarity":"Bm25f", "tokenizer":"UnicodeAlphanumeric"
    }))
    .unwrap();
    let index = create_index(path, meta, &schema, 11, true, Some(1))
        .await
        .unwrap();
    index.close().await.unwrap();
    drop(index);
    let metadata = path.join("index.json");
    let valid: serde_json::Value = serde_json::from_slice(&fs::read(&metadata).unwrap()).unwrap();
    for (field, value) in [
        ("access_type", "Ram"),
        ("document_compression", "Lz4"),
        ("document_compression", "Zstd"),
    ] {
        let mut invalid = valid.clone();
        invalid[field] = json!(value);
        fs::write(&metadata, serde_json::to_vec(&invalid).unwrap()).unwrap();
        match open_index(path).await {
            Ok(_) => panic!("retired {field}={value} accepted"),
            Err(error) => assert!(error.contains(value), "{error}"),
        }
    }
    fs::write(&metadata, serde_json::to_vec(&valid).unwrap()).unwrap();
    let index = open_index(path).await.unwrap();
    index.close().await.unwrap();
    drop(index);
    fs::remove_dir_all(path).unwrap();
}
