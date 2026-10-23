#![cfg(feature = "server")]
use anyseek::record::RecordSearch;
#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};

// Retired storage options fail explicitly; retained API defaults are stable.
use anyseek::index::{AccessType};
use anyseek::server::api_types::CreateIndexRequest;
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
    let schema: Vec<anyseek::index::SchemaField> = serde_json::from_value(json!([
        {"field":"body","field_type":"Text","store":true,"index_lexical":true}
    ]))
    .unwrap();
    let meta = serde_json::from_value(json!({
        "id":0,"name":"storage_contract", "access_type":"Mmap",
        "lexical_similarity":"Bm25f", "tokenizer":"UnicodeAlphanumeric"
    }))
    .unwrap();
    let index = Ok::<_, String>(records::create(path, meta, (&schema).to_vec()).await)
        .unwrap();
    
    drop(index);
    // Unknown codecs/access types are rejected while decoding persisted configuration.
    // Record does not read the retired index.json format.
    let valid: serde_json::Value=json!({"id":0,"name":"typed","access_type":"Mmap","lexical_similarity":"Bm25f","tokenizer":"UnicodeAlphanumeric"});
    for (field,value) in [("access_type","Ram"),("document_compression","Lz4"),("document_compression","Zstd")] {
        let mut invalid=valid.clone();invalid[field]=json!(value);
        assert!(serde_json::from_value::<anyseek::index::IndexMetaObject>(invalid).is_err());
    }
    let reopened=RecordPartition::open(path,records::JOURNAL_LIMIT).await.unwrap();
    drop(reopened);fs::remove_dir_all(path).unwrap();
}
