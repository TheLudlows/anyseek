
// Retired storage options fail explicitly; retained API defaults are stable.
use anyseek::server::api_types::CreateIndexRequest;
use serde_json::json;

#[test]
fn api_defaults_and_retired_codecs() {
    let request = json!({"index_name":"storage_contract", "schema":[
        {"field":"body","field_type":"Text","store":true,"index_lexical":true}
    ]});
    let parsed: CreateIndexRequest = serde_json::from_value(request.clone()).unwrap();
    assert_eq!(parsed.schema.len(), 1);
    for codec in ["None", "Snappy", "Lz4", "Zstd"] {
        let mut invalid = request.clone();
        invalid["document_compression"] = json!(codec);
        assert!(serde_json::from_value::<CreateIndexRequest>(invalid).is_err());
    }
    let mut invalid = request.clone();
    invalid["schema"][0]["longest"] = json!(true);
    assert!(serde_json::from_value::<CreateIndexRequest>(invalid).is_err());
}

#[test]
fn retired_disk_metadata_is_not_silently_reinterpreted() {
    // Unknown codecs/access types are rejected while decoding persisted configuration.
    // Record does not read the retired index.json format.
    let valid: serde_json::Value=json!({"name":"typed","lexical_similarity":"Bm25f","tokenizer":"TantivySimpleV1"});
    for (field,value) in [("access_type","Mmap"),("access_type","Ram"),("document_compression","None"),("document_compression","Snappy"),("document_compression","Lz4"),("document_compression","Zstd")] {
        let mut invalid=valid.clone();invalid[field]=json!(value);
        assert!(serde_json::from_value::<anyseek::index::IndexMetaObject>(invalid).is_err());
    }
}
