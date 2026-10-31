use anyseek::index::{IndexMetaObject, TokenizerType};
use serde_json::json;

const RETIRED: [&str; 6] = [
    "AsciiAlphabetic", "UnicodeAlphanumeric", "UnicodeAlphanumericFolded",
    "Whitespace", "WhitespaceLowercase", "UnicodeAlphanumericZH",
];

#[test]
fn retired_profiles_are_rejected_in_enum_and_persisted_metadata() {
    for name in RETIRED {
        let error = serde_json::from_value::<TokenizerType>(json!(name)).unwrap_err();
        assert!(error.to_string().contains("unknown variant"), "{error}");
        let meta = json!({"name":"profiles",
            "lexical_similarity":"Bm25f","tokenizer":name});
        assert!(serde_json::from_value::<IndexMetaObject>(meta).is_err(), "{name}");
    }
}

#[test]
fn supported_profiles_roundtrip_and_feature_defaults_are_stable() {
    let profiles = vec![TokenizerType::TantivySimpleV1];
    #[cfg(feature = "zh")]
    let profiles = { let mut p = profiles; p.push(TokenizerType::JiebaMixedV1); p };
    for profile in profiles {
        let value = serde_json::to_value(profile).unwrap();
        assert_eq!(value, json!(profile.to_string()));
        assert_eq!(serde_json::from_value::<TokenizerType>(value).unwrap(), profile);
    }
    #[cfg(feature = "zh")]
    assert_eq!(TokenizerType::default(), TokenizerType::JiebaMixedV1);
    #[cfg(not(feature = "zh"))]
    {
        assert_eq!(TokenizerType::default(), TokenizerType::TantivySimpleV1);
        assert!(serde_json::from_value::<TokenizerType>(json!("JiebaMixedV1")).is_err());
    }
}

#[cfg(feature = "server")]
#[test]
fn api_rejects_retired_profiles_and_preserves_default() {
    use anyseek::server::api_types::CreateIndexRequest;
    let request = json!({"index_name":"profiles","schema":[
        {"field":"body","field_type":"Text","store":true,"index_lexical":true}
    ]});
    let parsed: CreateIndexRequest = serde_json::from_value(request.clone()).unwrap();
    assert_eq!(parsed.tokenizer, TokenizerType::default());
    for name in RETIRED {
        let mut invalid = request.clone();
        invalid["tokenizer"] = json!(name);
        let error = serde_json::from_value::<CreateIndexRequest>(invalid).err().unwrap();
        assert!(error.to_string().contains("unknown variant"), "{error}");
    }
}

#[cfg(feature = "server")]
#[test]
fn openapi_exposes_only_enabled_standard_profiles() {
    use anyseek::server::api_types::CreateIndexRequest;
    use utoipa::OpenApi;
    #[derive(utoipa::OpenApi)]
    #[openapi(components(schemas(CreateIndexRequest)))]
    struct Api;
    let doc = serde_json::to_value(Api::openapi()).unwrap();
    let profiles = doc["components"]["schemas"]["TokenizerType"]["enum"]
        .as_array().expect("tokenizer enum schema");
    let expected = vec![json!("TantivySimpleV1")];
    #[cfg(feature = "zh")]
    let expected = { let mut p = expected; p.insert(0, json!("JiebaMixedV1")); p };
    assert_eq!(profiles, &expected);
}
