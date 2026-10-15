//! Retired aggregation options must fail deserialization, including empty options.
use seekpower_server::api_types::SearchRequestObject;
use serde_json::json;

#[test]
fn retired_aggregations_are_rejected() {
    for facets in [json!([]), json!([{"U8":{"field":"age","range_type":"CountWithinRange","ranges":[]}}]), json!([{"String16":{"field":"category","prefix":"","length":10}}])] {
        let error = serde_json::from_value::<SearchRequestObject>(json!({"query":"hello","query_facets":facets})).unwrap_err();
        assert!(error.to_string().contains("unknown field `query_facets`"));
    }
}
