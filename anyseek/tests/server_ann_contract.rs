#![cfg(feature = "server")]
use anyseek::search::SearchMode;
use anyseek::vector_similarity::AnnMode;
use anyseek::server::api_types::SearchRequestObject;
use serde_json::{Value, json};
use std::num::NonZeroUsize;

fn mode(value: Value) -> Result<SearchMode, String> {
    serde_json::from_value::<SearchRequestObject>(value)
        .map_err(|e| e.to_string())?
        .search_mode(NonZeroUsize::new(37).unwrap())
}

#[test]
fn ann_defaults_overrides_and_all_resolve_for_both_vector_modes() {
    for text in ["", "hello"] {
        for (options, expected) in [
            (json!({}), AnnMode::Nprobe(37)),
            (json!({"all":false}), AnnMode::Nprobe(37)),
            (json!({"nprobe":1}), AnnMode::Nprobe(1)),
            (json!({"nprobe":usize::MAX}), AnnMode::Nprobe(usize::MAX)),
            (json!({"all":false,"nprobe":5}), AnnMode::Nprobe(5)),
            (json!({"all":true}), AnnMode::All),
        ] {
            let mut request = json!({"query":text,"query_vector":[1,0,0]});
            request
                .as_object_mut()
                .unwrap()
                .extend(options.as_object().unwrap().clone());
            match mode(request).unwrap() {
                SearchMode::Vector { ann_mode, .. } if text.is_empty() => {
                    assert_eq!(ann_mode, expected)
                }
                SearchMode::Hybrid { ann_mode, .. } if !text.is_empty() => {
                    assert_eq!(ann_mode, expected)
                }
                _ => panic!("incorrect retrieval mode"),
            }
        }
    }
    assert!(matches!(mode(json!({})).unwrap(), SearchMode::Lexical));
}

#[test]
fn ann_rejects_conflicts_and_parameters_without_vectors() {
    assert!(
        mode(json!({"query_vector":[1,0,0],"all":true,"nprobe":1}))
            .unwrap_err()
            .contains("cannot be combined")
    );
    for text in ["", "hello"] {
        for options in [
            json!({"all":true}),
            json!({"all":false}),
            json!({"nprobe":1}),
        ] {
            let mut request = json!({"query":text});
            request
                .as_object_mut()
                .unwrap()
                .extend(options.as_object().unwrap().clone());
            assert!(mode(request).unwrap_err().contains("require query_vector"));
        }
    }
}

#[test]
fn ann_json_types_are_strict() {
    for invalid in [
        json!(0),
        json!(-1),
        json!(1.5),
        json!(1.0),
        json!("1"),
        json!(true),
        Value::Null,
        json!([]),
        json!(18446744073709551616.0),
    ] {
        assert!(mode(json!({"query_vector":[1,0,0],"nprobe":invalid})).is_err());
    }
    for invalid in [json!(0), json!("true"), Value::Null, json!([])] {
        assert!(mode(json!({"query_vector":[1,0,0],"all":invalid})).is_err());
    }
}

#[test]
fn omitted_budgets_round_trip_without_explicit_nulls() {
    let request: SearchRequestObject =
        serde_json::from_value(json!({"query_vector":[1,0,0]})).unwrap();
    let value = serde_json::to_value(request).unwrap();
    assert!(value.get("all").is_none());
    assert!(value.get("nprobe").is_none());
    assert!(mode(value).is_ok());
}
