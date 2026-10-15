use seekpower_server::api_types::{SearchRequestObject, CreateIndexRequest};
use seekpower::{
    commit::Commit,
    index::{AccessType, Close, Clustering, Document, DocumentCompression,
        FrequentwordType, IndexArc, IndexDocument, IndexMetaObject, LexicalSimilarity, NgramSet,
        SchemaField, StopwordType, TokenizerType, create_index, open_index},
    search::{QueryType, ResultType, Search, SearchMode},
    vector::Inference,
};
use serde_json::json;
use std::{fs, path::Path};

fn schema() -> Vec<SchemaField> {
    serde_json::from_value(json!([{"field":"body","field_type":"Text","store":true,"index_lexical":true}])).unwrap()
}

#[test]
fn retired_query_options_are_rejected() {
    let create = json!({"index_name":"contract","schema":[{"field":"body","field_type":"Text"}]});
    for field in ["spelling_correction", "query_completion", "synonyms", "stemmer"] {
        for value in [json!(null), json!({}), json!([]), json!("None"), json!("English")] {
            let mut request = create.clone(); request[field] = value;
            assert!(serde_json::from_value::<CreateIndexRequest>(request).is_err());
        }
    }
    for value in [json!("SearchOnly"), json!({"SearchRewrite":{"distance":1}})] {
        assert!(serde_json::from_value::<SearchRequestObject>(json!({"query":"red","query_rewriting":value})).is_err());
    }
    for field in ["dictionary_source", "completion_source"] {
        for value in [false,true] {
            let mut request = create.clone(); request["schema"][0][field] = json!(value);
            assert!(serde_json::from_value::<CreateIndexRequest>(request).is_err());
        }
    }
    assert!(serde_json::from_value::<CreateIndexRequest>(create).is_ok());
}

async fn assert_query(index: &IndexArc, text: &str, expected: usize) {
    let result = index.search(text.into(), None, QueryType::Intersection, SearchMode::Lexical,
        false, 0, 10, ResultType::TopkCount, false, vec![],  vec![], vec![]).await;
    assert!(result.error.is_none(), "{:?}", result.error);
    assert_eq!(result.original_query, text);
    assert_eq!(result.query, text);
    assert_eq!(result.result_count_total, expected, "{text}");
    assert!(serde_json::to_value(result).unwrap().get("suggestions").is_none());
}

#[tokio::test]
async fn literal_queries_and_strict_metadata() {
    let path = Path::new("tests/index_query_contract"); let _ = fs::remove_dir_all(path);
    let meta = IndexMetaObject { id:0, name:"literal_queries".into(),
        lexical_similarity:LexicalSimilarity::Bm25f, tokenizer:TokenizerType::UnicodeAlphanumeric,
        stop_words:StopwordType::None, frequent_words:FrequentwordType::English,
        ngram_indexing:NgramSet::NgramFF as u8 | NgramSet::NgramFFF as u8,
        document_compression:DocumentCompression::Snappy, access_type:AccessType::Mmap,
        clustering:Clustering::None, inference:Inference::None };
    let index = create_index(path, meta, &schema(), 11, true, Some(1)).await.unwrap();
    for body in ["red apple the search engine ERR_CONNECTION_RESET 中文混合检索", "red green apple fishing car"] {
        let doc: Document = serde_json::from_value(json!({"body":body})).unwrap();
        index.index_document(doc).await.unwrap();
    }
    index.commit().await;
    for (query,count) in [("red",2),("\"red apple\"",1),("+red -green",1),
        ("\"the search engine\"",1),("ERR_CONNECTION_RESET",1),("中文混合检索",1),("applle",0),("FISHING",1),("fish",0),("car",1),("automobile",0)] {
        assert_query(&index, query, count).await;
    }
    index.close().await; drop(index);
    assert!(!path.join("synonyms.json").exists());
    assert!(!path.join("shards/0/synonyms.json").exists());
    assert!(!path.join("dictionary.csv").exists());
    assert!(!path.join("completions.csv").exists());
    let meta_path=path.join("index.json");
    let mut saved: serde_json::Value=serde_json::from_slice(&fs::read(&meta_path).unwrap()).unwrap();
    let reopened=open_index(path).await.unwrap(); assert_query(&reopened,"\"red apple\"",1).await;
    for (query,count) in [("fishing",1),("fish",0),("car",1),("automobile",0)] { assert_query(&reopened,query,count).await; }
    reopened.close().await;drop(reopened);
    for field in ["spelling_correction","query_completion", "unknown_option", "stemmer", "synonyms"] {
        for value in [json!(null), json!({}), json!([]), json!("None"), json!("English")] {
            saved[field]=value;
            fs::write(&meta_path,serde_json::to_vec(&saved).unwrap()).unwrap();
            match open_index(path).await {
                Ok(_) => panic!("unknown metadata accepted"),
                Err(error) => assert!(error.contains("unknown field"),"{error}"),
            }
            saved.as_object_mut().unwrap().remove(field);
        }
    }
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn retired_aggregations_are_rejected() {
    for facets in [json!([]), json!([{"U8":{"field":"age","range_type":"CountWithinRange","ranges":[]}}]), json!([{"String16":{"field":"category","prefix":"","length":10}}])] {
        let error = serde_json::from_value::<SearchRequestObject>(json!({"query":"hello","query_facets":facets})).unwrap_err();
        assert!(error.to_string().contains("unknown field `query_facets`"));
    }
}
