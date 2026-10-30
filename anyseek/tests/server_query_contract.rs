#![cfg(feature = "server")]
use anyseek::record::RecordSearch;
#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};

use anyseek::server::api_types::{SearchRequestObject, CreateIndexRequest};
use anyseek::{
    index::{Clustering, Document, FrequentwordType, IndexMetaObject, LexicalSimilarity, NgramSet,
        SchemaField, StopwordType, TokenizerType},
    search::{QueryType, ResultType, SearchMode},
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
        let mut request = create.clone(); request[field] = json!(null);
        assert!(serde_json::from_value::<CreateIndexRequest>(request).is_err());
    }
    for value in [json!("SearchOnly"), json!({"SearchRewrite":{"distance":1}})] {
        assert!(serde_json::from_value::<SearchRequestObject>(json!({"query":"red","query_rewriting":value})).is_err());
    }
    for field in ["dictionary_source", "completion_source"] {
        let mut request = create.clone(); request["schema"][0][field] = json!(false);
        assert!(serde_json::from_value::<CreateIndexRequest>(request).is_err());
    }
    assert!(serde_json::from_value::<CreateIndexRequest>(create).is_ok());
}

async fn assert_query(index: &RecordPartition, text: &str, expected: usize) {
    let result = index.query(RecordQuery {text:text.into(),vector:None,query_type:QueryType::Intersection,mode:SearchMode::Lexical,offset:0,length:10,result_type:ResultType::TopkCount,fields:vec![],filters:vec![],sort:vec![],temporal:None}).await.unwrap();
    assert_eq!(result.count_total, expected, "{text}");
    assert!(serde_json::to_value(result).unwrap().get("suggestions").is_none());
}

#[tokio::test]
async fn literal_queries_and_strict_metadata() {
    let path = Path::new("tests/index_query_contract"); let _ = fs::remove_dir_all(path);
    let meta = IndexMetaObject { id:0, name:"literal_queries".into(),
        lexical_similarity:LexicalSimilarity::Bm25f, tokenizer:TokenizerType::TantivySimpleV1,
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
        stop_words:StopwordType::None, frequent_words:FrequentwordType::English,
        ngram_indexing:NgramSet::NgramFF as u8 | NgramSet::NgramFFF as u8,
        clustering:Clustering::None, inference:Inference::None };
    let index = records::create(path, meta, (&schema()).to_vec()).await;
    for body in ["red apple the search engine ERR_CONNECTION_RESET 中文混合检索", "red green apple fishing car"] {
        let doc: Document = serde_json::from_value(json!({"body":body})).unwrap();
        records::insert(&index, vec![doc]).await.unwrap();
    }
    index.compact_journal().await.unwrap();
    for (query,count) in [("red",2),("\"red apple\"",1),("+red -green",1),
        ("\"the search engine\"",1),("ERR_CONNECTION_RESET",1),("中文混合检索",1),("applle",0),("FISHING",1),("fish",0),("car",1),("automobile",0)] {
        assert_query(&index, query, count).await;
    }
     drop(index);
    assert!(!path.join("synonyms.json").exists());
    assert!(!path.join("shards/0/synonyms.json").exists());
    assert!(!path.join("dictionary.csv").exists());
    assert!(!path.join("completions.csv").exists());
    let mut saved=serde_json::to_value(RecordPartition::open(path, records::JOURNAL_LIMIT).await.unwrap().configuration().await.unwrap().index).unwrap();
    let reopened=RecordPartition::open(path, records::JOURNAL_LIMIT).await.unwrap(); assert_query(&reopened,"\"red apple\"",1).await;
    for (query,count) in [("fishing",1),("fish",0),("car",1),("automobile",0)] { assert_query(&reopened,query,count).await; }
    drop(reopened);
    for field in ["spelling_correction","query_completion", "unknown_option", "stemmer", "synonyms"] {
        for value in [json!(null)] {
            saved[field]=value;
            match serde_json::from_value::<IndexMetaObject>(saved.clone()) {
                Ok(_) => panic!("unknown metadata accepted"),
                Err(error) => assert!(error.to_string().contains("unknown field"),"{error}"),
            }
            saved.as_object_mut().unwrap().remove(field);
        }
    }
    fs::remove_dir_all(path).unwrap();
}

#[test]
fn retired_aggregations_are_rejected() {
    for facets in [json!([]), json!([{"Int":{"field":"age","range_type":"CountWithinRange","ranges":[]}}]), json!([{"String":{"field":"category","prefix":"","length":10}}])] {
        let error = serde_json::from_value::<SearchRequestObject>(json!({"query":"hello","query_facets":facets})).unwrap_err();
        assert!(error.to_string().contains("unknown field `query_facets`"));
    }
}
