#![cfg(feature = "server")]
use anyseek::record::RecordSearch;
#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};

// Stored records remain lossless when presentation leaves the search core.
use anyseek::server::api_types::{SearchRequestObject, GetDocumentRequest};
use anyseek::{
    index::{Clustering, Document, FrequentwordType,
        IndexMetaObject, LexicalSimilarity, NgramSet,
        SchemaField, StopwordType, TokenizerType},
    vector::Inference,
};
use serde_json::json;
use std::{collections::HashSet, fs, path::Path, time::Duration};

#[test]
fn retired_presentation_options_are_rejected() {
    for highlights in [json!([]), json!([{"field":"body","highlight_markup":true}])] {
        assert!(serde_json::from_value::<SearchRequestObject>(json!({
            "query":"raw", "highlights":highlights})).is_err());
        assert!(serde_json::from_value::<GetDocumentRequest>(json!({
            "highlights":highlights})).is_err());
    }
    for query_terms in [json!([]), json!(["raw"])] {
        assert!(serde_json::from_value::<GetDocumentRequest>(json!({"query_terms":query_terms})).is_err());
    }
    assert!(serde_json::from_value::<GetDocumentRequest>(json!({})).is_ok());
    assert!(serde_json::from_value::<GetDocumentRequest>(json!({"fields":"body"})).is_ok());
}

#[tokio::test]
async fn stored_records_and_projection_survive_reopen() {
    let directory="tests/index_raw_document".to_string();
    let path=Path::new(&directory);
    let _=fs::remove_dir_all(path);
    let schema: Vec<SchemaField>=serde_json::from_value(json!([
        {"field":"body","field_type":"Text","store":true,"index_lexical":true},
        {"field":"data","field_type":"Json","store":true,"index_lexical":true},
        {"field":"hidden","field_type":"Text","store":false,"index_lexical":true}
    ])).unwrap();
    let meta=IndexMetaObject { id:0, name:"raw_document".into(),
        lexical_similarity:LexicalSimilarity::Bm25f, tokenizer:TokenizerType::TantivySimpleV1,
    text_normalization: anyseek::index::TextNormalization::None,
    field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
        stop_words:StopwordType::None, frequent_words:FrequentwordType::None,
        ngram_indexing:NgramSet::SingleTerm as u8,
        clustering:Clustering::None, inference:Inference::None };
    let index=records::create(path, meta, (&schema).to_vec()).await;
    let body=format!("raw <b>literal</b> & 中文\r\n第二行\n{}END", "完整原文 ".repeat(1000));
    let expected: Document=serde_json::from_value(json!({"body":body,
        "data":{"nested":["<em>中文</em>",0,false,null],"line":"a\nb"}})).unwrap();
    let mut input=expected.clone();
    input.insert("hidden".into(),json!("indexed only"));
    input.insert("unknown".into(),json!("not in schema"));
    records::insert(&index, vec![input.clone()]).await.unwrap();
    for stage in 0..2 {
        if stage==1 {index.compact_journal().await.unwrap();}
        assert_eq!(index.get("00000000000000000000").await.unwrap().unwrap().document.as_ref(),Some(&input));
    }
    drop(index);
    let reopened=RecordPartition::open(path,records::JOURNAL_LIMIT).await.unwrap();
    assert_eq!(reopened.get("00000000000000000000").await.unwrap().unwrap().document.as_ref(),Some(&input));
    // Presentation projection belongs to HTTP; Record returns exact Payload.
    drop(reopened);fs::remove_dir_all(path).unwrap();

}
