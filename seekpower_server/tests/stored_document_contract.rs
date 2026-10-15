//! Stored records remain lossless when presentation leaves the search core.
use seekpower_server::api_types::{SearchRequestObject, GetDocumentRequest};
use seekpower::{
    commit::Commit,
    index::{AccessType, Close, Clustering, Document, DocumentCompression, FrequentwordType,
        IndexDocument, IndexMetaObject, LexicalSimilarity, NgramSet,
        SchemaField, StopwordType, TokenizerType,
        create_index, open_index},
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
    assert!(serde_json::from_value::<GetDocumentRequest>(json!({"fields":["body"]})).is_ok());
}

#[tokio::test]
async fn stored_records_and_projection_survive_reopen() {
    for (name, document_compression) in [("none", DocumentCompression::None), ("snappy", DocumentCompression::Snappy)] {
        let directory=format!("tests/index_raw_document_{name}");
        let path=Path::new(&directory);
        let _=fs::remove_dir_all(path);
        let schema: Vec<SchemaField>=serde_json::from_value(json!([
            {"field":"body","field_type":"Text","store":true,"index_lexical":true},
            {"field":"data","field_type":"Json","store":true,"index_lexical":true},
            {"field":"hidden","field_type":"Text","store":false,"index_lexical":true}
        ])).unwrap();
        let meta=IndexMetaObject { id:0, name:"raw_document".into(),
            lexical_similarity:LexicalSimilarity::Bm25f, tokenizer:TokenizerType::UnicodeAlphanumeric,
            stop_words:StopwordType::None, frequent_words:FrequentwordType::None,
            ngram_indexing:NgramSet::SingleTerm as u8, document_compression,
            access_type:AccessType::Mmap, clustering:Clustering::None, inference:Inference::None };
        let index=create_index(path,meta,&schema, 11,true,Some(2)).await.unwrap();
        let body=format!("raw <b>literal</b> & 中文\r\n第二行\n{}END", "完整原文 ".repeat(1000));
        let expected: Document=serde_json::from_value(json!({"body":body,
            "data":{"nested":["<em>中文</em>",0,false,null],"line":"a\nb"}})).unwrap();
        let mut input=expected.clone();
        input.insert("hidden".into(),json!("indexed only"));
        input.insert("unknown".into(),json!("not in schema"));
        index.index_document(input).await.unwrap();
        for _ in 0..200 {
            if index.read().await.get_document(0,true,&HashSet::new()).await.is_ok(){break;}
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(index.read().await.get_document(0,true,&HashSet::new()).await.unwrap(),expected);
        index.commit().await;
        assert_eq!(index.read().await.get_document(0,false,&HashSet::new()).await.unwrap(),expected);
        index.close().await;drop(index);
        let reopened=open_index(path).await.unwrap();
        let reader=reopened.read().await;
        assert_eq!(reader.get_document(0,false,&HashSet::new()).await.unwrap(),expected);
        let fields=HashSet::from(["data".into(),"hidden".into(),"missing".into()]);
        let projected=reader.get_document(0,false,&fields).await.unwrap();
        assert_eq!(projected.len(),1);
        assert_eq!(projected["data"],expected["data"]);
        assert!(reader.get_document(0,false,&HashSet::from(["missing".into()])).await.unwrap().is_empty());
        drop(reader);reopened.close().await;drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }
}
