#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
// Count and TopkCount must exclude deleted records in the unsealed tail.
// Fixtures cover both single-term and OR queries, including a frozen base.

use anyseek::index::{
    Clustering, FrequentwordType,
    IndexMetaObject, LexicalSimilarity, NgramSet, StopwordType,
    TokenizerType, };
use anyseek::search::{QueryType, ResultType, SearchMode};
use anyseek::vector::Inference;
use std::{fs, path::Path, time::Duration};

fn meta() -> IndexMetaObject {
    IndexMetaObject {
        name: "realtime_delete_consistency".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::TantivySimpleV1,
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: NgramSet::SingleTerm as u8,
        clustering: Clustering::None,
        inference: Inference::None,
    }
}

fn schema() -> Vec<anyseek::index::SchemaField> {
    serde_json::from_str(
        r#"[{"field":"title","field_type":"Text","store":true,"index_lexical":true}]"#,
    )
    .unwrap()
}

fn doc(title: &str) -> anyseek::index::Document {
    serde_json::from_str(&format!(r#"{{"title":"{title}"}}"#)).unwrap()
}

async fn search(
    index: &RecordPartition,
    query: &str,
    query_type: QueryType,
    result_type: ResultType,
) -> anyseek::record::RecordSearch {
    index.query(RecordQuery {text:query.into(),vector:None,query_type:query_type,mode:SearchMode::Lexical,offset:0,length:100,result_type:result_type,fields:Vec::new(),filters:Vec::new(),sort:Vec::new(),temporal:None}).await.unwrap()
}

async fn fresh(dir: &str) -> RecordPartition {
    let path = Path::new(dir);
    let _ = fs::remove_dir_all(path);
    records::create(path, meta(), (&schema()).to_vec()).await
}

/// Wait until the async indexer has settled `expected` docs (bounded).
/// Observe search visibility directly rather than inferring it from a count.
async fn settle_search(
    index: &RecordPartition,
    query: &str,
    query_type: QueryType,
    expected: usize,
) {
    for _ in 0..200 {
        let r = search(
            index,
            query,
            query_type.clone(),
            ResultType::TopkCount)
        .await;
        if r.count_total == expected && r.hits.len() == expected.min(100) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("realtime search did not settle to {expected} docs in time");
}

/// Single-term realtime totals must drop a deleted uncommitted document.
#[tokio::test]
async fn realtime_single_count_excludes_deleted() {
    let index = fresh("tests/index_delete_realtime_single/").await;
    let docs: Vec<_> = (0..5).map(|i| doc(&format!("base doc{i}"))).collect();
    records::insert(&index, docs).await.unwrap();
    index.compact_journal().await.unwrap();
    let docs: Vec<_> = (0..5).map(|i| doc(&format!("base extra{i}"))).collect();
    records::insert(&index, docs).await.unwrap();
    settle_search(&index, "base", QueryType::Union, 10).await;
    records::delete(&index, vec![9, 0]).await;

    let topk = search(
        &index,
        "base",
        QueryType::Union,
        ResultType::TopkCount)
    .await;
    let count = search(&index, "base", QueryType::Union, ResultType::Count).await;
    assert_eq!(topk.count_total, 8);
    assert_eq!(count.count_total, 8);

}

/// OR realtime totals must drop deleted uncommitted documents (no double count).
#[tokio::test]
async fn realtime_union_count_excludes_deleted() {
    let index = fresh("tests/index_delete_realtime_union/").await;
    let mut docs = vec![];
    for i in 0..10 {
        docs.push(doc(&format!("alpha uniq{i}")));
    }
    for i in 0..10 {
        docs.push(doc(&format!("beta uniqb{i}")));
    }
    records::insert(&index, docs).await.unwrap();
    settle_search(&index, "alpha beta", QueryType::Union, 20).await;
    records::delete(&index, vec![0, 1, 10]).await;

    let topk = search(
        &index,
        "alpha beta",
        QueryType::Union,
        ResultType::TopkCount)
    .await;
    let count = search(
        &index,
        "alpha beta",
        QueryType::Union,
        ResultType::Count)
    .await;
    assert_eq!(topk.count_total, 17);
    assert_eq!(topk.hits.len(), 17);
    assert_eq!(count.count_total, 17);

}
