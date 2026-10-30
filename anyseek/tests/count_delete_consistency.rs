#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
// Regression tests: single-term `ResultType::Count` must exclude deleted
// documents, matching `TopkCount`/`results.len()`.
//
// Root cause was `single_docid` (anyseek/src/single.rs) treating
// "deletes exist" as unfiltered and taking the `Count` fast path,
// while `single_blockid` already treated deletes as filtered.
//
// Self-contained (own index directory), deterministic, no sleeps.


use anyseek::index::{
    Clustering, FrequentwordType,
    IndexMetaObject, LexicalSimilarity, NgramSet, StopwordType,
    TokenizerType, };
use anyseek::search::{QueryType, ResultType, SearchMode};
use anyseek::vector::Inference;
use std::{fs, path::Path};

fn meta() -> IndexMetaObject {
    IndexMetaObject {
        id: 0,
        name: "count_delete_consistency".into(),
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
    result_type: ResultType,
) -> anyseek::record::RecordSearch {
    index.query(RecordQuery {text:query.into(),vector:None,query_type:QueryType::Union,mode:SearchMode::Lexical,offset:0,length:100,result_type:result_type,fields:Vec::new(),filters:Vec::new(),sort:Vec::new(),temporal:None}).await.unwrap()
}

async fn fresh(dir: &str) -> RecordPartition {
    let path = Path::new(dir);
    let _ = fs::remove_dir_all(path);
    records::create(path, meta(), (&schema()).to_vec()).await
}

/// Count must agree with TopkCount once documents are deleted.
#[tokio::test]
async fn single_count_excludes_deleted() {
    let index = fresh("tests/index_delete_count/").await;
    let docs: Vec<_> = (0..10).map(|i| doc(&format!("common word{i}"))).collect();
    records::insert(&index, docs).await.unwrap();
    index.compact_journal().await.unwrap();
    records::delete(&index, vec![0, 1, 2]).await;
    index.compact_journal().await.unwrap();

    let count = search(&index, "common", ResultType::Count).await;
    let topk_count = search(&index, "common", ResultType::TopkCount).await;
    assert_eq!(count.count_total, 7);
    assert_eq!(topk_count.count_total, 7);
    assert_eq!(topk_count.hits.len(), 7);
    let mut ids: Vec<usize> = topk_count.hits.iter().map(|r| r.record.record_id.parse::<usize>().unwrap()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![3, 4, 5, 6, 7, 8, 9]);

}

/// Control: without deletes the Count fast path is untouched and exact.
#[tokio::test]
async fn single_count_no_deletes_control() {
    let index = fresh("tests/index_delete_count_control/").await;
    let docs: Vec<_> = (0..10).map(|i| doc(&format!("common word{i}"))).collect();
    records::insert(&index, docs).await.unwrap();
    index.compact_journal().await.unwrap();

    let count = search(&index, "common", ResultType::Count).await;
    let topk_count = search(&index, "common", ResultType::TopkCount).await;
    assert_eq!(count.count_total, 10);
    assert_eq!(topk_count.count_total, 10);
    assert_eq!(topk_count.hits.len(), 10);

}
