//! Regression tests: multi-term `Union` (OR) `result_count_total` must exclude
//! deleted documents, matching `results.len()`.
//!
//! Root cause was `union_docid_2` (anyseek/src/union.rs) computing the
//! total from raw `posting_count`s whenever no NOT/field filter was present,
//! ignoring `delete_hashset` (commit does not rewrite posting lists).
//! Depends on the single-term Count fix (#71) for the per-term slow path.
//!
//! Self-contained (own index directory), deterministic, no sleeps.

use anyseek::commit::Commit;
use anyseek::index::{
    AccessType, Close, Clustering, DeleteDocuments, DocumentCompression, FrequentwordType,
    IndexDocuments, IndexMetaObject, LexicalSimilarity, NgramSet, StopwordType,
    TokenizerType, create_index,
};
use anyseek::search::{QueryType, ResultType, Search, SearchMode};
use anyseek::vector::Inference;
use std::{fs, path::Path};

fn meta() -> IndexMetaObject {
    IndexMetaObject {
        id: 0,
        name: "union_delete_consistency".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: NgramSet::SingleTerm as u8,
        document_compression: DocumentCompression::Snappy,
        access_type: AccessType::Mmap,
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
    index: &anyseek::index::IndexArc,
    query: &str,
    result_type: ResultType,
) -> anyseek::search::ResultObject {
    index
        .search(
            query.into(),
            None,
            QueryType::Union,
            SearchMode::Lexical,
            false,
            0,
            100,
            result_type,
            false,
            Vec::new(),

            Vec::new(),
            Vec::new(),
        )
        .await
}

async fn fresh(dir: &str) -> anyseek::index::IndexArc {
    let path = Path::new(dir);
    let _ = fs::remove_dir_all(path);
    create_index(path, meta(), &schema(), 11, true, None)
        .await
        .unwrap()
}

/// OR total must drop deleted documents (disjoint terms).
#[tokio::test]
async fn union_count_excludes_deleted() {
    let index = fresh("tests/index_delete_union/").await;
    let mut docs = vec![];
    for i in 0..10 {
        docs.push(doc(&format!("alpha uniq{i}")));
    }
    for i in 0..10 {
        docs.push(doc(&format!("beta uniqb{i}")));
    }
    index.index_documents(docs).await.unwrap();
    index.commit().await.unwrap();
    index.delete_documents(vec![0, 1, 10]).await;
    index.commit().await.unwrap();

    let count = search(&index, "alpha beta", ResultType::Count).await;
    let topk_count = search(&index, "alpha beta", ResultType::TopkCount).await;
    assert_eq!(count.result_count_total, 17);
    assert_eq!(topk_count.result_count_total, 17);
    assert_eq!(topk_count.results.len(), 17);
    index.close().await.unwrap();
}

/// Control: without deletes the OR fast path is untouched and exact.
#[tokio::test]
async fn union_count_no_deletes_control() {
    let index = fresh("tests/index_delete_union_control/").await;
    let mut docs = vec![];
    for i in 0..10 {
        docs.push(doc(&format!("alpha uniq{i}")));
    }
    for i in 0..10 {
        docs.push(doc(&format!("beta uniqb{i}")));
    }
    index.index_documents(docs).await.unwrap();
    index.commit().await.unwrap();

    let count = search(&index, "alpha beta", ResultType::Count).await;
    let topk_count = search(&index, "alpha beta", ResultType::TopkCount).await;
    assert_eq!(count.result_count_total, 20);
    assert_eq!(topk_count.result_count_total, 20);
    index.close().await.unwrap();
}
