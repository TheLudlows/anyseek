#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
// Regression tests: deletes of not-yet-committed documents must survive
// the next commit instead of being silently dropped.
//
// Self-contained (own index directory), deterministic, no sleeps.


use anyseek::index::{
    AccessType, Clustering, DocumentCompression, FrequentwordType,
    IndexMetaObject, LexicalSimilarity, NgramSet, StopwordType,
    TokenizerType, };
use anyseek::search::{QueryType, ResultType, SearchMode};
use anyseek::vector::Inference;
use std::{fs, path::Path};

fn meta() -> IndexMetaObject {
    IndexMetaObject {
        id: 0,
        name: "delete_uncommitted".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::TantivySimpleV1,
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

async fn count(index: &RecordPartition, query: &str, uncommitted: bool) -> usize {
    index.query(RecordQuery {text:query.into(),vector:None,query_type:QueryType::Union,mode:SearchMode::Lexical,offset:0,length:100,result_type:ResultType::TopkCount,fields:Vec::new(),filters:Vec::new(),sort:Vec::new(),temporal:None}).await.unwrap()
        .count_total
}

async fn fresh(dir: &str) -> RecordPartition {
    let path = Path::new(dir);
    let _ = fs::remove_dir_all(path);
    Ok::<_, String>(records::create(path, meta(), (&schema()).to_vec()).await)
        .unwrap()
}

/// Delete-before-commit must stick: the document stays gone after commit.
#[tokio::test]
async fn delete_uncommitted_survives_commit() {
    let index = fresh("tests/index_delete_uncommitted/").await;
    records::insert(&index, vec![doc("hello world"), doc("foo bar")])
        .await.unwrap();
    records::delete(&index, vec![0]).await;
    // Must not underflow/panic while the async index task is still running.
    let _ = index.statistics().await.unwrap().active_records;
    index.compact_journal().await.unwrap();
    assert_eq!(count(&index, "hello", false).await, 0);
    assert_eq!(index.statistics().await.unwrap().active_records, 1);

}

/// Control: delete-after-commit keeps working as before.
#[tokio::test]
async fn delete_committed_control() {
    let index = fresh("tests/index_delete_committed/").await;
    records::insert(&index, vec![doc("hello world"), doc("foo bar")])
        .await.unwrap();
    index.compact_journal().await.unwrap();
    records::delete(&index, vec![0]).await;
    index.compact_journal().await.unwrap();
    assert_eq!(count(&index, "hello", false).await, 0);
    assert_eq!(index.statistics().await.unwrap().active_records, 1);

}

/// Out-of-range deletes stay silent no-ops: no panic, counts unchanged.
#[tokio::test]
async fn delete_out_of_range_ignored() {
    let index = fresh("tests/index_delete_range/").await;
    records::insert(&index, vec![doc("hello world"), doc("foo bar")])
        .await.unwrap();
    index.compact_journal().await.unwrap();
    records::delete(&index, Vec::new()).await;
    records::delete(&index, vec![usize::MAX]).await;
    records::delete(&index, vec![999_999]).await;
    records::delete(&index, vec![0, 0, 0]).await;
    index.compact_journal().await.unwrap();
    assert_eq!(count(&index, "hello", false).await, 0);
    assert_eq!(index.statistics().await.unwrap().active_records, 1);

}

/// Deleted identities retain versions; stale requests cannot target later inserts.
#[tokio::test]
async fn stale_identity_after_clear_cannot_delete_new_records() {
    let index = fresh("tests/index_delete_clear/").await;
    records::insert(&index, vec![doc("hello world"), doc("foo bar")])
        .await.unwrap();
    index.compact_journal().await.unwrap();
    records::clear(&index).await;
    records::insert(&index, vec![doc("alphaone"), doc("betaone")])
        .await.unwrap();
    use anyseek::record::{ExpectedVersion, OperationId, RecordWrite};
    let stale=RecordWrite {
        operation_id:OperationId::new("stale-version".into()),record_id:"00000000000000000000".into(),
        routing_key:"fixture".into(),expected:ExpectedVersion::Match(1),document:None,
        vector_patch:None,generation:None,temporal:None,system:None,
    };
    assert!(matches!(index.write(stale.clone()).await,Err(anyseek::record::RecordError::Conflict(_))));
    // Absent describes logical absence, including a tombstone. It does not reset
    // the retained version or target another identity created after the clear.
    let absent=RecordWrite {operation_id:OperationId::new("absent-tombstone".into()),expected:ExpectedVersion::Absent,..stale.clone()};
    assert_eq!(index.write(absent).await.unwrap().version,3);
    assert!(matches!(index.write(stale).await,Err(anyseek::record::RecordError::Conflict(_))));
    assert!(index.get("00000000000000000000").await.unwrap().is_none());
    assert_eq!(index.get("00000000000000000002").await.unwrap().unwrap().version,1);
    assert_eq!(count(&index, "alphaone", false).await,1);
    records::delete(&index, vec![2]).await;
    index.compact_journal().await.unwrap();
    assert_eq!(count(&index, "alphaone", false).await,0);
    assert_eq!(count(&index, "betaone", false).await,1);
    assert_eq!(index.statistics().await.unwrap().retained_identities,4);

}
