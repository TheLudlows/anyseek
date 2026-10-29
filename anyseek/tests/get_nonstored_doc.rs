//! Payload retains original fields independently of derived index storage options.
#[path = "common/records.rs"] mod records;
use anyseek::index::{AccessType, Clustering, DocumentCompression, FrequentwordType,
    IndexMetaObject, LexicalSimilarity, NgramSet, StopwordType, TokenizerType};
use anyseek::{record::RecordPartition, vector::Inference};
fn meta(access_type: AccessType) -> IndexMetaObject {
    IndexMetaObject {
        id: 0,
        name: "get_nonstored_doc".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::TantivySimpleV1,
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: NgramSet::SingleTerm as u8,
        document_compression: DocumentCompression::Snappy,
        access_type,
        clustering: Clustering::None,
        inference: Inference::None,
    }
}

fn schema() -> Vec<anyseek::index::SchemaField> {
    serde_json::from_str(
        r#"[{"field":"nostore","field_type":"Text","store":false,"index_lexical":true},{"field":"title","field_type":"Text","store":true,"index_lexical":true}]"#,
    )
    .unwrap()
}


async fn check(document: serde_json::Value) {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("records");
    let mut partition = records::create(&path, meta(AccessType::Mmap), schema()).await;
    records::append(&partition, vec![serde_json::from_value(document.clone()).unwrap()]).await;
    for stage in 0..3 {
        if stage == 1 { partition.compact_journal().await.unwrap(); }
        if stage == 2 {
            drop(partition);
            partition = RecordPartition::open(&path, records::JOURNAL_LIMIT).await.unwrap();
        }
        let stored = partition.get("00000000000000000000").await.unwrap().unwrap();
        assert_eq!(serde_json::to_value(stored.document.unwrap()).unwrap(), document);
        assert!(partition.get("absent").await.unwrap().is_none());
    }
}
#[tokio::test]
async fn nonstored_fields_remain_canonical_payload() {
    check(serde_json::json!({"nostore":"only indexed content"})).await;
}
#[tokio::test]
async fn stored_fields_remain_canonical_payload() {
    check(serde_json::json!({"title":"hello world"})).await;
}
