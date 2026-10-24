//! Canonical payload reads cannot depend on neighboring derived storage slots.
#[path = "common/records.rs"] mod records;
use anyseek::index::{AccessType, Clustering, Document, DocumentCompression, FrequentwordType,
    IndexMetaObject, LexicalSimilarity, NgramSet, StopwordType, TokenizerType};
use anyseek::{record::RecordPartition, vector::Inference};
#[tokio::test]
async fn mixed_stored_and_nonstored_payloads_roundtrip() {
    let root=tempfile::tempdir().unwrap();let path=root.path().join("records");
    let access=AccessType::Mmap;let compression=DocumentCompression::Snappy;
    let schema_json = r#"
    [{"field":"title","field_type":"Text","store":true,"index_lexical":true,"longest":true},
    {"field":"tag","field_type":"Text","store":false,"index_lexical":true}]"#;
    let schema: Vec<anyseek::index::SchemaField> = serde_json::from_str(schema_json).unwrap();
    let meta = IndexMetaObject {
        id: 0,
        name: "succ".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: NgramSet::SingleTerm as u8,
        document_compression: compression,
        access_type: access,
        clustering: Clustering::None,
        inference: Inference::None,
    };
    let mut partition=records::create(&path,meta,schema).await;
    let documents: Vec<Document> = [r#"{"tag":"hello"}"#,r#"{"tag":"world"}"#,
        r#"{"title":"hello world"}"#,r#"{"title":"baseline doc"}"#,
        r#"{"tag":"again"}"#,r#"{"title":"mixed successor"}"#]
        .into_iter().map(|s|serde_json::from_str(s).unwrap()).collect();
    records::append(&partition,documents.clone()).await;
    for stage in 0..3 {
        if stage==1 {partition.compact_journal().await.unwrap();}
        if stage==2 {drop(partition);partition=RecordPartition::open(&path,records::JOURNAL_LIMIT).await.unwrap();}
        for (id,want) in documents.iter().enumerate() {
            let found=partition.get(&format!("{id:020}")).await.unwrap().unwrap();
            assert_eq!(found.document.as_ref(),Some(want));
        }
    }
}
