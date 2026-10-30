//! Public Record configuration and statistics are observational reads.
#[path = "common/records.rs"] mod records;
use anyseek::index::{Clustering, FrequentwordType,
    IndexMetaObject, LexicalSimilarity, NgramSet, SchemaField, StopwordType, TokenizerType};
use anyseek::vector::Inference;
use serde_json::json;
#[tokio::test]
async fn typed_snapshot_preserves_visible_position_and_configuration() {
    let root=tempfile::tempdir().unwrap();let path=root.path().join("records");
    let meta = IndexMetaObject {
        id: 17,
        name: "typed info".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::TantivySimpleV1,
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: NgramSet::SingleTerm as u8,
        clustering: Clustering::None,
        inference: Inference::None,
    };
    let schema: Vec<SchemaField> = serde_json::from_value(json!([
        {"field":"body", "field_type":"Text", "store":true, "index_lexical":true}
    ]))
    .unwrap();
    let partition=records::create(&path, meta, schema).await;
    records::append(&partition, vec![serde_json::from_value(json!({"body":"hello"})).unwrap()]).await;
    let before=partition.statistics().await.unwrap();
    let config=partition.configuration().await.unwrap();
    assert_eq!(config.index.id,17);
    assert_eq!(config.schema.len(),1);
    assert!(config.schema[0].index_lexical);
    assert_eq!(before.active_records,1);
    assert_eq!(partition.statistics().await.unwrap().visible_seq,before.visible_seq);
    partition.compact_journal().await.unwrap();
    assert_eq!(partition.statistics().await.unwrap().active_records,1);
}
