use anyseek::{
    commit::Commit,
    index::{
        AccessType, Close, Clustering, DocumentCompression, FrequentwordType, IndexDocument,
        IndexMetaObject, LexicalSimilarity, NgramSet, SchemaField, StopwordType,
        TokenizerType, create_index,
    },
    info::collect_index_info,
    vector::Inference,
};
use serde_json::json;
use std::{
    fs,
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

struct CallerLogger(AtomicUsize);
impl log::Log for CallerLogger {
    fn enabled(&self, record: &log::Metadata<'_>) -> bool {
        record.target().starts_with("anyseek")
    }
    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn flush(&self) {}
}
static LOGGER: CallerLogger = CallerLogger(AtomicUsize::new(0));

#[tokio::test]
async fn typed_snapshot_does_not_commit_and_caller_controls_logging() {
    log::set_logger(&LOGGER).unwrap();
    log::set_max_level(log::LevelFilter::Info);
    let path = Path::new("tests/index_core_info");
    let _ = fs::remove_dir_all(path);
    let meta = IndexMetaObject {
        id: 17,
        name: "typed info".into(),
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
    };
    let schema: Vec<SchemaField> = serde_json::from_value(json!([
        {"field":"body", "field_type":"Text", "store":true, "index_lexical":true}
    ]))
    .unwrap();
    let index = create_index(path, meta, &schema, 11, false, Some(1))
        .await
        .unwrap();
    assert!(
        LOGGER.0.load(Ordering::SeqCst) > 0,
        "core events reach caller logger"
    );
    index
        .index_document(serde_json::from_value(json!({"body":"hello"})).unwrap())
        .await
        .unwrap();
    for _ in 0..200 {
        if collect_index_info(&index).await.indexed_documents == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let info = collect_index_info(&index).await;
    assert_eq!(info.meta.id, 17);
    assert_eq!(info.indexed_documents, 1);
    assert_eq!(info.fields, 1);
    assert_eq!(info.indexed_fields, 1);
    assert_eq!(info.stored_fields, 1);
    assert_eq!(info.shards, 1);
    assert!(info.index_size_bytes > 0);
    assert!(info.lexical && !info.vector);
    assert_eq!(
        index.read().await.committed_doc_count().await,
        0,
        "observing info must not commit"
    );
    index.commit().await.unwrap();
    assert_eq!(collect_index_info(&index).await.indexed_documents, 1);
    log::set_max_level(log::LevelFilter::Off);
    let events = LOGGER.0.load(Ordering::SeqCst);
    index.close().await.unwrap();
    assert_eq!(
        LOGGER.0.load(Ordering::SeqCst),
        events,
        "caller can disable core output"
    );
    drop(index);
    fs::remove_dir_all(path).unwrap();
}
