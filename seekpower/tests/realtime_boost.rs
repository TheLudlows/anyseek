//! Independent formula for the realtime single-term multifield boost regression.
use seekpower::{
    commit::Commit,
    index::{
        AccessType, Close, Clustering, DocumentCompression, FrequentwordType, IndexDocuments,
        IndexMetaObject, LexicalSimilarity, StopwordType, TextNormalization, TokenizerType,
        create_index, open_index,
    },
    search::{QueryType, ResultType, Search, SearchMode},
    vector::Inference,
};
use serde_json::json;
#[tokio::test]
async fn boosted_single_term_matches_formula_in_every_state() {
    let path =
        std::env::temp_dir().join(format!("seekpower-realtime-boost-{}", std::process::id()));
    assert!(!path.exists());
    let meta = IndexMetaObject {
        id: 0,
        name: "boost".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        text_normalization: TextNormalization::None,
        field_length_normalization: seekpower::index::FieldLengthNormalization::Shared,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: 0,
        document_compression: DocumentCompression::Snappy,
        access_type: AccessType::Mmap,
        clustering: Clustering::None,
        inference: Inference::None,
    };
    let schema=serde_json::from_value(json!([{"field":"a","field_type":"Text","index_lexical":true,"store":true,"boost":2},{"field":"b","field_type":"Text","index_lexical":true,"store":true,"boost":4}])).unwrap();
    let mut index = create_index(&path, meta, &schema, 11, true, Some(1))
        .await
        .unwrap();
    index
        .index_documents(vec![
            serde_json::from_value(json!({"a":"alpha","b":"alpha"})).unwrap(),
            serde_json::from_value(json!({"a":"beta","b":"beta"})).unwrap(),
        ])
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while index.read().await.indexed_doc_count().await != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let expected = 6.0 * 2.0_f64.ln() * 2.2 / (1.0 + 1.2 * (0.25 + 0.75 / 2.0));
    for stage in 0..3 {
        if stage == 1 {
            index.commit().await;
        }
        if stage == 2 {
            index.close().await;
            drop(index);
            index = open_index(&path).await.unwrap();
        }
        let x = index
            .search(
                "alpha".into(),
                None,
                QueryType::Union,
                SearchMode::Lexical,
                false,
                0,
                10,
                ResultType::TopkCount,
                stage == 0,
                vec![],
                vec![],
                vec![],
            )
            .await;
        assert!(x.error.is_none());
        assert_eq!(x.result_count_total, 1);
        assert_eq!(x.results.len(), 1);
        assert_eq!(x.results[0].doc_id, 0);
        assert!(
            (x.results[0].score as f64 - expected).abs() < 2e-6,
            "stage={stage}, actual={}",
            x.results[0].score
        );
    }
    index.close().await;
    drop(index);
    std::fs::remove_dir_all(path).unwrap();
}
