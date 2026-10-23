#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
// Independent formula for the realtime single-term multifield boost regression.
use anyseek::{
    index::{
        AccessType, Clustering, DocumentCompression, FrequentwordType, IndexMetaObject, LexicalSimilarity, StopwordType, TextNormalization, TokenizerType,
        },
    search::{QueryType, ResultType, SearchMode},
    vector::Inference,
};
use serde_json::json;
#[tokio::test]
async fn boosted_single_term_matches_formula_in_every_state() {
    let path =
        std::env::temp_dir().join(format!("anyseek-realtime-boost-{}", std::process::id()));
    assert!(!path.exists());
    let meta = IndexMetaObject {
        id: 0,
        name: "boost".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        text_normalization: TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: 0,
        document_compression: DocumentCompression::Snappy,
        access_type: AccessType::Mmap,
        clustering: Clustering::None,
        inference: Inference::None,
    };
    let schema: Vec<anyseek::index::SchemaField>=serde_json::from_value(json!([{"field":"a","field_type":"Text","index_lexical":true,"store":true,"boost":2},{"field":"b","field_type":"Text","index_lexical":true,"store":true,"boost":4}])).unwrap();
    let mut index = Ok::<_, String>(records::create(&path, meta, (&schema).to_vec()).await)
        .unwrap();
    records::insert(&index, vec![
            serde_json::from_value(json!({"a":"alpha","b":"alpha"})).unwrap(),
            serde_json::from_value(json!({"a":"beta","b":"beta"})).unwrap(),
        ])
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while index.statistics().await.unwrap().physical_versions != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let expected = 6.0 * 2.0_f64.ln() * 2.2 / (1.0 + 1.2 * (0.25 + 0.75 / 2.0));
    for stage in 0..3 {
        if stage == 1 {
            index.compact_journal().await.unwrap();
        }
        if stage == 2 {
            
            drop(index);
            index = RecordPartition::open(&path, records::JOURNAL_LIMIT).await.unwrap();
        }
        let x = index.query(RecordQuery {text:"alpha".into(),vector:None,query_type:QueryType::Union,mode:SearchMode::Lexical,offset:0,length:10,result_type:ResultType::TopkCount,fields:vec![],filters:vec![],sort:vec![],temporal:None}).await.unwrap();
        assert_eq!(x.count_total, 1);
        assert_eq!(x.hits.len(), 1);
        assert_eq!(x.hits[0].record.record_id.parse::<usize>().unwrap(), 0);
        assert!(
            (x.hits[0].score as f64 - expected).abs() < 2e-6,
            "stage={stage}, actual={}",
            x.hits[0].score
        );
    }
    
    drop(index);
    std::fs::remove_dir_all(path).unwrap();
}
