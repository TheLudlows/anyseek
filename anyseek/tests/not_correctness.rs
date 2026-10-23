//! Independent NOT membership, posting-rank and count regression (#37).
use anyseek::{
    index::{
        AccessType, Clustering, DocumentCompression, FrequentwordType, 
         IndexMetaObject, LexicalSimilarity, StopwordType, TokenizerType,
        
    },
    search::{QueryType, ResultSort, ResultType, SearchMode, SortOrder},
    vector::Inference,
};
#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
use records::JOURNAL_LIMIT;
use serde_json::json;
use std::path::PathBuf;

async fn fresh(label: &str, ngrams: u8) -> (RecordPartition, PathBuf) {
    let path = std::env::temp_dir().join(format!("anyseek-t2s-{label}-{}", std::process::id()));
    assert!(!path.exists());
    let meta = IndexMetaObject {
        id: 0,
        name: label.into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: if label.starts_with("unicode-") {
            TokenizerType::UnicodeAlphanumeric
        } else {
            TokenizerType::UnicodeAlphanumericZH
        },
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: ngrams,
        document_compression: DocumentCompression::Snappy,
        access_type: AccessType::Mmap,
        clustering: Clustering::None,
        inference: Inference::None,
    };
    let schema: Vec<anyseek::index::SchemaField> = serde_json::from_value(
        json!([{"field":"body","field_type":"Text","store":true,"index_lexical":true}]),
    )
    .unwrap();
    (
        records::create(&path, meta, schema).await,
        path,
    )
}
async fn add(index: &RecordPartition, docs: &[&str]) {
    records::append(index, docs.iter().map(|body| serde_json::from_value(json!({"body":body})).unwrap()).collect()).await;
}
async fn rows_sorted(
    index: &RecordPartition,
    q: &str,
    _realtime: bool,
    explicit: bool,
    count: bool,
) -> (Vec<(usize, f32)>, usize) {
    let r = index
        .query(RecordQuery {
            text: q.into(), vector: None, temporal: None, query_type: QueryType::Union, mode: SearchMode::Lexical, offset: 0, length: 100, result_type:
            if count {
                ResultType::TopkCount
            } else {
                ResultType::Topk
            },
            fields: vec![], filters: vec![], sort:
            if explicit {
                vec![ResultSort {
                    field: "_score".into(),
                    order: SortOrder::Descending,
                }]
            } else {
                vec![]
            },
        }).await.unwrap();
    
    (
        r.hits.iter().map(|r| (r.record.record_id.parse::<usize>().unwrap(), r.score)).collect(),
        r.count_total,
    )
}
#[tokio::test]
async fn not_filter_has_independent_membership_and_tf_oracle() {
    // Independent #37/#28 regression. Topk/TopkCount replace the B-only
    // normalization-profile dimension: 96 cases, no shared scoring oracle.
    let terms = [
        "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota",
    ];
    let mut failures = Vec::new();
    for tokenizer in ["unicode", "zh"] {
        for count_mode in [false, true] {
            for count in [2, 9] {
                for negative in ["banned", "禁用"] {
                    let (mut index, path) = fresh(
                        &format!("{tokenizer}-not-{count_mode}-{count}-{negative}"),
                        0,
                    )
                    .await;
                    let second = terms[..count].join(" ");
                    let first = format!("alpha alpha {second} {negative}");
                    add(&index, &[&first, &second]).await;
                    for stage in 0..3 {
                        if stage == 1 {
                            index.compact_journal().await.unwrap();
                        }
                        if stage == 2 {
                            
                            drop(index);
                            index = RecordPartition::open(&path, JOURNAL_LIMIT).await.unwrap();
                        }
                        for explicit in [false, true] {
                            let (result, total) = rows_sorted(
                                &index,
                                &format!("{second} -{negative}"),
                                stage == 0,
                                explicit,
                                count_mode,
                            )
                            .await;
                            let ids: Vec<_> = result.iter().map(|r| r.0).collect();
                            let average = count as f64 + 1.5;
                            let expected = count as f64 * (1.0_f64 + 0.5 / 2.5).ln() * 2.2
                                / (1.0 + 1.2 * (0.25 + 0.75 * count as f64 / average));
                            let membership = ids == vec![1] && (!count_mode || total == 1);
                            // The Latin fixture's literal token lengths are independent of a CJK
                            // segmenter; the Chinese fixture separately gates NOT membership.
                            let score_ok = !negative.is_ascii()
                                || (membership && (result[0].1 as f64 - expected).abs() < 2e-5);
                            let record = json!({"tokenizer":tokenizer,"topk_count":count_mode,"positive_terms":count,"negative":negative,"stage":stage,"explicit_score":explicit,"result":result,"membership_ok":membership,"score_ok":score_ok,"expected_latin_score":expected});
                            println!("NOT_ORACLE {record}");
                            if !membership || !score_ok {
                                failures.push(record);
                            }
                        }
                    }
                    
                    drop(index);
                    std::fs::remove_dir_all(path).unwrap();
                }
            }
        }
    }
    assert!(
        failures.is_empty(),
        "upstream NOT acceptance blocked (#37/#28): {failures:?}"
    );
}
