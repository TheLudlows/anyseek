//! Independent literal ngram scoring across candidate and persistence states.
#[path = "common/ngram_oracle.rs"]
mod oracle;
use anyseek::{
    index::{Clustering, FieldLengthNormalization, FrequentwordType, IndexMetaObject,
        LexicalSimilarity, StopwordType, TextNormalization, TokenizerType},
    record::{RecordPartition, RecordQuery},
    search::{QueryType, ResultType, SearchMode},
    vector::Inference,
};
use serde_json::json;
use oracle::{add, create_records, JOURNAL_LIMIT};
#[tokio::test]
async fn unequal_component_df_matches_literal_formula_in_all_states() {
    oracle::exercise(false).await;
}
#[tokio::test]
async fn committed_quantization_and_live_counts_remain_compatible() {
    for policy in [
        FieldLengthNormalization::Shared,
        FieldLengthNormalization::PerField,
    ] {
        let label = format!("component-quantization-{policy:?}-{}", std::process::id());
        let path = std::env::temp_dir().join(&label);
        assert!(!path.exists());
        let meta = IndexMetaObject {
            name: label,
            lexical_similarity: LexicalSimilarity::Bm25f,
            tokenizer: TokenizerType::TantivySimpleV1,
            text_normalization: TextNormalization::None,
            field_length_normalization: policy,
            stop_words: StopwordType::None,
            frequent_words: FrequentwordType::English,
            ngram_indexing: 1,
            clustering: Clustering::None,
            inference: Inference::None,
        };
        let schema: Vec<anyseek::index::SchemaField> = serde_json::from_value(
            json!([{"field":"a","field_type":"Text","index_lexical":true,"store":false}]),
        )
        .unwrap();
        let mut index = create_records(&path, meta, &schema).await;
        let mut docs = vec![json!({"a":"of neutral"}); 100];
        docs[0] = json!({"a":"the of"});
        add(&index, &docs, 100).await;
        index.compact_journal().await.unwrap();
        for stage in 0..6 {
            if stage == 1 {
                add(&index, &vec![json!({"a":"of neutral"}); 4], 104).await;
            }
            if stage == 2 || stage == 5 {
                index.compact_journal().await.unwrap();
            }
            if stage == 3 {

                drop(index);
                index = RecordPartition::open(&path, JOURNAL_LIMIT).await.unwrap();
            }
            if stage == 4 {
                add(&index, &[json!({"a":"the of"})], 105).await;
            }
            // Record has always used active exact DF, overriding compressed
            // physical posting DF (legacy Shard::scoring_df with active view).
            // Retain the boundary-sized corpus, but assert that public contract.
            let n = if stage == 0 {
                100.0
            } else if stage < 4 {
                104.0
            } else {
                105.0
            };
            let df_the = if stage < 4 { 1.0 } else { 2.0 };
            let df_of = n;
            let expected = ((n - df_the + 0.5) / (df_the + 0.5) + 1.0_f64).ln()
                + ((n - df_of + 0.5) / (df_of + 0.5) + 1.0_f64).ln();
            for _repeat in 0..2 {
                let r = index
                    .query(RecordQuery {
                        text: "\"the of\"".into(), vector: None, temporal: None, query_type: QueryType::Union, mode: SearchMode::Lexical, offset: 0, length: 100, result_type: ResultType::TopkCount, fields: vec![], filters: vec![], sort:
                        vec![],
                    }).await.unwrap();
                let want = if stage >= 4 {
                    vec![0, 104]
                } else {
                    vec![0]
                };
                assert_eq!(r.hits.iter().map(|x| x.record.record_id.parse::<usize>().unwrap()).collect::<Vec<_>>(), want);
                assert_eq!(r.count_total, want.len());
                for hit in r.hits {
                    assert!(
                        (hit.score as f64 - expected).abs() < 2e-5,
                        "stage={stage} got={} expected={expected}",
                        hit.score
                    );
                }
            }
        }

        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }
}
