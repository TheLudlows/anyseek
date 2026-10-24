use anyseek::{
    index::{
        AccessType, Clustering, DocumentCompression, FieldLengthNormalization,
        FrequentwordType,  IndexMetaObject, LexicalSimilarity,
        StopwordType, TextNormalization, TokenizerType,
    },
    search::{QueryType, ResultSort, ResultType, SearchMode, SortOrder},
    vector::Inference,
};
use serde_json::{Value, json};
use anyseek::record::{RecordPartition, RecordConfig, RecordQuery, RecordWrite, ExpectedVersion, OperationId};
const JOURNAL_LIMIT: u64 = 64 * 1024 * 1024;
async fn create_records(path: &std::path::Path, index: IndexMetaObject, schema: &[anyseek::index::SchemaField]) -> RecordPartition {
    RecordPartition::create(path, RecordConfig {
        change_feed: Default::default(), index, schema: schema.to_vec(), embedding_space_id: None,
        max_records: 1000, max_record_bytes: 64*1024, max_operations: 10000,
        max_journal_bytes: JOURNAL_LIMIT, max_candidates: 1000,
    }).await.unwrap()
}
async fn add(index: &RecordPartition, docs: &[Value], total: usize) {
    for (ordinal, document) in docs.iter().enumerate() {
        let id = format!("{:020}", total - docs.len() + ordinal);
        index.write(RecordWrite {
            operation_id: OperationId::new(format!("insert-{id}")), record_id: id,
            routing_key: "oracle".into(), expected: ExpectedVersion::Absent,
            document: Some(serde_json::from_value(document.clone()).unwrap()),
            vector_patch: None, generation: None, temporal: None, system: None,
        }).await.unwrap();
    }
    assert_eq!(index.statistics().await.unwrap().active_records, total);
}
fn idf(n: f64, df: f64) -> f64 {
    (1.0 + (n - df + 0.5) / (df + 0.5)).ln()
}
fn expected(
    fields: usize,
    policy: FieldLengthNormalization,
    proximity: bool,
    mask: u8,
    words: usize,
    extra_match: bool,
) -> f64 {
    let specs = if fields == 1 {
        vec![(1.0, 5.0, 5.0, 1.0)]
    } else {
        vec![(2.0, 8.0, 8.0, 2.0), (0.5, 5.0, 5.0, 1.0)]
    };
    let shared = if fields == 1 { 5.0 } else { 13.0 };
    let n = if extra_match { 5.0 } else { 4.0 };
    let extra_df = if extra_match { 1.0 } else { 0.0 };
    specs
        .into_iter()
        .map(|(boost, dl, mean, tf)| {
            let avg = if policy == FieldLengthNormalization::Shared {
                shared
            } else {
                mean
            };
            let component = |df, frequency: f64| {
                boost * idf(n, df + extra_df) * frequency * 2.2
                    / (frequency + 1.2 * (0.25 + 0.75 * dl / avg))
            };
            let mut score = if !proximity || mask == 0 {
                (1..=words.min(3))
                    .map(|df| component(df as f64, tf + if df == 2 { 1.0 } else { 0.0 }))
                    .sum()
            } else if words == 2 || mask != 1 {
                component(1.0, tf)
            } else {
                component(1.0, tf) + component(3.0, tf)
            };
            if words == 4 {
                score += component(1.0, 1.0);
            }
            score
        })
        .sum()
}
/// Check literal membership, physical DF, boost, TF and denominator contributions.
pub async fn exercise(proximity: bool) {
    let mut failures = Vec::new();
    let mut checked = 0;
    for policy in [
        FieldLengthNormalization::Shared,
        FieldLengthNormalization::PerField,
    ] {
        for fields in [1, 2] {
            for mask in [0, 1, 9, 127] {
                for (mixed, extra_match) in [(false, false), (true, false), (true, true)] {
                    let label = format!(
                        "ngram-{proximity}-{policy:?}-{fields}-{mask}-{mixed}-{extra_match}-{}",
                        std::process::id()
                    );
                    let path = std::env::temp_dir().join(&label);
                    assert!(!path.exists());
                    let meta = IndexMetaObject {
                        id: 0,
                        name: label.clone(),
                        lexical_similarity: if proximity {
                            LexicalSimilarity::Bm25fProximity
                        } else {
                            LexicalSimilarity::Bm25f
                        },
                        tokenizer: TokenizerType::UnicodeAlphanumeric,
                        text_normalization: TextNormalization::None,
                        field_length_normalization: policy,
                        stop_words: StopwordType::None,
                        frequent_words: FrequentwordType::English,
                        ngram_indexing: mask,
                        document_compression: DocumentCompression::Snappy,
                        access_type: AccessType::Mmap,
                        clustering: Clustering::None,
                        inference: Inference::None,
                    };
                    let schema: Vec<anyseek::index::SchemaField>=serde_json::from_value(if fields==1 {json!([{"field":"a","field_type":"Text","index_lexical":true,"store":false,"boost":1.0}])}else{json!([{"field":"a","field_type":"Text","index_lexical":true,"store":false,"boost":2.0},{"field":"b","field_type":"Text","index_lexical":true,"store":false,"boost":0.5}])}).unwrap();
                    // Constant per-field lengths keep legacy Shared committed means unchanged on append,
                    // isolating ngram scoring from the separate length-update policy.
                    // Literal physical DFs: the/of/and/anchor=1/2/3/1 (plus one with extra_match).
                    // The extra nonadjacent `of` raises component TF but not whole-gram TF.
                    // Lengths <=8 decode exactly; all docs have constant per-field lengths.
                    let mut docs = if fields == 1 {
                        vec![
                            json!({"a":"the of and anchor of"}),
                            json!({"a":"of neutral neutral neutral neutral"}),
                            json!({"a":"and neutral neutral neutral neutral"}),
                            json!({"a":"and neutral neutral neutral neutral"}),
                        ]
                    } else {
                        vec![
                            json!({"a":"the of and the of and anchor of","b":"the of and anchor of"}),
                            json!({"a":"of neutral neutral neutral neutral neutral neutral neutral","b":"of neutral neutral neutral neutral"}),
                            json!({"a":"and neutral neutral neutral neutral neutral neutral neutral","b":"and neutral neutral neutral neutral"}),
                            json!({"a":"and neutral neutral neutral neutral neutral neutral neutral","b":"and neutral neutral neutral neutral"}),
                        ]
                    };
                    if extra_match {
                        docs.push(docs[0].clone());
                    }
                    let mut index = create_records(&path, meta, &schema).await;
                    if mixed {
                        add(&index, &docs[..1], 1).await;
                        index.compact_journal().await.unwrap();
                        add(&index, &docs[1..], docs.len()).await;
                    } else {
                        add(&index, &docs, docs.len()).await;
                    }
                    for stage in 0..3 {
                        if stage == 1 {
                            index.compact_journal().await.unwrap();
                        }
                        if stage == 2 {

                            drop(index);
                            index = RecordPartition::open(&path, JOURNAL_LIMIT).await.unwrap();
                        }
                        for (words, q) in [
                            (2, "\"the of\""),
                            (3, "\"the of and\""),
                            (4, "\"the of and anchor\""),
                        ] {
                            for realtime in [false, true] {
                                for explicit in [false, true] {
                                    let r = index
                                        .query(RecordQuery {
                                            text: q.into(), vector: None, temporal: None, query_type: QueryType::Union, mode: SearchMode::Lexical, offset: 0, length: 100, result_type: ResultType::TopkCount, fields: vec![], filters: vec![], sort:
                                            if explicit {
                                                vec![ResultSort {
                                                    field: "_score".into(),
                                                    order: SortOrder::Descending,
                                                }]
                                            } else {
                                                vec![]
                                            },
                                        }).await.unwrap();

                                    let visible = true; // Successful Record writes are durable and visible.
                                    let want_ids = if !visible {
                                        vec![]
                                    } else if extra_match {
                                        vec![0, 4]
                                    } else {
                                        vec![0]
                                    };
                                    assert_eq!(
                                        r.hits.iter().map(|x| x.record.record_id.parse::<usize>().unwrap()).collect::<Vec<_>>(),
                                        want_ids,
                                        "{label} stage={stage} q={q}"
                                    );
                                    assert_eq!(r.count_total, want_ids.len());
                                    for hit in r.hits {
                                        let want = expected(
                                            fields,
                                            policy,
                                            proximity,
                                            mask,
                                            words,
                                            extra_match,
                                        );
                                        let got = hit.score as f64;
                                        assert!(
                                            got.is_finite(),
                                            "{label} stage={stage} q={q} nonfinite score={got}"
                                        );
                                        checked += 1;
                                        if (got - want).abs() > 2e-5 * want.abs().max(1.0) {
                                            failures.push(format!("{label} stage={stage} rt={realtime} explicit={explicit} q={q} got={got} want={want}"));
                                        }
                                    }
                                }
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
        "{} / {checked} score failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// Preserve the established committed-DF quantization while refreshing its source.
pub async fn exercise_quantized_df() {
    for policy in [
        FieldLengthNormalization::Shared,
        FieldLengthNormalization::PerField,
    ] {
        let label = format!("component-quantization-{policy:?}-{}", std::process::id());
        let path = std::env::temp_dir().join(&label);
        assert!(!path.exists());
        let meta = IndexMetaObject {
            id: 0,
            name: label,
            lexical_similarity: LexicalSimilarity::Bm25f,
            tokenizer: TokenizerType::UnicodeAlphanumeric,
            text_normalization: TextNormalization::None,
            field_length_normalization: policy,
            stop_words: StopwordType::None,
            frequent_words: FrequentwordType::English,
            ngram_indexing: 1,
            document_compression: DocumentCompression::Snappy,
            access_type: AccessType::Mmap,
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
