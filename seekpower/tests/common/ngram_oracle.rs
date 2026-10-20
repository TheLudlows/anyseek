use seekpower::{
    commit::Commit,
    index::{
        AccessType, Close, Clustering, DocumentCompression, FieldLengthNormalization,
        FrequentwordType, IndexArc, IndexDocuments, IndexMetaObject, LexicalSimilarity,
        StopwordType, TextNormalization, TokenizerType, create_index, open_index,
    },
    search::{QueryType, ResultSort, ResultType, Search, SearchMode, SortOrder},
    vector::Inference,
};
use serde_json::{Value, json};
async fn add(index: &IndexArc, docs: &[Value], total: usize) {
    index
        .index_documents(
            docs.iter()
                .cloned()
                .map(|d| serde_json::from_value(d).unwrap())
                .collect(),
        )
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while index.read().await.indexed_doc_count().await != total {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
fn idf(df: f64) -> f64 {
    (1.0 + (4.0 - df + 0.5) / (df + 0.5)).ln()
}
fn expected(
    fields: usize,
    policy: FieldLengthNormalization,
    proximity: bool,
    mask: u8,
    words: usize,
) -> f64 {
    let specs = if fields == 1 {
        vec![(1.0, 4.0, 4.0, 1.0)]
    } else {
        vec![(2.0, 7.0, 7.0, 2.0), (0.5, 4.0, 4.0, 1.0)]
    };
    let shared = if fields == 1 { 4.0 } else { 11.0 };
    specs
        .into_iter()
        .map(|(boost, dl, mean, tf)| {
            let avg = if policy == FieldLengthNormalization::Shared {
                shared
            } else {
                mean
            };
            let component = |df, frequency: f64| {
                boost * idf(df) * frequency * 2.2 / (frequency + 1.2 * (0.25 + 0.75 * dl / avg))
            };
            let mut score = if !proximity || mask == 0 {
                (1..=words.min(3)).map(|df| component(df as f64, tf)).sum()
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
                for mixed in [false, true] {
                    let label = format!(
                        "ngram-{proximity}-{policy:?}-{fields}-{mask}-{mixed}-{}",
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
                    let schema=serde_json::from_value(if fields==1 {json!([{"field":"a","field_type":"Text","index_lexical":true,"store":false,"boost":1.0}])}else{json!([{"field":"a","field_type":"Text","index_lexical":true,"store":false,"boost":2.0},{"field":"b","field_type":"Text","index_lexical":true,"store":false,"boost":0.5}])}).unwrap();
                    // Constant per-field lengths keep legacy Shared committed means unchanged on append,
                    // isolating ngram scoring from the separate length-update policy.
                    // Literal physical DFs: the=1, of=2, and=3, anchor=1. Lengths <=7 decode exactly.
                    let docs = if fields == 1 {
                        vec![
                            json!({"a":"the of and anchor"}),
                            json!({"a":"of neutral neutral neutral"}),
                            json!({"a":"and neutral neutral neutral"}),
                            json!({"a":"and neutral neutral neutral"}),
                        ]
                    } else {
                        vec![
                            json!({"a":"the of and the of and anchor","b":"the of and anchor"}),
                            json!({"a":"of neutral neutral neutral neutral neutral neutral","b":"of neutral neutral neutral"}),
                            json!({"a":"and neutral neutral neutral neutral neutral neutral","b":"and neutral neutral neutral"}),
                            json!({"a":"and neutral neutral neutral neutral neutral neutral","b":"and neutral neutral neutral"}),
                        ]
                    };
                    let mut index = create_index(&path, meta, &schema, 11, true, Some(1))
                        .await
                        .unwrap();
                    if mixed {
                        add(&index, &docs[..1], 1).await;
                        index.commit().await;
                        add(&index, &docs[1..], 4).await;
                    } else {
                        add(&index, &docs, 4).await;
                    }
                    for stage in 0..3 {
                        if stage == 1 {
                            index.commit().await;
                        }
                        if stage == 2 {
                            index.close().await;
                            drop(index);
                            index = open_index(&path).await.unwrap();
                        }
                        for (words, q) in [
                            (2, "\"the of\""),
                            (3, "\"the of and\""),
                            (4, "\"the of and anchor\""),
                        ] {
                            for realtime in [false, true] {
                                for explicit in [false, true] {
                                    let r = index
                                        .search(
                                            q.into(),
                                            None,
                                            QueryType::Union,
                                            SearchMode::Lexical,
                                            false,
                                            0,
                                            100,
                                            ResultType::TopkCount,
                                            realtime,
                                            vec![],
                                            vec![],
                                            if explicit {
                                                vec![ResultSort {
                                                    field: "_score".into(),
                                                    order: SortOrder::Descending,
                                                }]
                                            } else {
                                                vec![]
                                            },
                                        )
                                        .await;
                                    assert!(r.error.is_none(), "{:?}", r.error);
                                    let visible = stage > 0 || mixed || realtime;
                                    assert_eq!(
                                        r.results.iter().map(|x| x.doc_id).collect::<Vec<_>>(),
                                        if visible { vec![0] } else { vec![] },
                                        "{label} stage={stage} q={q}"
                                    );
                                    assert_eq!(r.result_count_total, usize::from(visible));
                                    if visible {
                                        let want = expected(fields, policy, proximity, mask, words);
                                        let got = r.results[0].score as f64;
                                        checked += 1;
                                        if (got - want).abs() > 2e-5 * want.abs().max(1.0) {
                                            failures.push(format!("{label} stage={stage} rt={realtime} explicit={explicit} q={q} got={got} want={want}"));
                                        }
                                    }
                                }
                            }
                        }
                    }
                    index.close().await;
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
