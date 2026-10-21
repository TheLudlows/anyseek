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
                    let schema=serde_json::from_value(if fields==1 {json!([{"field":"a","field_type":"Text","index_lexical":true,"store":false,"boost":1.0}])}else{json!([{"field":"a","field_type":"Text","index_lexical":true,"store":false,"boost":2.0},{"field":"b","field_type":"Text","index_lexical":true,"store":false,"boost":0.5}])}).unwrap();
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
                    let mut index = create_index(&path, meta, &schema, 11, true, Some(1))
                        .await
                        .unwrap();
                    if mixed {
                        add(&index, &docs[..1], 1).await;
                        index.commit().await.unwrap();
                        add(&index, &docs[1..], docs.len()).await;
                    } else {
                        add(&index, &docs, docs.len()).await;
                    }
                    for stage in 0..3 {
                        if stage == 1 {
                            index.commit().await.unwrap();
                        }
                        if stage == 2 {
                            index.close().await.unwrap();
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
                                    let want_ids = if !visible {
                                        vec![]
                                    } else if extra_match && (stage > 0 || realtime) {
                                        vec![0, 4]
                                    } else {
                                        vec![0]
                                    };
                                    assert_eq!(
                                        r.results.iter().map(|x| x.doc_id).collect::<Vec<_>>(),
                                        want_ids,
                                        "{label} stage={stage} q={q}"
                                    );
                                    assert_eq!(r.result_count_total, want_ids.len());
                                    for hit in r.results {
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
                    index.close().await.unwrap();
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
        let schema = serde_json::from_value(
            json!([{"field":"a","field_type":"Text","index_lexical":true,"store":false}]),
        )
        .unwrap();
        let mut index = create_index(&path, meta, &schema, 11, true, Some(1))
            .await
            .unwrap();
        let mut docs = vec![json!({"a":"of neutral"}); 100];
        docs[0] = json!({"a":"the of"});
        add(&index, &docs, 100).await;
        index.commit().await.unwrap();
        for stage in 0..6 {
            if stage == 1 {
                add(&index, &vec![json!({"a":"of neutral"}); 4], 104).await;
            }
            if stage == 2 || stage == 5 {
                index.commit().await.unwrap();
            }
            if stage == 3 {
                index.close().await.unwrap();
                drop(index);
                index = open_index(&path).await.unwrap();
            }
            if stage == 4 {
                add(&index, &[json!({"a":"the of"})], 105).await;
            }
            // Literal codec boundaries:100 ->96;104/105 ->104. Realtime counts
            // are added without quantization, preserving the previous convention.
            let n = if stage == 0 {
                100.0
            } else if stage < 4 {
                104.0
            } else {
                105.0
            };
            let df_the = if stage < 4 { 1.0 } else { 2.0 };
            let df_of = match stage {
                0 => 96.0,
                1 => 100.0,
                4 => 105.0,
                _ => 104.0,
            };
            let expected = ((n - df_the + 0.5) / (df_the + 0.5) + 1.0_f64).ln()
                + ((n - df_of + 0.5) / (df_of + 0.5) + 1.0_f64).ln();
            for realtime in [false, true] {
                let r = index
                    .search(
                        "\"the of\"".into(),
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
                        vec![],
                    )
                    .await;
                let want = if stage == 5 || (stage == 4 && realtime) {
                    vec![0, 104]
                } else {
                    vec![0]
                };
                assert_eq!(r.results.iter().map(|x| x.doc_id).collect::<Vec<_>>(), want);
                assert_eq!(r.result_count_total, want.len());
                for hit in r.results {
                    assert!(
                        (hit.score as f64 - expected).abs() < 2e-5,
                        "stage={stage} got={} expected={expected}",
                        hit.score
                    );
                }
            }
        }
        index.close().await.unwrap();
        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }
}
