//! Versioned analysis must preserve source text and use one rule in every state.
use anyseek::{
    index::{
        Clustering, FrequentwordType,
         IndexMetaObject, LexicalSimilarity, StopwordType, TextNormalization,
        TokenizerType,
    },
    search::{QueryType, ResultSort, ResultType, SearchMode, SortOrder},
    vector::Inference,
};
#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
use records::JOURNAL_LIMIT;
use serde_json::json;
use std::{
    collections::BTreeSet,
    path::PathBuf,
};

async fn fresh(label: &str, rule: TextNormalization, ngrams: u8) -> (RecordPartition, PathBuf) {
    let path = std::env::temp_dir().join(format!("anyseek-t2s-{label}-{}", std::process::id()));
    assert!(!path.exists());
    let meta = IndexMetaObject {
        id: 0,
        name: label.into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: if label.starts_with("simple-") {
            TokenizerType::TantivySimpleV1
        } else {
            TokenizerType::default()
        },
        text_normalization: rule,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::English,
        ngram_indexing: ngrams,
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
async fn rows(index: &RecordPartition, q: &str, realtime: bool) -> Vec<(usize, f32)> {
    rows_sorted(index, q, realtime, false).await
}
async fn rows_sorted(
    index: &RecordPartition,
    q: &str,
    _realtime: bool,
    explicit: bool,
) -> Vec<(usize, f32)> {
    let r = index
        .query(RecordQuery {
            text: q.into(), vector: None, temporal: None, query_type: QueryType::Union, mode: SearchMode::Lexical, offset: 0, length: 100, result_type:
            ResultType::Topk,
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

    r.hits.iter().map(|r| (r.record.record_id.parse::<usize>().unwrap(), r.score)).collect()
}
async fn ids(index: &RecordPartition, q: &str, realtime: bool) -> BTreeSet<usize> {
    rows(index, q, realtime)
        .await
        .into_iter()
        .map(|r| r.0)
        .collect()
}
#[tokio::test]
async fn t2s_phrases_original_utf8_ngram_and_reopen() {
    let docs = [
        "乾隆 皇后 頭髮 臺灣 OpenAI2026 café 😀",
        "乾燥 皇後 頭發 台湾 OPENAI2026",
        "軟體 滑鼠 記憶體",
        "软件 鼠标 内存",
        "二噁英 𠮷 東京",
        "臺灣\0頭髮",
        "the of and 臺灣 頭髮",
        "the of and 台湾 头发",
    ];
    for mask in [0, 9, 127] {
        let (mut index, path) = fresh(
            &format!("states-{mask}"),
            TextNormalization::OpenCcT2s1_1_9,
            mask,
        ).await;
        add(&index, &docs).await;
        for stage in 0..3 {
            if stage == 1 {
                index.compact_journal().await.unwrap();
            }
            if stage == 2 {

                drop(index);
                index = RecordPartition::open(&path, JOURNAL_LIMIT).await.unwrap();
            }
            let rt = stage == 0;
            assert_eq!(
                index.configuration().await.unwrap().index.text_normalization,
                TextNormalization::OpenCcT2s1_1_9
            );
            for i in 0..docs.len() {
                let doc = index.get(&format!("{:020}", i)).await.unwrap().unwrap().document.unwrap();
                assert_eq!(doc.len(), 1);
                assert_eq!(doc["body"], docs[i]);
            }
            for (traditional, simplified) in [
                ("乾隆", "乾隆"),
                ("皇後", "皇后"),
                ("頭髮", "头发"),
                ("臺灣", "台湾"),
                ("二噁英", "二𫫇英"),
                ("\"the of and 臺灣\"", "\"the of and 台湾\""),
                ("+臺灣 +頭髮", "+台湾 +头发"),
                ("臺灣 -乾燥", "台湾 -干燥"),
                ("\"乾隆 皇后\"", "\"乾隆 皇后\""),
                ("臺灣\0頭髮", "台湾\0头发"),
            ] {
                assert_eq!(
                    rows(&index, traditional, rt).await,
                    rows(&index, simplified, rt).await,
                    "{traditional}, stage={stage}, mask={mask}"
                );
            }
            assert_eq!(ids(&index, "乾隆", rt).await, [0].into_iter().collect());
            assert_eq!(
                ids(&index, "OpenAI2026", rt).await,
                [0, 1].into_iter().collect()
            );
            assert_eq!(ids(&index, "軟體", rt).await, [2].into_iter().collect());
            assert_eq!(ids(&index, "软件", rt).await, [3].into_iter().collect());
            assert_eq!(
                ids(&index, "\"the of and 臺灣\"", rt).await,
                [6, 7].into_iter().collect()
            );
        }

        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }
}
#[tokio::test]
async fn none_default_and_persisted_rule_are_explicit() {
    let (index, path) = fresh("default", TextNormalization::None, 0).await;
    add(&index, &["臺灣", "台湾"]).await;
    index.compact_journal().await.unwrap();
    assert_ne!(
        ids(&index, "臺灣", false).await,
        ids(&index, "台湾", false).await
    );
    let mut value = serde_json::to_value(index.configuration().await.unwrap().index.clone()).unwrap();
    value.as_object_mut().unwrap().remove("text_normalization");
    assert_eq!(
        serde_json::from_value::<IndexMetaObject>(value.clone())
            .unwrap()
            .text_normalization,
        TextNormalization::None
    );
    value["text_normalization"] = json!("OpenCcLatest");
    assert!(serde_json::from_value::<IndexMetaObject>(value).is_err());

    drop(index);
    std::fs::remove_dir_all(path).unwrap();
}
#[tokio::test]
async fn not_filter_has_independent_membership_and_tf_oracle() {
    // Known upstream #37 and Chinese operator issue noted in #28. Keep this
    // acceptance gate visible: shared-reference agreement cannot certify NOT.
    let terms = [
        "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota",
    ];
    let mut failures = Vec::new();
    #[cfg(not(feature = "zh"))]
    let profiles = ["simple"];
    #[cfg(feature = "zh")]
    let profiles = ["simple", "zh"];
    for tokenizer in profiles {
        for rule in [TextNormalization::None, TextNormalization::OpenCcT2s1_1_9] {
            for count in [2, 9] {
                for negative in ["banned", "禁用"] {
                    let (mut index, path) = fresh(
                        &format!("{tokenizer}-not-{rule:?}-{count}-{negative}"),
                        rule,
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
                            let result = rows_sorted(
                                &index,
                                &format!("{second} -{negative}"),
                                stage == 0,
                                explicit,
                            )
                            .await;
                            let ids: Vec<_> = result.iter().map(|r| r.0).collect();
                            let average = count as f64 + 1.5;
                            let expected = count as f64 * (1.0_f64 + 0.5 / 2.5).ln() * 2.2
                                / (1.0 + 1.2 * (0.25 + 0.75 * count as f64 / average));
                            let membership = ids == vec![1];
                            // The Latin fixture's literal token lengths are independent of a CJK
                            // segmenter; the Chinese fixture separately gates NOT membership.
                            let score_ok = !negative.is_ascii()
                                || (membership && (result[0].1 as f64 - expected).abs() < 2e-5);
                            let record = json!({"tokenizer":tokenizer,"rule":format!("{rule:?}"),"positive_terms":count,"negative":negative,"stage":stage,"explicit_score":explicit,"result":result,"membership_ok":membership,"score_ok":score_ok,"expected_latin_score":expected});
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

#[tokio::test]
async fn nul_suffix_alone_has_independent_hits_in_every_state() {
    #[cfg(not(feature = "zh"))]
    let profiles = ["simple"];
    #[cfg(feature = "zh")]
    let profiles = ["simple", "zh"];
    for tokenizer in profiles {
        for mask in [0, 9, 127] {
            let (mut index, path) = fresh(
                &format!("{tokenizer}-nul-{mask}"),
                TextNormalization::OpenCcT2s1_1_9,
                mask,
            )
            .await;
            let mut docs = vec!["臺灣\0頭髮", "臺灣"];
            add(&index, &docs).await;
            for stage in 0..5 {
                if stage == 1 || stage == 3 {
                    index.compact_journal().await.unwrap();
                }
                if stage == 2 {
                    docs.push("臺灣\0後綴");
                    records::append(&index, vec![serde_json::from_value(json!({"body":docs[2]})).unwrap()]).await;
                }
                if stage == 4 {

                    drop(index);
                    index = RecordPartition::open(&path, JOURNAL_LIMIT).await.unwrap();
                }
                for rt in [false, true] {
                    let want: BTreeSet<usize> = if false {
                        BTreeSet::new()
                    } else {
                        [0].into_iter().collect()
                    };
                    // Neither query contains NUL or the shared prefix: truncating both
                    // query/document sides cannot make this independent assertion pass.
                    assert_eq!(
                        ids(&index, "头发", rt).await,
                        want,
                        "stage={stage} rt={rt} mask={mask}"
                    );
                    let want: BTreeSet<usize> = if stage >= 2 {
                        [2].into_iter().collect()
                    } else {
                        BTreeSet::new()
                    };
                    assert_eq!(
                        ids(&index, "后缀", rt).await,
                        want,
                        "stage={stage} rt={rt} mask={mask}"
                    );
                }
                for (i, original) in docs.iter().enumerate() {
                    let stored = index.get(&format!("{:020}", i)).await.unwrap().unwrap().document.unwrap();
                    assert_eq!(
                        stored["body"].as_str().unwrap().as_bytes(),
                        original.as_bytes()
                    );
                }
            }

            drop(index);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
}
