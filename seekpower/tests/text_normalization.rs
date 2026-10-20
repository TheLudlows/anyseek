//! Versioned analysis must preserve source text and use one rule in every state.
use seekpower::{
    commit::Commit,
    index::{
        AccessType, Close, Clustering, DocumentCompression, FrequentwordType, IndexArc,
        IndexDocuments, IndexMetaObject, LexicalSimilarity, StopwordType, TextNormalization,
        TokenizerType, create_index, open_index,
    },
    search::{QueryType, ResultSort, ResultType, Search, SearchMode, SortOrder},
    vector::Inference,
};
use serde_json::json;
use std::{
    collections::{BTreeSet, HashSet},
    path::PathBuf,
};

async fn fresh(label: &str, rule: TextNormalization, ngrams: u8) -> (IndexArc, PathBuf) {
    let path = std::env::temp_dir().join(format!("seekpower-t2s-{label}-{}", std::process::id()));
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
        text_normalization: rule,
        field_length_normalization: seekpower::index::FieldLengthNormalization::Shared,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::English,
        ngram_indexing: ngrams,
        document_compression: DocumentCompression::Snappy,
        access_type: AccessType::Mmap,
        clustering: Clustering::None,
        inference: Inference::None,
    };
    let schema = serde_json::from_value(
        json!([{"field":"body","field_type":"Text","store":true,"index_lexical":true}]),
    )
    .unwrap();
    (
        create_index(&path, meta, &schema, 11, true, Some(1))
            .await
            .unwrap(),
        path,
    )
}
async fn add(index: &IndexArc, docs: &[&str]) {
    index
        .index_documents(
            docs.iter()
                .map(|body| serde_json::from_value(json!({"body":body})).unwrap())
                .collect(),
        )
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while index.read().await.indexed_doc_count().await != docs.len() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
async fn rows(index: &IndexArc, q: &str, realtime: bool) -> Vec<(usize, f32)> {
    rows_sorted(index, q, realtime, false).await
}
async fn rows_sorted(
    index: &IndexArc,
    q: &str,
    realtime: bool,
    explicit: bool,
) -> Vec<(usize, f32)> {
    let r = index
        .search(
            q.into(),
            None,
            QueryType::Union,
            SearchMode::Lexical,
            false,
            0,
            100,
            ResultType::Topk,
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
    r.results.iter().map(|r| (r.doc_id, r.score)).collect()
}
async fn ids(index: &IndexArc, q: &str, realtime: bool) -> BTreeSet<usize> {
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
        )
        .await;
        add(&index, &docs).await;
        for stage in 0..3 {
            if stage == 1 {
                index.commit().await;
            }
            if stage == 2 {
                index.close().await;
                drop(index);
                index = open_index(&path).await.unwrap();
            }
            let rt = stage == 0;
            assert_eq!(
                index.read().await.meta.text_normalization,
                TextNormalization::OpenCcT2s1_1_9
            );
            for i in 0..docs.len() {
                let doc = index
                    .read()
                    .await
                    .get_document(i, rt, &HashSet::new())
                    .await
                    .unwrap();
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
        index.close().await;
        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }
}
#[tokio::test]
async fn none_default_and_persisted_rule_are_explicit() {
    let (index, path) = fresh("default", TextNormalization::None, 0).await;
    add(&index, &["臺灣", "台湾"]).await;
    index.commit().await;
    assert_ne!(
        ids(&index, "臺灣", false).await,
        ids(&index, "台湾", false).await
    );
    let mut value = serde_json::to_value(index.read().await.meta.clone()).unwrap();
    value.as_object_mut().unwrap().remove("text_normalization");
    assert_eq!(
        serde_json::from_value::<IndexMetaObject>(value.clone())
            .unwrap()
            .text_normalization,
        TextNormalization::None
    );
    value["text_normalization"] = json!("OpenCcLatest");
    assert!(serde_json::from_value::<IndexMetaObject>(value).is_err());
    index.close().await;
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
    for tokenizer in ["unicode", "zh"] {
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
                            index.commit().await;
                        }
                        if stage == 2 {
                            index.close().await;
                            drop(index);
                            index = open_index(&path).await.unwrap();
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
                    index.close().await;
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
