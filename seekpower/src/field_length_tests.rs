//! Literal token/TF/DF oracles, independent of posting decoding and score helpers.
use crate::{
    commit::Commit,
    index::{
        AccessType, Close, Clustering, DeleteDocument, DocumentCompression,
        FieldLengthNormalization, FrequentwordType, IndexArc, IndexDocuments, IndexMetaObject,
        LexicalSimilarity, StopwordType, TextNormalization, TokenizerType, create_index,
        open_index,
    },
    search::{QueryType, ResultSort, ResultType, Search, SearchMode, SortOrder},
    vector::Inference,
};
use serde_json::{Value, json};
use std::{collections::BTreeSet, path::PathBuf};
async fn fresh(label: &str, shards: usize, ngrams: u8) -> (IndexArc, PathBuf) {
    fresh_policy(label, shards, ngrams, FieldLengthNormalization::PerField).await
}
async fn fresh_policy(
    label: &str,
    shards: usize,
    ngrams: u8,
    policy: FieldLengthNormalization,
) -> (IndexArc, PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "seekpower-field-length-{label}-{}",
        std::process::id()
    ));
    assert!(!path.exists());
    let meta = IndexMetaObject {
        id: 0,
        name: label.into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        text_normalization: TextNormalization::None,
        field_length_normalization: policy,
        stop_words: StopwordType::None,
        frequent_words: if ngrams == 0 {
            FrequentwordType::None
        } else {
            FrequentwordType::English
        },
        ngram_indexing: ngrams,
        document_compression: DocumentCompression::Snappy,
        access_type: AccessType::Mmap,
        clustering: Clustering::None,
        inference: Inference::None,
    };
    let schema=serde_json::from_value(json!([{"field":"title","field_type":"Text","store":true,"index_lexical":true,"boost":2.0},{"field":"body","field_type":"Text","store":true,"index_lexical":true,"boost":0.5}])).unwrap();
    (
        create_index(&path, meta, &schema, 11, true, Some(shards))
            .await
            .unwrap(),
        path,
    )
}
async fn add(index: &IndexArc, docs: &[Value], expected: usize) {
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
        while index.read().await.indexed_doc_count().await != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
fn tokens<'a>(d: &'a Value, field: &str) -> Vec<&'a str> {
    d.get(field)
        .and_then(Value::as_str)
        .unwrap_or("")
        .split_whitespace()
        .collect()
}
fn decoded(n: usize) -> usize {
    match n {
        0..=23 => n,
        100 => 96,
        _ => panic!("fixture length has no independent literal decode: {n}"),
    }
}
async fn assert_stats(index: &IndexArc, docs: &[Value]) {
    let root = index.read().await;
    let nshards = root.shard_vec.len();
    for (sid, arc) in root.shard_vec.iter().enumerate() {
        let shard = arc.read().await;
        for field in &shard.indexed_field_vec {
            let lens: Vec<_> = docs
                .iter()
                .enumerate()
                .filter(|(i, _)| i % nshards == sid)
                .map(|(_, d)| decoded(tokens(d, &field.schema_field_name).len()))
                .collect();
            assert_eq!(field.field_length_sum, lens.iter().sum::<usize>());
            assert_eq!(
                field.field_document_count,
                lens.iter().filter(|&&n| n > 0).count() as u64
            );
        }
    }
}
fn expected(
    docs: &[Value],
    deleted: &BTreeSet<usize>,
    q: &str,
    nshards: usize,
) -> Vec<(usize, f64)> {
    let positive: BTreeSet<_> = q
        .split_whitespace()
        .filter(|x| !x.starts_with('-'))
        .collect();
    let negative: Vec<_> = q
        .split_whitespace()
        .filter_map(|x| x.strip_prefix('-'))
        .collect();
    let mut out = vec![];
    for (id, doc) in docs.iter().enumerate() {
        if deleted.contains(&id) {
            continue;
        }
        let has = |term: &str| {
            ["title", "body"]
                .iter()
                .any(|f| tokens(doc, f).contains(&term))
        };
        if !positive.iter().any(|x| has(x)) || negative.iter().any(|x| has(x)) {
            continue;
        }
        let partition: Vec<_> = docs
            .iter()
            .enumerate()
            .filter(|(i, _)| i % nshards == id % nshards)
            .map(|(_, d)| d)
            .collect();
        let n = partition.len() as f64;
        let mut score = 0.0;
        for term in &positive {
            let df = partition
                .iter()
                .filter(|d| {
                    ["title", "body"]
                        .iter()
                        .any(|f| tokens(d, f).contains(term))
                })
                .count() as f64;
            if df == 0.0 {
                continue;
            }
            let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
            for (field, boost) in [("title", 2.0), ("body", 0.5)] {
                let ts = tokens(doc, field);
                let tf = ts.iter().filter(|t| *t == term).count() as f64;
                if tf == 0.0 {
                    continue;
                }
                let lens: Vec<_> = partition
                    .iter()
                    .map(|d| decoded(tokens(d, field).len()))
                    .filter(|&l| l > 0)
                    .collect();
                let avg = lens.iter().sum::<usize>() as f64 / lens.len() as f64;
                score += boost * idf * tf * 2.2
                    / (tf + 1.2 * (0.25 + 0.75 * decoded(ts.len()) as f64 / avg));
            }
        }
        out.push((id, score));
    }
    out
}
async fn check(
    index: &IndexArc,
    docs: &[Value],
    deleted: &BTreeSet<usize>,
    q: &str,
    nshards: usize,
) {
    let want = expected(docs, deleted, q, nshards);
    for explicit in [false, true] {
        let x = index
            .search(
                q.into(),
                None,
                QueryType::Union,
                SearchMode::Lexical,
                false,
                0,
                200,
                ResultType::TopkCount,
                true,
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
        assert!(x.error.is_none(), "{:?}", x.error);
        assert_eq!(
            x.results.iter().map(|r| r.doc_id).collect::<BTreeSet<_>>(),
            want.iter().map(|r| r.0).collect(),
            "query={q}"
        );
        assert_eq!(x.result_count_total, want.len());
        for row in x.results {
            let e = want.iter().find(|r| r.0 == row.doc_id).unwrap().1;
            assert!(
                (row.score as f64 - e).abs() < 1e-5 * e.abs().max(1.0),
                "q={q} id={} actual={} expected={e}",
                row.doc_id,
                row.score
            );
        }
    }
}
#[tokio::test]
async fn field_lengths_empty_append_delete_clear_and_reopen() {
    let (mut index, path) = fresh("states", 1, 0).await;
    let mut docs = vec![
        json!({"title":"alpha alpha","body":"alpha beta beta beta"}),
        json!({"title":"","body":"beta"}),
        json!({"title":"gamma"}),
        json!({"body":""}),
    ];
    let mut deleted = BTreeSet::new();
    add(&index, &docs, 4).await;
    for stage in 0..6 {
        if stage == 1 || stage == 3 {
            index.commit().await;
        }
        if stage == 2 {
            docs.push(json!({"title":"alpha","body":"gamma gamma"}));
            add(&index, &docs[4..], 5).await;
        }
        if stage == 4 {
            index.close().await;
            drop(index);
            index = open_index(&path).await.unwrap();
        }
        if stage == 5 {
            index.delete_document(0).await;
            index.delete_document(0).await;
            index.delete_document(9000).await;
            deleted.insert(0);
        }
        assert_stats(&index, &docs).await;
        for q in [
            "alpha",
            "beta",
            "gamma",
            "alpha beta",
            "gamma -beta",
            "alpha beta -gamma",
        ] {
            check(&index, &docs, &deleted, q, 1).await;
        }
    }
    index.close().await;
    drop(index);
    index = open_index(&path).await.unwrap();
    assert_stats(&index, &docs).await;
    check(&index, &docs, &deleted, "alpha beta", 1).await;
    index.write().await.clear_index().await;
    assert_stats(&index, &[]).await;
    let reset = vec![json!({"title":"alpha","body":"beta"})];
    add(&index, &reset, 1).await;
    index.commit().await;
    index.close().await;
    drop(index);
    index = open_index(&path).await.unwrap();
    assert_stats(&index, &reset).await;
    check(&index, &reset, &BTreeSet::new(), "alpha beta", 1).await;
    index.close().await;
    drop(index);
    std::fs::remove_dir_all(path).unwrap();
}
#[tokio::test]
async fn field_lengths_are_shard_local_and_global_df_is_not_per_field() {
    let (mut index, path) = fresh("shards", 2, 0).await;
    let docs = vec![
        json!({"title":"alpha alpha","body":"alpha"}),
        json!({"title":"alpha","body":"beta beta beta"}),
        json!({"body":"beta"}),
        json!({"title":"gamma gamma","body":"alpha beta"}),
        json!({}),
        json!({"body":"gamma"}),
    ];
    add(&index, &docs, docs.len()).await;
    for stage in 0..3 {
        if stage == 1 {
            index.commit().await;
        }
        if stage == 2 {
            index.close().await;
            drop(index);
            index = open_index(&path).await.unwrap();
        }
        assert_stats(&index, &docs).await;
        for q in ["alpha", "beta", "gamma", "alpha beta", "alpha beta -gamma"] {
            check(&index, &docs, &BTreeSet::new(), q, 2).await;
        }
    }
    index.close().await;
    drop(index);
    std::fs::remove_dir_all(path).unwrap();
}
#[tokio::test]
async fn field_lengths_hundred_terms_and_decoded_length_literal() {
    let (mut index, path) = fresh("hundred", 1, 0).await;
    let terms = (0..100)
        .map(|i| format!("t{i:03}"))
        .collect::<Vec<_>>()
        .join(" ");
    let docs = vec![json!({"title":terms}), json!({"title":"t000"})];
    add(&index, &docs, 2).await;
    for stage in 0..3 {
        if stage == 1 {
            index.commit().await;
        }
        if stage == 2 {
            index.close().await;
            drop(index);
            index = open_index(&path).await.unwrap();
        }
        assert_stats(&index, &docs).await;
        check(&index, &docs, &BTreeSet::new(), &terms, 1).await;
    }
    index.close().await;
    drop(index);
    std::fs::remove_dir_all(path).unwrap();
}
#[tokio::test]
async fn field_lengths_ngram_components_use_same_denominators() {
    for mask in [0, 9, 127] {
        let (mut index, path) = fresh(&format!("ngrams-{mask}"), 1, mask).await;
        let docs = vec![
            json!({"title":"the of","body":"the of neutral neutral"}),
            json!({"title":"and to","body":"and to"}),
            json!({}),
        ];
        add(&index, &docs, 3).await;
        for stage in 0..3 {
            if stage == 1 {
                index.commit().await;
            }
            if stage == 2 {
                index.close().await;
                drop(index);
                index = open_index(&path).await.unwrap();
            }
            assert_stats(&index, &docs).await;
            check(&index, &docs, &BTreeSet::new(), "the of", 1).await;
        }
        index.close().await;
        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }
}

#[tokio::test]
async fn mixed_global_df_uses_all_physical_documents_without_changing_visibility() {
    for policy in [
        FieldLengthNormalization::Shared,
        FieldLengthNormalization::PerField,
    ] {
        let (mut index, path) = fresh_policy(&format!("mixed-df-{policy:?}"), 1, 0, policy).await;
        add(
            &index,
            &[
                json!({"title":"alpha","body":"alpha"}),
                json!({"title":"beta","body":"beta"}),
            ],
            2,
        )
        .await;
        index.commit().await;
        add(&index, &[json!({"title":"alpha","body":"alpha"})], 3).await;
        let avg = if policy == FieldLengthNormalization::Shared {
            2.0
        } else {
            1.0
        };
        let expected = 2.5 * 1.6_f64.ln() * 2.2 / (1.0 + 1.2 * (0.25 + 0.75 / avg));
        for stage in 0..3 {
            if stage == 1 {
                index.commit().await;
            }
            if stage == 2 {
                index.close().await;
                drop(index);
                index = open_index(&path).await.unwrap();
            }
            for rt in [false, true] {
                let result = index
                    .search(
                        "alpha".into(),
                        None,
                        QueryType::Union,
                        SearchMode::Lexical,
                        false,
                        0,
                        10,
                        ResultType::TopkCount,
                        rt,
                        vec![],
                        vec![],
                        vec![],
                    )
                    .await;
                let want = if stage == 0 && !rt {
                    vec![0]
                } else {
                    vec![0, 2]
                };
                assert_eq!(
                    result.results.iter().map(|x| x.doc_id).collect::<Vec<_>>(),
                    want
                );
                for row in &result.results {
                    assert!(
                        (row.score as f64 - expected).abs() < 2e-6,
                        "stage={stage},rt={rt},policy={policy:?},id={},score={},expected={expected}",
                        row.doc_id,
                        row.score
                    );
                }
            }
        }
        index.close().await;
        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }
}
