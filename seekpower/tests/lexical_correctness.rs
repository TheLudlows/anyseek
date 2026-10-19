//! Independent posting membership and BM25 formula regressions for #32.
//! Explicit score sorting shares engine code: it is a consistency check, not an oracle.
use seekpower::{
    commit::Commit,
    index::{
        AccessType, Close, Clustering, DocumentCompression, FrequentwordType, IndexArc,
        IndexDocuments, IndexMetaObject, LexicalSimilarity, NgramSet, StopwordType, TokenizerType,
        create_index, open_index,
    },
    search::{QueryType, ResultSort, ResultType, Search, SearchMode, SortOrder},
    vector::Inference,
};
use serde_json::json;
use std::{collections::BTreeSet, path::PathBuf};

async fn fresh(label: &str) -> (IndexArc, PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "seekpower-correctness-{label}-{}",
        std::process::id()
    ));
    assert!(!path.exists());
    let schema = serde_json::from_value(json!([
        {"field":"title","field_type":"Text","index_lexical":true,"store":false},
        {"field":"text","field_type":"Text","index_lexical":true,"store":false}
    ]))
    .unwrap();
    let meta = IndexMetaObject {
        id: 0,
        name: label.into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: NgramSet::SingleTerm as u8,
        document_compression: DocumentCompression::Snappy,
        access_type: AccessType::Mmap,
        clustering: Clustering::None,
        inference: Inference::None,
    };
    (
        create_index(&path, meta, &schema, 11, true, Some(1))
            .await
            .unwrap(),
        path,
    )
}
async fn search(
    index: &IndexArc,
    text: &str,
    and: bool,
    k: usize,
    explicit: bool,
    realtime: bool,
) -> Vec<(usize, f32)> {
    let result = index
        .search(
            text.into(),
            None,
            if and {
                QueryType::Intersection
            } else {
                QueryType::Union
            },
            SearchMode::Lexical,
            false,
            0,
            k,
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
    assert!(result.error.is_none(), "{:?}", result.error);
    result.results.iter().map(|r| (r.doc_id, r.score)).collect()
}
async fn add(index: &IndexArc, start: usize, end: usize, doc: impl Fn(usize) -> serde_json::Value) {
    for begin in (start..end).step_by(256) {
        index
            .index_documents(
                (begin..(begin + 256).min(end))
                    .map(|i| serde_json::from_value(doc(i)).unwrap())
                    .collect(),
            )
            .await
            .unwrap();
    }
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while index.read().await.indexed_doc_count().await != end {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}
fn contribution(n: usize, df: usize, dl: usize, avg: f64) -> f64 {
    (1.0 + (n as f64 - df as f64 + 0.5) / (df as f64 + 0.5)).ln() * 2.2
        / (1.0 + 1.2 * (0.25 + 0.75 * dl as f64 / avg))
}
fn close_score(actual: f32, expected: f64) {
    assert!(
        (actual as f64 - expected).abs() <= expected.abs() * 2e-6 + 2e-6,
        "{actual} != {expected}"
    );
}
#[tokio::test]
async fn mixed_rle_array_bitmap_membership_scores_and_order() {
    let (mut index, path) = fresh("mixed").await;
    let sets = [
        (100..104).collect::<BTreeSet<_>>(),
        [100, 103, 200, 300, 400].into_iter().collect(),
        (1..8194).step_by(2).collect(),
    ];
    let terms = ["alpha", "beta", "gamma"];
    add(&index, 0, 8194, |i| {
        let words: Vec<_> = terms
            .iter()
            .zip(&sets)
            .filter(|(_, s)| s.contains(&i))
            .map(|(t, _)| *t)
            .collect();
        json!({"title":"","text":if words.is_empty(){"neutral".into()}else{words.join(" ")}})
    })
    .await;
    let union: BTreeSet<_> = sets.iter().flat_map(|s| s.iter().copied()).collect();
    let avg = (0..8194)
        .map(|i| sets.iter().filter(|s| s.contains(&i)).count().max(1))
        .sum::<usize>() as f64
        / 8194.0;
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
            let rows = search(
                &index,
                "alpha beta gamma",
                false,
                8195,
                explicit,
                stage == 0,
            )
            .await;
            assert_eq!(rows.iter().map(|r| r.0).collect::<BTreeSet<_>>(), union);
            for (id, score) in &rows {
                let dl = sets.iter().filter(|s| s.contains(id)).count();
                let expected = sets
                    .iter()
                    .filter(|s| s.contains(id))
                    .map(|s| contribution(8194, s.len(), dl, avg))
                    .sum();
                close_score(*score, expected);
            }
            assert_eq!(
                search(&index, "alpha beta gamma", true, 8195, explicit, stage == 0)
                    .await
                    .iter()
                    .map(|r| r.0)
                    .collect::<Vec<_>>(),
                vec![103]
            );
            for q in ["gamma alpha beta", "beta gamma alpha"] {
                assert_eq!(
                    rows,
                    search(&index, q, false, 8195, explicit, stage == 0).await,
                    "stage {stage}, query {q}"
                );
            }
        }
    }
    index.close().await;
    drop(index);
    std::fs::remove_dir_all(path).unwrap();
}
#[tokio::test]
async fn append_changes_representative_but_not_upper_bound_safety() {
    let (mut index, path) = fresh("bound").await;
    let doc = |i| match i {
        0 => json!({"title":"alpha","text":format!("alpha {}","neutral ".repeat(19))}),
        1 => json!({"title":"alpha neutral neutral","text":"alpha neutral neutral"}),
        65536 => json!({"title":"alpha","text":format!("alpha {}","neutral ".repeat(12))}),
        _ => json!({"title":"","text":"neutral ".repeat(if i<65536 {1}else{5})}),
    };
    add(&index, 0, 65536, doc).await;
    index.commit().await;
    add(&index, 65536, 73728, doc).await;
    index.commit().await;
    for reopened in [false, true] {
        if reopened {
            index.close().await;
            drop(index);
            index = open_index(&path).await.unwrap();
        }
        let avg = (65561 + 8191 * 5 + 14) as f64 / 73728.0;
        let all = search(&index, "alpha", false, 4, false, false).await;
        for &(id, score) in &all {
            let lens = match id {
                0 => [1, 20],
                1 => [3, 3],
                65536 => [1, 13],
                _ => panic!("unexpected doc {id}; results={all:?}; reopened={reopened}"),
            };
            close_score(
                score,
                lens.into_iter()
                    .map(|dl| contribution(73728, 3, dl, avg))
                    .sum(),
            );
        }
        assert_eq!(all[0].0, 1);
        for explicit in [false, true] {
            assert_eq!(
                search(&index, "alpha", false, 1, explicit, false).await,
                all[..1]
            );
        }
    }
    index.close().await;
    drop(index);
    std::fs::remove_dir_all(path).unwrap();
}
#[tokio::test]
async fn partial_heap_cannot_mask_later_two_term_winner() {
    let (index, path) = fresh("mask").await;
    let terms = [
        "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota",
    ];
    add(&index,0,65546,|i|json!({"title":"","text":if i==0 {terms.join(" ")}else if i<65536 {"neutral ".repeat(10)}else if i<65545 {terms[i-65536].into()}else{"theta iota".into()}})).await;
    index.commit().await;
    let q = terms.join(" ");
    let all = search(&index, &q, false, 65547, false, false).await;
    assert_eq!(all.len(), 11);
    for &(id, score) in &all {
        let present = if id == 0 {
            (0..9).collect::<Vec<_>>()
        } else if id < 65545 {
            vec![id - 65536]
        } else {
            vec![7, 8]
        };
        close_score(
            score,
            present
                .iter()
                .map(|&t| {
                    contribution(
                        65546,
                        if t >= 7 { 3 } else { 2 },
                        present.len(),
                        655370.0 / 65546.0,
                    )
                })
                .sum(),
        );
    }
    assert_eq!(
        all[..2].iter().map(|r| r.0).collect::<Vec<_>>(),
        vec![0, 65545]
    );
    for explicit in [false, true] {
        assert_eq!(
            search(&index, &q, false, 2, explicit, false).await,
            all[..2]
        );
    }
    index.close().await;
    drop(index);
    std::fs::remove_dir_all(path).unwrap();
}

/// Five terms force recursive OR subsets; literal posting sets and scores are
/// independent of both the subset enumerator and the explicit-sort reference.
#[tokio::test]
async fn five_term_or_subsets_keep_all_pair_contributions() {
    let (index, path) = fresh("subsets").await;
    let terms = ["alpha", "beta", "gamma", "delta", "epsilon"];
    let sets: Vec<BTreeSet<usize>> = vec![
        (100..104).collect(),
        [100, 103, 200, 300, 400].into_iter().collect(),
        (1..8194).step_by(2).collect(),
        (0..8194).step_by(3).collect(),
        (0..8194).step_by(7).collect(),
    ];
    add(&index, 0, 8194, |i| {
        let words: Vec<_> = terms
            .iter()
            .zip(&sets)
            .filter(|(_, s)| s.contains(&i))
            .map(|(t, _)| *t)
            .collect();
        json!({"title":"", "text":if words.is_empty() {"neutral".into()} else {words.join(" ")}})
    })
    .await;
    index.commit().await;
    let avg = (0..8194)
        .map(|i| sets.iter().filter(|s| s.contains(&i)).count().max(1))
        .sum::<usize>() as f64
        / 8194.0;
    let union: BTreeSet<_> = sets.iter().flat_map(|s| s.iter().copied()).collect();
    for explicit in [false, true] {
        let all = search(&index, &terms.join(" "), false, 8195, explicit, false).await;
        assert_eq!(all.iter().map(|r| r.0).collect::<BTreeSet<_>>(), union);
        for &(id, score) in &all {
            let dl = sets.iter().filter(|s| s.contains(&id)).count();
            close_score(
                score,
                sets.iter()
                    .filter(|s| s.contains(&id))
                    .map(|s| contribution(8194, s.len(), dl, avg))
                    .sum(),
            );
        }
        for k in [1, 10, 100] {
            assert_eq!(
                search(&index, &terms.join(" "), false, k, explicit, false).await,
                all[..k]
            );
        }
    }
    index.close().await;
    drop(index);
    std::fs::remove_dir_all(path).unwrap();
}
