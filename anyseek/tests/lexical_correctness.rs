// Legacy physical block dimensions are reduced; literal score/membership oracles remain.
use anyseek::record::RecordSearch;
#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
// Independent posting membership and BM25 formula regressions for #32.
// Explicit score sorting shares engine code: it is a consistency check, not an oracle.
use anyseek::{
    index::{
        AccessType, Clustering, DocumentCompression, FrequentwordType, IndexMetaObject, LexicalSimilarity, NgramSet, StopwordType, TokenizerType,
        },
    search::{QueryType, ResultSort, ResultType, SearchMode, SortOrder},
    vector::Inference,
};
use serde_json::json;
use std::{collections::BTreeSet, path::PathBuf};

async fn fresh(
    label: &str,
    policy: anyseek::index::FieldLengthNormalization,
) -> (RecordPartition, PathBuf) {
    let path = std::env::temp_dir().join(format!(
        "anyseek-correctness-{label}-{}",
        std::process::id()
    ));
    assert!(!path.exists());
    let schema: Vec<anyseek::index::SchemaField> = serde_json::from_value(json!([
        {"field":"title","field_type":"Text","index_lexical":true,"store":false},
        {"field":"text","field_type":"Text","index_lexical":true,"store":false}
    ]))
    .unwrap();
    let meta = IndexMetaObject {
        id: 0,
        name: label.into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: policy,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: NgramSet::SingleTerm as u8,
        document_compression: DocumentCompression::Snappy,
        access_type: AccessType::Mmap,
        clustering: Clustering::None,
        inference: Inference::None,
    };
    (
        Ok::<_, String>(records::create(&path, meta, (&schema).to_vec()).await)
            .unwrap(),
        path,
    )
}
async fn search(
    index: &RecordPartition,
    text: &str,
    and: bool,
    k: usize,
    explicit: bool,
    realtime: bool,
) -> Vec<(usize, f32)> {
    let result = index.query(RecordQuery {text:text.into(),vector:None,query_type:if and {
                QueryType::Intersection
            } else {
                QueryType::Union
            },mode:SearchMode::Lexical,offset:0,length:k,result_type:ResultType::Topk,fields:vec![],filters:vec![],sort:if explicit {
                vec![ResultSort {
                    field: "_score".into(),
                    order: SortOrder::Descending,
                }]
            } else {
                vec![]
            },temporal:None}).await.unwrap();
    result.hits.iter().map(|r| (r.record.record_id.parse::<usize>().unwrap(), r.score)).collect()
}
async fn add(index: &RecordPartition, start: usize, end: usize, doc: impl Fn(usize) -> serde_json::Value) {
    for begin in (start..end).step_by(256) {
        records::insert(&index, (begin..(begin + 256).min(end))
                    .map(|i| serde_json::from_value(doc(i)).unwrap())
                    .collect())
            .await
            .unwrap();
    }
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while index.statistics().await.unwrap().physical_versions != end {
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
    for policy in [
        anyseek::index::FieldLengthNormalization::Shared,
        anyseek::index::FieldLengthNormalization::PerField,
    ] {
        let (mut index, path) = fresh("mixed", policy).await;
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
                index.compact_journal().await.unwrap();
            }
            if stage == 2 {

                drop(index);
                index = RecordPartition::open(&path, records::JOURNAL_LIMIT).await.unwrap();
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

        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }
}
#[tokio::test]
async fn append_changes_representative_but_not_upper_bound_safety() {
    for policy in [
        anyseek::index::FieldLengthNormalization::Shared,
        anyseek::index::FieldLengthNormalization::PerField,
    ] {
        let (mut index, path) = fresh("bound", policy).await;
        let doc = |i| match i {
            0 => json!({"title":"alpha","text":format!("alpha {}","neutral ".repeat(19))}),
            1 => json!({"title":"alpha neutral neutral","text":"alpha neutral neutral"}),
            512 => json!({"title":"alpha","text":format!("alpha {}","neutral ".repeat(12))}),
            _ => json!({"title":"","text":"neutral ".repeat(if i<512 {1}else{5})}),
        };
        add(&index, 0, 512, doc).await;
        index.compact_journal().await.unwrap();
        add(&index, 512, 576, doc).await;
        index.compact_journal().await.unwrap();
        for reopened in [false, true] {
            if reopened {

                drop(index);
                index = RecordPartition::open(&path, records::JOURNAL_LIMIT).await.unwrap();
            }
            let avg = (537 + 63 * 5 + 14) as f64 / 576.0;
            let all = search(&index, "alpha", false, 4, false, false).await;
            assert_eq!(
                all.iter().map(|r| r.0).collect::<BTreeSet<_>>(),
                [0, 1, 512].into_iter().collect(),
                "complete append fixture membership"
            );
            for &(id, score) in &all {
                let lens = match id {
                    0 => [1, 20],
                    1 => [3, 3],
                    512 => [1, 13],
                    _ => panic!("unexpected doc {id}; results={all:?}; reopened={reopened}"),
                };
                close_score(
                    score,
                    lens.into_iter()
                        .enumerate()
                        .map(|(field, dl)| {
                            contribution(
                                576,
                                3,
                                dl,
                                if policy == anyseek::index::FieldLengthNormalization::Shared {
                                    avg
                                } else if field == 0 {
                                    5.0 / 3.0
                                } else {
                                    (537 + 63 * 5 + 14 - 5) as f64 / 576.0
                                },
                            )
                        })
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

        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }
}
#[tokio::test]
async fn partial_heap_cannot_mask_later_two_term_winner() {
    for policy in [
        anyseek::index::FieldLengthNormalization::Shared,
        anyseek::index::FieldLengthNormalization::PerField,
    ] {
        let (index, path) = fresh("mask", policy).await;
        let terms = [
            "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota",
        ];
        add(&index,0,522,|i|json!({"title":"","text":if i==0 {terms.join(" ")}else if i<512 {"neutral ".repeat(10)}else if i<521 {terms[i-512].into()}else{"theta iota".into()}})).await;
        index.compact_journal().await.unwrap();
        let q = terms.join(" ");
        let all = search(&index, &q, false, 523, false, false).await;
        assert_eq!(all.len(), 11);
        for &(id, score) in &all {
            let present = if id == 0 {
                (0..9).collect::<Vec<_>>()
            } else if id < 521 {
                vec![id - 512]
            } else {
                vec![7, 8]
            };
            close_score(
                score,
                present
                    .iter()
                    .map(|&t| {
                        contribution(
                            522,
                            if t >= 7 { 3 } else { 2 },
                            present.len(),
                            5130.0 / 522.0,
                        )
                    })
                    .sum(),
            );
        }
        assert_eq!(
            all[..2].iter().map(|r| r.0).collect::<Vec<_>>(),
            vec![0, 521]
        );
        for explicit in [false, true] {
            assert_eq!(
                search(&index, &q, false, 2, explicit, false).await,
                all[..2]
            );
        }

        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }
}

/// Five terms force recursive OR subsets; literal posting sets and scores are
/// independent of both the subset enumerator and the explicit-sort reference.
#[tokio::test]
async fn five_term_or_subsets_keep_all_pair_contributions() {
    for policy in [
        anyseek::index::FieldLengthNormalization::Shared,
        anyseek::index::FieldLengthNormalization::PerField,
    ] {
        let (index, path) = fresh("subsets", policy).await;
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
        index.compact_journal().await.unwrap();
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

        drop(index);
        std::fs::remove_dir_all(path).unwrap();
    }
}
