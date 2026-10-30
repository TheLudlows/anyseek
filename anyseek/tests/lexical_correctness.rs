#[path = "common/scale.rs"]
mod scale;
// Retained literal membership/score corpora; both length policies run in full.
#[path = "common/records.rs"]
mod records;
use anyseek::record::{RecordPartition, RecordQuery};
// Literal document membership and an independent BM25 formula are the oracles.
// Explicit score sorting shares engine code: it is a consistency check, not an oracle.
use anyseek::{
    index::{
        AccessType, Clustering, DocumentCompression, FieldLengthNormalization, FrequentwordType,
        IndexMetaObject, LexicalSimilarity, NgramSet, StopwordType, TokenizerType,
    },
    search::{QueryType, ResultSort, ResultType, SearchMode, SortOrder},
    vector::Inference,
};
use serde_json::json;
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    time::Instant,
};

struct Progress {
    label: String,
    started: Instant,
}
impl Progress {
    fn new(case: &str, policy: FieldLengthNormalization) -> Self {
        let progress = Self {
            label: format!("{case}/{policy:?}"),
            started: Instant::now(),
        };
        progress.log("fixture start");
        progress
    }
    fn log(&self, phase: impl std::fmt::Display) {
        eprintln!(
            "[lexical-progress] {} elapsed={:.3}s {phase}",
            self.label,
            self.started.elapsed().as_secs_f64()
        );
    }
}

async fn checkpoint(index: &RecordPartition) {
    let started = Instant::now();
    eprintln!("[lexical-progress] checkpoint begin");
    index.compact_journal().await.unwrap();
    eprintln!(
        "[lexical-progress] checkpoint complete elapsed={:.3}s",
        started.elapsed().as_secs_f64()
    );
}

async fn reopen(path: &Path) -> RecordPartition {
    let started = Instant::now();
    eprintln!("[lexical-progress] reopen begin");
    let index = RecordPartition::open(path, records::JOURNAL_LIMIT)
        .await
        .unwrap();
    eprintln!(
        "[lexical-progress] reopen complete elapsed={:.3}s",
        started.elapsed().as_secs_f64()
    );
    index
}

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
        tokenizer: TokenizerType::TantivySimpleV1,
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
    (records::create(&path, meta, schema).await, path)
}
async fn search(
    index: &RecordPartition,
    text: &str,
    and: bool,
    k: usize,
    explicit: bool,
) -> Vec<(usize, f32)> {
    let started = Instant::now();
    eprintln!("[lexical-progress] query begin text={text:?} and={and} k={k} explicit={explicit}");
    let result = index
        .query(RecordQuery {
            text: text.into(),
            vector: None,
            query_type: if and {
                QueryType::Intersection
            } else {
                QueryType::Union
            },
            mode: SearchMode::Lexical,
            offset: 0,
            length: k,
            result_type: ResultType::Topk,
            fields: vec![],
            filters: vec![],
            sort: if explicit {
                vec![ResultSort {
                    field: "_score".into(),
                    order: SortOrder::Descending,
                }]
            } else {
                vec![]
            },
            temporal: None,
        })
        .await
        .unwrap();
    eprintln!(
        "[lexical-progress] query complete hits={} elapsed={:.3}s",
        result.hits.len(),
        started.elapsed().as_secs_f64()
    );
    result
        .hits
        .iter()
        .map(|r| (r.record.record_id.parse::<usize>().unwrap(), r.score))
        .collect()
}
async fn add(
    index: &RecordPartition,
    start: usize,
    end: usize,
    progress: &Progress,
    doc: impl Fn(usize) -> serde_json::Value,
) {
    for begin in (start..end).step_by(256) {
        progress.log(format!(
            "write begin records={begin}..{} target={end}",
            (begin + 256).min(end)
        ));
        records::insert(
            index,
            (begin..(begin + 256).min(end))
                .map(|i| serde_json::from_value(doc(i)).unwrap())
                .collect(),
        )
        .await
        .unwrap();
        progress.log(format!(
            "write complete records={}/{end}",
            (begin + 256).min(end)
        ));
    }
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        while index.statistics().await.unwrap().physical_versions != end {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    progress.log(format!("physical versions confirmed={end}"));
}
// Build literal input independently of index/query internals. Each corpus keeps
// its own term sets and population; adding terms would change its score oracle.
fn term_document(terms: &[&str], sets: &[BTreeSet<usize>], id: usize) -> serde_json::Value {
    let words: Vec<_> = terms
        .iter()
        .zip(sets)
        .filter(|(_, members)| members.contains(&id))
        .map(|(term, _)| *term)
        .collect();
    json!({"title":"", "text":if words.is_empty() {"neutral".into()} else {words.join(" ")}})
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
async fn mixed_term_membership_scores_and_query_order_case(policy: FieldLengthNormalization) {
    let population = scale::choose(514, 8194);
    assert!(population > 400 && population / 2 > 100);
    let progress = Progress::new("mixed", policy);
    let (mut index, path) = fresh("mixed", policy).await;
    let sets = [
        (100..104).collect::<BTreeSet<_>>(),
        [100, 103, 200, 300, 400].into_iter().collect(),
        (1..population).step_by(2).collect(),
    ];
    let terms = ["alpha", "beta", "gamma"];
    add(&index, 0, population, &progress, |i| {
        term_document(&terms, &sets, i)
    })
    .await;
    let union: BTreeSet<_> = sets.iter().flat_map(|s| s.iter().copied()).collect();
    let avg = (0..population)
        .map(|i| sets.iter().filter(|s| s.contains(&i)).count().max(1))
        .sum::<usize>() as f64
        / population as f64;
    for stage in 0..3 {
        if stage == 1 {
            checkpoint(&index).await;
        }
        if stage == 2 {
            drop(index);
            index = reopen(&path).await;
        }
        for explicit in [false, true] {
            let rows = search(&index, "alpha beta gamma", false, population + 1, explicit).await;
            assert_eq!(rows.iter().map(|r| r.0).collect::<BTreeSet<_>>(), union);
            for (id, score) in &rows {
                let dl = sets.iter().filter(|s| s.contains(id)).count();
                let expected = sets
                    .iter()
                    .filter(|s| s.contains(id))
                    .map(|s| contribution(population, s.len(), dl, avg))
                    .sum();
                close_score(*score, expected);
            }
            assert_eq!(
                search(&index, "alpha beta gamma", true, population + 1, explicit)
                    .await
                    .iter()
                    .map(|r| r.0)
                    .collect::<Vec<_>>(),
                vec![103]
            );
            for q in ["gamma alpha beta", "beta gamma alpha"] {
                assert_eq!(
                    rows,
                    search(&index, q, false, population + 1, explicit).await,
                    "stage {stage}, query {q}"
                );
            }
        }
    }

    drop(index);
    std::fs::remove_dir_all(path).unwrap();
    progress.log("fixture complete; all assertions passed");
}
#[tokio::test]
async fn appended_documents_preserve_scores_and_top1() {
    for policy in [
        anyseek::index::FieldLengthNormalization::Shared,
        anyseek::index::FieldLengthNormalization::PerField,
    ] {
        let progress = Progress::new("append", policy);
        let (mut index, path) = fresh("append", policy).await;
        let doc = |i| match i {
            0 => json!({"title":"alpha","text":format!("alpha {}","neutral ".repeat(19))}),
            1 => json!({"title":"alpha neutral neutral","text":"alpha neutral neutral"}),
            512 => json!({"title":"alpha","text":format!("alpha {}","neutral ".repeat(12))}),
            _ => json!({"title":"","text":"neutral ".repeat(if i<512 {1}else{5})}),
        };
        add(&index, 0, 512, &progress, doc).await;
        checkpoint(&index).await;
        add(&index, 512, 576, &progress, doc).await;
        checkpoint(&index).await;
        for reopened in [false, true] {
            if reopened {
                drop(index);
                index = reopen(&path).await;
            }
            let avg = (537 + 63 * 5 + 14) as f64 / 576.0;
            let all = search(&index, "alpha", false, 4, false).await;
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
                assert_eq!(search(&index, "alpha", false, 1, explicit).await, all[..1]);
            }
        }

        drop(index);
        std::fs::remove_dir_all(path).unwrap();
        progress.log("fixture complete; all assertions passed");
    }
}
#[tokio::test]
async fn late_matching_document_remains_in_top2() {
    for policy in [
        anyseek::index::FieldLengthNormalization::Shared,
        anyseek::index::FieldLengthNormalization::PerField,
    ] {
        let progress = Progress::new("late-top2", policy);
        let (index, path) = fresh("late-top2", policy).await;
        let terms = [
            "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota",
        ];
        add(&index,0,522,&progress,|i|json!({"title":"","text":if i==0 {terms.join(" ")}else if i<512 {"neutral ".repeat(10)}else if i<521 {terms[i-512].into()}else{"theta iota".into()}})).await;
        checkpoint(&index).await;
        let q = terms.join(" ");
        let all = search(&index, &q, false, 523, false).await;
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
            assert_eq!(search(&index, &q, false, 2, explicit).await, all[..2]);
        }

        drop(index);
        std::fs::remove_dir_all(path).unwrap();
        progress.log("fixture complete; all assertions passed");
    }
}

/// Five overlapping term sets exercise additive union scores and Top-K prefixes.
/// Expected membership and scores do not depend on the query implementation.
async fn five_term_union_scores_and_topk_prefix_case(policy: FieldLengthNormalization) {
    let population = scale::choose(514, 8194);
    assert!(population > 400 && population / 2 > 100);
    let progress = Progress::new("five-term", policy);
    let (index, path) = fresh("five-term", policy).await;
    let terms = ["alpha", "beta", "gamma", "delta", "epsilon"];
    let sets: Vec<BTreeSet<usize>> = vec![
        (100..104).collect(),
        [100, 103, 200, 300, 400].into_iter().collect(),
        (1..population).step_by(2).collect(),
        (0..population).step_by(3).collect(),
        (0..population).step_by(7).collect(),
    ];
    add(&index, 0, population, &progress, |i| {
        term_document(&terms, &sets, i)
    })
    .await;
    checkpoint(&index).await;
    let avg = (0..population)
        .map(|i| sets.iter().filter(|s| s.contains(&i)).count().max(1))
        .sum::<usize>() as f64
        / population as f64;
    let union: BTreeSet<_> = sets.iter().flat_map(|s| s.iter().copied()).collect();
    for explicit in [false, true] {
        let all = search(&index, &terms.join(" "), false, population + 1, explicit).await;
        assert_eq!(all.iter().map(|r| r.0).collect::<BTreeSet<_>>(), union);
        for &(id, score) in &all {
            let dl = sets.iter().filter(|s| s.contains(&id)).count();
            close_score(
                score,
                sets.iter()
                    .filter(|s| s.contains(&id))
                    .map(|s| contribution(population, s.len(), dl, avg))
                    .sum(),
            );
        }
        for k in [1, 10, 100] {
            assert_eq!(
                search(&index, &terms.join(" "), false, k, explicit).await,
                all[..k]
            );
        }
    }

    drop(index);
    std::fs::remove_dir_all(path).unwrap();
    progress.log("fixture complete; all assertions passed");
}

#[tokio::test]
async fn mixed_term_membership_scores_and_query_order_shared() {
    mixed_term_membership_scores_and_query_order_case(FieldLengthNormalization::Shared).await;
}

#[tokio::test]
async fn mixed_term_membership_scores_and_query_order_per_field() {
    mixed_term_membership_scores_and_query_order_case(FieldLengthNormalization::PerField).await;
}

#[tokio::test]
async fn five_term_union_scores_and_topk_prefix_shared() {
    five_term_union_scores_and_topk_prefix_case(FieldLengthNormalization::Shared).await;
}

#[tokio::test]
async fn five_term_union_scores_and_topk_prefix_per_field() {
    five_term_union_scores_and_topk_prefix_case(FieldLengthNormalization::PerField).await;
}
