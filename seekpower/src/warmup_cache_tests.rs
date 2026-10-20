//! Deterministic compute/mutate/publish schedules; no timing sleeps.
use crate::{
    commit::Commit,
    index::{
        AccessType, Close, Clustering, DocumentCompression, FieldLengthNormalization,
        FrequentwordType, IndexArc, IndexDocuments, IndexMetaObject, LexicalSimilarity,
        StopwordType, TextNormalization, TokenizerType, WarmupPause, create_index, warmup_inner,
    },
    search::{QueryType, ResultSort, ResultType, Search, SearchMode, SortOrder},
    vector::Inference,
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
#[derive(Debug, Clone, Copy)]
enum Mutation {
    Append,
    Commit,
    Clear,
}
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
    tokio::time::timeout(Duration::from_secs(10), async {
        while index.read().await.indexed_doc_count().await != total {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
fn expected(policy: FieldLengthNormalization, mutation: Mutation) -> BTreeMap<usize, f64> {
    let (n, df) = if matches!(mutation, Mutation::Clear) {
        (2.0, 1.0)
    } else {
        (3.0, 2.0)
    };
    let idf = (1.0_f64 + (n - df + 0.5) / (df + 0.5)).ln();
    let avg = if policy == FieldLengthNormalization::Shared {
        4.0
    } else {
        2.0
    };
    let component =
        |boost, tf: f64| boost * idf * tf * 2.2 / (tf + 1.2 * (0.25 + 0.75 * 2.0 / avg));
    let strong = component(2.0, 2.0) + component(0.5, 1.0);
    match mutation {
        Mutation::Append => [(0, strong)].into_iter().collect(),
        Mutation::Commit => [(0, strong), (2, component(2.0, 1.0) + component(0.5, 1.0))]
            .into_iter()
            .collect(),
        Mutation::Clear => [(1, strong)].into_iter().collect(),
    }
}
#[tokio::test]
async fn stale_warmup_cannot_publish_after_append_commit_or_clear() {
    let mut failures = vec![];
    for policy in [
        FieldLengthNormalization::Shared,
        FieldLengthNormalization::PerField,
    ] {
        for mutation in [Mutation::Append, Mutation::Commit, Mutation::Clear] {
            let label = format!("warmup-{policy:?}-{mutation:?}-{}", std::process::id());
            let path = std::env::temp_dir().join(&label);
            assert!(!path.exists());
            let meta = IndexMetaObject {
                id: 0,
                name: label.clone(),
                lexical_similarity: LexicalSimilarity::Bm25f,
                tokenizer: TokenizerType::UnicodeAlphanumeric,
                text_normalization: TextNormalization::None,
                field_length_normalization: policy,
                stop_words: StopwordType::None,
                frequent_words: FrequentwordType::Custom {
                    terms: vec!["the".into()],
                },
                ngram_indexing: 0,
                document_compression: DocumentCompression::Snappy,
                access_type: AccessType::Mmap,
                clustering: Clustering::None,
                inference: Inference::None,
            };
            let schema=serde_json::from_value(json!([{"field":"a","field_type":"Text","index_lexical":true,"store":false,"boost":2.0},{"field":"b","field_type":"Text","index_lexical":true,"store":false,"boost":0.5}])).unwrap();
            let index = create_index(&path, meta, &schema, 11, true, Some(1))
                .await
                .unwrap();
            let strong = json!({"a":"the the","b":"the neutral"});
            let neutral = json!({"a":"neutral neutral","b":"neutral neutral"});
            let appended = json!({"a":"the neutral","b":"the neutral"});
            add(&index, &[strong.clone(), neutral.clone()], 2).await;
            index.commit().await;
            if matches!(mutation, Mutation::Commit) {
                add(&index, &[appended.clone()], 3).await;
            }
            let shard = index.read().await.shard_vec[0].clone();
            let pause = Arc::new(WarmupPause::default());
            let task_shard = shard.clone();
            let task_pause = pause.clone();
            let task = tokio::spawn(async move {
                warmup_inner(&task_shard, Some(&task_pause)).await;
            });
            // Notify stores a permit, so completing computation before this await cannot
            // lose the signal. Mutation only starts after the search read lock is released.
            tokio::time::timeout(Duration::from_secs(10), pause.computed.notified())
                .await
                .unwrap();
            match mutation {
                Mutation::Append => add(&index, &[appended], 3).await,
                Mutation::Commit => index.commit().await,
                Mutation::Clear => {
                    index.write().await.clear_index().await;
                    add(&index, &[neutral, strong], 2).await;
                    index.commit().await;
                }
            }
            pause.resume.notify_one();
            tokio::time::timeout(Duration::from_secs(10), task)
                .await
                .unwrap()
                .unwrap();
            let want = expected(policy, mutation);
            for explicit in [false, true] {
                let r = index
                    .search(
                        "the".into(),
                        None,
                        QueryType::Union,
                        SearchMode::Lexical,
                        false,
                        0,
                        100,
                        ResultType::TopkCount,
                        false,
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
                let got_ids = r.results.iter().map(|h| h.doc_id).collect::<Vec<_>>();
                let want_ids = want.keys().copied().collect::<Vec<_>>();
                if got_ids != want_ids || r.result_count_total != want.len() {
                    failures.push(format!(
                        "{label} explicit={explicit} ids={got_ids:?} total={} wanted={want_ids:?}",
                        r.result_count_total
                    ));
                }
                for hit in r.results {
                    if let Some(score) = want.get(&hit.doc_id) {
                        if !hit.score.is_finite() || (hit.score as f64 - score).abs() > 2e-5 {
                            failures.push(format!(
                                "{label} explicit={explicit} id={} got={} wanted={score}",
                                hit.doc_id, hit.score
                            ));
                        }
                    }
                }
            }
            index.close().await;
            drop(index);
            drop(shard);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
    assert!(
        failures.is_empty(),
        "{} deterministic stale-publication failures:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
