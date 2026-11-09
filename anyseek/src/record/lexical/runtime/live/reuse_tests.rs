use super::*;
use crate::record::tests::{config, write};
use crate::record::{ExpectedVersion, RecordPartition};
use crate::{
    index::{TextNormalization, TokenizerType},
    search::SearchMode,
};
use std::sync::atomic::Ordering;

fn profiles() -> Vec<TokenizerType> {
    let profiles = vec![TokenizerType::TantivySimpleV1];
    #[cfg(feature = "zh")]
    let profiles = {
        let mut profiles = profiles;
        profiles.push(TokenizerType::JiebaMixedV1);
        profiles
    };
    profiles
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn visible_fields_are_analyzed_once_across_native_handoff() {
    for profile in profiles() {
        for ngram in [0, 127] {
            let temp = tempfile::tempdir().unwrap();
            let root = temp.path().join("records");
            let mut cfg = config();
            cfg.index.tokenizer = profile;
            cfg.index.ngram_indexing = ngram;
            cfg.index.frequent_words = crate::index::FrequentwordType::Custom {
                terms: vec!["alpha".into(), "beta".into(), "gamma".into()],
            };
            cfg.index.text_normalization = TextNormalization::OpenCcT2s1_1_9;
            for name in ["json", "missing"] {
                cfg.schema.push(serde_json::from_value(serde_json::json!({"field":name,"field_type":"Json","store":false,"index_lexical":true})).unwrap());
            }
            let p = RecordPartition::create(&root, cfg.clone()).await.unwrap();
            let native = p
                .inner
                .lock()
                .await
                .index
                .test_index()
                .read()
                .await
                .segments[0]
                .lexical
                .clone();
            let pause = native.package_path().join(".test-visible-pause");
            std::fs::write(&pause, b"pause").unwrap();
            let mut receipts = Vec::new();
            for (i,value) in [serde_json::json!({"nested":{"text":"ALPHA beta 臺灣搜索"},"list":["gamma","beta"]}),serde_json::json!(["ALPHA",42]),serde_json::Value::Null].into_iter().enumerate() {
            let id=format!("row{i}");let mut r=write(&id,&id,if i==0 {"ALPHA beta beta 臺灣搜索"}else{"beta gamma"},ExpectedVersion::Absent);
            r.document.as_mut().unwrap().insert("json".into(),value);
            receipts.push((r.clone(),p.write(r).await.unwrap()));
        }
            let held = native.snapshot().await.unwrap();
            assert_eq!(held.overlay.len(), 3);
            let calls = native.analyzer.field_calls.load(Ordering::Relaxed);
            assert_eq!(calls, 9);
            let mut before = Vec::new();
            for text in [
                "alpha",
                "beta",
                "\"alpha beta\"",
                "臺灣搜索",
                "alpha -gamma",
                "",
            ] {
                let result = p
                    .search(text.into(), None, SearchMode::Lexical, vec![], 10)
                    .await
                    .unwrap();
                before.push((
                    text,
                    result
                        .hits
                        .iter()
                        .map(|h| (h.record.record_id.clone(), h.score))
                        .collect::<Vec<_>>(),
                ));
            }
            assert_eq!(
                native.analyzer.field_calls.load(Ordering::Relaxed),
                calls,
                "queries are not field analysis"
            );
            std::fs::remove_file(&pause).unwrap();
            native.join_visible().await.unwrap();
            let after = native.analyzer.field_calls.load(Ordering::Relaxed);
            assert!(native.snapshot().await.unwrap().overlay.is_empty());
            for (text, expected) in &before {
                let result = p
                    .search((*text).into(), None, SearchMode::Lexical, vec![], 10)
                    .await
                    .unwrap();
                assert_eq!(
                    *expected,
                    result
                        .hits
                        .iter()
                        .map(|h| (h.record.record_id.clone(), h.score))
                        .collect::<Vec<_>>()
                );
            }
            assert!(native.visible_bytes.load(Ordering::Acquire) > 0);
            drop(held);
            assert_eq!(native.visible_bytes.load(Ordering::Acquire), 0);
            p.finish_service_shutdown().await.unwrap();
            drop(native);
            drop(p);
            let p = RecordPartition::open(&root, cfg.max_journal_bytes)
                .await
                .unwrap();
            for (r, receipt) in receipts {
                assert_eq!(p.write(r).await.unwrap(), receipt);
            }
            for (text, expected) in before {
                let result = p
                    .search(text.into(), None, SearchMode::Lexical, vec![], 10)
                    .await
                    .unwrap();
                assert_eq!(
                    expected,
                    result
                        .hits
                        .iter()
                        .map(|h| (h.record.record_id.clone(), h.score))
                        .collect::<Vec<_>>()
                );
            }
            p.finish_service_shutdown().await.unwrap();
            assert_eq!(
                after, calls,
                "native materialization must reuse the original analysis"
            );
        }
    }
}

/// Same fixed workload on parent/candidate; a microbenchmark, not capacity evidence.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore]
#[cfg(target_os = "linux")]
async fn lexical_analysis_reuse_measurement() {
    fn cpu_ticks() -> u64 {
        let stat = std::fs::read_to_string("/proc/self/stat").unwrap();
        let fields = stat
            .rsplit_once(')')
            .unwrap()
            .1
            .split_whitespace()
            .collect::<Vec<_>>();
        fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap()
    }
    let count = 24;
    let body = "ALPHA beta gamma delta 臺灣搜索 中文分詞 ".repeat(128);
    for profile in profiles() {
        for ngram in [0, 127] {
            let temp = tempfile::tempdir().unwrap();
            let mut cfg = config();
            cfg.max_record_bytes = 128 * 1024;
            cfg.max_journal_bytes = 16 * 1024 * 1024;
            cfg.index.tokenizer = profile;
            cfg.index.ngram_indexing = ngram;
            cfg.index.frequent_words = crate::index::FrequentwordType::Custom {
                terms: vec!["alpha".into(), "beta".into(), "gamma".into()],
            };
            cfg.index.text_normalization = TextNormalization::OpenCcT2s1_1_9;
            let p = RecordPartition::create(&temp.path().join("records"), cfg)
                .await
                .unwrap();
            p.write(write("warm", "warm", &body, ExpectedVersion::Absent))
                .await
                .unwrap();
            let native = p
                .inner
                .lock()
                .await
                .index
                .test_index()
                .read()
                .await
                .segments[0]
                .lexical
                .clone();
            native.join_visible().await.unwrap();
            let calls = native.analyzer.field_calls.load(Ordering::Relaxed);
            let logical_before = crate::storage::build_budget::used_memory_for_test();
            let cpu_before = cpu_ticks();
            let started = std::time::Instant::now();
            let mut latencies = Vec::new();
            for i in 0..count {
                let id = format!("r{i}");
                let t = std::time::Instant::now();
                p.write(write(&id, &id, &body, ExpectedVersion::Absent))
                    .await
                    .unwrap();
                latencies.push(t.elapsed().as_secs_f64() * 1000.0);
                if i % 4 == 3 {
                    native.join_visible().await.unwrap();
                }
            }
            native.join_visible().await.unwrap();
            let seconds = started.elapsed().as_secs_f64();
            assert_eq!(
                p.search("alpha".into(), None, SearchMode::Lexical, vec![], 32)
                    .await
                    .unwrap()
                    .hits
                    .len(),
                count + 1
            );
            eprintln!(
                "REUSE_BENCH {}",
                serde_json::json!({"profile":format!("{profile:?}"),"ngram":ngram,"records":count,"seconds":seconds,"records_s":count as f64/seconds,"cpu_ticks":cpu_ticks()-cpu_before,"logical_before":logical_before,"logical_after":crate::storage::build_budget::used_memory_for_test(),"ack_ms":latencies,"field_calls":native.analyzer.field_calls.load(Ordering::Relaxed)-calls,"status":std::fs::read_to_string("/proc/self/status").unwrap()})
            );
            p.finish_service_shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn retained_analysis_preserves_full_tokens_and_lexical_only_estimate() {
    use crate::record::lexical::{Identity, memory::Row, query::Analyzer};
    for profile in profiles() {
        for ngram in [0, 127] {
            let mut cfg = config();
            cfg.index.tokenizer = profile;
            cfg.index.ngram_indexing = ngram;
            cfg.index.text_normalization = TextNormalization::OpenCcT2s1_1_9;
            cfg.index.frequent_words = crate::index::FrequentwordType::Custom {
                terms: vec!["alpha".into(), "beta".into(), "gamma".into()],
            };
            for name in ["json", "missing"] {
                cfg.schema.push(serde_json::from_value(serde_json::json!({"field":name,"field_type":"Json","store":false,"index_lexical":true})).unwrap());
            }
            let document:Document=serde_json::from_value(serde_json::json!({"body":"ALPHA beta gamma 臺灣 搜索", "json":{"list":["delta",7,"beta"],"nested":{"text":"ALPHA beta"}},"vector":vec![1.0;768],"not_indexed":"x".repeat(8192)})).unwrap();
            let analyzer = Analyzer::new(cfg.index.clone()).unwrap();
            let reservation = Reservation::new(&document, &cfg, "row").unwrap();
            let full_estimate = reservation.analysis_bytes;
            let row = Row::analyze(
                Identity {
                    id: "row".into(),
                    version: 1,
                    revision: 1,
                },
                &document,
                &cfg,
                &analyzer,
                reservation,
            )
            .await
            .unwrap();
            assert_eq!(analyzer.field_calls.load(Ordering::Relaxed), 3);
            let oracle = Analyzer::new(cfg.index.clone()).unwrap();
            // Object flattening preserves only recursive string values in map order;
            // a missing lexical field is still analyzed as an empty string.
            let expected = vec![
                oracle.field("ALPHA beta gamma 臺灣 搜索").await,
                oracle.field("delta beta ALPHA beta").await,
                oracle.field("").await,
            ];
            assert_eq!(row.analyzed, expected); // Token Eq includes offsets and position_length.
            assert_eq!(
                row.analyzed.clone(),
                expected,
                "owned native copies preserve every token field"
            );
            for (i, field) in row.analyzed.iter().enumerate() {
                assert_eq!(row.lengths[i], u64::from(field.length));
                let mut positions: BTreeMap<String, Vec<u32>> = BTreeMap::new();
                for token in &field.tokens {
                    assert_eq!(&field.text[token.offset_from..token.offset_to], token.text);
                    positions
                        .entry(token.text.clone())
                        .or_default()
                        .push(token.position as u32);
                }
                assert_eq!(row.postings[i], positions);
            }
            if ngram != 0 {
                assert!(
                    row.analyzed[0]
                        .tokens
                        .iter()
                        .any(|token| token.position_length == 3),
                    "fixture must actually contain multi-position grams"
                );
            }
            assert!(row.analyzed[0].text.contains("台湾"));
            assert_eq!(row.lengths[2], 0);
            assert!(row.analyzed[2].tokens.is_empty());
            let projection = document
                .iter()
                .filter(|(name, _)| {
                    cfg.schema
                        .iter()
                        .any(|f| f.index_lexical && f.field == **name)
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            assert_eq!(
                row.native_estimate,
                crate::record::build_budget::document_estimate(&projection, &cfg.schema).unwrap()
            );
            assert!(
                row.native_estimate < full_estimate,
                "vectors/non-indexed values must not change native drain cadence"
            );
        }
    }
}
