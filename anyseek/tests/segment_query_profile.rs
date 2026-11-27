//! Manual before/after profile for vector and filtered hybrid queries over multiple components.
#[path = "common/records.rs"] mod records;
use anyseek::{record::RecordQuery, search::{QueryType, ResultType, SearchMode, RrfOptions},
    vector::Embedding, vector_similarity::AnnMode};
use serde_json::json;
use std::{path::PathBuf, time::Instant};

#[tokio::test]
#[ignore = "manual paired topology query profile"]
async fn segment_query_profile() {
    let output = PathBuf::from(std::env::var("ANYSEEK_SEGMENT_PROFILE_OUTPUT").expect("output file"));
    let matrix = std::env::var_os("ANYSEEK_SEGMENT_PROFILE_MATRIX").is_some();
    let batch_size = if matrix { 512 } else { 30 };
    let temporary = tempfile::tempdir().unwrap(); let root = temporary.path().join("records");
    let meta = serde_json::from_value(json!({"name":"profile","lexical_similarity":"Bm25f",
        "tokenizer":"TantivySimpleV1","text_normalization":"None","field_length_normalization":"Shared",
        "stop_words":"None","frequent_words":"None","ngram_indexing":0,
        "clustering":"None","inference":{"External":{"dimensions":3,"precision":"F32","quantization":"None","similarity":"Cosine"}}})).unwrap();
    let schema = serde_json::from_value(json!([
        {"field":"body","field_type":"Text","store":true,"index_lexical":true},
        {"field":"vector","field_type":"Json","store":false,"index_lexical":false,"index_vector":true},
        {"field":"bucket","field_type":"Int","store":false,"index_lexical":false,"facet":true}
    ])).unwrap();
    let p = records::create(&root,meta,schema).await;
    for batch in 0..4 {
        records::insert(&p,(0..batch_size).map(|i|serde_json::from_value(json!({
            "body":if (i+batch)%3==0 {"alpha beta"} else {"alpha gamma"},
            "vector":[1.0,(i%7) as f32/10.0,batch as f32/10.0], "bucket":i % if matrix { 20 } else { 4 }
        })).unwrap()).collect()).await.unwrap();
        if batch < 3 {p.compact_journal().await.unwrap();}
    }
    if matrix {
        records::close(p).await;
        profile_matrix(&root, output, batch_size * 4).await;
        return;
    }
    let mut rows = Vec::new();
    let vector = SearchMode::Vector { similarity_threshold: None, ann_mode: AnnMode::All };
    let hybrid = SearchMode::Hybrid { similarity_threshold: None, ann_mode: AnnMode::All, rrf: RrfOptions::default() };
    let embedding = Embedding::F32(vec![1.0, 0.0, 0.0]);
    let filters: Vec<anyseek::search::FacetFilter> = serde_json::from_value(json!([
        {"Int":{"field":"bucket","filter":{"start":1,"end":2}}}
    ])).unwrap();
    for (label, mode, vector, filters) in [
        ("lexical", SearchMode::Lexical, None, vec![]),
        ("vector", vector.clone(), Some(embedding.clone()), vec![]),
        ("hybrid", hybrid.clone(), Some(embedding.clone()), vec![]),
        ("filtered_vector", vector, Some(embedding.clone()), filters.clone()),
        ("filtered_hybrid", hybrid, Some(embedding), filters),
    ] {
        let mut samples = Vec::new(); let mut expected = None;
        for i in 0..120 {
            let query = RecordQuery {text:"alpha beta".into(),vector:vector.clone(),mode:mode.clone(),query_type:QueryType::Union,
                offset:0,length:10,result_type:ResultType::TopkCount,fields:vec![],filters:filters.clone(),sort:vec![],temporal:None};
            let start = Instant::now(); let result = p.query(query).await.unwrap(); let elapsed = start.elapsed().as_micros();
            let hits = json!(result.hits.iter().map(|h| (&h.record.record_id,h.score.to_bits(),
                h.channels.iter().map(|c|(&c.channel,c.rank,c.raw_score.map(f32::to_bits),c.rrf_contribution.map(f32::to_bits))).collect::<Vec<_>>())).collect::<Vec<_>>());
            if i == 0 {expected=Some(hits.clone());} else {assert_eq!(expected.as_ref().unwrap(),&hits);}
            if i >= 20 {samples.push(elapsed);}
        }
        rows.push(json!({"mode":label,"hits":expected,"samples_us":samples}));
    }
    let memory = high_water();
    std::fs::write(output,serde_json::to_vec_pretty(&json!({"records":120,"rows":rows,"process_high_water":memory})).unwrap()).unwrap();
}

fn high_water() -> Option<String> {
    std::fs::read_to_string("/proc/self/status").ok().and_then(|status| {
        status.lines().find(|line| line.starts_with("VmHWM:")).map(str::to_owned)
    })
}

async fn profile_matrix(root: &std::path::Path, output: PathBuf, count: usize) {
    use anyseek::{
        record::{RecordError, RecordPartition},
        resources::{Governor, Lane},
    };
    use std::sync::Arc;

    let governor = Governor::from_env().unwrap();
    let traced = std::env::var_os("ANYSEEK_SEGMENT_PROFILE_TRACE").is_some();
    let mut rows = Vec::new();
    for hybrid in [false, true] {
        for (selectivity, bounds) in [("none", None), ("5pct", Some((0, 1))), ("90pct", Some((0, 18)))] {
            for concurrency in [1, 4] {
                let label = format!(
                    "{}-{selectivity}-c{concurrency}",
                    if hybrid { "hybrid" } else { "vector" }
                );
                let query = || RecordQuery {
                    text: "alpha beta".into(),
                    vector: Some(Embedding::F32(vec![1.0, 0.0, 0.0])),
                    mode: if hybrid {
                        SearchMode::Hybrid {
                            similarity_threshold: None,
                            ann_mode: AnnMode::All,
                            rrf: RrfOptions::default(),
                        }
                    } else {
                        SearchMode::Vector { similarity_threshold: None, ann_mode: AnnMode::All }
                    },
                    query_type: QueryType::Union,
                    offset: 0,
                    length: 10,
                    result_type: ResultType::TopkCount,
                    fields: vec![],
                    filters: bounds.map(|(start, end)| serde_json::from_value(json!([
                        {"Int":{"field":"bucket","filter":{"start":start,"end":end}}}
                    ])).unwrap()).unwrap_or_default(),
                    sort: vec![],
                    temporal: None,
                };
                // The cold observation is the first query after a fresh reopen.
                // Reopen itself can populate OS pages, so report this separately.
                let partition = RecordPartition::open(root, records::JOURNAL_LIMIT).await.unwrap();
                let trials = if traced { 3 } else { 120 };
                let mut samples = Vec::new();
                let mut cold = Vec::new();
                let mut busy = [0, 0];
                let mut expected = None;
                for round in 0..trials {
                    let measured = round == 0 || round >= if traced { 2 } else { 20 };
                    if traced && measured {
                        eprintln!("ANYSEEK_PROFILE BEGIN {label} {round}");
                    }
                    let results = futures::future::join_all((0..concurrency).map(|_| {
                        let query = query();
                        let governor: Arc<Governor> = governor.clone();
                        let partition = &partition;
                        async move {
                            let start = Instant::now();
                            let scope = governor.admit("profile".into(), Lane::Foreground, 0).unwrap();
                            let admitted = start.elapsed().as_nanos();
                            let result = scope.run(partition.query(query)).await;
                            (start.elapsed().as_nanos(), admitted, result)
                        }
                    })).await;
                    if traced && measured {
                        eprintln!("ANYSEEK_PROFILE END {label} {round}");
                    }
                    for (elapsed, admission, result) in results {
                        let sample = (elapsed, admission);
                        let result = match result {
                            Ok(result) => result,
                            Err(RecordError::Busy) if concurrency > 1 => {
                                if measured {
                                    busy[usize::from(round != 0)] += 1;
                                }
                                continue;
                            }
                            Err(error) => panic!("{label}: {error}"),
                        };
                        let hits = json!({
                            "count": result.count_total,
                            "hits": result.hits.iter().map(|hit| (
                                &hit.record.record_id, hit.score.to_bits(),
                                hit.channels.iter().map(|channel| (
                                    &channel.channel, channel.rank, channel.raw_score.map(f32::to_bits),
                                    channel.rrf_contribution.map(f32::to_bits),
                                )).collect::<Vec<_>>()
                            )).collect::<Vec<_>>()
                        });
                        if let Some(expected) = &expected {
                            assert_eq!(expected, &hits, "{label}");
                        } else {
                            expected = Some(hits);
                        }
                        if round == 0 {
                            cold.push(sample);
                        } else if measured {
                            samples.push(sample);
                        }
                    }
                }
                records::close(partition).await;
                rows.push(json!({
                    "mode": label, "hits": expected.expect("at least one completed query"),
                    "first_after_reopen_ns": cold, "samples_ns": samples, "busy": busy,
                }));
            }
        }
    }
    let data = json!({"records": count, "rows": rows, "process_high_water": high_water()});
    std::fs::write(output, serde_json::to_vec_pretty(&data).unwrap()).unwrap();
}
