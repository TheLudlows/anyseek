//! Manual before/after profile for the private vector topology cleanup.
#[path = "common/records.rs"] mod records;
use anyseek::{record::RecordQuery, search::{QueryType, ResultType, SearchMode, RrfOptions},
    vector::Embedding, vector_similarity::AnnMode};
use serde_json::json;
use std::{path::PathBuf, time::Instant};

#[tokio::test]
#[ignore = "manual paired topology query profile"]
async fn segment_query_profile() {
    let output = PathBuf::from(std::env::var("ANYSEEK_SEGMENT_PROFILE_OUTPUT").expect("output file"));
    let temporary = tempfile::tempdir().unwrap(); let root = temporary.path().join("records");
    let meta = serde_json::from_value(json!({"name":"profile","lexical_similarity":"Bm25f",
        "tokenizer":"TantivySimpleV1","stop_words":"None","frequent_words":"None","ngram_indexing":0,
        "clustering":"None","inference":{"External":{"dimensions":3,"precision":"F32","quantization":"None","similarity":"Cosine"}}})).unwrap();
    let schema = serde_json::from_value(json!([
        {"field":"body","field_type":"Text","store":true,"index_lexical":true},
        {"field":"vector","field_type":"Json","store":false,"index_lexical":false,"index_vector":true}
    ])).unwrap();
    let p = records::create(&root,meta,schema).await;
    for batch in 0..4 {
        records::insert(&p,(0..30).map(|i|serde_json::from_value(json!({
            "body":if (i+batch)%3==0 {"alpha beta"} else {"alpha gamma"},
            "vector":[1.0,(i%7) as f32/10.0,batch as f32/10.0]
        })).unwrap()).collect()).await.unwrap();
        if batch < 3 {p.compact_journal().await.unwrap();}
    }
    let mut rows = Vec::new();
    for (label,mode,vector) in [
        ("lexical",SearchMode::Lexical,None),
        ("vector",SearchMode::Vector {similarity_threshold:None,ann_mode:AnnMode::All},Some(Embedding::F32(vec![1.0,0.0,0.0]))),
        ("hybrid",SearchMode::Hybrid {similarity_threshold:None,ann_mode:AnnMode::All,rrf:RrfOptions::default()},Some(Embedding::F32(vec![1.0,0.0,0.0])))
    ] {
        let mut samples = Vec::new(); let mut expected = None;
        for i in 0..120 {
            let query = RecordQuery {text:"alpha beta".into(),vector:vector.clone(),mode:mode.clone(),query_type:QueryType::Union,
                offset:0,length:10,result_type:ResultType::TopkCount,fields:vec![],filters:vec![],sort:vec![],temporal:None};
            let start = Instant::now(); let result = p.query(query).await.unwrap(); let elapsed = start.elapsed().as_micros();
            let hits = json!(result.hits.iter().map(|h| (&h.record.record_id,h.score.to_bits(),
                h.channels.iter().map(|c|(&c.channel,c.rank,c.raw_score.map(f32::to_bits),c.rrf_contribution.map(f32::to_bits))).collect::<Vec<_>>())).collect::<Vec<_>>());
            if i == 0 {expected=Some(hits.clone());} else {assert_eq!(expected.as_ref().unwrap(),&hits);}
            if i >= 20 {samples.push(elapsed);}
        }
        rows.push(json!({"mode":label,"hits":expected,"samples_us":samples}));
    }
    let memory = std::fs::read_to_string("/proc/self/status").ok()
        .and_then(|s|s.lines().find(|line|line.starts_with("VmHWM:")).map(str::to_owned));
    std::fs::write(output,serde_json::to_vec_pretty(&json!({"records":120,"rows":rows,"process_high_water":memory})).unwrap()).unwrap();
}
