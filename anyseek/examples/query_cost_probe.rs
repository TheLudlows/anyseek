//! Manual query cost probe; uses only disposable local Record partitions.
use anyseek::{record::{RecordConfig, RecordPartition, RecordQuery, RecordWrite, OperationId, ExpectedVersion, TemporalMetadata, CommittedItem}, search::{QueryType, ResultType, SearchMode, RrfOptions, ResultSort, SortOrder}, vector::Embedding, vector_similarity::AnnMode};
use serde_json::json;
use std::{path::PathBuf, time::{Duration, Instant}};
#[tokio::main(flavor="multi_thread", worker_threads=2)]
async fn main() {
    let args: Vec<_> = std::env::args().collect();
    let root=PathBuf::from(&args[1]);
    let n:usize=args[2].parse().unwrap();
    let ttl:bool=args[3].parse().unwrap();
    let output=&args[4];
    let p=if root.exists() {RecordPartition::open(&root,256*1024*1024).await.unwrap()} else {
        let config=RecordConfig {
            index:serde_json::from_value(json!({"name":"query-cost","lexical_similarity":"Bm25f","tokenizer":"TantivySimpleV1","stop_words":"None","frequent_words":"None","ngram_indexing":0,"clustering":"None","inference":{"External":{"dimensions":3,"precision":"F32","quantization":"None","similarity":"Cosine"}}})).unwrap(),
            schema:serde_json::from_value(json!([
                {"field":"body","field_type":"Text","store":true,"index_lexical":true},
                {"field":"rank","field_type":"Int","store":true,"index_lexical":false,"facet":true},
                {"field":"vector","field_type":"Json","store":false,"index_lexical":false,"index_vector":true}
            ])).unwrap(), history:Default::default(),embedding_space_id:Some("probe".into()),
            max_records:10000,max_record_bytes:65536,max_operations:20000,max_journal_bytes:256*1024*1024,max_candidates:10000,
        };
        let p=RecordPartition::create(&root,config).await.unwrap();
        for start in (0..n).step_by(32) {
            let requests=(start..n.min(start+32)).map(|i| RecordWrite {
                operation_id:OperationId::new(format!("probe-{i}")),record_id:format!("{i:020}"),expected:ExpectedVersion::Absent,
                document:Some(serde_json::from_value(json!({"body":if i%3==0 {"alpha beta"} else {"alpha gamma"},"rank":i%17,"vector":[1.0,(i%7) as f32/10.0,0.2]})).unwrap()),vector_patch:None,
                temporal:ttl.then_some(TemporalMetadata {expire_at_ms:Some(4_000_000_000_000),..Default::default()}),
            }).collect::<Vec<_>>();
            let deadline=Instant::now();
            loop {
                match p.commit_writes(requests.clone()).await {
                    Ok(batch) if matches!(batch.stop, Some(anyseek::record::RecordError::Busy)) && deadline.elapsed()<Duration::from_secs(60) => {
                        assert!(batch.results.iter().all(|r| matches!(r,CommittedItem::Committed(_) | CommittedItem::NotAttempted)));
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    },
                    Ok(batch) => {assert!(batch.stop.is_none(),"{:?}",batch.stop); assert!(batch.results.iter().all(|r| matches!(r,CommittedItem::Committed(_))));break;},
                    Err(anyseek::record::RecordError::Busy) if deadline.elapsed()<Duration::from_secs(60)=>tokio::time::sleep(Duration::from_millis(10)).await,
                    result=>panic!("{result:?}"),
                }
            }
            let deadline=Instant::now();
            loop {let q=p.committed_progress().await;assert!(q.index_error.is_none());if q.lexical_seq==q.vector_seq && q.committed_seq==Some(q.lexical_seq) {break;}assert!(deadline.elapsed()<Duration::from_secs(60));tokio::time::sleep(Duration::from_millis(2)).await;}
        }
        let deadline=Instant::now();
        loop {match p.compact_journal().await {
            Ok(_)=>break,
            Err(anyseek::record::RecordError::Busy) if deadline.elapsed()<Duration::from_secs(60)=>tokio::time::sleep(Duration::from_millis(10)).await,
            result=>panic!("compact: {result:?}"),
        }}
        p
    };
    assert_eq!(p.statistics().await.unwrap().active_records,n);
    let mut rows=vec![];
    for label in ["lexical","count","facet","selective","hybrid","vector","statistics"] {
        let mut samples=vec![];let mut oracle=None;
        for i in 0..70 {
            if std::env::var_os("ANYSEEK_QUERY_PROBE_TRACE").is_some() {eprintln!("CASE {label} {i}");}
            let start=Instant::now();
            let (elapsed,value)=if label=="statistics" {
                let r=p.statistics().await.unwrap();let elapsed=start.elapsed().as_nanos();
                (elapsed,json!([r.active_records,r.retained_identities,r.physical_versions]))
            } else {
                let mode=match label {
                    "hybrid"=>SearchMode::Hybrid {similarity_threshold:None,ann_mode:AnnMode::All,rrf:RrfOptions {keyword_candidates:Some(32),vector_candidates:Some(32),..Default::default()}},
                    "vector"=>SearchMode::Vector {similarity_threshold:None,ann_mode:AnnMode::All},_=>SearchMode::Lexical};
                let r=p.query(RecordQuery {text:"alpha beta".into(),vector:matches!(label,"hybrid"|"vector").then_some(Embedding::F32(vec![1.0,0.0,0.0])),mode,query_type:QueryType::Union,offset:0,length:10,result_type:if label=="count" {ResultType::Count} else {ResultType::TopkCount},fields:vec![],filters:if label=="selective" {vec![anyseek::search::FacetFilter::Int {field:"rank".into(),filter:0..1}]} else {vec![]},sort:if label=="facet" {vec![ResultSort {field:"rank".into(),order:SortOrder::Ascending}]} else {vec![]},temporal:None}).await.unwrap();
                let elapsed=start.elapsed().as_nanos();
                (elapsed,json!({"count":r.count_total,"active":r.statistics.active_records,"physical":r.statistics.physical_versions,"hits":r.hits.iter().map(|h|json!([h.record.record_id,h.score.to_bits(),h.channels.iter().map(|c|json!([c.channel,c.rank,c.raw_score.map(f32::to_bits),c.rrf_contribution.map(f32::to_bits)])).collect::<Vec<_>>()])).collect::<Vec<_>>()}))
            };
            if let Some(expected)=&oracle {assert_eq!(expected,&value)} else {oracle=Some(value)};
            if i>=10 {samples.push(elapsed)}
        }
        rows.push(json!({"case":label,"samples_ns":samples,"result":oracle}));
    }
    let mixed=if let Ok(label)=std::env::var("ANYSEEK_QUERY_PROBE_MIXED") {Some(mixed(&p,&label).await)} else {None};
    std::fs::write(output,serde_json::to_vec_pretty(&json!({"records":n,"ttl":ttl,"rows":rows,"mixed":mixed})).unwrap()).unwrap();
    p.finish_service_shutdown().await.unwrap();
}

async fn mixed(p: &RecordPartition, label: &str) -> serde_json::Value {
    use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
    assert!(matches!(label,"lexical"|"hybrid"|"facet"));
    let stop=Arc::new(AtomicBool::new(false));
    let reader={let p=p.clone();let label=label.to_owned();let stop=stop.clone();tokio::spawn(async move {
        let mut samples=vec![];let mut busy=0;
        while !stop.load(Ordering::Acquire) {
            let start=Instant::now();
            let result=p.query(RecordQuery {text:"alpha beta".into(),vector:(label=="hybrid").then_some(Embedding::F32(vec![1.0,0.0,0.0])),
                mode:if label=="hybrid" {SearchMode::Hybrid {similarity_threshold:None,ann_mode:AnnMode::All,rrf:RrfOptions {keyword_candidates:Some(32),vector_candidates:Some(32),..Default::default()}}} else {SearchMode::Lexical},
                query_type:QueryType::Union,offset:0,length:10,result_type:ResultType::TopkCount,fields:vec![],filters:vec![],
                sort:if label=="facet" {vec![ResultSort {field:"rank".into(),order:SortOrder::Ascending}]} else {vec![]},temporal:None}).await;
            match result {Ok(_)=>samples.push(start.elapsed().as_nanos()),Err(anyseek::record::RecordError::Busy)=>busy+=1,Err(e)=>panic!("query: {e:?}")}
        }
        (samples,busy)
    })};
    let mut commits=vec![];let mut published=vec![];let mut retries=0;
    for i in 0..30 {
        let request=RecordWrite {operation_id:OperationId::new(format!("mixed-{i}")),record_id:format!("{:020}",i%16),expected:ExpectedVersion::Any,
            document:Some(serde_json::from_value(json!({"body":"alpha beta","rank":i%17,"vector":[1.0,0.1,0.2]})).unwrap()),vector_patch:None,temporal:None};
        let start=Instant::now();
        let seq=loop {assert!(start.elapsed()<Duration::from_secs(60));match p.commit_writes(vec![request.clone()]).await {
            Ok(batch)=>match batch.results.as_slice() {
                [CommittedItem::Committed(receipt)] if batch.stop.is_none()=>break receipt.seq,
                [CommittedItem::NotAttempted] if matches!(batch.stop,Some(anyseek::record::RecordError::Busy))=>{retries+=1;tokio::time::sleep(Duration::from_millis(2)).await;},
                _=>panic!("batch: {batch:?}"),
            },
            Err(anyseek::record::RecordError::Busy)=>{retries+=1;tokio::time::sleep(Duration::from_millis(2)).await;},
            Err(e)=>panic!("write: {e:?}"),
        }};
        commits.push(start.elapsed().as_nanos());
        loop {let q=p.committed_progress().await;assert!(q.index_error.is_none());if q.lexical_seq>=seq && q.vector_seq>=seq {break;}assert!(start.elapsed()<Duration::from_secs(60));tokio::time::sleep(Duration::from_millis(1)).await;}
        published.push(start.elapsed().as_nanos());
    }
    stop.store(true,Ordering::Release);let (queries,busy)=reader.await.unwrap();assert!(!queries.is_empty());
    json!({"mode":label,"commit_ns":commits,"published_ns":published,"query_ns":queries,"query_busy":busy,"write_retries":retries})
}
