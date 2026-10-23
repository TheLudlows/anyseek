use anyseek::record::RecordSearch;
#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
// External embedding contract: reject invalid inputs before writes or vector search.
use anyseek::{
    index::{AccessType, Clustering, Document, DocumentCompression,
        FrequentwordType, IndexMetaObject, LexicalSimilarity,
        NgramSet, SchemaField, StopwordType, TokenizerType},
    search::{RrfOptions, QueryType,  ResultType, SearchMode},
    vector::{Embedding, Inference, Precision, Quantization},
    vector_similarity::{AnnMode, VectorSimilarity},
};
use serde_json::json;


fn inference() -> Inference {
    Inference::External { dimensions: 3, precision: Precision::F32,
        quantization: Quantization::None, similarity: VectorSimilarity::Cosine }
}
fn schema() -> Vec<SchemaField> {
    serde_json::from_value(json!([
        {"field":"body","field_type":"Text","store":true,"index_lexical":true},
        {"field":"vector","field_type":"Json","store":true,"index_lexical":false,"index_vector":true}
    ])).unwrap()
}
fn document(body: &str, vector: serde_json::Value) -> Document {
    serde_json::from_value(json!({"body":body,"vector":vector})).unwrap()
}
fn meta() -> IndexMetaObject {
    IndexMetaObject { id:0, name:"external_contract".into(), lexical_similarity:LexicalSimilarity::Bm25f,
        tokenizer:TokenizerType::UnicodeAlphanumeric,
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared, stop_words:StopwordType::None, frequent_words:FrequentwordType::None,
        ngram_indexing:NgramSet::SingleTerm as u8, document_compression:DocumentCompression::Snappy,
        access_type:AccessType::Mmap, clustering:Clustering::None, inference:inference() }
}
async fn try_query(index: &RecordPartition, mode: SearchMode, vector: Option<Embedding>) -> Result<RecordSearch, anyseek::record::RecordError> {
    index.query(RecordQuery {text:"alpha".into(),vector:vector,query_type:QueryType::Union,mode:mode,offset:0,length:10,result_type:ResultType::TopkCount,fields:vec![],filters:vec![],sort:vec![],temporal:None}).await
}

async fn query(index: &RecordPartition, mode: SearchMode, vector: Option<Embedding>) -> RecordSearch { try_query(index,mode,vector).await.unwrap() }

fn weighted(rrf: RrfOptions) -> SearchMode {
    SearchMode::WeightedHybrid { similarity_threshold: None, ann_mode: AnnMode::All, rrf }
}
#[test]
fn invalid_rrf() {
    for v in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -1.0, 0.0] {
        assert!(RrfOptions { constant:v, ..Default::default() }.validate().is_err());
    }
    for v in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY, -1.0] {
        assert!(RrfOptions { keyword_weight:v, ..Default::default() }.validate().is_err());
        assert!(RrfOptions { vector_weight:v, ..Default::default() }.validate().is_err());
    }
    for r in [RrfOptions { keyword_weight:0.0, vector_weight:0.0, ..Default::default() },
        RrfOptions { constant:f32::MIN_POSITIVE, keyword_weight:f32::MAX, ..Default::default() },
        RrfOptions { keyword_candidates:Some(0), ..Default::default() },
        RrfOptions { vector_candidates:Some(0), ..Default::default() }] {
        assert!(r.validate().is_err());
    }
    assert_eq!(serde_json::from_str::<RrfOptions>("{}").unwrap(), RrfOptions::default());
    assert!(serde_json::from_str::<RrfOptions>(r#"{"constant":null}"#).is_err());
    assert!(serde_json::from_str::<RrfOptions>(r#"{"unknown":1}"#).is_err());
}
#[tokio::test]
async fn weighted_global_shards() {
    for shards in [1, 3] {
        let path = std::env::temp_dir().join(format!("rrf-{}-{shards}", std::process::id()));
        assert!(!path.exists());
        let index = Ok::<_, String>(records::create(&path, meta(), (&schema()).to_vec()).await).unwrap();
        records::insert(&index, (0..18).map(|i| document(if i % 2 == 0 {"alpha"} else {"beta"}, json!([1,0,0]))).collect()).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while index.statistics().await.unwrap().physical_versions != 18 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }).await.unwrap();
        let realtime = index.query(RecordQuery {text:"alpha".into(),vector:Some(Embedding::F32(vec![1.0,0.0,0.0])),query_type:QueryType::Union,mode:weighted(RrfOptions { keyword_candidates:Some(2), vector_candidates:Some(3), ..Default::default() }),offset:0,length:10,result_type:ResultType::Topk,fields:vec![],filters:vec![],sort:vec![],temporal:None}).await.unwrap();
        assert_eq!(realtime.hits.len(),3);
        index.compact_journal().await.unwrap();
        let vector = Some(Embedding::F32(vec![1.0,0.0,0.0]));
        let old = query(&index, SearchMode::Hybrid { similarity_threshold:None, ann_mode:AnnMode::All }, vector.clone()).await;
        let new = query(&index, weighted(RrfOptions::default()), vector.clone()).await;
        assert_eq!(serde_json::to_value(&old.hits).unwrap(), serde_json::to_value(&new.hits).unwrap());
        for hit in &old.hits {
            let keyword = if hit.record.record_id.parse::<usize>().unwrap() % 2 == 0 { 1.0 / (0.6 + (hit.record.record_id.parse::<usize>().unwrap() / 2) as f32) } else { 0.0 };
            let vector = if shards > 1 || hit.record.record_id.parse::<usize>().unwrap() < 10 { 1.0 / (0.6 + hit.record.record_id.parse::<usize>().unwrap() as f32) } else { 0.0 };
            assert_eq!(hit.score.to_bits(), (keyword + vector).to_bits());
        }
        let options = RrfOptions { constant:5.0, keyword_weight:0.5, vector_weight:1.0, keyword_candidates:Some(2), vector_candidates:Some(3) };
        let result = query(&index, weighted(options), vector.clone()).await;
        // Keyword ranks: 0,2. Vector ranks: 0,1,2. Missing keyword hit 1 contributes only vector.
        let scores: std::collections::BTreeMap<_,_> = result.hits.iter().map(|r|(r.record.record_id.parse::<usize>().unwrap(),r.score)).collect();
        assert_eq!(scores.len(),3);
        assert_eq!(scores[&0],0.5/5.0 + 1.0/5.0);
        assert_eq!(scores[&2],0.5/6.0 + 1.0/7.0);
        assert_eq!(scores[&1],1.0/6.0);
        for (kw,vw,ids) in [(0.0,1.0,vec![0,1,2]),(1.0,0.0,vec![0,2])] {
            let result = query(&index, weighted(RrfOptions {keyword_weight:kw,vector_weight:vw,..options}),vector.clone()).await;
            assert_eq!(result.hits.iter().map(|r|r.record.record_id.parse::<usize>().unwrap()).collect::<Vec<_>>(), ids);
        }
        let keyword_only_hits = query(&index, weighted(RrfOptions {keyword_candidates:Some(3),vector_candidates:Some(1),..options}),vector.clone()).await;
        assert_eq!(keyword_only_hits.hits.iter().find(|r|r.record.record_id.parse::<usize>().unwrap() == 4).unwrap().score, 0.5/7.0);
        for _ in 0..3 {
            let repeated = query(&index, weighted(options), vector.clone()).await;
            assert_eq!(serde_json::to_value(&result.hits).unwrap(),serde_json::to_value(&repeated.hits).unwrap());
        }
        let counted = index.query(RecordQuery {text:"alpha".into(),vector:vector.clone(),query_type:QueryType::Union,mode:weighted(options),offset:0,length:10,result_type:ResultType::Count,fields:vec![],filters:vec![],sort:vec![],temporal:None}).await.unwrap();
        assert!(counted.hits.is_empty());
        // Vector budget must reach beyond output length=1, not only truncate after retrieval.
        // doc 1 is keyword rank 0 and vector rank 1; it beats vector-only doc 0.
        let deeper = index.query(RecordQuery {text:"beta".into(),vector:vector.clone(),query_type:QueryType::Union,mode:weighted(RrfOptions {constant:5.0, keyword_candidates:Some(1),vector_candidates:Some(2),..Default::default()}),offset:0,length:1,result_type:ResultType::Topk,fields:vec![],filters:vec![],sort:vec![],temporal:None}).await.unwrap();
        assert_eq!(deeper.hits[0].record.record_id.parse::<usize>().unwrap(),1);
        assert_eq!(deeper.hits[0].score,1.0/5.0 + 1.0/6.0);
        let missing_vector = try_query(&index, weighted(options), None).await;
        assert!(missing_vector.is_err());
        let invalid = try_query(&index, weighted(RrfOptions {constant:f32::NAN,..options}),vector.clone()).await;
        assert!(invalid.is_err());
        // Fixed explicit budgets make pagination a slice of the same fused ranking.
        let page = index.query(RecordQuery {text:"alpha".into(),vector:vector,query_type:QueryType::Union,mode:weighted(options),offset:1,length:1,result_type:ResultType::Topk,fields:vec![],filters:vec![],sort:vec![],temporal:None}).await.unwrap();
        assert_eq!(page.hits[0].record.record_id.parse::<usize>().unwrap(),result.hits[1].record.record_id.parse::<usize>().unwrap());

        drop(index);
        let reopened = RecordPartition::open(&path, records::JOURNAL_LIMIT).await.unwrap();
        let again = query(&reopened, weighted(options), Some(Embedding::F32(vec![1.0,0.0,0.0]))).await;
        assert_eq!(serde_json::to_value(&result.hits).unwrap(),serde_json::to_value(&again.hits).unwrap());

        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }
}
