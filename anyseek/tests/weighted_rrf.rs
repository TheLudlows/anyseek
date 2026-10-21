//! External embedding contract: reject invalid inputs before writes or vector search.
use anyseek::{
    commit::Commit,
    index::{create_index, open_index, AccessType, Close, Clustering, Document, DocumentCompression,
        FrequentwordType, IndexArc, IndexDocuments, IndexMetaObject, LexicalSimilarity,
        NgramSet, SchemaField, StopwordType, TokenizerType},
    search::{RrfOptions, QueryType, ResultObject, ResultType, Search, SearchMode},
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
async fn query(index: &IndexArc, mode: SearchMode, vector: Option<Embedding>) -> ResultObject {
    index.search("alpha".into(), vector, QueryType::Union, mode, false, 0, 10,
        ResultType::TopkCount, false, vec![],  vec![], vec![],).await
}

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
        let index = create_index(&path, meta(), &schema(), 11, true, Some(shards)).await.unwrap();
        index.index_documents((0..18).map(|i| document(if i % 2 == 0 {"alpha"} else {"beta"}, json!([1,0,0]))).collect()).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while index.read().await.indexed_doc_count().await != 18 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }).await.unwrap();
        let realtime = index.search("alpha".into(), Some(Embedding::F32(vec![1.0,0.0,0.0])), QueryType::Union,
            weighted(RrfOptions { keyword_candidates:Some(2), vector_candidates:Some(3), ..Default::default() }),
            false,0,10,ResultType::Topk,true,vec![],vec![],vec![]).await;
        assert!(realtime.error.is_none());
        assert_eq!(realtime.results.len(),3);
        index.commit().await.unwrap();
        let vector = Some(Embedding::F32(vec![1.0,0.0,0.0]));
        let old = query(&index, SearchMode::Hybrid { similarity_threshold:None, ann_mode:AnnMode::All }, vector.clone()).await;
        let new = query(&index, weighted(RrfOptions::default()), vector.clone()).await;
        assert!(new.error.is_none());
        assert_eq!(serde_json::to_value(&old.results).unwrap(), serde_json::to_value(&new.results).unwrap());
        for hit in &old.results {
            let keyword = if hit.doc_id % 2 == 0 { 1.0 / (0.6 + (hit.doc_id / 2) as f32) } else { 0.0 };
            let vector = if shards > 1 || hit.doc_id < 10 { 1.0 / (0.6 + hit.doc_id as f32) } else { 0.0 };
            assert_eq!(hit.score.to_bits(), (keyword + vector).to_bits());
        }
        let options = RrfOptions { constant:5.0, keyword_weight:0.5, vector_weight:1.0, keyword_candidates:Some(2), vector_candidates:Some(3) };
        let result = query(&index, weighted(options), vector.clone()).await;
        assert!(result.error.is_none());
        // Keyword ranks: 0,2. Vector ranks: 0,1,2. Missing keyword hit 1 contributes only vector.
        let scores: std::collections::BTreeMap<_,_> = result.results.iter().map(|r|(r.doc_id,r.score)).collect();
        assert_eq!(scores.len(),3);
        assert_eq!(scores[&0],0.5/5.0 + 1.0/5.0);
        assert_eq!(scores[&2],0.5/6.0 + 1.0/7.0);
        assert_eq!(scores[&1],1.0/6.0);
        for (kw,vw,ids) in [(0.0,1.0,vec![0,1,2]),(1.0,0.0,vec![0,2])] {
            let result = query(&index, weighted(RrfOptions {keyword_weight:kw,vector_weight:vw,..options}),vector.clone()).await;
            assert_eq!(result.results.iter().map(|r|r.doc_id).collect::<Vec<_>>(), ids);
        }
        let keyword_only_hits = query(&index, weighted(RrfOptions {keyword_candidates:Some(3),vector_candidates:Some(1),..options}),vector.clone()).await;
        assert_eq!(keyword_only_hits.results.iter().find(|r|r.doc_id == 4).unwrap().score, 0.5/7.0);
        for _ in 0..3 {
            let repeated = query(&index, weighted(options), vector.clone()).await;
            assert_eq!(serde_json::to_value(&result.results).unwrap(),serde_json::to_value(&repeated.results).unwrap());
        }
        let counted = index.search("alpha".into(), vector.clone(), QueryType::Union, weighted(options), false,0,10,ResultType::Count,false,vec![],vec![],vec![]).await;
        assert!(counted.error.is_none());
        assert!(counted.results.is_empty());
        // Vector budget must reach beyond output length=1, not only truncate after retrieval.
        // doc 1 is keyword rank 0 and vector rank 1; it beats vector-only doc 0.
        let deeper = index.search("beta".into(), vector.clone(), QueryType::Union,
            weighted(RrfOptions {constant:5.0, keyword_candidates:Some(1),vector_candidates:Some(2),..Default::default()}),
            false,0,1,ResultType::Topk,false,vec![],vec![],vec![]).await;
        assert!(deeper.error.is_none());
        assert_eq!(deeper.results[0].doc_id,1);
        assert_eq!(deeper.results[0].score,1.0/5.0 + 1.0/6.0);
        let missing_vector = query(&index, weighted(options), None).await;
        assert!(missing_vector.error.is_some());
        let invalid = query(&index, weighted(RrfOptions {constant:f32::NAN,..options}),vector.clone()).await;
        assert!(invalid.error.is_some());
        // Fixed explicit budgets make pagination a slice of the same fused ranking.
        let page = index.search("alpha".into(), vector, QueryType::Union, weighted(options), false,1,1,ResultType::Topk,false,vec![],vec![],vec![]).await;
        assert_eq!(page.results[0].doc_id,result.results[1].doc_id);
        index.close().await.unwrap();
        drop(index);
        let reopened = open_index(&path).await.unwrap();
        let again = query(&reopened, weighted(options), Some(Embedding::F32(vec![1.0,0.0,0.0]))).await;
        assert_eq!(serde_json::to_value(&result.results).unwrap(),serde_json::to_value(&again.results).unwrap());
        reopened.close().await.unwrap();
        drop(reopened);
        std::fs::remove_dir_all(path).unwrap();
    }
}
