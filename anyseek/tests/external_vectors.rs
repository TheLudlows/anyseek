use anyseek::record::RecordSearch;
#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
// External embedding contract: reject invalid inputs before writes or vector search.
use anyseek::{
    index::{AccessType, Clustering, Document, DocumentCompression,
        FrequentwordType, IndexMetaObject,
        LexicalSimilarity, NgramSet, SchemaField, StopwordType, TokenizerType},
    search::{QueryType,  ResultType, SearchMode},
    vector::{embedding_to_bytes_be, parse_external_embedding, validate_embedding, validate_vector_schema,
        Embedding, Inference, Precision, Quantization},
    vector_similarity::{AnnMode, VectorSimilarity},
    utils::encode_bytes_to_base64_string,
};
use serde_json::json;
use std::{fs, path::Path};

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
        tokenizer:TokenizerType::TantivySimpleV1,
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared, stop_words:StopwordType::None, frequent_words:FrequentwordType::None,
        ngram_indexing:NgramSet::SingleTerm as u8, document_compression:DocumentCompression::Snappy,
        access_type:AccessType::Mmap, clustering:Clustering::None, inference:inference() }
}
async fn try_query(index: &RecordPartition, mode: SearchMode, vector: Option<Embedding>) -> Result<RecordSearch, anyseek::record::RecordError> {
    index.query(RecordQuery {text:"alpha".into(),vector:vector,query_type:QueryType::Union,mode:mode,offset:0,length:10,result_type:ResultType::TopkCount,fields:vec![],filters:vec![],sort:vec![],temporal:None}).await
}
async fn query(index: &RecordPartition, mode: SearchMode, vector: Option<Embedding>) -> RecordSearch {try_query(index,mode,vector).await.unwrap()}
#[test]
fn external_config_and_numeric_validation() {
    assert!(validate_vector_schema(&inference(), &schema()).is_ok());
    assert!(validate_vector_schema(&Inference::None, &schema()).is_err());
    let mut text = schema(); text[1].field_type = anyseek::index::FieldType::Text;
    assert!(validate_vector_schema(&inference(), &text).is_err());
    for config in [
        json!({"Model2Vec":{"model":"PotionBase2M","chunk_size":1000}}),
        json!({"External":{"dimensions":3,"precision":"F32","quantization":"None","similarity":"invalid"}}),
        json!({"External":{"dimensions":3,"precision":"F32","quantization":"None","similarity":"Cosine","chunk_size":1000}})
    ] { assert!(serde_json::from_value::<Inference>(config).is_err()); }
    for dimensions in [0, usize::MAX] {
        let mut config = inference(); if let Inference::External { dimensions: d, .. } = &mut config { *d = dimensions; }
        assert!(validate_vector_schema(&config, &schema()).is_err());
    }
    for vector in [vec![0.0;3], vec![1.0;2], vec![f32::NAN,0.0,1.0], vec![f32::INFINITY,0.0,1.0], vec![f32::MAX;3]] {
        assert!(validate_embedding(&inference(), &Embedding::F32(vector.clone())).is_err());
        let encoded = encode_bytes_to_base64_string(&embedding_to_bytes_be(&Embedding::F32(vector)));
        assert!(parse_external_embedding(&json!(encoded), &inference()).is_err());
    }
    assert!(validate_embedding(&inference(), &Embedding::I8(vec![1,0,0])).is_err());
    for value in [json!([1,2]), json!([1,"x",3]), json!("!bad-base64"), json!(null)] {
        assert!(parse_external_embedding(&value, &inference()).is_err());
    }
    let encoded = encode_bytes_to_base64_string(&embedding_to_bytes_be(&Embedding::F32(vec![1.0,0.0,0.0])));
    assert!(parse_external_embedding(&json!(encoded), &inference()).is_ok());
}
#[tokio::test]
async fn external_write_query_reopen_contract() {
    let path = Path::new("tests/index_external_contract"); let _ = fs::remove_dir_all(path);
    let index = Ok::<_, String>(records::create(path, meta(), (&schema()).to_vec()).await).unwrap();
    let missing: Document = serde_json::from_value(json!({"body":"invalid vector","vector":[1,2]})).unwrap();
    assert!(records::insert(&index, vec![missing.clone()]).await.is_err());
    // Record writes are individually atomic; no retired batch API emulation.
    index.compact_journal().await.unwrap();
    assert_eq!(index.statistics().await.unwrap().physical_versions, 0);
    records::insert(&index, vec![document("alpha",json!([1,0,0])),document("beta",json!([0,1,0]))]).await.unwrap();
    index.compact_journal().await.unwrap();
    let old=index.get("00000000000000000000").await.unwrap().unwrap();
    assert!(index.write(anyseek::record::RecordWrite {
        operation_id:anyseek::record::OperationId::new("invalid-update".into()),record_id:old.record_id.clone(),
        routing_key:old.routing_key.clone(),expected:anyseek::record::ExpectedVersion::Match(old.version),
        document:Some(missing),vector_patch:None,generation:None,temporal:None,system:None,
    }).await.is_err());
    assert_eq!(index.get(&old.record_id).await.unwrap().unwrap().version,old.version);
    index.compact_journal().await.unwrap();
    assert_eq!(query(&index,SearchMode::Lexical,None).await.hits.len(),1);
    for mode in [SearchMode::Vector { similarity_threshold:None, ann_mode:AnnMode::All },
                 SearchMode::Hybrid { similarity_threshold:None, ann_mode:AnnMode::All }] {
        assert!(try_query(&index,mode.clone(),None).await.is_err());
        assert!(try_query(&index,mode.clone(),Some(Embedding::F32(vec![1.0]))).await.is_err());
        let result = query(&index,mode,Some(Embedding::F32(vec![1.0,0.0,0.0]))).await;
        assert_eq!(result.hits[0].record.record_id.parse::<usize>().unwrap(),0);
    }
    let pending: Document = serde_json::from_value(json!({"body":"alpha pending"})).unwrap();
    records::insert(&index, vec![pending]).await.unwrap(); index.compact_journal().await.unwrap();
    assert_eq!(query(&index,SearchMode::Lexical,None).await.hits.len(),2);
     drop(index);
    let reopened = RecordPartition::open(path, records::JOURNAL_LIMIT).await.unwrap();
    let result = query(&reopened,SearchMode::Vector { similarity_threshold:Some(0.7),ann_mode:AnnMode::All },
        Some(Embedding::F32(vec![1.0,0.0,0.0]))).await; assert_eq!(result.hits.len(),1); assert_eq!(result.hits[0].record.record_id.parse::<usize>().unwrap(),0);
     drop(reopened);
    let mut old_meta=serde_json::to_value(meta()).unwrap();
    old_meta["inference"] = json!({"Model2Vec":{"model":"PotionBase2M","chunk_size":1000,"quantization":"None"}});
    assert!(serde_json::from_value::<IndexMetaObject>(old_meta).is_err());
    fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn external_f32_and_quantized_thresholds() {
    for similarity in [VectorSimilarity::Cosine, VectorSimilarity::Dot, VectorSimilarity::Euclidean] {
    for dimensions in [3, 32] {
    for quantization in [Quantization::None, Quantization::ScalarQuantizationI8, Quantization::TurboQuantI8] {
        let path = std::env::temp_dir().join(format!("anyseek-threshold-{}-{quantization}-{dimensions}-{similarity}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        let mut config = meta();
        if let Inference::External { quantization: q, dimensions: d, similarity: metric, .. } = &mut config.inference { *q = quantization; *d = dimensions; *metric = similarity; }
        let mut alpha = vec![0.0; dimensions]; alpha[0] = 1.0;
        let mut beta = vec![0.0; dimensions]; beta[1] = 1.0;
        let index = Ok::<_, String>(records::create(&path, config, (&schema()).to_vec()).await).unwrap();
        records::insert(&index, vec![document("alpha",json!(alpha.clone())), document("beta",json!(beta))]).await.unwrap();
        index.compact_journal().await.unwrap();
        for ann_mode in [AnnMode::All, AnnMode::Nprobe(1)] {
            let result = query(&index,SearchMode::Vector {similarity_threshold:Some(0.7),ann_mode},
                Some(Embedding::F32(alpha.clone()))).await;
            assert_eq!(result.hits.len(),1,"{similarity} {quantization} dimensions={dimensions}");
            assert_eq!(result.hits[0].record.record_id.parse::<usize>().unwrap(),0,"{similarity} {quantization} dimensions={dimensions}");
        }
         drop(index); fs::remove_dir_all(path).unwrap();
    }
}
}
}
