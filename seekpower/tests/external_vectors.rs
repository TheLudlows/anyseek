//! External embedding contract: reject invalid inputs before writes or vector search.
use seekpower::{
    commit::Commit,
    index::{create_index, open_index, AccessType, Close, Clustering, Document, DocumentCompression,
        FrequentwordType, IndexArc, IndexDocument, IndexDocuments, IndexMetaObject,
        LexicalSimilarity, NgramSet, SchemaField, StopwordType, TokenizerType, UpdateDocument, UpdateDocuments},
    search::{QueryType, ResultObject, ResultType, Search, SearchMode},
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
        tokenizer:TokenizerType::UnicodeAlphanumeric, stop_words:StopwordType::None, frequent_words:FrequentwordType::None,
        ngram_indexing:NgramSet::SingleTerm as u8, document_compression:DocumentCompression::Snappy,
        access_type:AccessType::Mmap, clustering:Clustering::None, inference:inference() }
}
async fn query(index: &IndexArc, mode: SearchMode, vector: Option<Embedding>) -> ResultObject {
    index.search("alpha".into(), vector, QueryType::Union, mode, false, 0, 10,
        ResultType::TopkCount, false, vec![],  vec![], vec![],).await
}
#[test]
fn external_config_and_numeric_validation() {
    assert!(validate_vector_schema(&inference(), &schema()).is_ok());
    assert!(validate_vector_schema(&Inference::None, &schema()).is_err());
    let mut text = schema(); text[1].field_type = seekpower::index::FieldType::Text;
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
    let index = create_index(path, meta(), &schema(), 11, true, Some(1)).await.unwrap();
    let missing: Document = serde_json::from_value(json!({"body":"missing"})).unwrap();
    assert!(index.index_document(missing.clone()).await.is_err());
    assert!(index.index_documents(vec![document("partial",json!([1,0,0])), missing.clone()]).await.is_err());
    index.commit().await;
    assert_eq!(index.read().await.indexed_doc_count().await, 0);
    index.index_documents(vec![document("alpha",json!([1,0,0])),document("beta",json!([0,1,0]))]).await.unwrap();
    index.commit().await;
    assert!(index.update_document((0,missing.clone())).await.is_err());
    assert!(index.update_documents(vec![(0,document("changed",json!([0,0,1]))),(1,missing)]).await.is_err());
    index.commit().await;
    assert_eq!(query(&index,SearchMode::Lexical,None).await.results.len(),1);
    for mode in [SearchMode::Vector { similarity_threshold:None, ann_mode:AnnMode::All },
                 SearchMode::Hybrid { similarity_threshold:None, ann_mode:AnnMode::All }] {
        assert!(query(&index,mode.clone(),None).await.error.is_some());
        assert!(query(&index,mode.clone(),Some(Embedding::F32(vec![1.0]))).await.error.is_some());
        let result = query(&index,mode,Some(Embedding::F32(vec![1.0,0.0,0.0]))).await;
        assert!(result.error.is_none(),"{:?}",result.error);
        assert_eq!(result.results[0].doc_id,0);
    }
    index.close().await; drop(index);
    let reopened = open_index(path).await.unwrap();
    let result = query(&reopened,SearchMode::Vector { similarity_threshold:Some(0.7),ann_mode:AnnMode::All },
        Some(Embedding::F32(vec![1.0,0.0,0.0]))).await;
    assert!(result.error.is_none()); assert_eq!(result.results.len(),1); assert_eq!(result.results[0].doc_id,0);
    reopened.close().await; drop(reopened);
    let meta_path = path.join("index.json");
    let mut old_meta: serde_json::Value = serde_json::from_slice(&fs::read(&meta_path).unwrap()).unwrap();
    old_meta["inference"] = json!({"Model2Vec":{"model":"PotionBase2M","chunk_size":1000,"quantization":"None"}});
    fs::write(meta_path, serde_json::to_vec(&old_meta).unwrap()).unwrap();
    assert!(open_index(path).await.err().unwrap().contains("unsupported or invalid index metadata"));
    fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn external_f32_and_quantized_thresholds() {
    for similarity in [VectorSimilarity::Cosine, VectorSimilarity::Dot, VectorSimilarity::Euclidean] {
    for dimensions in [3, 32] {
    for quantization in [Quantization::None, Quantization::ScalarQuantizationI8, Quantization::TurboQuantI8] {
        let path = std::env::temp_dir().join(format!("seekpower-threshold-{}-{quantization}-{dimensions}-{similarity}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        let mut config = meta();
        if let Inference::External { quantization: q, dimensions: d, similarity: metric, .. } = &mut config.inference { *q = quantization; *d = dimensions; *metric = similarity; }
        let mut alpha = vec![0.0; dimensions]; alpha[0] = 1.0;
        let mut beta = vec![0.0; dimensions]; beta[1] = 1.0;
        let index = create_index(&path, config, &schema(), 11, true, Some(1)).await.unwrap();
        index.index_documents(vec![document("alpha",json!(alpha.clone())), document("beta",json!(beta))]).await.unwrap();
        index.commit().await;
        for ann_mode in [AnnMode::All, AnnMode::Nprobe(1)] {
            let result = query(&index,SearchMode::Vector {similarity_threshold:Some(0.7),ann_mode},
                Some(Embedding::F32(alpha.clone()))).await;
            assert!(result.error.is_none());
            assert_eq!(result.results.len(),1,"{similarity} {quantization} dimensions={dimensions}");
            assert_eq!(result.results[0].doc_id,0,"{similarity} {quantization} dimensions={dimensions}");
        }
        index.close().await; drop(index); fs::remove_dir_all(path).unwrap();
    }
}
}
}
