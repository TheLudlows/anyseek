
#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
// Test crate: tests need to run sequentially in the defined order (in alphabetical order of method name), not in parallel (see RUST_TEST_THREADS = "1" in .cargo/config.toml).
// Use: cargo test
// To show output use: cargo test -- --show-output


use anyseek::index::{
    Clustering, FrequentwordType,
    IndexMetaObject, LexicalSimilarity, NgramSet,
    StopwordType, TokenizerType, };

use anyseek::search::{
    FacetFilter, QueryType, ResultSort, ResultType, SearchMode, SortOrder,
};
use anyseek::vector::{Embedding, Inference, Precision, Quantization};
use anyseek::vector_similarity::{AnnMode, VectorSimilarity};
use std::collections::HashSet;
use std::{fs, path::Path};

#[tokio::test]
/// create_index test
async fn test_01_create_index() {
    let index_path = Path::new("tests/index_test/");
    let _ = fs::remove_dir_all(index_path);

    let schema_json = r#"
    [{"field":"title","field_type":"Text","store":false,"index_lexical":false},
    {"field":"body","field_type":"Text","store":true,"index_lexical":true},
    {"field":"url","field_type":"Text","store":false,"index_lexical":false}]"#;
    let schema: Vec<anyseek::index::SchemaField> = serde_json::from_str(schema_json).unwrap();

    let meta = IndexMetaObject {
        name: "test_index".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::TantivySimpleV1,
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::English,
        ngram_indexing: NgramSet::NgramFF as u8 | NgramSet::NgramFFF as u8,
        clustering: Clustering::None,
        inference: Inference::None,
    };

    let segment_number_bits1 = 11;
    let index_arc = records::create(index_path, meta, (&schema).to_vec()).await;
    let result = index_arc.configuration().await.unwrap().index.name;
    assert_eq!(result, "test_index");

}

#[tokio::test]
/// index document
async fn test_02_index_document() {
    // open index
    let index_path = Path::new("tests/index_test/");
    let index_arc = RecordPartition::open(index_path, records::JOURNAL_LIMIT).await.unwrap();

    // index document
    let document_json = r#"
    {"title":"title1 test","body":"body1","url":"url1"}"#;
    let document = serde_json::from_str(document_json).unwrap();
    records::insert(&index_arc, vec![document]).await.unwrap();

    // index documents
    let documents_json = r#"
    [{"title":"title1 test","body":"body1","url":"url1"},
    {"title":"title2","body":"body2 test","url":"url2"},
    {"title":"title3 test","body":"body3 test","url":"url3"}]"#;
    let documents_vec = serde_json::from_str(documents_json).unwrap();
    records::insert(&index_arc, documents_vec).await.unwrap();

    // wait until all index threads are finished and commit
    index_arc.compact_journal().await.unwrap();

    let result = index_arc.statistics().await.unwrap().active_records;
    assert_eq!(result, 4);


}

#[tokio::test]
/// get iterator
async fn test_03_get_iterator() {
    // open index
    let index_path = Path::new("tests/index_test/");
    let index_arc = RecordPartition::open(index_path, records::JOURNAL_LIMIT).await.unwrap();

    let result = index_arc.statistics().await.unwrap().active_records;
    assert_eq!(result, 4);

    let first=index_arc.page(None,2).await.unwrap();
    assert_eq!(first.records.iter().map(|r|r.record_id.parse::<usize>().unwrap()).collect::<Vec<_>>(),vec![0,1]);
    let second=index_arc.page(first.next,2).await.unwrap();
    assert_eq!(second.records.iter().map(|r|r.record_id.parse::<usize>().unwrap()).collect::<Vec<_>>(),vec![2,3]);
    assert!(second.next.is_none());

}

#[tokio::test]
/// query index
async fn test_04_query_index() {
    // open index
    let index_path = Path::new("tests/index_test/");
    let index_arc = RecordPartition::open(index_path, records::JOURNAL_LIMIT).await.unwrap();

    let result = index_arc.statistics().await.unwrap().active_records;
    assert_eq!(result, 4);

    let query = "+body2 +test".into();
    let result_object = index_arc.query(RecordQuery {text:query,vector:None,query_type:QueryType::Intersection,mode:SearchMode::Lexical,offset:0,length:10,result_type:ResultType::TopkCount,fields:Vec::new(),filters:Vec::new(),sort:Vec::new(),temporal:None}).await.unwrap();

    let result = result_object.hits.len();
    assert_eq!(result, 1);

    let result = result_object.hits.len();
    assert_eq!(result, 1);

    let result = result_object.count_total;
    assert_eq!(result, 1);

    //

    let query = "test".into();
    let result_object = index_arc.query(RecordQuery {text:query,vector:None,query_type:QueryType::Union,mode:SearchMode::Lexical,offset:0,length:10,result_type:ResultType::Count,fields:Vec::new(),filters:Vec::new(),sort:Vec::new(),temporal:None}).await.unwrap();

    let result = result_object.hits.len();
    assert_eq!(result, 0);

    let result = result_object.hits.len();
    assert_eq!(result, 0);

    let result = result_object.count_total;
    assert_eq!(result, 2);


}

#[tokio::test]
/// empty query
async fn test_05_empty_query() {
    // open index
    let index_path = Path::new("tests/index_test/");
    let index_arc = RecordPartition::open(index_path, records::JOURNAL_LIMIT).await.unwrap();

    let result = index_arc.statistics().await.unwrap().active_records;
    assert_eq!(result, 4);

    // Record browse ties use stable identity ascending, as on the original Record path.

    let result_object = index_arc.query(RecordQuery {text:"".into(),vector:None,query_type:QueryType::Intersection,mode:SearchMode::Lexical,offset:0,length:10,result_type:ResultType::TopkCount,fields:Vec::new(),filters:Vec::new(),sort:Vec::new(),temporal:None}).await.unwrap();

    let result = result_object.hits.first().unwrap().record.record_id.parse::<usize>().unwrap();
    assert_eq!(result, 0);

    let result = result_object.hits.len();
    assert_eq!(result, 4);

    let result = result_object.hits.len();
    assert_eq!(result, 4);

    let result = result_object.count_total;
    assert_eq!(result, 4);

    // descending

    let result_sort = vec![ResultSort {
        field: "_id".into(),
        order: SortOrder::Descending,
    }];

    let result_object = index_arc.query(RecordQuery {text:"".into(),vector:None,query_type:QueryType::Union,mode:SearchMode::Lexical,offset:0,length:10,result_type:ResultType::TopkCount,fields:Vec::new(),filters:Vec::new(),sort:result_sort,temporal:None}).await.unwrap();

    let result = result_object.hits.first().unwrap().record.record_id.parse::<usize>().unwrap();
    assert_eq!(result, 3);

    let result = result_object.hits.len();
    assert_eq!(result, 4);

    let result = result_object.hits.len();
    assert_eq!(result, 4);

    let result = result_object.count_total;
    assert_eq!(result, 4);

    // ascending

    let result_sort = vec![ResultSort {
        field: "_id".into(),
        order: SortOrder::Ascending,
    }];

    let result_object = index_arc.query(RecordQuery {text:"".into(),vector:None,query_type:QueryType::Union,mode:SearchMode::Lexical,offset:0,length:10,result_type:ResultType::TopkCount,fields:Vec::new(),filters:Vec::new(),sort:result_sort,temporal:None}).await.unwrap();

    let result = result_object.hits.first().unwrap().record.record_id.parse::<usize>().unwrap();
    assert_eq!(result, 0);

    let result = result_object.hits.len();
    assert_eq!(result, 4);

    let result = result_object.hits.len();
    assert_eq!(result, 4);

    let result = result_object.count_total;
    assert_eq!(result, 4);


}

#[tokio::test]
/// clear index
async fn test_06_clear_index() {
    // open index
    let index_path = Path::new("tests/index_test/");
    let index_arc = RecordPartition::open(index_path, records::JOURNAL_LIMIT).await.unwrap();

    let result = index_arc.statistics().await.unwrap().active_records;
    assert_eq!(result, 4);

    // clear index
    records::clear(&index_arc).await;

    let result = index_arc.statistics().await.unwrap().active_records;
    assert_eq!(result, 0);

    // index document
    let document_json = r#"
    {"title":"title1 test","body":"body1","url":"url1"}"#;
    let document = serde_json::from_str(document_json).unwrap();
    records::insert(&index_arc, vec![document]).await.unwrap();

    // wait until all index threads are finished and commit
    index_arc.compact_journal().await.unwrap();

    let result = index_arc.statistics().await.unwrap().active_records;
    assert_eq!(result, 1);

    // query index
    let query = "body1".into();
    let result_object = index_arc.query(RecordQuery {text:query,vector:None,query_type:QueryType::Union,mode:SearchMode::Lexical,offset:0,length:10,result_type:ResultType::TopkCount,fields:Vec::new(),filters:Vec::new(),sort:Vec::new(),temporal:None}).await.unwrap();

    let result = result_object.count_total;
    assert_eq!(result, 1);


}

#[tokio::test]
/// get document
async fn test_07_get_document() {
    // open index
    let index_path = Path::new("tests/index_test/");
    let index_arc = RecordPartition::open(index_path, records::JOURNAL_LIMIT).await.unwrap();

    let result = index_arc.statistics().await.unwrap().active_records;
    assert_eq!(result, 1);
    let row=index_arc.page(None,1).await.unwrap();
    let doc=row.records[0].document.as_ref().unwrap();
    let value = doc.get("body").unwrap().to_owned();
    let result = serde_json::from_value::<String>(value).unwrap();

    assert_eq!(result, "body1");

}

#[tokio::test]
/// delete document
async fn test_08_delete_document() {
    // open index
    let index_path = Path::new("tests/index_test/");
    let index_arc = RecordPartition::open(index_path, records::JOURNAL_LIMIT).await.unwrap();

    let result = index_arc.statistics().await.unwrap().active_records;
    assert_eq!(result, 1);

    // query index before delete
    let query = "body1".into();
    let result_object = index_arc.query(RecordQuery {text:query,vector:None,query_type:QueryType::Union,mode:SearchMode::Lexical,offset:0,length:10,result_type:ResultType::TopkCount,fields:Vec::new(),filters:Vec::new(),sort:Vec::new(),temporal:None}).await.unwrap();

    let result = result_object.count_total;
    assert_eq!(result, 1);

    // delete document
    records::delete(&index_arc, vec![result_object.hits[0].record.record_id.parse::<usize>().unwrap()])
        .await;

    // query index after delete
    let query = "body1".into();
    let result_object = index_arc.query(RecordQuery {text:query,vector:None,query_type:QueryType::Union,mode:SearchMode::Lexical,offset:0,length:10,result_type:ResultType::TopkCount,fields:Vec::new(),filters:Vec::new(),sort:Vec::new(),temporal:None}).await.unwrap();

    let result = result_object.count_total;
    assert_eq!(result, 0);

    let result = index_arc.statistics().await.unwrap().active_records;
    assert_eq!(result, 0);
}

//#### vector search: caller-supplied embeddings

#[tokio::test]
/// create_index test
async fn test_09_create_index_vector_supplied() {
    let index_path = Path::new("tests/index_test/");
    let _ = fs::remove_dir_all(index_path);

    let schema_json = r#"
    [{"field":"vector","field_type":"Json","store":false,"index_lexical":false,"index_vector":true},
    {"field":"body","field_type":"Text","store":true,"index_lexical":true,"index_vector":false},
    {"field":"url","field_type":"Text","store":false,"index_lexical":false,"index_vector":false}]"#;
    let schema: Vec<anyseek::index::SchemaField> = serde_json::from_str(schema_json).unwrap();

    let meta = IndexMetaObject {
        name: "test_index".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::TantivySimpleV1,
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::English,
        ngram_indexing: NgramSet::SingleTerm as u8,
        clustering: Clustering::None,
        inference: Inference::External {
            dimensions: 3,
            precision: Precision::F32,
            similarity: VectorSimilarity::Cosine,
            quantization: Quantization::None,
        },
    };

    let segment_number_bits1 = 11;
    let index_arc = records::create(index_path, meta, (&schema).to_vec()).await;
    let result = index_arc.configuration().await.unwrap().index.name;
    assert_eq!(result, "test_index");

}

#[tokio::test]
/// index document
async fn test_10_index_document_vector_supplied() {
    // open index
    let index_path = Path::new("tests/index_test/");
    let index_arc = RecordPartition::open(index_path, records::JOURNAL_LIMIT).await.unwrap();

    // index documents
    let documents_json = r#"
    [{"vector":[1,0,0],"body":"animal from a comedy","url":"url1"},
    {"vector":[0,1,0],"body":"largest mammal in the ocean","url":"url2"},
    {"vector":[0,0,1],"body":"small carnivorous mammal","url":"url3"}]"#;
    let documents_vec = serde_json::from_str(documents_json).unwrap();
    records::insert(&index_arc, documents_vec).await.unwrap();

    // wait until all index threads are finished and commit
    index_arc.compact_journal().await.unwrap();

    let result = index_arc.statistics().await.unwrap().active_records;
    assert_eq!(result, 3);


}

#[tokio::test]
/// query index
async fn test_11_query_index_vector_supplied() {
    // open index
    let index_path = Path::new("tests/index_test/");
    let index_arc = RecordPartition::open(index_path, records::JOURNAL_LIMIT).await.unwrap();

    let result = index_arc.statistics().await.unwrap().active_records;
    assert_eq!(result, 3);

    let query = "rosy panther".into();
    let result_object = index_arc.query(RecordQuery {text:query,vector:Some(Embedding::F32(vec![1.0, 0.0, 0.0])),query_type:QueryType::Union,mode:SearchMode::Vector {
                similarity_threshold: Some(0.7),
                ann_mode: AnnMode::All,
            },offset:0,length:10,result_type:ResultType::TopkCount,fields:Vec::new(),filters:Vec::new(),sort:Vec::new(),temporal:None}).await.unwrap();

    let result = result_object.hits.len();
    assert_eq!(result, 1);

    let result = result_object.hits.len();
    assert_eq!(result, 1);

    let result = result_object.count_total;
    assert_eq!(result, 1);
}

//#### vector search: external inference

#[tokio::test]
/// create_index test
async fn test_12_create_index_vector_external() {
    let index_path = Path::new("tests/index_test/");
    let _ = fs::remove_dir_all(index_path);

    let schema_json = r#"
    [{"field":"vector","field_type":"Json","store":false,"index_lexical":false,"index_vector":true},
    {"field":"index","field_type":"Text","store":true,"index_lexical":false,"index_vector":false}]"#;
    let schema: Vec<anyseek::index::SchemaField> = serde_json::from_str(schema_json).unwrap();

    let meta = IndexMetaObject {
        name: "test_index".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::TantivySimpleV1,
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::English,
        ngram_indexing: NgramSet::SingleTerm as u8,
        clustering: Clustering::None,
        inference: Inference::External {
            dimensions: 128,
            precision: Precision::F32,
            quantization: Quantization::None,
            similarity: VectorSimilarity::Euclidean,
        },
    };

    let segment_number_bits1 = 11;
    let index_arc = records::create(index_path, meta, (&schema).to_vec()).await;
    let result = index_arc.configuration().await.unwrap().index.name;
    assert_eq!(result, "test_index");

}

#[tokio::test]
/// index document
async fn test_13_index_document_vector_external() {
    // open index
    let index_path = Path::new("tests/index_test/");
    let index_arc = RecordPartition::open(index_path, records::JOURNAL_LIMIT).await.unwrap();

    // index documents
    let documents_json = r#"
    [{"vector":[0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.009, 0.010, 0.011, 0.012, 0.013, 0.014, 0.015, 0.016, 0.017, 0.018, 0.019, 0.020, 0.021, 0.022, 0.023, 0.024, 0.025, 0.026, 0.027, 0.028, 0.029, 0.030, 0.031, 0.032, 0.033, 0.034, 0.035, 0.036, 0.037, 0.038, 0.039, 0.040, 0.041, 0.042, 0.043, 0.044, 0.045, 0.046, 0.047, 0.048, 0.049, 0.050, 0.051, 0.052, 0.053, 0.054, 0.055, 0.056, 0.057, 0.058, 0.059, 0.060, 0.061, 0.062, 0.063, 0.064, 0.065, 0.066, 0.067, 0.068, 0.069, 0.070, 0.071, 0.072, 0.073, 0.074, 0.075, 0.076, 0.077, 0.078, 0.079, 0.080, 0.081, 0.082, 0.083, 0.084, 0.085, 0.086, 0.087, 0.088, 0.089, 0.090, 0.091, 0.092, 0.093, 0.094, 0.095, 0.096, 0.097, 0.098, 0.099, 0.100, 0.101, 0.102, 0.103, 0.104, 0.105, 0.106, 0.107, 0.108, 0.109, 0.110, 0.111, 0.112, 0.113, 0.114, 0.115, 0.116, 0.117, 0.118, 0.119, 0.120, 0.121, 0.122, 0.123, 0.124, 0.125, 0.126, 0.127, 0.128],"index":"0"},
    {"vector":[0.129, 0.130, 0.131, 0.132, 0.133, 0.134, 0.135, 0.136, 0.137, 0.138, 0.139, 0.140, 0.141, 0.142, 0.143, 0.144, 0.145, 0.146, 0.147, 0.148, 0.149, 0.150, 0.151, 0.152, 0.153, 0.154, 0.155, 0.156, 0.157, 0.158, 0.159, 0.160, 0.161, 0.162, 0.163, 0.164, 0.165, 0.166, 0.167, 0.168, 0.169, 0.170, 0.171, 0.172, 0.173, 0.174, 0.175, 0.176, 0.177, 0.178, 0.179, 0.180, 0.181, 0.182, 0.183, 0.184, 0.185, 0.186, 0.187, 0.188, 0.189, 0.190, 0.191, 0.192, 0.193, 0.194, 0.195, 0.196, 0.197, 0.198, 0.199, 0.200, 0.201, 0.202, 0.203, 0.204, 0.205, 0.206, 0.207, 0.208, 0.209, 0.210, 0.211, 0.212, 0.213, 0.214, 0.215, 0.216, 0.217, 0.218, 0.219, 0.220, 0.221, 0.222, 0.223, 0.224, 0.225, 0.226, 0.227, 0.228, 0.229, 0.230, 0.231, 0.232, 0.233, 0.234, 0.235, 0.236, 0.237, 0.238, 0.239, 0.240, 0.241, 0.242, 0.243, 0.244, 0.245, 0.246, 0.247, 0.248, 0.249, 0.250, 0.251, 0.252, 0.253, 0.254, 0.255, 0.256],"index":"1"},
    {"vector":[0.257, 0.258, 0.259, 0.260, 0.261, 0.262, 0.263, 0.264, 0.265, 0.266, 0.267, 0.268, 0.269, 0.270, 0.271, 0.272, 0.273, 0.274, 0.275, 0.276, 0.277, 0.278, 0.279, 0.280, 0.281, 0.282, 0.283, 0.284, 0.285, 0.286, 0.287, 0.288, 0.289, 0.290, 0.291, 0.292, 0.293, 0.294, 0.295, 0.296, 0.297, 0.298, 0.299, 0.300, 0.301, 0.302, 0.303, 0.304, 0.305, 0.306, 0.307, 0.308, 0.309, 0.310, 0.311, 0.312, 0.313, 0.314, 0.315, 0.316, 0.317, 0.318, 0.319, 0.320, 0.321, 0.322, 0.323, 0.324, 0.325, 0.326, 0.327, 0.328, 0.329, 0.330, 0.331, 0.332, 0.333, 0.334, 0.335, 0.336, 0.337, 0.338, 0.339, 0.340, 0.341, 0.342, 0.343, 0.344, 0.345, 0.346, 0.347, 0.348, 0.349, 0.350, 0.351, 0.352, 0.353, 0.354, 0.355, 0.356, 0.357, 0.358, 0.359, 0.360, 0.361, 0.362, 0.363, 0.364, 0.365, 0.366, 0.367, 0.368, 0.369, 0.370, 0.371, 0.372, 0.373, 0.374, 0.375, 0.376, 0.377, 0.378, 0.379, 0.380, 0.381, 0.382, 0.383, 0.384],"index":"2"}]"#;
    let documents_vec = serde_json::from_str(documents_json).unwrap();
    records::insert(&index_arc, documents_vec).await.unwrap();

    // wait until all index threads are finished and commit
    index_arc.compact_journal().await.unwrap();

    let result = index_arc.statistics().await.unwrap().active_records;
    assert_eq!(result, 3);


}

#[tokio::test]
/// query index
async fn test_14_query_index_vector_external() {
    // open index
    let index_path = Path::new("tests/index_test/");
    let index_arc = RecordPartition::open(index_path, records::JOURNAL_LIMIT).await.unwrap();

    let result = index_arc.statistics().await.unwrap().active_records;
    assert_eq!(result, 3);

    let query = String::new();
    let query_vector = vec![
        0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.009, 0.010, 0.011, 0.012, 0.013,
        0.014, 0.015, 0.016, 0.017, 0.018, 0.019, 0.020, 0.021, 0.022, 0.023, 0.024, 0.025, 0.026,
        0.027, 0.028, 0.029, 0.030, 0.031, 0.032, 0.033, 0.034, 0.035, 0.036, 0.037, 0.038, 0.039,
        0.040, 0.041, 0.042, 0.043, 0.044, 0.045, 0.046, 0.047, 0.048, 0.049, 0.050, 0.051, 0.052,
        0.053, 0.054, 0.055, 0.056, 0.057, 0.058, 0.059, 0.060, 0.061, 0.062, 0.063, 0.064, 0.065,
        0.066, 0.067, 0.068, 0.069, 0.070, 0.071, 0.072, 0.073, 0.074, 0.075, 0.076, 0.077, 0.078,
        0.079, 0.080, 0.081, 0.082, 0.083, 0.084, 0.085, 0.086, 0.087, 0.088, 0.089, 0.090, 0.091,
        0.092, 0.093, 0.094, 0.095, 0.096, 0.097, 0.098, 0.099, 0.100, 0.101, 0.102, 0.103, 0.104,
        0.105, 0.106, 0.107, 0.108, 0.109, 0.110, 0.111, 0.112, 0.113, 0.114, 0.115, 0.116, 0.117,
        0.118, 0.119, 0.120, 0.121, 0.122, 0.123, 0.124, 0.125, 0.126, 0.127, 0.128,
    ];
    let query_embedding = Embedding::F32(query_vector);
    let result_object = index_arc.query(RecordQuery {text:query,vector:Some(query_embedding),query_type:QueryType::Union,mode:SearchMode::Vector {
                similarity_threshold: None,
                ann_mode: AnnMode::All,
            },offset:0,length:10,result_type:ResultType::TopkCount,fields:Vec::new(),filters:Vec::new(),sort:Vec::new(),temporal:None}).await.unwrap();

    let result = result_object.hits.len();
    assert_eq!(result, 3);

    let result = result_object.hits.len();
    assert_eq!(result, 3);

    let result = result_object.count_total;
    assert_eq!(result, 3);
}

#[tokio::test]
/// Filtering and result counts remain correct across shards after aggregate removal.
async fn test_15_filter_scope() {
    let index_path = Path::new("tests/index_facet_test/");
    let _ = fs::remove_dir_all(index_path);

    let schema_json = r#"
    [{"field":"id","field_type":"String","store":true,"index_lexical":false},
    {"field":"text","field_type":"Text","store":true,"index_lexical":true},
    {"field":"kind","field_type":"String","store":true,"index_lexical":false,"facet":true}]"#;
    let schema: Vec<anyseek::index::SchemaField> = serde_json::from_str(schema_json).unwrap();

    let meta = IndexMetaObject {
        name: "test_facet_index".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::TantivySimpleV1,
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: NgramSet::SingleTerm as u8,
        clustering: Clustering::None,
        inference: Inference::None,
    };

    // Public Record fixture: three stable identities with two facet values.
    let index_arc = records::create(index_path, meta, (&schema).to_vec()).await;

    let documents_json = r#"
    [{"id":"a","text":"alpha only","kind":"invoice"},
    {"id":"b","text":"alpha and bravo","kind":"invoice"},
    {"id":"c","text":"charlie only","kind":"minutes"}]"#;
    let documents_vec = serde_json::from_str(documents_json).unwrap();
    records::insert(&index_arc, documents_vec).await.unwrap();
    index_arc.compact_journal().await.unwrap();

    let result = index_arc.statistics().await.unwrap().active_records;
    assert_eq!(result, 3);

    async fn kind_results(
        index_arc: &RecordPartition,
        query: &str,
        facet_filter: Vec<FacetFilter>,
    ) -> (usize, Vec<String>) {
        let result_object = index_arc.query(RecordQuery {text:query.into(),vector:None,query_type:QueryType::Intersection,mode:SearchMode::Lexical,offset:0,length:10,result_type:ResultType::TopkCount,fields:Vec::new(),filters:facet_filter,sort:Vec::new(),temporal:None}).await.unwrap();

        let mut ids = Vec::new();
        for result in &result_object.hits {
            let doc = result.record.document.as_ref().unwrap();
            ids.push(doc["id"].as_str().unwrap().to_owned());
        }
        ids.sort();
        (result_object.count_total, ids)
    }

    let (count, facets) = kind_results(&index_arc, "alpha", Vec::new()).await;
    assert_eq!(count, 2);
    assert_eq!(facets, vec!["a".to_string(), "b".to_string()]);

    let (count, facets) = kind_results(&index_arc, "charlie", Vec::new()).await;
    assert_eq!(count, 1);
    assert_eq!(facets, vec!["c".to_string()]);

    let (count, facets) = kind_results(&index_arc, "zulu", Vec::new()).await;
    assert_eq!(count, 0);
    assert!(facets.is_empty());

    let (count, facets) = kind_results(&index_arc, "", Vec::new()).await;
    assert_eq!(count, 3);
    assert_eq!(
        facets,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );

    let facet_filter = vec![FacetFilter::String {
        field: "kind".into(),
        filter: vec!["invoice".into()],
    }];
    let (count, facets) = kind_results(&index_arc, "", facet_filter).await;
    assert_eq!(count, 2);
    assert_eq!(facets, vec!["a".to_string(), "b".to_string()]);

    // Record always enables empty browsing; an unmatched filter still yields no hits.
    let (count, ids) = kind_results(&index_arc, "", vec![FacetFilter::String {
        field: "kind".into(), filter: vec!["missing".into()],
    }]).await;
    assert_eq!(count, 0);
    assert!(ids.is_empty());
    let page = index_arc.query(RecordQuery {text:"".into(),vector:None,query_type:QueryType::Intersection,mode:SearchMode::Lexical,offset:1,length:1,result_type:ResultType::TopkCount,fields:vec![],filters:vec![FacetFilter::String {field:"kind".into(),filter:vec!["invoice".into()]}],sort:vec![],temporal:None}).await.unwrap();
    assert_eq!(page.count_total, 2);
    assert_eq!(page.hits.len(), 1);
    assert_eq!(page.hits[0].record.document.as_ref().unwrap()["id"], "b");


    let _ = fs::remove_dir_all(index_path);
}
