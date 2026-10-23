#![cfg(feature = "server")]
use anyseek::record::RecordSearch;
#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};

// Aggregate removal must retain metadata dictionaries, filters and sort heaps.
use anyseek::server::api_types::{SearchRequestObject, CreateIndexRequest, GetDocumentRequest};
use anyseek::{
    index::{AccessType, Clustering, Document, DocumentCompression,
        FrequentwordType, IndexMetaObject,
        LexicalSimilarity, NgramSet, SchemaField, StopwordType,
        TokenizerType},

    vector::Inference,
};
use serde_json::{Value, json};
use std::{collections::HashSet, fs, path::Path, time::Duration};

fn schema() -> Vec<SchemaField> {
    serde_json::from_value(json!([
        {"field":"body","field_type":"Text","store":true,"index_lexical":true},
        {"field":"category","field_type":"String16","store":true,"index_lexical":false,"facet":true},
        {"field":"tags","field_type":"StringSet32","store":true,"index_lexical":false,"facet":true},
        {"field":"price","field_type":"F64","store":true,"index_lexical":false,"facet":true},
        {"field":"date","field_type":"Timestamp","store":true,"index_lexical":false,"facet":true}
    ])).unwrap()
}

async fn create(path: &Path, access_type: AccessType) -> RecordPartition {
    let _ = fs::remove_dir_all(path);
    let meta = IndexMetaObject { id:0, name:"metadata_contract".into(),
        lexical_similarity:LexicalSimilarity::Bm25f, tokenizer:TokenizerType::UnicodeAlphanumeric,
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
        stop_words:StopwordType::None, frequent_words:FrequentwordType::None,
        ngram_indexing:NgramSet::SingleTerm as u8, document_compression:DocumentCompression::Snappy,
        access_type, clustering:Clustering::None, inference:Inference::None };
    Ok::<_, String>(records::create(path, meta, (&schema()).to_vec()).await).unwrap()
}

async fn search(index: &RecordPartition, options: Value, realtime: bool) -> RecordSearch {
    let mut value = json!({"query":"common", "length":20, "sort":[{"field":"price","order":"Ascending"}]});
    value.as_object_mut().unwrap().extend(options.as_object().unwrap().clone());
    let result_type = value.as_object_mut().unwrap().remove("result_type")
        .map(|v| serde_json::from_value(v).unwrap()).unwrap_or(anyseek::search::ResultType::TopkCount);
    let r: SearchRequestObject = serde_json::from_value(value).unwrap();
    let result = index.query(RecordQuery {text:r.query_string.clone(),vector:None,query_type:r.query_type(),mode:r.search_mode(std::num::NonZeroUsize::new(anyseek::server::api_types::DEFAULT_NPROBE).unwrap()).unwrap(),offset:r.offset,length:r.length,result_type:result_type,fields:r.field_filter,filters:r.facet_filter,sort:r.result_sort,temporal:None}).await.unwrap();
    result
}

async fn check_prices(index: &RecordPartition, options: Value, realtime: bool, expected: &[f64]) {
    let result = search(index, options.clone(), realtime).await;
    assert_eq!(result.count_total, expected.len(), "{options}");
    let mut prices = Vec::new();
    for result in result.hits {
        let doc = result.record.document.as_ref().unwrap();
        prices.push(doc["price"].as_f64().unwrap());
    }
    assert_eq!(prices, expected, "{options}");
}

async fn check_metadata(index: &RecordPartition, realtime: bool) {
    check_prices(index,json!({}),realtime,&[10.,20.,30.,40.]).await;
    check_prices(index,json!({"query":""}),realtime,&[10.,20.,30.,40.]).await;
    check_prices(index,json!({"query":"alpha beta","operator":"or"}),realtime,&[10.,20.,30.,40.]).await;
    check_prices(index,json!({"query":"common alpha","operator":"and"}),realtime,&[10.,30.]).await;
    for (filter, expected) in [
        (json!({"String16":{"field":"category","filter":["a"]}}), vec![10.,30.]),
        (json!({"StringSet32":{"field":"tags","filter":["red"]}}), vec![10.,20.]),
        (json!({"F64":{"field":"price","filter":{"start":20.,"end":40.}}}), vec![20.,30.]),
        (json!({"Timestamp":{"field":"date","filter":{"start":200,"end":400}}}), vec![20.,30.]),
    ] {
        for query in ["common", "alpha beta", "north south", ""] {
            check_prices(index,json!({"query":query,"operator":"or","filter":[filter.clone()]}),realtime,&expected).await;
        }
    }
    check_prices(index,json!({"filter":[
        {"String16":{"field":"category","filter":["a"]}},
        {"F64":{"field":"price","filter":{"start":20.,"end":40.}}}
    ]}),realtime,&[30.]).await;
    check_prices(index,json!({"sort":[{"field":"date","order":"Descending"}]}),realtime,&[40.,30.,20.,10.]).await;
    check_prices(index,json!({"sort":[{"field":"category","order":"Ascending"},{"field":"price","order":"Descending"}]}),realtime,&[30.,10.,40.,20.]).await;
    // Cover both single-term-per-shard and multi-term-per-shard union paths.
    for query in ["alpha beta", "north south"] {
        for result_type in ["Count", "TopkCount"] {
            for filtered in [false, true] {
                let result = search(index,json!({"query":query,"operator":"or","result_type":result_type,
                    "filter": if filtered { json!([{"String16":{"field":"category","filter":["a"]}}]) } else { json!([]) }}),realtime).await;
                let per_bin = if filtered {1} else {2};
                assert_eq!(result.count_total,2 * per_bin,"{query} {result_type} realtime={realtime}");

            }
        }
    }
}

#[test]
fn retired_geo_requests_are_rejected() {
    assert!(serde_json::from_value::<CreateIndexRequest>(json!({"index_name":"geo","schema":[{"field":"location","field_type":"Point","store":true,"index_lexical":false,"facet":true}]})).is_err());
    for options in [
        json!({"distance_fields":[]}),
        json!({"filter":[{"Point":{"field":"location","filter":[[0.,0.],{"start":0.,"end":10.},"Kilometers"]}}]}),
        json!({"query_facets":[{"Point":{"field":"location","base":[0.,0.],"unit":"Kilometers","range_type":"CountWithinRange","ranges":[["near",0.]]}}]}),
        json!({"sort":[{"field":"date","order":"Ascending","base":"None"}]}),
        json!({"sort":[{"field":"location","order":"Ascending","base":{"Point":[0.,0.]}}]}),
    ] {
        let mut request = json!({"query":"common"});
        request.as_object_mut().unwrap().extend(options.as_object().unwrap().clone());
        assert!(serde_json::from_value::<SearchRequestObject>(request).is_err(),"{options}");
    }
    assert!(serde_json::from_value::<GetDocumentRequest>(json!({"distance_fields":[]})).is_err());
    assert!(serde_json::from_value::<GetDocumentRequest>(json!({"fields":"body"})).is_ok());
}

#[tokio::test]
async fn ordinary_metadata_survives_aggregate_removal() {
    for (name, access) in [("mmap",AccessType::Mmap)] {
        let path_buf = format!("tests/index_metadata_{name}"); let path = Path::new(&path_buf);
        let index = create(path, access).await;
        for doc in [
            json!({"body":"common alpha north","category":"a","tags":["green","blue"],"price":30.,"date":300}),
            json!({"body":"common beta north","category":"b","tags":["red"],"price":20.,"date":200}),
            json!({"body":"common alpha south","category":"a","tags":["red"],"price":10.,"date":100}),
            json!({"body":"common beta south","category":"b","tags":["blue"],"price":40.,"date":400}),
        ] {
            let doc: Document = serde_json::from_value(doc).unwrap();
            records::insert(&index, vec![doc]).await.unwrap();
        }
        for _ in 0..200 {
            if search(&index,json!({}),true).await.count_total==4 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        check_metadata(&index,true).await;
        index.compact_journal().await.unwrap();
        check_metadata(&index,false).await;
         drop(index);
        let reopened=RecordPartition::open(path, records::JOURNAL_LIMIT).await.unwrap();
        check_metadata(&reopened,false).await;
         drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn unknown_schema_type_is_rejected() {
    let mut fields=serde_json::to_value(schema()).unwrap();fields[0]["field_type"]=json!("Point");
    let error=serde_json::from_value::<Vec<SchemaField>>(fields).unwrap_err().to_string();
    assert!(error.contains("unknown variant") && error.contains("Point"));
}
#[tokio::test]
async fn legacy_index_headers_are_not_record_authority() {
    let root=tempfile::tempdir().unwrap();
    for bytes in [vec![6u8,0,0,0],vec![8,0,0,0],vec![],vec![8,0]] {
        let path=root.path().join("legacy");fs::create_dir(&path).unwrap();
        fs::write(path.join("index.bin"),&bytes).unwrap();
        assert!(RecordPartition::open(&path,records::JOURNAL_LIMIT).await.is_err());
        assert_eq!(fs::read(path.join("index.bin")).unwrap(),bytes);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn metadata_dictionary_has_no_statistics_slots() {
    use anyseek::index::FacetField;
    for value in [
        json!({"name":"tags","values":{"red":[["red"],42]}}),
        json!({"name":"price","values":{},"min":"None","max":"None"}),
    ] {
        assert!(serde_json::from_value::<FacetField>(value).is_err());
    }
    let value = json!({"name":"tags","values":{"red":["red"]}});
    let field: FacetField = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(field).unwrap(), value);
}
