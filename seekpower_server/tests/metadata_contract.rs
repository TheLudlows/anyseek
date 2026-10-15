//! Aggregate removal must retain metadata dictionaries, filters and sort heaps.
use seekpower_server::api_types::{SearchRequestObject, CreateIndexRequest, GetDocumentRequest};
use seekpower::{
    commit::Commit,
    index::{AccessType, Close, Clustering, Document, DocumentCompression,
        FrequentwordType, IndexArc, IndexDocument, IndexMetaObject,
        LexicalSimilarity, NgramSet, SchemaField, StopwordType,
        TokenizerType, create_index, open_index},
    search::{ResultObject, Search},
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

async fn create(path: &Path, access_type: AccessType) -> IndexArc {
    let _ = fs::remove_dir_all(path);
    let meta = IndexMetaObject { id:0, name:"metadata_contract".into(),
        lexical_similarity:LexicalSimilarity::Bm25f, tokenizer:TokenizerType::UnicodeAlphanumeric,
        stop_words:StopwordType::None, frequent_words:FrequentwordType::None,
        ngram_indexing:NgramSet::SingleTerm as u8, document_compression:DocumentCompression::Snappy,
        access_type, clustering:Clustering::None, inference:Inference::None };
    create_index(path, meta, &schema(), 11, true, Some(2)).await.unwrap()
}

async fn search(index: &IndexArc, options: Value, realtime: bool) -> ResultObject {
    let mut value = json!({"query":"common", "length":20, "sort":[{"field":"price","order":"Ascending"}]});
    value.as_object_mut().unwrap().extend(options.as_object().unwrap().clone());
    let result_type = value.as_object_mut().unwrap().remove("result_type")
        .map(|v| serde_json::from_value(v).unwrap()).unwrap_or(seekpower::search::ResultType::TopkCount);
    let r: SearchRequestObject = serde_json::from_value(value).unwrap();
    let result = index.search(r.query_string.clone(), None, r.query_type(),
        r.search_mode(), true, r.offset, r.length, result_type, realtime,
        r.field_filter,  r.facet_filter, r.result_sort).await;
    assert!(result.error.is_none(), "{:?}", result.error);
    result
}

async fn check_prices(index: &IndexArc, options: Value, realtime: bool, expected: &[f64]) {
    let result = search(index, options.clone(), realtime).await;
    assert_eq!(result.result_count_total, expected.len(), "{options}");
    let mut prices = Vec::new();
    for result in result.results {
        let doc = index.read().await.get_document(result.doc_id, realtime, &HashSet::new()).await.unwrap();
        prices.push(doc["price"].as_f64().unwrap());
    }
    assert_eq!(prices, expected, "{options}");
}

async fn check_metadata(index: &IndexArc, realtime: bool) {
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
                assert_eq!(result.result_count_total,2 * per_bin,"{query} {result_type} realtime={realtime}");

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
            index.index_document(doc).await.unwrap();
        }
        for _ in 0..200 {
            if search(&index,json!({}),true).await.result_count_total==4 { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        check_metadata(&index,true).await;
        index.commit().await;
        check_metadata(&index,false).await;
        index.close().await; drop(index);
        // The current dictionary contains only names and ordered string/set values.
        for shard in 0..2 {
            let file=path.join(format!("shards/{shard}/facet.json"));
            let fields: Value=serde_json::from_slice(&fs::read(&file).unwrap()).unwrap();
            for field in fields.as_array().unwrap() {
                assert_eq!(field.as_object().unwrap().len(), 2);
                assert!(field.get("min").is_none() && field.get("max").is_none());
                for entry in field["values"].as_object().unwrap().values() {
                    assert!(entry.as_array().unwrap().iter().all(Value::is_string));
                }
            }
        }
        let reopened=open_index(path).await.unwrap();
        check_metadata(&reopened,false).await;
        reopened.close().await; drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }
}

#[tokio::test]
async fn unknown_schema_type_is_rejected() {
    let path=Path::new("tests/index_unknown_schema");
    let index=create(path,AccessType::Mmap).await;
    index.close().await;drop(index);
    for schema_path in [path.join("schema.json"),path.join("shards/0/schema.json")] {
        let original=fs::read(&schema_path).unwrap();
        let mut schema: Value=serde_json::from_slice(&original).unwrap();
        schema[0]["field_type"]=json!("Point");
        let legacy=serde_json::to_vec(&schema).unwrap();
        fs::write(&schema_path,&legacy).unwrap();
        match open_index(path).await {
            Ok(_) => panic!("retired Point schema accepted"),
            Err(error) => assert!(error.contains("unknown variant") && error.contains("Point"),"{error}"),
        }
        assert_eq!(fs::read(&schema_path).unwrap(),legacy,"legacy schema was modified");
        fs::write(&schema_path,original).unwrap();
    }
    fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn only_current_index_format_is_opened() {
    use seekpower::index::{INDEX_FORMAT_VERSION_MAJOR, INDEX_FORMAT_VERSION_MINOR};
    for (name, access) in [("mmap", AccessType::Mmap)] {
        let path_buf = format!("tests/index_format_{name}");
        let path = Path::new(&path_buf);
        let index = create(path, access).await;
        index.close().await;
        drop(index);
        for file in [path.join("index.bin"), path.join("shards/0/index.bin")] {
            let original = fs::read(&file).unwrap();
            assert_eq!(u16::from_le_bytes(original[0..2].try_into().unwrap()), INDEX_FORMAT_VERSION_MAJOR);
            assert_eq!(u16::from_le_bytes(original[2..4].try_into().unwrap()), INDEX_FORMAT_VERSION_MINOR);
            for (major, minor) in [(6u16, 0u16), (6, 1), (INDEX_FORMAT_VERSION_MAJOR, INDEX_FORMAT_VERSION_MINOR + 1)] {
                let mut bytes = original.clone();
                bytes[0..2].copy_from_slice(&major.to_le_bytes());
                bytes[2..4].copy_from_slice(&minor.to_le_bytes());
                fs::write(&file, &bytes).unwrap();
                match open_index(path).await {
                    Ok(_) => panic!("unsupported format {major}.{minor} accepted"),
                    Err(error) => assert!(error.contains("unsupported index format"), "{error}"),
                }
                assert_eq!(fs::read(&file).unwrap(), bytes);
                fs::write(&file, &original).unwrap();
            }
        }
        let reopened = open_index(path).await.unwrap();
        reopened.close().await;
        drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }
}

#[test]
fn metadata_dictionary_has_no_statistics_slots() {
    use seekpower::index::FacetField;
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
