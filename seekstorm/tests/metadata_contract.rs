//! The geo removal must retain the shared facet layout, filters and sort heaps.
use seekstorm::{
    commit::Commit,
    index::{AccessType, Close, Clustering, CreateIndexRequest, Document, DocumentCompression,
        FrequentwordType, GetDocumentRequest, IndexArc, IndexDocument, IndexMetaObject,
        LexicalSimilarity, NgramSet, SchemaField, SearchRequestObject, StemmerType, StopwordType,
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
        stemmer:StemmerType::None, stop_words:StopwordType::None, frequent_words:FrequentwordType::None,
        ngram_indexing:NgramSet::SingleTerm as u8, document_compression:DocumentCompression::Snappy,
        access_type, clustering:Clustering::None, inference:Inference::None };
    create_index(path, meta, &schema(), &vec![], 11, true, Some(2)).await.unwrap()
}

async fn search(index: &IndexArc, options: Value, realtime: bool) -> ResultObject {
    let mut value = json!({"query":"common", "length":20, "enable_empty_query":true,
        "result_type":"TopkCount", "result_sort":[{"field":"price","order":"Ascending"}]});
    value.as_object_mut().unwrap().extend(options.as_object().unwrap().clone());
    let r: SearchRequestObject = serde_json::from_value(value).unwrap();
    let result = index.search(r.query_string, None, r.query_type_default,
        r.search_mode, r.enable_empty_query, r.offset, r.length, r.result_type, realtime,
        r.field_filter, r.query_facets, r.facet_filter, r.result_sort).await;
    assert!(result.error.is_none(), "{:?}", result.error);
    result
}

async fn check_prices(index: &IndexArc, options: Value, realtime: bool, expected: &[f64]) {
    let result = search(index, options.clone(), realtime).await;
    assert_eq!(result.result_count_total, expected.len(), "{options}");
    let mut prices = Vec::new();
    for result in result.results {
        let doc = index.read().await.get_document(result.doc_id, realtime, &None, &HashSet::new()).await.unwrap();
        prices.push(doc["price"].as_f64().unwrap());
    }
    assert_eq!(prices, expected, "{options}");
}

async fn check_metadata(index: &IndexArc, realtime: bool) {
    check_prices(index,json!({}),realtime,&[10.,20.,30.,40.]).await;
    check_prices(index,json!({"query":""}),realtime,&[10.,20.,30.,40.]).await;
    check_prices(index,json!({"query":"alpha beta","query_type_default":"Union"}),realtime,&[10.,20.,30.,40.]).await;
    check_prices(index,json!({"query":"common alpha","query_type_default":"Intersection"}),realtime,&[10.,30.]).await;
    for (filter, expected) in [
        (json!({"String16":{"field":"category","filter":["a"]}}), vec![10.,30.]),
        (json!({"StringSet32":{"field":"tags","filter":["red"]}}), vec![10.,20.]),
        (json!({"F64":{"field":"price","filter":{"start":20.,"end":40.}}}), vec![20.,30.]),
        (json!({"Timestamp":{"field":"date","filter":{"start":200,"end":400}}}), vec![20.,30.]),
    ] {
        for query in ["common", "alpha beta", ""] {
            check_prices(index,json!({"query":query,"query_type_default":"Union","facet_filter":[filter.clone()]}),realtime,&expected).await;
        }
    }
    check_prices(index,json!({"facet_filter":[
        {"String16":{"field":"category","filter":["a"]}},
        {"F64":{"field":"price","filter":{"start":20.,"end":40.}}}
    ]}),realtime,&[30.]).await;
    check_prices(index,json!({"result_sort":[{"field":"date","order":"Descending"}]}),realtime,&[40.,30.,20.,10.]).await;
    check_prices(index,json!({"result_sort":[{"field":"category","order":"Ascending"},{"field":"price","order":"Descending"}]}),realtime,&[30.,10.,40.,20.]).await;
    // Cover both single-term-per-shard and multi-term-per-shard union paths.
    for query in ["alpha beta", "north south"] {
        for result_type in ["Count", "TopkCount"] {
            for filtered in [false, true] {
                let result = search(index,json!({"query":query,"query_type_default":"Union","result_type":result_type,
                    "facet_filter": if filtered { json!([{"String16":{"field":"category","filter":["a"]}}]) } else { json!([]) },
                    "query_facets":[{"F64":{"field":"price","range_type":"CountWithinRange","ranges":[["low",0.],["high",25.]]}}]}),realtime).await;
                let per_bin = if filtered {1} else {2};
                assert_eq!(result.result_count_total,2 * per_bin,"{query} {result_type} realtime={realtime}");
                // Root aggregation ranks facets by count; ties have no label-order guarantee.
                let mut bins = result.facets["price"].clone();
                bins.sort_by(|a,b| a.0.cmp(&b.0));
                assert_eq!(bins,vec![("high".into(),per_bin),("low".into(),per_bin)],"{query} {result_type} realtime={realtime}");
            }
        }
    }
}

#[test]
fn retired_geo_requests_are_rejected() {
    assert!(serde_json::from_value::<CreateIndexRequest>(json!({"index_name":"geo","schema":[{"field":"location","field_type":"Point","store":true,"index_lexical":false,"facet":true}]})).is_err());
    for options in [
        json!({"distance_fields":[]}),
        json!({"facet_filter":[{"Point":{"field":"location","filter":[[0.,0.],{"start":0.,"end":10.},"Kilometers"]}}]}),
        json!({"query_facets":[{"Point":{"field":"location","base":[0.,0.],"unit":"Kilometers","range_type":"CountWithinRange","ranges":[["near",0.]]}}]}),
        json!({"result_sort":[{"field":"date","order":"Ascending","base":"None"}]}),
        json!({"result_sort":[{"field":"location","order":"Ascending","base":{"Point":[0.,0.]}}]}),
    ] {
        let mut request = json!({"query":"common"});
        request.as_object_mut().unwrap().extend(options.as_object().unwrap().clone());
        assert!(serde_json::from_value::<SearchRequestObject>(request).is_err(),"{options}");
    }
    assert!(serde_json::from_value::<GetDocumentRequest>(json!({"distance_fields":[]})).is_err());
    assert!(serde_json::from_value::<GetDocumentRequest>(json!({"fields":["body"]})).is_ok());
}

#[tokio::test]
async fn ordinary_metadata_survives_geo_removal() {
    for (name, access) in [("mmap",AccessType::Mmap),("ram",AccessType::Ram)] {
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
        let reopened=open_index(path).await.unwrap();
        check_metadata(&reopened,false).await;
        reopened.close().await; drop(reopened);
        fs::remove_dir_all(path).unwrap();
    }
}

#[tokio::test]
async fn legacy_point_schema_fails_with_rebuild_message() {
    let path=Path::new("tests/index_legacy_point");
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
            Err(error) => assert!(error.contains("Point fields have been removed") && error.contains("rebuild"),"{error}"),
        }
        assert_eq!(fs::read(&schema_path).unwrap(),legacy,"legacy schema was modified");
        fs::write(&schema_path,original).unwrap();
    }
    fs::remove_dir_all(path).unwrap();
}
