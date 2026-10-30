#![cfg(feature = "server")]
use anyseek::record::RecordSearch;
#[path = "common/records.rs"]
mod records;
use anyseek::record::{RecordPartition, RecordQuery};

// Aggregate removal must retain metadata dictionaries, filters and sort heaps.
use anyseek::server::api_types::{CreateIndexRequest, GetDocumentRequest, SearchRequestObject};
use anyseek::{
    index::{
        Clustering, Document, FrequentwordType, IndexMetaObject,
        LexicalSimilarity, NgramSet, SchemaField, StopwordType, TokenizerType,
    },
    vector::Inference,
};
use serde_json::{Value, json};
use std::{collections::HashSet, fs, path::Path, time::Duration};

fn schema() -> Vec<SchemaField> {
    serde_json::from_value(json!([
        {"field":"body","field_type":"Text","store":true,"index_lexical":true},
        {"field":"category","field_type":"String","store":true,"index_lexical":false,"facet":true},
        {"field":"tags","field_type":"StringSet","store":true,"index_lexical":false,"facet":true},
        {"field":"price","field_type":"Float","store":true,"index_lexical":false,"facet":true},
        {"field":"date","field_type":"Timestamp","store":true,"index_lexical":false,"facet":true},
        {"field":"rank","field_type":"Int","store":true,"index_lexical":false,"facet":true},
        {"field":"data","field_type":"Json","store":true,"index_lexical":false}
    ]))
    .unwrap()
}

async fn create(path: &Path) -> RecordPartition {
    let _ = fs::remove_dir_all(path);
    let meta = IndexMetaObject {
        id: 0,
        name: "metadata_contract".into(),
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
    records::create(path, meta, (&schema()).to_vec()).await
}

async fn search(index: &RecordPartition, options: Value, realtime: bool) -> RecordSearch {
    let mut value =
        json!({"query":"common", "length":20, "sort":[{"field":"price","order":"Ascending"}]});
    value
        .as_object_mut()
        .unwrap()
        .extend(options.as_object().unwrap().clone());
    let result_type = value
        .as_object_mut()
        .unwrap()
        .remove("result_type")
        .map(|v| serde_json::from_value(v).unwrap())
        .unwrap_or(anyseek::search::ResultType::TopkCount);
    let r: SearchRequestObject = serde_json::from_value(value).unwrap();
    let result = index
        .query(RecordQuery {
            text: r.query_string.clone(),
            vector: None,
            query_type: r.query_type(),
            mode: r
                .search_mode(
                    std::num::NonZeroUsize::new(anyseek::server::api_types::DEFAULT_NPROBE)
                        .unwrap(),
                )
                .unwrap(),
            offset: r.offset,
            length: r.length,
            result_type: result_type,
            fields: r.field_filter,
            filters: r.facet_filter,
            sort: r.result_sort,
            temporal: None,
        })
        .await
        .unwrap();
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
    check_prices(index, json!({}), realtime, &[10., 20., 30., 40.]).await;
    check_prices(index, json!({"query":""}), realtime, &[10., 20., 30., 40.]).await;
    check_prices(
        index,
        json!({"query":"alpha beta","operator":"or"}),
        realtime,
        &[10., 20., 30., 40.],
    )
    .await;
    check_prices(
        index,
        json!({"query":"common alpha","operator":"and"}),
        realtime,
        &[10., 30.],
    )
    .await;
    for (filter, expected) in [
        (
            json!({"String":{"field":"category","filter":["a"]}}),
            vec![10., 30.],
        ),
        (
            json!({"StringSet":{"field":"tags","filter":["red"]}}),
            vec![10., 20.],
        ),
        (
            json!({"Float":{"field":"price","filter":{"start":20.,"end":40.}}}),
            vec![20., 30.],
        ),
        (
            json!({"Timestamp":{"field":"date","filter":{"start":200,"end":400}}}),
            vec![20., 30.],
        ),
    ] {
        for query in ["common", "alpha beta", "north south", ""] {
            check_prices(
                index,
                json!({"query":query,"operator":"or","filter":[filter.clone()]}),
                realtime,
                &expected,
            )
            .await;
        }
    }
    check_prices(
        index,
        json!({"filter":[
            {"String":{"field":"category","filter":["a"]}},
            {"Float":{"field":"price","filter":{"start":20.,"end":40.}}}
        ]}),
        realtime,
        &[30.],
    )
    .await;
    check_prices(
        index,
        json!({"sort":[{"field":"date","order":"Descending"}]}),
        realtime,
        &[40., 30., 20., 10.],
    )
    .await;
    check_prices(index,json!({"sort":[{"field":"category","order":"Ascending"},{"field":"price","order":"Descending"}]}),realtime,&[30.,10.,40.,20.]).await;
    // Cover both single-term-per-shard and multi-term-per-shard union paths.
    for query in ["alpha beta", "north south"] {
        for result_type in ["Count", "TopkCount"] {
            for filtered in [false, true] {
                let result = search(index,json!({"query":query,"operator":"or","result_type":result_type,
                    "filter": if filtered { json!([{"String":{"field":"category","filter":["a"]}}]) } else { json!([]) }}),realtime).await;
                let per_bin = if filtered { 1 } else { 2 };
                assert_eq!(
                    result.count_total,
                    2 * per_bin,
                    "{query} {result_type} realtime={realtime}"
                );
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
        request
            .as_object_mut()
            .unwrap()
            .extend(options.as_object().unwrap().clone());
        assert!(
            serde_json::from_value::<SearchRequestObject>(request).is_err(),
            "{options}"
        );
    }
    assert!(serde_json::from_value::<GetDocumentRequest>(json!({"distance_fields":[]})).is_err());
    assert!(serde_json::from_value::<GetDocumentRequest>(json!({"fields":"body"})).is_ok());
}

#[tokio::test]
async fn ordinary_metadata_survives_aggregate_removal() {
    let path_buf = "tests/index_metadata".to_string();
    let path = Path::new(&path_buf);
    let index = create(path).await;
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
        if search(&index, json!({}), true).await.count_total == 4 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    check_metadata(&index, true).await;
    index.compact_journal().await.unwrap();
    check_metadata(&index, false).await;
    drop(index);
    let reopened = RecordPartition::open(path, records::JOURNAL_LIMIT)
        .await
        .unwrap();
    check_metadata(&reopened, false).await;
    drop(reopened);
    fs::remove_dir_all(path).unwrap();

}

#[test]
fn unknown_schema_type_is_rejected() {
    let mut fields = serde_json::to_value(schema()).unwrap();
    fields[0]["field_type"] = json!("Point");
    let error = serde_json::from_value::<Vec<SchemaField>>(fields)
        .unwrap_err()
        .to_string();
    assert!(error.contains("unknown variant") && error.contains("Point"));
}
#[tokio::test]
async fn legacy_index_headers_are_not_record_authority() {
    let root = tempfile::tempdir().unwrap();
    for bytes in [vec![6u8, 0, 0, 0], vec![8, 0, 0, 0], vec![], vec![8, 0]] {
        let path = root.path().join("legacy");
        fs::create_dir(&path).unwrap();
        fs::write(path.join("index.bin"), &bytes).unwrap();
        assert!(
            RecordPartition::open(&path, records::JOURNAL_LIMIT)
                .await
                .is_err()
        );
        assert_eq!(fs::read(path.join("index.bin")).unwrap(), bytes);
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

#[test]
fn only_eight_field_types_and_five_filter_types_are_accepted() {
    use anyseek::{index::FieldType, search::FacetFilter};
    for name in [
        "String",
        "StringSet",
        "Int",
        "Float",
        "Text",
        "Timestamp",
        "Json",
        "Binary",
    ] {
        let value = json!(name);
        let parsed: FieldType = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), value);
    }
    for name in [
        "U8",
        "U16",
        "U32",
        "U64",
        "I8",
        "I16",
        "I32",
        "I64",
        "F32",
        "F64",
        "Bool",
        "String16",
        "String32",
        "StringSet16",
        "StringSet32",
    ] {
        assert!(
            serde_json::from_value::<FieldType>(json!(name)).is_err(),
            "{name}"
        );
        assert!(
            serde_json::from_value::<FacetFilter>(
                json!({name:{"field":"x","filter":{"start":0,"end":1}}})
            )
            .is_err(),
            "{name}"
        );
    }
    for bad in [
        json!(9223372036854775808u64),
        json!(-9223372036854775809.0),
        json!(1.5),
    ] {
        assert!(
            serde_json::from_value::<FacetFilter>(
                json!({"Int":{"field":"rank","filter":{"start":bad,"end":0}}})
            )
            .is_err()
        );
    }
}

#[tokio::test]
async fn scalar_boundaries_nulls_and_sets_survive_wal_and_checkpoint() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("fields");
    let index = create(&path).await;
    for bad in [
        json!(9223372036854775808u64),
        json!(u64::MAX),
        json!(-9223372036854775809.0),
        json!(1.5),
        json!("7"),
        json!(true),
    ] {
        let doc = serde_json::from_value(json!({"body":"common", "rank":bad})).unwrap();
        assert!(records::insert(&index, vec![doc]).await.is_err(), "{bad}");
    }
    for bad in [json!("1.5"), json!(true), json!([])] {
        assert!(
            records::insert(
                &index,
                vec![serde_json::from_value(json!({"body":"common","price":bad})).unwrap()]
            )
            .await
            .is_err()
        );
    }
    let documents = [
        json!({"body":"common", "rank":i64::MIN,"price":-1.25,"category":"first","tags":["a_b","c"],"data":{"flag":true}}),
        json!({"body":"common", "rank":0,"price":0.0,"category":"second","tags":["a","b_c","a"]}),
        json!({"body":"common", "rank":i64::MAX,"price":1.0000000000000002,"tags":[]}),
        json!({"body":"common", "rank":null,"price":null,"category":null,"tags":null}),
        json!({"body":"common"}),
    ];
    for doc in documents {
        records::insert(&index, vec![serde_json::from_value(doc).unwrap()])
            .await
            .unwrap();
    }
    async fn verify(index: &RecordPartition) {
        for (filter, count) in [
            (
                json!({"Int":{"field":"rank","filter":{"start":i64::MIN,"end":1}}}),
                2,
            ),
            (
                json!({"Float":{"field":"price","filter":{"start":-0.5,"end":0.5}}}),
                1,
            ),
            (json!({"String":{"field":"category","filter":["first"]}}), 1),
            (json!({"StringSet":{"field":"tags","filter":["a_b"]}}), 1),
            (json!({"StringSet":{"field":"tags","filter":["b_c"]}}), 1),
        ] {
            let result = search(index, json!({"filter":[filter]}), true).await;
            assert_eq!(result.count_total, count);
        }
        for order in ["Ascending", "Descending"] {
            let result = search(
                index,
                json!({"sort":[{"field":"rank","order":order}]}),
                true,
            )
            .await;
            let ranks: Vec<_> = result
                .hits
                .iter()
                .map(|h| {
                    h.record
                        .document
                        .as_ref()
                        .unwrap()
                        .get("rank")
                        .and_then(Value::as_i64)
                })
                .collect();
            let mut expected = vec![Some(i64::MIN), Some(0), Some(i64::MAX)];
            if order == "Descending" {
                expected.reverse();
            }
            expected.extend([None, None]);
            assert_eq!(ranks, expected);
            assert!(
                result
                    .hits
                    .iter()
                    .any(|h| h.record.document.as_ref().unwrap().get("data")
                        == Some(&json!({"flag":true})))
            );
        }
    }
    verify(&index).await;
    drop(index);
    let index = RecordPartition::open(&path, records::JOURNAL_LIMIT)
        .await
        .unwrap();
    verify(&index).await;
    index.compact_journal().await.unwrap();
    drop(index);
    let index = RecordPartition::open(&path, records::JOURNAL_LIMIT)
        .await
        .unwrap();
    verify(&index).await;
}

#[tokio::test]
async fn empty_string_sets_sort_with_nulls_after_wal_and_checkpoint_recovery() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("set_sort");
    let index = create(&path).await;
    for (rank, tags) in [
        Some(json!([])),
        Some(json!(["a"])),
        Some(json!(["z", "a", "a"])),
        Some(json!(["b"])),
        Some(Value::Null),
        None,
    ]
    .into_iter()
    .enumerate()
    {
        let mut doc = json!({"body":"common", "rank":rank});
        if let Some(tags) = tags {
            doc["tags"] = tags;
        }
        records::insert(&index, vec![serde_json::from_value(doc).unwrap()])
            .await
            .unwrap();
    }
    async fn verify(index: &RecordPartition) {
        for (order, expected) in [
            ("Ascending", vec![0, 1, 2, 3, 4, 5]),
            ("Descending", vec![3, 1, 2, 0, 4, 5]),
        ] {
            for query in ["common", ""] {
                for length in [1, 3, 20] {
                    let result = search(index, json!({"query":query,"length":length,"sort":[{"field":"tags","order":order},{"field":"rank","order":"Ascending"}]}), true).await;
                    let ranks: Vec<_> = result
                        .hits
                        .iter()
                        .map(|hit| {
                            hit.record.document.as_ref().unwrap()["rank"]
                                .as_i64()
                                .unwrap()
                        })
                        .collect();
                    assert_eq!(result.count_total, 6);
                    assert_eq!(
                        ranks,
                        expected[..expected.len().min(length)],
                        "{order} {query:?} length={length}"
                    );
                }
            }
        }
    }
    verify(&index).await;
    drop(index);
    let index = RecordPartition::open(&path, records::JOURNAL_LIMIT)
        .await
        .unwrap();
    verify(&index).await;
    index.compact_journal().await.unwrap();
    drop(index);
    let index = RecordPartition::open(&path, records::JOURNAL_LIMIT)
        .await
        .unwrap();
    verify(&index).await;
}
