//! A closer excluded vector must not consume the filtered result window.
use seekpower_server::api_types::SearchRequestObject;
use seekpower::{
    commit::Commit,
    index::{
        AccessType, Close, Clustering, DeleteDocument, Document, DocumentCompression,
        FrequentwordType, IndexArc, IndexDocuments, IndexMetaObject, LexicalSimilarity, NgramSet,
        SchemaField, StopwordType, TokenizerType, UpdateDocument,
        create_index, open_index,
    },
    search::{ResultObject, Search},
    vector::{Embedding, Inference, Precision, Quantization},
    vector_similarity::VectorSimilarity,
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    fs,
    time::Duration,
};

fn schema() -> Vec<SchemaField> {
    let mut value = json!([
        {"field":"body","field_type":"Text","store":true,"index_lexical":true},
        {"field":"vector","field_type":"Json","store":true,"index_lexical":false,"index_vector":true},
        {"field":"label","field_type":"U32","store":true,"index_lexical":false,"facet":true},
        {"field":"category","field_type":"String16","store":true,"index_lexical":false,"facet":true},
        {"field":"category32","field_type":"String32","store":false,"index_lexical":false,"facet":true},
        {"field":"tags","field_type":"StringSet32","store":true,"index_lexical":false,"facet":true},
        {"field":"tags16","field_type":"StringSet16","store":false,"index_lexical":false,"facet":true},
        {"field":"price","field_type":"F64","store":false,"index_lexical":false,"facet":true},
        {"field":"date","field_type":"Timestamp","store":false,"index_lexical":false,"facet":true}
    ]);
    for kind in ["U8", "U16", "U64", "I8", "I16", "I32", "I64", "F32"] {
        value.as_array_mut().unwrap().push(json!({"field":kind.to_lowercase(),"field_type":kind,"store":false,"index_lexical":false,"facet":true}));
    }
    serde_json::from_value(value).unwrap()
}
fn doc(i: usize) -> Document {
    let category = if i < 16 { "blocked" } else { "allowed" };
    let mut value = json!({"body":if i%2==0 {"common alpha"} else {"common beta"},
        "vector":[1.0, i as f32 / 24.0, 0.0], "label":i, "category":category,
        "category32":category,"tags":["all",if i%2==0 {"even"} else {"odd"}],
        "tags16":["all",if i%2==0 {"even"} else {"odd"}],"price":i as f64,"date":i as i64});
    for kind in ["u8", "u16", "u64", "i8", "i16", "i32", "i64", "f32"] {
        value[kind] = json!(i);
    }
    serde_json::from_value(value).unwrap()
}
fn mode(name: &str, ann: Value) -> Value {
    if name == "Lexical" {
        json!(name)
    } else {
        json!({name:{"ann_mode":ann,"similarity_threshold":null}})
    }
}
async fn query(
    index: &IndexArc,
    name: &str,
    filter: Value,
    realtime: bool,
    length: usize,
    ann: Value,
) -> ResultObject {
    let r: SearchRequestObject = serde_json::from_value(json!({"query":"common", "length":length,
        "filter":filter}))
    .unwrap();
    index
        .search(
            r.query_string.clone(),
            if name == "Lexical" {
                None
            } else {
                Some(Embedding::F32(vec![1., 0., 0.]))
            },
            r.query_type(),
            serde_json::from_value(mode(name, ann.clone())).unwrap(),
            false,
            r.offset,
            r.length,
            r.result_type(),
            realtime,
            r.field_filter,
            r.facet_filter,
            r.result_sort,
        )
        .await
}
async fn labels(index: &IndexArc, result: &ResultObject, realtime: bool) -> Vec<usize> {
    assert!(result.error.is_none(), "{:?}", result.error);
    let mut labels = Vec::new();
    for hit in &result.results {
        labels.push(
            index
                .read()
                .await
                .get_document(hit.doc_id, realtime, &HashSet::new())
                .await
                .unwrap()["label"]
                .as_u64()
                .unwrap() as usize,
        );
    }
    labels
}
async fn check(index: &IndexArc, realtime: bool, removed: &[usize]) {
    let mut cases = vec![
        (
            json!([{"String16":{"field":"category","filter":["allowed"]}}]),
            (16..24).collect::<Vec<_>>(),
        ),
        (
            json!([{"String32":{"field":"category32","filter":["allowed"]}}]),
            (16..24).collect(),
        ),
        (
            json!([{"StringSet32":{"field":"tags","filter":["even"]}}]),
            (0..24).filter(|i| i % 2 == 0).collect(),
        ),
        (
            json!([{"StringSet16":{"field":"tags16","filter":["even"]}}]),
            (0..24).filter(|i| i % 2 == 0).collect(),
        ),
        (
            json!([{"F64":{"field":"price","filter":{"start":20.,"end":24.}}}]),
            (20..24).collect(),
        ),
        (
            json!([{"Timestamp":{"field":"date","filter":{"start":20,"end":24}}}]),
            (20..24).collect(),
        ),
        (
            json!([{"U32":{"field":"label","filter":{"start":23,"end":24}}}]),
            vec![23],
        ),
        (
            json!([{"String16":{"field":"category","filter":["absent"]}}]),
            vec![],
        ),
        (
            json!([{"String16":{"field":"category","filter":[]}}]),
            vec![],
        ),
        (
            json!([{"U32":{"field":"label","filter":{"start":20,"end":20}}}]),
            vec![],
        ),
        (
            json!([{"String16":{"field":"category","filter":["allowed"]}},
            {"StringSet32":{"field":"tags","filter":["even"]}},
            {"Timestamp":{"field":"date","filter":{"start":20,"end":24}}}]),
            vec![20, 22],
        ),
        // Multiple predicates for the same field are ANDed, rather than overwritten.
        (
            json!([{"U32":{"field":"label","filter":{"start":16,"end":24}}},
            {"U32":{"field":"label","filter":{"start":20,"end":22}}}]),
            vec![20, 21],
        ),
    ];
    for kind in ["U8", "U16", "U64", "I8", "I16", "I32", "I64", "F32"] {
        cases.push((
            json!([{kind:{"field":kind.to_lowercase(),"filter":{"start":20,"end":24}}}]),
            (20..24).collect(),
        ));
    }
    cases.push((
        json!([{"StringSet32":{"field":"tags","filter":["all_even"]}}]),
        vec![],
    ));
    for (filter, eligible) in cases {
        let eligible: HashSet<_> = eligible
            .into_iter()
            .filter(|i| !removed.contains(i))
            .collect();
        for name in ["Lexical", "Vector", "Hybrid"] {
            let r = query(index, name, filter.clone(), realtime, 30, json!("All")).await;
            let actual: HashSet<_> = labels(index, &r, realtime).await.into_iter().collect();
            assert_eq!(actual, eligible, "{name} realtime={realtime} {filter}");
            for ann in [json!("All"), json!({"Nprobe":1})] {
                let r = query(index, name, filter.clone(), realtime, 3, ann).await;
                let actual = labels(index, &r, realtime).await;
                assert_eq!(
                    actual.len(),
                    eligible.len().min(3),
                    "{name} realtime={realtime} {filter}"
                );
                assert!(actual.iter().all(|i| eligible.contains(i)));
            }
        }
    }
    for bad in [
        json!([{"U32":{"field":"absent","filter":{"start":0,"end":1}}}]),
        json!([{"F64":{"field":"category","filter":{"start":0.,"end":1.}}}]),
        json!([{"String16":{"field":"body","filter":["common"]}}]),
        json!([{"Timestamp":{"field":"date","filter":{"start":2,"end":1}}}]),
    ] {
        for name in ["Lexical", "Vector", "Hybrid"] {
            assert!(
                query(index, name, bad.clone(), realtime, 3, json!("All"))
                    .await
                    .error
                    .as_ref()
                    .unwrap()
                    .contains("facet_filter")
            );
        }
    }
}

#[tokio::test]
async fn shared_filters_before_topk_realtime_commit_reopen_update_delete() {
    for (tag, access, shards) in [("mmap1", AccessType::Mmap, 1), ("mmap3", AccessType::Mmap, 3)] {
        let path = std::env::temp_dir().join(format!(
            "seekpower-shared-filters-{}-{tag}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        let meta = IndexMetaObject {
            id: 0,
            name: "shared_filters".into(),
            lexical_similarity: LexicalSimilarity::Bm25f,
            tokenizer: TokenizerType::UnicodeAlphanumeric,
        text_normalization: seekpower::index::TextNormalization::None,
        field_length_normalization: seekpower::index::FieldLengthNormalization::Shared,
            stop_words: StopwordType::None,
            frequent_words: FrequentwordType::None,
            ngram_indexing: NgramSet::SingleTerm as u8,
            document_compression: DocumentCompression::Snappy,
            access_type: access,
            clustering: Clustering::None,
            inference: Inference::External {
                dimensions: 3,
                precision: Precision::F32,
                quantization: Quantization::None,
                similarity: VectorSimilarity::Cosine,
            },
        };
        let index = create_index(&path, meta, &schema(), 11, true, Some(shards))
            .await
            .unwrap();
        index
            .index_documents((0..24).map(doc).collect())
            .await
            .unwrap();
        for attempt in 0..300 {
            let r = query(&index, "Lexical", json!([]), true, 30, json!("All")).await;
            if r.results.len() == 24 {
                break;
            }
            assert!(attempt < 299, "index queue did not finish");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        check(&index, true, &[]).await;
        // Deleted uncommitted vectors must not enter the candidate heap.
        let all = query(&index, "Lexical", json!([]), true, 30, json!("All")).await;
        let map: HashMap<_, _> = labels(&index, &all, true)
            .await
            .into_iter()
            .zip(all.results.iter().map(|hit| hit.doc_id as u64))
            .collect();
        index.delete_document(map[&23]).await;
        check(&index, true, &[23]).await;
        index.commit().await.unwrap();
        check(&index, false, &[23]).await;
        index.delete_document(map[&22]).await;
        index.update_document((map[&21], doc(25))).await.unwrap();
        index.commit().await.unwrap();
        // Updated/deleted IDs do not retain old metadata or old vectors.
        for name in ["Lexical", "Vector", "Hybrid"] {
            let r = query(
                &index,
                name,
                json!([{"U32":{"field":"label","filter":{"start":21,"end":24}}}]),
                true,
                30,
                json!("All"),
            )
            .await;
            assert!(labels(&index, &r, true).await.is_empty());
            let r = query(
                &index,
                name,
                json!([{"U32":{"field":"label","filter":{"start":25,"end":26}}}]),
                false,
                3,
                json!("All"),
            )
            .await;
            assert_eq!(labels(&index, &r, false).await, vec![25]);
        }
        index.close().await.unwrap();
        drop(index);
        let index = open_index(&path).await.unwrap();
        for name in ["Lexical", "Vector", "Hybrid"] {
            let r = query(
                &index,
                name,
                json!([{"U32":{"field":"label","filter":{"start":25,"end":26}}}]),
                false,
                3,
                json!("All"),
            )
            .await;
            assert_eq!(labels(&index, &r, false).await, vec![25]);
        }
        index.close().await.unwrap();
        drop(index);
        fs::remove_dir_all(path).unwrap();
    }
}

fn meta() -> IndexMetaObject {
    IndexMetaObject {
        id: 0,
        name: "shared_filters".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        text_normalization: seekpower::index::TextNormalization::None,
        field_length_normalization: seekpower::index::FieldLengthNormalization::Shared,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: NgramSet::SingleTerm as u8,
        document_compression: DocumentCompression::Snappy,
        access_type: AccessType::Mmap,
        clustering: Clustering::None,
        inference: Inference::External {
            dimensions: 3,
            precision: Precision::F32,
            quantization: Quantization::None,
            similarity: VectorSimilarity::Cosine,
        },
    }
}

#[tokio::test]
async fn filtered_quantization_metrics_and_cluster_limits() {
    for metric in [
        VectorSimilarity::Cosine,
        VectorSimilarity::Dot,
        VectorSimilarity::Euclidean,
    ] {
        for (precision, quantization) in [
            (Precision::F32, Quantization::None),
            (Precision::F32, Quantization::ScalarQuantizationI8),
            (Precision::F32, Quantization::TurboQuantI8),
            (Precision::I8, Quantization::None),
        ] {
            let path = std::env::temp_dir().join(format!(
                "seekpower-filter-matrix-{}-{metric}-{precision}-{quantization}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            let mut config = meta();
            config.clustering = Clustering::Fixed(3);
            config.inference = Inference::External {
                dimensions: 3,
                precision,
                quantization,
                similarity: metric,
            };
            let mut vector_schema = schema();
            vector_schema.push(serde_json::from_value(json!({"field":"other_vector","field_type":"Json","store":false,"index_lexical":false,"index_vector":true})).unwrap());
            let index = create_index(&path, config, &vector_schema, 11, true, Some(1))
                .await
                .unwrap();
            let mut docs: Vec<_> = (0..24).map(doc).collect();
            for (i, d) in docs.iter_mut().enumerate() {
                d.insert(
                    "other_vector".into(),
                    if precision == Precision::I8 {
                        json!([24, 0, 0])
                    } else {
                        json!([1., 0., 0.])
                    },
                );
                if precision == Precision::I8 {
                    d.insert("vector".into(), json!([24, i, 0]));
                }
            }
            index.index_documents(docs).await.unwrap();
            index.commit().await.unwrap();
            let predicate = json!([{"String16":{"field":"category","filter":["allowed"]}}]);
            for name in ["Vector", "Hybrid"] {
                for ann in [
                    json!("All"),
                    json!({"Nprobe":1}),
                    json!({"NprobeSimilaritythreshold":[3,if metric==VectorSimilarity::Euclidean {1000.0} else {-1000.0}]}),
                    json!({"Similaritythreshold":if metric==VectorSimilarity::Euclidean {1000.0} else {-1000.0}}),
                ] {
                    let r:SearchRequestObject=serde_json::from_value(json!({"query":"common","length":3,"filter":predicate,"search_fields":["vector"]})).unwrap();
                    let embedding = if precision == Precision::I8 {
                        Embedding::I8(vec![24, 0, 0])
                    } else {
                        Embedding::F32(vec![1., 0., 0.])
                    };
                    let result = index
                        .search(
                            r.query_string.clone(),
                            Some(embedding),
                            r.query_type(),
                            serde_json::from_value(mode(name, ann.clone())).unwrap(),
                            false,
                            0,
                            r.length,
                            r.result_type(),
                            false,
                            r.field_filter,
                            r.facet_filter,
                            vec![],
                        )
                        .await;
                    let actual = labels(&index, &result, false).await;
                    assert!(
                        actual.iter().all(|i| *i >= 16 && *i < 24),
                        "{name} {metric} {precision} {quantization} {ann}"
                    );
                    if ann != json!({"Nprobe":1}) {
                        assert_eq!(
                            actual.len(),
                            3,
                            "{name} {metric} {precision} {quantization} {ann}"
                        );
                    }
                    if name == "Vector"
                        && metric == VectorSimilarity::Cosine
                        && precision == Precision::F32
                        && quantization == Quantization::None
                        && ann == json!("All")
                    {
                        assert_eq!(actual, vec![16, 17, 18]);
                    }
                }
            }
            index.write().await.clear_index().await;
            let mut replacement = doc(1);
            replacement.insert(
                "other_vector".into(),
                if precision == Precision::I8 {
                    json!([24, 0, 0])
                } else {
                    json!([1., 0., 0.])
                },
            );
            if precision == Precision::I8 {
                replacement.insert("vector".into(), json!([24, 1, 0]));
            }
            index.index_documents(vec![replacement]).await.unwrap();
            index.commit().await.unwrap();
            // The set dictionary reuses ID 0 after clear; old memberships must not survive.
            let r:SearchRequestObject=serde_json::from_value(json!({"query":"common","filter":[{"StringSet32":{"field":"tags","filter":["even"]}}]})).unwrap();
            let result = index
                .search(
                    r.query_string.clone(),
                    None,
                    r.query_type(),
                    r.search_mode(std::num::NonZeroUsize::new(seekpower_server::api_types::DEFAULT_NPROBE).unwrap()).unwrap(),
                    false,
                    0,
                    10,
                    r.result_type(),
                    false,
                    vec![],
                    r.facet_filter,
                    vec![],
                )
                .await;
            assert!(labels(&index, &result, false).await.is_empty());
            index.close().await.unwrap();
            drop(index);
            fs::remove_dir_all(path).unwrap();
        }
    }
}
