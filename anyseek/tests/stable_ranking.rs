use anyseek::record::RecordSearch;
#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
// Rank ties must be resolved before local TopK and again after global merging.
use anyseek::{
    index::{
        AccessType, Clustering, DocumentCompression, FrequentwordType, IndexMetaObject, LexicalSimilarity, NgramSet, StopwordType, TokenizerType,
        },
    search::{QueryType, ResultSort, ResultType, SearchMode, SortOrder},
    vector::{Embedding, Inference, Precision, Quantization},
    vector_similarity::{AnnMode, VectorSimilarity},
};
use serde_json::json;

async fn query(
    index: &RecordPartition,
    mode: usize,
    sorts: Vec<ResultSort>,
    offset: usize,
    qt: QueryType,
    realtime: bool,
) -> Vec<(usize, u32)> {
    let sm = match mode {
        0 => SearchMode::Lexical,
        1 => SearchMode::Vector {
            similarity_threshold: None,
            ann_mode: AnnMode::All,
        },
        2 => SearchMode::Hybrid {
            similarity_threshold: None,
            ann_mode: AnnMode::All,
        },
        _ => SearchMode::Lexical,
    };
    let result = index.query(RecordQuery {text:if mode == 1 {
                ""
            } else if mode == 3 {
                "alpha"
            } else {
                "alpha beta"
            }
            .into(),vector:(mode == 1 || mode == 2).then(|| Embedding::F32(vec![1.0, 0.0, 0.0])),query_type:qt,mode:sm,offset:offset,length:7,result_type:ResultType::Topk,fields:vec![],filters:vec![],sort:sorts,temporal:None}).await.unwrap();
    result
        .hits
        .iter()
        .map(|r| (r.record.record_id.parse::<usize>().unwrap(), r.score.to_bits()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stable_ranking_shards_pagination_explicit_sort_and_concurrency() {
    for shards in [1, 3] {
        let path =
            std::env::temp_dir().join(format!("anyseek-stable-{}-{shards}", std::process::id()));
        assert!(!path.exists(), "test directory must be new");
        let schema: Vec<anyseek::index::SchemaField> = serde_json::from_value(json!([
            {"field":"body","field_type":"Text","store":false,"index_lexical":true},
            {"field":"priority","field_type":"Int","store":false,"index_lexical":false,"facet":true},
            {"field":"vector","field_type":"Json","store":false,"index_lexical":false,"index_vector":true}
        ])).unwrap();
        let meta = IndexMetaObject {
            id: 0,
            name: "stable".into(),
            lexical_similarity: LexicalSimilarity::Bm25f,
            tokenizer: TokenizerType::TantivySimpleV1,
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
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
        };
        let index = Ok::<_, String>(records::create(&path, meta, (&schema).to_vec()).await)
            .unwrap();
        // Identical scores, with reverse-valued facets to test secondary ordering.
        for component in 0..shards {
            let start = component * (90 / shards);
            records::insert(&index, (start..start + 90 / shards).map(|i| {
                serde_json::from_value(json!({"body":"alpha beta","priority":89-i,"vector":[1,0,0]})).unwrap()
            }).collect()).await.unwrap();
            if component + 1 < shards { index.compact_journal().await.unwrap(); }
        }
        // All public writes must already be visible, including the unsealed last component.
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while index.statistics().await.unwrap().physical_versions != 90 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("all realtime documents must finish indexing");
        // Cover realtime lexical selection before commit.
        let rt = query(&index, 0, vec![], 0, QueryType::Union, true).await;
        assert_eq!(
            rt.iter().map(|x| x.0).collect::<Vec<_>>(),
            (0..7).collect::<Vec<_>>(), "realtime shards={shards}, results={rt:?}"
        );
        index.compact_journal().await.unwrap();
        for mode in 0..4 {
            let expected = query(&index, mode, vec![], 0, QueryType::Union, false).await;
            assert_eq!(
                expected.iter().map(|x| x.0).collect::<Vec<_>>(),
                (0..7).collect::<Vec<_>>(),
                "mode={mode}, shards={shards}"
            );
            for _ in 0..3 {
                assert_eq!(
                    query(&index, mode, vec![], 0, QueryType::Union, false).await,
                    expected
                );
                let queries =
                    (0..4).map(|_| query(&index, mode, vec![], 0, QueryType::Union, false));
                for result in futures::future::join_all(queries).await {
                    assert_eq!(result, expected);
                }
            }
            let page = query(&index, mode, vec![], 7, QueryType::Union, false).await;
            assert_eq!(
                page.iter().map(|x| x.0).collect::<Vec<_>>(),
                (7..14).collect::<Vec<_>>()
            );
        }
        for qt in [QueryType::Union, QueryType::Intersection, QueryType::Phrase] {
            let sorts = vec![
                ResultSort {
                    field: "_score".into(),
                    order: SortOrder::Descending,
                },
                ResultSort {
                    field: "priority".into(),
                    order: SortOrder::Ascending,
                },
            ];
            let actual = query(&index, 0, sorts, 0, qt, false).await;
            assert_eq!(
                actual.iter().map(|x| x.0).collect::<Vec<_>>(),
                (83..90).rev().collect::<Vec<_>>()
            );
        }
        for order in [SortOrder::Ascending, SortOrder::Descending] {
            let actual = query(
                &index,
                0,
                vec![ResultSort {
                    field: "_score".into(),
                    order,
                }],
                0,
                QueryType::Union,
                false,
            )
            .await;
            assert_eq!(
                actual.iter().map(|x| x.0).collect::<Vec<_>>(),
                (0..7).collect::<Vec<_>>()
            );
        }
        // Record empty browsing and explicit score sort (all zero) share the
        // existing stable-identity ascending tie break.
        for order in [SortOrder::Ascending, SortOrder::Descending] {
            let result = index.query(RecordQuery {text:"".into(),vector:None,query_type:QueryType::Union,mode:SearchMode::Lexical,offset:0,length:7,result_type:ResultType::Topk,fields:vec![],filters:vec![],sort:vec![ResultSort {
                        field: "_score".into(),
                        order,
                    }],temporal:None}).await.unwrap();
            assert_eq!(
                result.hits.iter().map(|r| r.record.record_id.parse::<usize>().unwrap()).collect::<Vec<_>>(),
                (0..7).collect::<Vec<_>>()
            );
        }
        let result = index.query(RecordQuery {text:"".into(),vector:None,query_type:QueryType::Union,mode:SearchMode::Lexical,offset:0,length:7,result_type:ResultType::Topk,fields:vec![],filters:vec![],sort:vec![],temporal:None}).await.unwrap();
        assert_eq!(
            result.hits.iter().map(|r| r.record.record_id.parse::<usize>().unwrap()).collect::<Vec<_>>(),
            (0..7).collect::<Vec<_>>()
        );

        drop(index);
        std::fs::remove_dir_all(path).unwrap(); // Only this test's newly created fixture.
    }
}

/// Equal-score block bounds may skip later IDs, never earlier IDs or user sorts.
#[tokio::test]
async fn stable_equal_score_block_boundary_and_explicit_id_sort() {
    let path = std::env::temp_dir().join(format!("anyseek-stable-blocks-{}", std::process::id()));
    assert!(!path.exists());
    let schema: Vec<anyseek::index::SchemaField> = serde_json::from_value(json!([
        {"field":"body","field_type":"Text","store":false,"index_lexical":true}
    ]))
    .unwrap();
    let meta = IndexMetaObject {
        id: 0,
        name: "stable-blocks".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::TantivySimpleV1,
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: NgramSet::SingleTerm as u8,
        document_compression: DocumentCompression::Snappy,
        access_type: AccessType::Mmap,
        clustering: Clustering::None,
        inference: Inference::None,
    };
    let index = Ok::<_, String>(records::create(&path, meta, (&schema).to_vec()).await)
        .unwrap();
    for _ in 0..7 {
        records::insert(&index, (0..100)
                    .map(|_| serde_json::from_value(json!({"body":"alpha beta gamma delta epsilon zeta eta theta iota"})).unwrap())
                    .collect())
            .await
            .unwrap();
    }
    index.compact_journal().await.unwrap();
    for text in ["alpha", "alpha beta", "alpha beta gamma", "alpha beta gamma delta epsilon zeta eta theta iota"] {
        for qt in [QueryType::Union, QueryType::Intersection] {
            for rt in [ResultType::Topk, ResultType::TopkCount] {
                for (sort, expected) in [
                    (vec![], (0..100).collect::<Vec<_>>()),
                    (
                        vec![ResultSort {
                            field: "_id".into(),
                            order: SortOrder::Descending,
                        }],
                        (600..700).rev().collect(),
                    ),
                ] {
                    let result = index.query(RecordQuery {text:text.into(),vector:None,query_type:qt.clone(),mode:SearchMode::Lexical,offset:0,length:100,result_type:rt.clone(),fields:vec![],filters:vec![],sort:sort,temporal:None}).await.unwrap();
                    assert_eq!(
                        result.hits.iter().map(|x| x.record.record_id.parse::<usize>().unwrap()).collect::<Vec<_>>(),
                        expected
                    );
                    if rt == ResultType::TopkCount {
                        assert_eq!(result.count_total, 700);
                    }
                }
            }
        }
    }

    drop(index);
    std::fs::remove_dir_all(path).unwrap();
}
