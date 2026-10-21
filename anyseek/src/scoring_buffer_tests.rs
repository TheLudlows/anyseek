//! Deterministic corpus for before/after score-bit comparisons across buffer sizes.
use crate::{
    commit::Commit,
    index::{create_index, open_index, AccessType, Close, Clustering, DocumentCompression,
        FieldLengthNormalization, FrequentwordType, IndexDocuments, IndexMetaObject,
        LexicalSimilarity, StopwordType, TextNormalization, TokenizerType},
    search::{QueryType, ResultSort, ResultType, RrfOptions, Search, SearchMode, SortOrder},
    vector::{Embedding, Inference, Precision, Quantization},
    vector_similarity::{AnnMode, VectorSimilarity},
};
use serde_json::json;

#[tokio::test]
async fn scoring_buffer_field_count_matrix() {
    let mut snapshots = Vec::new();
    for fields in [1, 2, 3, 4, 5, 9, 32] {
        for policy in [FieldLengthNormalization::Shared, FieldLengthNormalization::PerField] {
            for similarity in [LexicalSimilarity::Bm25f, LexicalSimilarity::Bm25fProximity] {
                for shards in [1, 3] {
                    let has_vector = fields > 1;
                    let text_fields = fields - usize::from(has_vector);
                    let label = format!("scoring-buffer-{fields}-{policy:?}-{similarity:?}-{shards}");
                    let path = std::env::temp_dir().join(format!("{label}-{}", std::process::id()));
                    assert!(!path.exists());
                    let mut schema: Vec<_> = (0..text_fields).map(|f| json!({
                        "field":format!("text{f}"),"field_type":"Text","index_lexical":true,
                        "store":false,"boost":if f % 2 == 0 {1.0} else {0.5}
                    })).collect();
                    if has_vector {
                        schema.push(json!({"field":"vector","field_type":"Json","store":false,
                            "index_lexical":false,"index_vector":true}));
                    }
                    let meta = IndexMetaObject {
                        id:0, name:label.clone(), lexical_similarity:similarity.clone(),
                        tokenizer:TokenizerType::UnicodeAlphanumeric, text_normalization:TextNormalization::None,
                        field_length_normalization:policy, stop_words:StopwordType::None,
                        frequent_words:FrequentwordType::English, ngram_indexing:127,
                        document_compression:DocumentCompression::Snappy, access_type:AccessType::Mmap,
                        clustering:Clustering::None,
                        inference:if has_vector { Inference::External { dimensions:3, precision:Precision::F32,
                            quantization:Quantization::None, similarity:VectorSimilarity::Cosine } } else { Inference::None },
                    };
                    let schema = serde_json::from_value(json!(schema)).unwrap();
                    let mut index = create_index(&path, meta, &schema, 11, true, Some(shards)).await.unwrap();
                    index.index_documents((0..36).map(|i| {
                        let mut doc = serde_json::Map::new();
                        for f in 0..text_fields {
                            let text = match (i + f) % 4 {
                                0 => "the of and alpha beta alpha",
                                1 => "alpha the of and beta",
                                2 => "beta neutral the alpha",
                                _ => "neutral the of and neutral",
                            };
                            let text = if f == 0 {
                                format!("{text} {}", (0..if i % 2 == 0 {100} else {33})
                                    .map(|n|format!("q{n:03}")).collect::<Vec<_>>().join(" "))
                            } else { text.to_string() };
                            doc.insert(format!("text{f}"),json!(text));
                        }
                        if has_vector { doc.insert("vector".into(),json!([1.0,(i % 3) as f32,0.0])); }
                        serde_json::from_value(json!(doc)).unwrap()
                    }).collect()).await.unwrap();
                    tokio::time::timeout(std::time::Duration::from_secs(10), async {
                        while index.read().await.indexed_doc_count().await != 36 { tokio::task::yield_now().await; }
                    }).await.unwrap();
                    assert_eq!(index.read().await.shard_vec[0].read().await.indexed_field_vec.len(),fields);
                    for stage in 0..3 {
                        if stage == 1 { index.commit().await.unwrap(); }
                        if stage == 2 {
                            index.close().await.unwrap();
                            drop(index);
                            index = open_index(&path).await.unwrap();
                        }
                        for case in 0..17 {
                            if matches!(case, 7 | 8) && !has_vector { continue; }
                            let (short_text, qt) = match case {
                                0 => ("alpha beta the",QueryType::Union),
                                1 => ("alpha beta",QueryType::Intersection),
                                2 => ("the of and",QueryType::Phrase),
                                3 => ("the of and",QueryType::Union),
                                4 => ("alpha -neutral",QueryType::Union),
                                5 => ("alpha",QueryType::Union),
                                _ => ("alpha beta",QueryType::Union),
                            };
                            let text = if case >= 9 {
                                let count = [8, 32, 33, 100][(case - 9) / 2];
                                let mut terms: Vec<_> = (0..count).map(|n|format!("q{n:03}")).collect();
                                if case % 2 == 0 { terms.reverse(); }
                                terms.join(" ")
                            } else { short_text.to_string() };
                            let mode = match case {
                                7 => SearchMode::Hybrid { similarity_threshold:None, ann_mode:AnnMode::All },
                                8 => SearchMode::WeightedHybrid { similarity_threshold:None, ann_mode:AnnMode::All,
                                    rrf:RrfOptions {constant:5.0,keyword_weight:0.5,keyword_candidates:Some(20),
                                        vector_candidates:Some(20),..Default::default()} },
                                _ => SearchMode::Lexical,
                            };
                            let result = index.search(text.into(),(matches!(case, 7 | 8)).then(||Embedding::F32(vec![1.0,0.0,0.0])),
                                qt,mode,false,0,12,ResultType::TopkCount,stage == 0,
                                if case == 6 {vec![format!("text{}",text_fields-1)]} else {vec![]},vec![],
                                if case == 6 {vec![ResultSort {field:"_score".into(),order:SortOrder::Descending}]} else {vec![]}).await;
                            assert!(result.error.is_none(),"{label}/{stage}/{case}: {:?}",result.error);
                            if case != 4 && case != 6 { assert!(!result.results.is_empty(),"{label}/{stage}/{case}"); }
                            snapshots.push(json!({"fixture":label,"stage":stage,"case":case,
                                "count":result.result_count_total,
                                "hits":result.results.iter().map(|r|(r.doc_id,r.score.to_bits())).collect::<Vec<_>>() }));
                        }
                    }
                    index.close().await.unwrap();
                    drop(index);
                    std::fs::remove_dir_all(path).unwrap();
                }
            }
        }
    }
    if let Some(path) = std::env::var_os("ANYSEEK_SCORING_REFERENCE") {
        let expected: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(json!(snapshots), expected, "baseline result order, counts or score bits changed");
    }
    // Opt-in artifact for running this same test on the baseline and changed scorer.
    if let Some(path) = std::env::var_os("ANYSEEK_SCORING_SNAPSHOT") {
        std::fs::write(path,serde_json::to_vec(&snapshots).unwrap()).unwrap();
    }
}

#[test]
fn component_storage_inline_boundary_and_fallback() {
    use smallvec::{smallvec, SmallVec};
    for count in [0, 1, 2, 3, 4, 5, 9, 32, 256, 65536] {
        let mut actual = super::bm25_components(count);
        let mut old: SmallVec<[f32; 2]> = smallvec![0.0; count];
        assert_eq!(actual.len(), count);
        assert_eq!(actual.spilled(), count > 4, "fields={count}");
        assert_eq!(old.spilled(), count > 2);
        assert!(actual.iter().all(|v| v.to_bits() == 0));
        // Check the same lazy-cache writes and ordered f32 reads on inline and heap storage.
        // Include repeated visits, including a zero value that keeps the sentinel active.
        let mut actual_sum = 0.0f32;
        let mut old_sum = 0.0f32;
        for pass in 0..3 {
            for field in (0..count).rev() {
                let value = (field % 17) as f32 / 13.0;
                if actual[field] == 0.0 { actual[field] = value; }
                if old[field] == 0.0 { old[field] = value; }
                actual_sum += (pass + 1) as f32 / (1.0 + actual[field]);
                old_sum += (pass + 1) as f32 / (1.0 + old[field]);
                assert_eq!(actual_sum.to_bits(), old_sum.to_bits());
            }
        }
        assert_eq!(actual.iter().map(|v|v.to_bits()).collect::<Vec<_>>(),
            old.iter().map(|v|v.to_bits()).collect::<Vec<_>>());
    }
}
