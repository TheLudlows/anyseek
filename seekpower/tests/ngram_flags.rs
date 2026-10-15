//! Independent N-gram flags must preserve literal phrase matches through persistence.
use seekpower::{
    commit::Commit,
    index::{
        AccessType, Close, Clustering, DocumentCompression, FrequentwordType, IndexArc,
        IndexDocuments, IndexMetaObject, LexicalSimilarity, SchemaField, StopwordType,
        TokenizerType, create_index, open_index,
    },
    search::{QueryType, ResultType, Search, SearchMode},
    vector::Inference,
};
use serde_json::json;
use std::{collections::BTreeSet, fs, time::Duration};

const DOCUMENTS: &[&str] = &[
    "of the art",
    "of art the",
    "art of the",
    "of the and",
    "of the",
    "of art",
    "art of",
    "rust",
    "of blue the art",
];
const PHRASES: &[&str] = &[
    "of the art",
    "of art the",
    "art of the",
    "of the and",
    "of the",
    "of art",
    "art of",
    "the art",
    "of missing the",
];

async fn assert_phrases(index: &IndexArc, realtime: bool, context: &str) {
    for phrase in PHRASES {
        let terms: Vec<_> = phrase.split_whitespace().collect();
        let expected: BTreeSet<_> = DOCUMENTS
            .iter()
            .enumerate()
            .filter(|(_, body)| {
                body.split_whitespace()
                    .collect::<Vec<_>>()
                    .windows(terms.len())
                    .any(|window| window == terms)
            })
            .map(|(id, _)| id)
            .collect();
        for result_type in [ResultType::TopkCount, ResultType::Count] {
            let result = index
                .search(
                    format!("\"{phrase}\""),
                    None,
                    QueryType::Intersection,
                    SearchMode::Lexical,
                    false,
                    0,
                    DOCUMENTS.len(),
                    result_type.clone(),
                    realtime,
                    vec![],
                    vec![],
                    vec![],
                )
                .await;
            assert!(result.error.is_none(), "{context}: {:?}", result.error);
            assert_eq!(
                result.result_count_total,
                expected.len(),
                "{context}: {phrase}"
            );
            if result_type == ResultType::TopkCount {
                assert_eq!(
                    result
                        .results
                        .iter()
                        .map(|hit| hit.doc_id)
                        .collect::<BTreeSet<_>>(),
                    expected,
                    "{context}: {phrase}"
                );
            }
        }
    }
}

async fn check_mask(mask: u8, access: AccessType, similarity: LexicalSimilarity) {
    let path = std::env::temp_dir().join(format!(
        "seekpower-ngram-{}-{mask}-{access:?}-{similarity:?}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&path);
    let schema: Vec<SchemaField> = serde_json::from_value(json!([
        {"field":"body", "field_type":"Text", "store":true, "index_lexical":true}
    ]))
    .unwrap();
    let meta = IndexMetaObject {
        id: 0,
        name: "ngram_flags".into(),
        lexical_similarity: similarity,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::Custom {
            terms: vec!["of".into(), "the".into(), "and".into()],
        },
        ngram_indexing: mask,
        document_compression: DocumentCompression::Snappy,
        access_type: access,
        clustering: Clustering::None,
        inference: Inference::None,
    };
    let index = create_index(&path, meta, &schema, 11, true, Some(1))
        .await
        .unwrap();
    index
        .index_documents(
            DOCUMENTS
                .iter()
                .map(|body| serde_json::from_value(json!({"body":body})).unwrap())
                .collect(),
        )
        .await
        .unwrap();
    // Writes are queued. Wait for indexing completion, without committing the realtime data.
    tokio::time::timeout(Duration::from_secs(10), async {
        while index.read().await.indexed_doc_count().await != DOCUMENTS.len() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("indexing completes");
    assert_eq!(index.read().await.committed_doc_count().await, 0);
    assert_phrases(&index, true, &format!("mask={mask}, realtime")).await;
    index.commit().await;
    assert_phrases(&index, false, &format!("mask={mask}, committed")).await;
    index.close().await;
    drop(index);
    let index = open_index(&path).await.unwrap();
    assert_eq!(index.read().await.meta.ngram_indexing, mask);
    assert_phrases(&index, false, &format!("mask={mask}, reopened")).await;
    index.close().await;
    drop(index);
    fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn ffr_independent_flag_preserves_phrase_recall() {
    for access in [AccessType::Mmap] {
        for similarity in [LexicalSimilarity::Bm25f, LexicalSimilarity::Bm25fProximity] {
            check_mask(32, access.clone(), similarity).await;
        }
    }
}

#[tokio::test]
async fn frf_independent_flag_preserves_phrase_recall() {
    for access in [AccessType::Mmap] {
        for similarity in [LexicalSimilarity::Bm25f, LexicalSimilarity::Bm25fProximity] {
            check_mask(64, access.clone(), similarity).await;
        }
    }
}

#[tokio::test]
async fn remaining_flags_and_combinations_preserve_phrase_recall() {
    for mask in [0, 1, 2, 4, 8, 16, 9, 112, 127] {
        check_mask(mask, AccessType::Mmap, LexicalSimilarity::Bm25f).await;
    }
}
