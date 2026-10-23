//! Independent N-gram flags must preserve literal phrase matches through persistence.
use anyseek::{
    index::{
        AccessType, Clustering, DocumentCompression, FrequentwordType, 
         IndexMetaObject, LexicalSimilarity, SchemaField, StopwordType,
        TokenizerType, 
    },
    search::{QueryType, ResultType, SearchMode},
    vector::Inference,
};
#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
use serde_json::json;
use std::{collections::BTreeSet, fs};

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

async fn assert_phrases(index: &RecordPartition, _realtime: bool, context: &str) {
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
                .query(RecordQuery {
                    text: format!("\"{phrase}\""), vector: None, temporal: None,
                    query_type: QueryType::Intersection, mode: SearchMode::Lexical,
                    offset: 0, length: DOCUMENTS.len(), result_type: result_type.clone(),
                    fields: vec![], filters: vec![], sort: vec![],
                }).await.unwrap();
            assert_eq!(
                result.count_total,
                expected.len(),
                "{context}: {phrase}"
            );
            if result_type == ResultType::TopkCount {
                assert_eq!(
                    result
                        .hits
                        .iter()
                        .map(|hit| hit.record.record_id.parse::<usize>().unwrap())
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
        "anyseek-ngram-{}-{mask}-{access:?}-{similarity:?}",
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
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
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
    let index = records::create(&path, meta, schema).await;
    records::append(&index, DOCUMENTS.iter().map(|body| serde_json::from_value(json!({"body":body})).unwrap()).collect()).await;
    assert_eq!(index.statistics().await.unwrap().active_records, DOCUMENTS.len());
    assert_phrases(&index, true, &format!("mask={mask}, realtime")).await;
    index.compact_journal().await.unwrap();
    assert_phrases(&index, false, &format!("mask={mask}, committed")).await;
    
    drop(index);
    let index = RecordPartition::open(&path, records::JOURNAL_LIMIT).await.unwrap();
    assert_eq!(index.configuration().await.unwrap().index.ngram_indexing, mask);
    assert_phrases(&index, false, &format!("mask={mask}, reopened")).await;
    
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
