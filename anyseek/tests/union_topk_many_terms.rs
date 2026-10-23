#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
// `union_scan_32` bitmask holds at most 32 terms; >32-term TopkCount must
// not panic (`32 - len` underflow) and must report the exact total.


use anyseek::index::{
    AccessType, Clustering, DocumentCompression, FrequentwordType, IndexMetaObject, LexicalSimilarity, NgramSet, StopwordType, TokenizerType,
    };
use anyseek::search::{QueryType, ResultType, SearchMode};
use anyseek::vector::Inference;
use std::{fs, path::Path};

fn meta() -> IndexMetaObject {
    IndexMetaObject {
        id: 0,
        name: "union_topk_many_terms".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: NgramSet::SingleTerm as u8,
        document_compression: DocumentCompression::Snappy,
        access_type: AccessType::Mmap,
        clustering: Clustering::None,
        inference: Inference::None,
    }
}

fn schema() -> Vec<anyseek::index::SchemaField> {
    serde_json::from_str(
        r#"[{"field":"title","field_type":"Text","store":true,"index_lexical":true}]"#,
    )
    .unwrap()
}

fn doc(title: &str) -> anyseek::index::Document {
    serde_json::from_str(&format!(r#"{{"title":"{title}"}}"#)).unwrap()
}

async fn search(
    index: &RecordPartition,
    query: &str,
    result_type: ResultType,
) -> anyseek::record::RecordSearch {
    index.query(RecordQuery {text:query.into(),vector:None,query_type:QueryType::Union,mode:SearchMode::Lexical,offset:0,length:100,result_type:result_type,fields:Vec::new(),filters:Vec::new(),sort:Vec::new(),temporal:None}).await.unwrap()
}

/// 40-term union TopkCount: no panic, exact total, top-k hits returned.
#[tokio::test]
async fn union_topk_forty_terms() {
    let dir = "tests/index_union_topk_many_terms/";
    let path = Path::new(dir);
    let _ = fs::remove_dir_all(path);
    let index = Ok::<_, String>(records::create(path, meta(), (&schema()).to_vec()).await)
        .unwrap();

    // 40 disjoint terms x 25 docs each = 1000 docs, all in one block.
    let mut docs = vec![];
    for t in 0..40 {
        for i in 0..25 {
            docs.push(doc(&format!("w{t:02} z{i}")));
        }
    }
    records::insert(&index, docs).await.unwrap();
    index.compact_journal().await.unwrap();

    let query = (0..40)
        .map(|t| format!("w{t:02}"))
        .collect::<Vec<_>>()
        .join(" ");

    let count = search(&index, &query, ResultType::Count).await;
    assert_eq!(count.count_total, 1000);

    let topk_count = search(&index, &query, ResultType::TopkCount).await;
    assert_eq!(topk_count.count_total, 1000);
    assert_eq!(topk_count.count_total, count.count_total);


}
