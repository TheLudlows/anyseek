#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
// Repro: multi-term Topk/TopkCount over all-frequent terms must return results,
// not an empty heap. Pure public API.
// Run: cargo test -p anyseek --test frequent_topk_consistency

use anyseek::index::{
    AccessType, Clustering, Document, DocumentCompression, FrequentwordType,
    IndexMetaObject, LexicalSimilarity, NgramSet, StopwordType,
    TokenizerType, };
use anyseek::search::{QueryType, ResultType, SearchMode};
use anyseek::vector::Inference;
use std::{fs, path::Path};

async fn search(
    index_arc: &RecordPartition,
    q: &str,
    qt: QueryType,
    rt: ResultType,
    len: usize,
) -> anyseek::record::RecordSearch {
    index_arc.query(RecordQuery {text:q.into(),vector:None,query_type:qt,mode:SearchMode::Lexical,offset:0,length:len,result_type:rt,fields:Vec::new(),filters:Vec::new(),sort:Vec::new(),temporal:None}).await.unwrap()
}

#[tokio::test]
async fn frequent_terms_topk_returns_results() {
    let index_path = Path::new("/tmp/repro_frequent_topk/");
    let _ = fs::remove_dir_all(index_path);
    let schema_json = r#"
    [{"field":"title","field_type":"Text","store":false,"index_lexical":true,"longest":true}]"#;
    let schema: Vec<anyseek::index::SchemaField> = serde_json::from_str(schema_json).unwrap();
    let meta = IndexMetaObject {
        id: 0,
        name: "repro".into(),
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
    };
    // NOTE: keep on the single-shard fast path; multi-shard covered by suite.
    let index_arc = Ok::<_, String>(records::create(index_path, meta, (&schema).to_vec()).await)
        .unwrap();

    // 3000 docs: "common" in all (100%), "half" in evens (50%) -> every query
    // term covers >=50% (all_terms_frequent); top_k=10 < 3000/256.
    for i in 0..3000 {
        let mut t = String::from("common ");
        if i % 2 == 0 {
            t.push_str("half ");
        }
        let doc: Document = serde_json::from_str(&format!(r#"{{"title":"{t}"}}"#)).unwrap();
        records::insert(&index_arc, vec![doc]).await.unwrap();
    }
    index_arc.compact_journal().await.unwrap();

    // Intersection Topk must not come back empty with 1500 matches.
    let rlo = search(
        &index_arc,
        "common half",
        QueryType::Intersection,
        ResultType::Topk,
        10,
    )
    .await;
    assert_eq!(
        rlo.hits.len(),
        10,
        "BUG: intersection Topk empty for all-frequent terms"
    );

    // TopkCount: same results, consistent totals.
    let rtc = search(
        &index_arc,
        "common half",
        QueryType::Intersection,
        ResultType::TopkCount,
        10,
    )
    .await;
    assert_eq!(
        rtc.hits.len(),
        10,
        "BUG: intersection TopkCount results empty"
    );
    assert_eq!(
        rtc.count_total, 1500,
        "intersection total must count all matches"
    );

    // Counts were (and stay) correct — the bug only dropped ranking.
    let c = search(
        &index_arc,
        "common half",
        QueryType::Intersection,
        ResultType::Count,
        10,
    )
    .await;
    assert_eq!(c.count_total, 1500);

    
    let _ = fs::remove_dir_all(index_path);
}
