#[path = "common/scale.rs"] mod scale;
#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
// Repro: multi-term Topk/TopkCount over all-frequent terms must return results,
// not an empty heap. Pure public API.
// Run: cargo test -p anyseek --test frequent_topk_consistency

use anyseek::index::{
    Clustering, Document, FrequentwordType,
    IndexMetaObject, LexicalSimilarity, NgramSet, StopwordType,
    TokenizerType, };
use anyseek::search::{QueryType, ResultType, SearchMode};
use anyseek::vector::Inference;

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
    let root = tempfile::tempdir().unwrap();
    let index_path = root.path().join("index");
    let schema_json = r#"
    [{"field":"title","field_type":"Text","store":false,"index_lexical":true}]"#;
    let schema: Vec<anyseek::index::SchemaField> = serde_json::from_str(schema_json).unwrap();
    let meta = IndexMetaObject {
        name: "repro".into(),
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
    // Independent public Record fixture.
    let index_arc = records::create(&index_path, meta, (&schema).to_vec()).await;

    let population = scale::choose(256, 3000);
    assert!(population % 2 == 0 && population / 2 > 10);
    // Both scales retain 100%/50% document frequencies and >10 matches.
    // Full scale retains the original 3000 records; legacy frequent-word
    // fast-path thresholds are not a current engine contract.
    for i in 0..population {
        let mut t = String::from("common ");
        if i % 2 == 0 {
            t.push_str("half ");
        }
        let doc: Document = serde_json::from_str(&format!(r#"{{"title":"{t}"}}"#)).unwrap();
        records::insert(&index_arc, vec![doc]).await.unwrap();
    }
    index_arc.compact_journal().await.unwrap();

    // Intersection Topk must not come back empty despite many matches.
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
        rtc.count_total, population / 2,
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
    assert_eq!(c.count_total, population / 2);


    drop(index_arc);
}
