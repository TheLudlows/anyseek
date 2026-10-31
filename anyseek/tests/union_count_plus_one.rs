#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
// Same-block overlapping union must count exactly (pins the `+1` encoding).


use anyseek::index::{
    Clustering, FrequentwordType, IndexMetaObject, LexicalSimilarity, NgramSet, StopwordType, TokenizerType,
    };
use anyseek::search::{QueryType, ResultType, SearchMode};
use anyseek::vector::Inference;
use std::{fs, path::Path};

fn meta() -> IndexMetaObject {
    IndexMetaObject {
        name: "union_count_plus_one".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::TantivySimpleV1,
        text_normalization: anyseek::index::TextNormalization::None,
        field_length_normalization: anyseek::index::FieldLengthNormalization::Shared,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: NgramSet::SingleTerm as u8,
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

#[tokio::test]
async fn union_count_overlap_same_block() {
    let dir = "tests/index_union_plus_one/";
    let path = Path::new(dir);
    let _ = fs::remove_dir_all(path);
    let index = records::create(path, meta(), (&schema()).to_vec()).await;

    // Overlapping docs: doc2, doc4 contain BOTH alpha and beta.
    let docs = vec![
        doc("alpha only item"),
        doc("beta only item"),
        doc("alpha beta both item"),
        doc("alpha only item"),
        doc("alpha beta both item"),
        doc("beta only item"),
    ];
    records::insert(&index, docs).await.unwrap();
    index.compact_journal().await.unwrap();

    // 6 docs total in the same block; all contain at least one of alpha/beta.
    let count = search(&index, "alpha beta", ResultType::Count).await;
    let topk_count = search(&index, "alpha beta", ResultType::TopkCount).await;

    let expected = 6;
    println!("Count result: {}", count.count_total);
    println!("TopkCount result: {}", topk_count.count_total);
    assert_eq!(count.count_total, expected);
    assert_eq!(topk_count.count_total, expected);


}
