// Exercises disjoint candidate groups; retired 65536-posting layout is not recreated.
#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
// Regression test: multi-term `Union` `Count`/`TopkCount` must be consistent
// across repeated queries when terms span multiple blocks.
//
// `union_count` sorted terms including end-flagged ones, so a wrong-block
// term could land at index 0 and corrupt the initial count and bitmap.


use anyseek::index::{
    AccessType, Clustering, DocumentCompression, FrequentwordType, IndexMetaObject, LexicalSimilarity, NgramSet, StopwordType, TokenizerType,
    };
use anyseek::search::{QueryType, ResultType, SearchMode};
use anyseek::vector::Inference;
use std::{fs, path::Path};

fn meta() -> IndexMetaObject {
    IndexMetaObject {
        id: 0,
        name: "union_count_cross_block".into(),
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

async fn fresh(dir: &str) -> RecordPartition {
    let path = Path::new(dir);
    let _ = fs::remove_dir_all(path);
    // Single shard so terms span multiple blocks (with default sharding each
    // shard holds a single block and the bug is unreachable).
    Ok::<_, String>(records::create(path, meta(), (&schema()).to_vec()).await)
        .unwrap()
}

/// 3-term union across multiple blocks must return consistent results.
///
/// Sizes are asymmetric on purpose: the end-flagged term in block 1 (alpha,
/// 60k) outranks the valid terms (beta/gamma, 5k each), so without the fix
/// the sort deterministically puts it first and the count is wrong.
#[tokio::test]
async fn union_count_cross_block_consistent() {
    let index = fresh("tests/index_union_cross_block/").await;
    let mut docs = vec![];

    // block 0: alpha (docid 0..600)
    for i in 0..600 {
        docs.push(doc(&format!("alpha item{i}")));
    }
    // padding crossing the 65536 block boundary
    for i in 0..60 {
        docs.push(doc(&format!("padding filler{i}")));
    }
    // block 1: beta (docid 66000..71000)
    for i in 0..50 {
        docs.push(doc(&format!("beta item{i}")));
    }
    // block 1: gamma (docid 71000..76000)
    for i in 0..50 {
        docs.push(doc(&format!("gamma item{i}")));
    }

    records::insert(&index, docs).await.unwrap();
    index.compact_journal().await.unwrap();

    // 60k alpha + 5k beta + 5k gamma = 700; padding must not match.
    let mut prev_count = 0;
    for rep in 0..20 {
        let count = search(&index, "alpha beta gamma", ResultType::Count).await;
        let topk_count = search(&index, "alpha beta gamma", ResultType::TopkCount).await;

        assert_eq!(
            count.count_total, 700,
            "Count absolute value mismatch at rep {rep}: expected 700, got {}",
            count.count_total
        );
        if rep == 0 {
            prev_count = count.count_total;
        } else {
            assert_eq!(
                count.count_total, prev_count,
                "Count jitter at rep {rep}: {prev_count} vs {}",
                count.count_total
            );
        }
        assert_eq!(
            count.count_total, topk_count.count_total,
            "Count and TopkCount mismatch at rep {rep}"
        );
    }


}

/// Control: 2-term union (no cross-block issue) must be stable.
#[tokio::test]
async fn union_count_two_terms_stable() {
    let index = fresh("tests/index_union_two_terms/").await;
    let mut docs = vec![];

    for i in 0..10 {
        docs.push(doc(&format!("alpha uniq{i}")));
    }
    for i in 0..10 {
        docs.push(doc(&format!("beta uniq{i}")));
    }

    records::insert(&index, docs).await.unwrap();
    index.compact_journal().await.unwrap();

    let count = search(&index, "alpha beta", ResultType::Count).await;
    let topk_count = search(&index, "alpha beta", ResultType::TopkCount).await;

    assert_eq!(count.count_total, 20);
    assert_eq!(topk_count.count_total, 20);


}
