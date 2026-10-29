#[path = "common/scale.rs"] mod scale;
#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
// Strict regression tests for the `Union` scan fill-exemption fix (B2).
//
// `union_scan_32` (anyseek/src/union.rs) had an inner gate
// `query_terms_max_score_sum > _elements[0].score` that lacked the
// "heap not yet full" (`current_heap_size < top_k`) exemption that every
// sibling path (scan_8, docid_2, docid_3) has. Once the heap filled, every
// same-score follower was vetoed, so many-term Unions under-filled the heap
// (observed: 14/100 on uniform low-score docs, 1/100 on disjoint docid-sorted
// corpora).
//
// Oracle strategy (no reimplementation of BM25): run the exact same engine at
// two truncation levels. With `length == total hits`, the heap is never
// full, so no candidate is ever vetoed and the result is the precise global
// ranking. The truncated `top_k` run must behave like an exact top-`k`
// selection over that global ranking: it returns exactly `k` hits, contains
// every hit whose score is strictly above the `k`th-best score, and contains
// no hit strictly below it. Doc identity at the exact-score tie boundary may
// legitimately differ between two deterministic-but-arbitrary tie selections,
// so the assertion is deliberately tie-tolerant.
//
// On top of that, a corpus with a strict score gap at the truncation boundary
// locks the exact doc set. Ties inside the selected set do not change membership.
//
// Self-contained (own index directories), deterministic, no sleeps.


use anyseek::index::{
    AccessType, Clustering, DocumentCompression, FrequentwordType, IndexMetaObject, LexicalSimilarity, NgramSet, StopwordType, TokenizerType,
    };
use anyseek::record::RecordSearch;
use anyseek::search::{QueryType, ResultType, SearchMode};
use anyseek::vector::Inference;
use std::{fs, path::Path};

fn meta() -> IndexMetaObject {
    IndexMetaObject {
        id: 0,
        name: "union_topk_fill_ranking".into(),
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
    offset: usize,
    length: usize,
    result_type: ResultType,
) -> RecordSearch {
    index.query(RecordQuery {text:query.into(),vector:None,query_type:QueryType::Union,mode:SearchMode::Lexical,offset:offset,length:length,result_type:result_type,fields:Vec::new(),filters:Vec::new(),sort:Vec::new(),temporal:None}).await.unwrap()
}

async fn fresh(dir: &str) -> RecordPartition {
    let path = Path::new(dir);
    let _ = fs::remove_dir_all(path);
    Ok::<_, String>(records::create(path, meta(), (&schema()).to_vec()).await)
        .unwrap()
}

/// Assert `topk` equals a valid exact top-`k` of the full ranking `oracle`.
/// Tie-tolerant: only the score threshold structure is compared.
fn assert_valid_topk(topk: &RecordSearch, oracle: &RecordSearch) {
    let k = topk.hits.len();
    assert!(k > 0, "top_k run returned nothing");
    assert_eq!(
        topk.count_total, oracle.count_total,
        "hit totals disagree between the truncated and full runs"
    );

    let mut scores: Vec<f32> = oracle.hits.iter().map(|r| r.score).collect();
    scores.sort_by(|a, b| b.partial_cmp(a).unwrap());
    let threshold = scores[k - 1];
    let strictly_better = scores.iter().filter(|s| **s > threshold).count();

    let above = topk.hits.iter().filter(|r| r.score > threshold).count();
    let below = topk.hits.iter().filter(|r| r.score < threshold).count();
    assert_eq!(
        above, strictly_better,
        "top-{} misses {}-size strictly-better group; threshold={threshold}",
        k, strictly_better
    );
    assert_eq!(
        below, 0,
        "top-{} returned {below} hits below the kth-best score {threshold}",
        k
    );

    for w in topk.hits.windows(2) {
        assert!(w[0].score >= w[1].score, "results not sorted desc");
    }
}

/// Sorted multiset of (doc_id, exact f32 score bits). Exact membership comparisons
/// require a strict score gap at the truncation boundary, not globally unique scores.
fn strict_key(results: &[(usize, u32)]) -> Vec<(usize, u32)> {
    let mut v = results.to_vec();
    v.sort_unstable();
    v
}

/// `>8`-term Union (hits `union_scan_32`) with a clear score gap: the top-100
/// must contain the entire high-score tier plus exactly 60 of the tied
/// low-score tier, none of them worse than the boundary score.
#[tokio::test]
async fn scan32_topk_matches_global_ranking() {
    let index = fresh("tests/index_scan32_ranking/").await;

    let words: Vec<String> = (1..=12).map(|i| format!("w{i:02}")).collect();

    let mut docs = vec![];
    // High-score tier: every doc carries all 12 terms.
    for i in 0..40 {
        let title = words.join(" ") + &format!(" hi{i}");
        docs.push(doc(&title));
    }
    let per_term = scale::choose(10, 80);
    let population = 40 + 12 * per_term;
    assert!(population > 100);
    // Low-score tier: each doc carries exactly one term (uniform, low idf).
    for (w, n) in words.iter().zip(0..12) {
        for i in (n * per_term)..(n * per_term + per_term) {
            docs.push(doc(&format!("{w} lo{i}")));
        }
    }
    records::insert(&index, docs).await.unwrap();
    index.compact_journal().await.unwrap();

    let query = words.join(" ");
    let total = search(&index, &query, 0, 1, ResultType::Count).await;
    assert_eq!(total.count_total, population);

    // Oracle: length == total hits => heap never full => exact global ranking.
    let oracle = search(&index, &query, 0, population, ResultType::TopkCount).await;
    assert_eq!(oracle.hits.len(), population);

    let topk = search(&index, &query, 0, 100, ResultType::TopkCount).await;
    assert_eq!(topk.hits.len(), 100);
    assert_valid_topk(&topk, &oracle);


}

/// Directly reproduces the original B2 puke case: disjoint terms, uniform
/// low idf, sparse docs — pre-fix this filled only a handful of the heap.
#[tokio::test]
async fn scan32_sparse_uniform_fills_heap() {
    let index = fresh("tests/index_scan32_sparse/").await;

    let per_term = scale::choose(10, 100);
    let population = 20 * per_term;
    assert!(population > 100);
    let mut titles = vec![];
    // Twenty disjoint term groups; full scale retains 100 docs per term.
    for w in 1..=20 {
        for i in 0..per_term {
            titles.push(format!("spar{w:02} uniq{}{w:02}", i));
        }
    }
    let docs: Vec<anyseek::index::Document> = titles.iter().map(|t| doc(t)).collect();
    records::insert(&index, docs).await.unwrap();
    index.compact_journal().await.unwrap();

    let query = (1..=20)
        .map(|i| format!("spar{i:02}"))
        .collect::<Vec<_>>()
        .join(" ");

    let total = search(&index, &query, 0, 1, ResultType::Count).await;
    assert_eq!(total.count_total, population);

    let oracle = search(&index, &query, 0, population, ResultType::Topk).await;
    assert_eq!(oracle.hits.len(), population);

    let topk = search(&index, &query, 0, 100, ResultType::Topk).await;
    assert_eq!(topk.hits.len(), 100, "scan_32 under-filled the heap");
    assert_valid_topk(&topk, &oracle);


}

/// Twenty score tiers of twenty docs each: top-100 selects five complete tiers.
/// Verify the boundary gap before requiring exact doc/score membership.
/// The former modulo-13 corpus split a tied tier at rank 100, making that
/// assertion depend on an unspecified tie selection.
#[tokio::test]
async fn scan32_score_gap_exact_selection() {
    let index = fresh("tests/index_scan32_score_gap/").await;

    let words: Vec<String> = (1..=11).map(|i| format!("d{i:02}")).collect();
    let mut docs = vec![];
    // Each doc contains all query terms. Equal-size length tiers keep the
    // top-100 boundary between tiers rather than inside a tied group.
    for i in 0..400 {
        let pad = "pad ".repeat(i % 20);
        docs.push(doc(&format!("{} {pad}rare{i} finite{i}", words.join(" "))));
    }
    records::insert(&index, docs).await.unwrap();
    index.compact_journal().await.unwrap();

    let query = words.join(" ");
    let oracle = search(&index, &query, 0, 400, ResultType::Topk).await;
    assert_eq!(oracle.hits.len(), 400);
    assert!(
        oracle.hits[99].score > oracle.hits[100].score,
        "fixture must have a strict score gap at the top-100 boundary"
    );

    let topk = search(&index, &query, 0, 100, ResultType::Topk).await;
    assert_eq!(topk.hits.len(), 100);

    let a: Vec<(usize, u32)> = topk
        .hits
        .iter()
        .map(|r| (r.record.record_id.parse::<usize>().unwrap(), r.score.to_bits()))
        .collect();
    let b: Vec<(usize, u32)> = oracle.hits[..100]
        .iter()
        .map(|r| (r.record.record_id.parse::<usize>().unwrap(), r.score.to_bits()))
        .collect();
    assert_eq!(
        strict_key(&a),
        strict_key(&b),
        "score-gap top-100 must match the global ranking exactly"
    );


}

/// TopkCount and Topk must agree on selection and on the returned count.
#[tokio::test]
async fn scan32_topkcount_and_topk_agree() {
    let index = fresh("tests/index_scan32_agree/").await;

    let words: Vec<String> = (1..=11).map(|i| format!("a{i:02}")).collect();
    let mut docs = vec![];
    for i in 0..120 {
        let terms = words
            .iter()
            .enumerate()
            .filter(|(j, _)| (i + j) % 3 != 0)
            .map(|(_, w)| w.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        docs.push(doc(&format!("{terms} doc{i}")));
    }
    for i in 0..40 {
        let title = words.iter().cloned().collect::<Vec<_>>().join(" ") + &format!(" g{i}");
        docs.push(doc(&title));
    }
    records::insert(&index, docs).await.unwrap();
    index.compact_journal().await.unwrap();

    let query = words.join(" ");
    let oracle = search(&index, &query, 0, 160, ResultType::Topk).await;
    let topk = search(&index, &query, 0, 50, ResultType::Topk).await;
    let topk_count = search(&index, &query, 0, 50, ResultType::TopkCount).await;

    assert_eq!(topk.count_total, oracle.count_total);
    assert_eq!(topk_count.count_total, oracle.count_total);
    assert_valid_topk(&topk, &oracle);
    assert_valid_topk(&topk_count, &oracle);

    let a: Vec<(usize, u32)> = topk
        .hits
        .iter()
        .map(|r| (r.record.record_id.parse::<usize>().unwrap(), r.score.to_bits()))
        .collect();
    let b: Vec<(usize, u32)> = topk_count
        .hits
        .iter()
        .map(|r| (r.record.record_id.parse::<usize>().unwrap(), r.score.to_bits()))
        .collect();
    assert_eq!(strict_key(&a), strict_key(&b));


}

/// Pagination sanity on the fixed path: an offset slice of a scan_32 ranking
/// must match the corresponding slice of the full ranking's scores.
#[tokio::test]
async fn scan32_pagination_is_consistent() {
    let index = fresh("tests/index_scan32_pagination/").await;

    let words: Vec<String> = (1..=13).map(|i| format!("p{i:02}")).collect();
    let mut docs = vec![];
    for i in 0..250 {
        let title = format!("{} doc{i}", words.join(" "));
        docs.push(doc(&title));
    }
    records::insert(&index, docs).await.unwrap();
    index.compact_journal().await.unwrap();

    let query = words.join(" ");
    let full = search(&index, &query, 0, 250, ResultType::Topk).await;
    let page = search(&index, &query, 30, 40, ResultType::Topk).await;

    assert_eq!(page.hits.len(), 40);
    let pa: Vec<u32> = page.hits.iter().map(|r| r.score.to_bits()).collect();
    let fb: Vec<u32> = full.hits[30..70]
        .iter()
        .map(|r| r.score.to_bits())
        .collect();
    assert_eq!(
        pa, fb,
        "pagination scores must match the full ranking slice"
    );


}
