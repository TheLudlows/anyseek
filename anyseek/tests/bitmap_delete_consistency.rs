#[path = "common/records.rs"] mod records;
use anyseek::record::{RecordPartition, RecordQuery};
// Dense AND/OR count regression with one shared deleted-record fixture.
// The retired custom Bitmap codec is not a current public contract.

use anyseek::index::{
    AccessType, Clustering, DocumentCompression, FrequentwordType,
    IndexMetaObject, LexicalSimilarity, NgramSet, StopwordType,
    TokenizerType, };
use anyseek::search::{QueryType, ResultType, SearchMode};
use anyseek::vector::Inference;
use std::{collections::HashSet, fs, path::Path};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn meta() -> IndexMetaObject {
    IndexMetaObject {
        id: 0,
        name: "bitmap_delete_consistency".into(),
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

async fn search(
    index: &RecordPartition,
    query: &str,
    query_type: QueryType,
    result_type: ResultType,
) -> anyseek::record::RecordSearch {
    index.query(RecordQuery {text:query.into(),vector:None,query_type:query_type,mode:SearchMode::Lexical,offset:0,length:100,result_type:result_type,fields:Vec::new(),filters:Vec::new(),sort:Vec::new(),temporal:None}).await.unwrap()
}

async fn setup() -> (RecordPartition, HashSet<u64>, HashSet<u64>) {
    const N: u64 = 2048;
    let path = Path::new("tests/index_delete_bitmap/");
    let _ = fs::remove_dir_all(path);
    // Preserve seeded dense AND/OR membership and deletion oracles.
    // The deleted legacy Bitmap codec boundary is no longer a public contract.
    let index = Ok::<_, String>(records::create(path, meta(), (&schema()).to_vec()).await)
        .unwrap();
    let mut rng = Rng(0xabc);
    let mut set_x = HashSet::new();
    while set_x.len() < 768 {
        set_x.insert(rng.below(N));
    }
    let mut set_y = HashSet::new();
    while set_y.len() < 768 {
        set_y.insert(rng.below(N));
    }
    let mut rng = Rng(0x1);
    let docs: Vec<anyseek::index::Document> = (0..N)
        .map(|i| {
            let mut t = format!("doc number {i}");
            if set_x.contains(&i) {
                t.push_str(" termx");
            }
            if set_y.contains(&i) {
                t.push_str(" termy");
            }
            t.push_str(&format!(" w{}", rng.below(100000)));
            serde_json::from_str(&format!(r#"{{"title":"{t}"}}"#)).unwrap()
        })
        .collect();
    records::insert(&index, docs).await.unwrap();
    index.compact_journal().await.unwrap();

    let vx: Vec<u64> = set_x.iter().copied().collect();
    let overlap: Vec<u64> = set_x.intersection(&set_y).copied().collect();
    let mut rng = Rng(0xdef);
    let mut del = HashSet::new();
    while del.len() < 400 {
        del.insert(vx[rng.below(vx.len() as u64) as usize]);
    }
    for i in 0..50.min(overlap.len()) {
        del.insert(overlap[i]);
    }
    records::delete(&index, del.iter().map(|id|*id as usize).collect()).await;
    index.compact_journal().await.unwrap();
    (index, set_x, set_y)
}

/// AND and OR totals must exclude deleted documents for both result modes.
#[tokio::test]
async fn dense_and_or_counts_exclude_deleted() {
    let (index, set_x, set_y) = setup().await;
    // Recompute ground truth with the same seeds as setup.
    let (live_and, live_or) = ground_truth(&set_x, &set_y);
    let count = search(
        &index,
        "termx termy",
        QueryType::Intersection,
        ResultType::Count,
    )
    .await;
    let topk = search(
        &index,
        "termx termy",
        QueryType::Intersection,
        ResultType::TopkCount,
    )
    .await;
    assert_eq!(count.count_total, live_and.len());
    assert_eq!(topk.count_total, live_and.len());

    // Same immutable post-delete cut: retain the independent OR oracle.
    let count = search(&index, "termx termy", QueryType::Union, ResultType::Count).await;
    let topk = search(
        &index,
        "termx termy",
        QueryType::Union,
        ResultType::TopkCount,
    )
    .await;
    assert_eq!(count.count_total, live_or.len());
    assert_eq!(topk.count_total, live_or.len());

}

fn ground_truth(set_x: &HashSet<u64>, set_y: &HashSet<u64>) -> (HashSet<u64>, HashSet<u64>) {
    let vx: Vec<u64> = set_x.iter().copied().collect();
    let overlap: Vec<u64> = set_x.intersection(set_y).copied().collect();
    let mut rng = Rng(0xdef);
    let mut del = HashSet::new();
    while del.len() < 400 {
        del.insert(vx[rng.below(vx.len() as u64) as usize]);
    }
    for i in 0..50.min(overlap.len()) {
        del.insert(overlap[i]);
    }
    let live_x: HashSet<u64> = set_x.difference(&del).copied().collect();
    let live_y: HashSet<u64> = set_y.difference(&del).copied().collect();
    let live_and: HashSet<u64> = live_x.intersection(&live_y).copied().collect();
    let live_or: HashSet<u64> = live_x.union(&live_y).copied().collect();
    (live_and, live_or)
}
