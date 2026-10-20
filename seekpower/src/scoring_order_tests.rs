use super::{prepare_scoring_order, scoring_terms};
use crate::index::{NgramType, PostingListObjectQuery};
use smallvec::SmallVec;

fn old_order<'a, 'b>(terms: &'a [PostingListObjectQuery<'b>]) -> SmallVec<[&'a PostingListObjectQuery<'b>; 32]> {
    let mut ordered: SmallVec<[_; 32]> = terms.iter().collect();
    ordered.sort_unstable_by(|a, b| a.term.cmp(&b.term)
        .then_with(|| (a.ngram_type.clone() as u8).cmp(&(b.ngram_type.clone() as u8))));
    ordered
}

fn assert_same(terms: &[PostingListObjectQuery]) {
    let old = old_order(terms);
    let actual: Vec<_> = scoring_terms(terms).collect();
    assert_eq!(actual.len(), old.len());
    for (a, b) in actual.iter().zip(&old) {
        assert!(std::ptr::eq(*a, *b), "canonical order or equal-key fallback changed");
    }
    let accumulate = |terms: Vec<&PostingListObjectQuery>| {
        let mut score = 0.0f32;
        for term in terms {
            if term.bm25_flag { score += term.idf; }
        }
        score.to_bits()
    };
    assert_eq!(accumulate(actual), accumulate(old.into_vec()));
}

#[test]
fn prepared_order_survives_permutation_subsets_and_clones() {
    for count in [0, 1, 2, 8, 31, 32, 33, 100] {
        let mut terms: Vec<_> = (0..count).map(|i| PostingListObjectQuery {
            term:format!("term{:03}",i / 2),
            ngram_type:if i % 2 == 0 {NgramType::SingleTerm} else {NgramType::NgramFF},
            term_index_unique:i,
            idf:if i % 3 == 0 {1e8} else {0.1},
            bm25_flag:i % 4 != 0,
            ..Default::default()
        }).collect();
        terms.reverse();
        let original: Vec<_> = terms.iter().map(|p|p.term_index_unique).collect();
        prepare_scoring_order(&mut terms);
        // Preparation does not reorder traversal objects or change their identities.
        assert_eq!(original,terms.iter().map(|p|p.term_index_unique).collect::<Vec<_>>());
        assert!(terms.iter().all(|p| if count <= 32 {p.scoring_order < 32} else {p.scoring_order == u8::MAX}));
        for _ in 0..count.max(1) {
            assert_same(&terms);
            let subset: Vec<_> = terms.iter().filter(|p|p.term_index_unique % 3 != 0).cloned().collect();
            assert_same(&subset);
            if !terms.is_empty() { terms.rotate_left(1); }
        }
    }
}

#[test]
fn equal_keys_unprepared_and_invalid_ordinals_preserve_old_sort() {
    for count in [2, 8, 32, 33, 100] {
        let mut terms: Vec<_> = (0..count).map(|i| PostingListObjectQuery {
            term:format!("term{}",i % 2),
            term_index_unique:i,
            idf:i as f32 * 0.1,
            bm25_flag:true,
            ..Default::default()
        }).collect();
        assert_same(&terms); // Default/unprepared objects.
        // Duplicate ranks must fall back rather than overwrite references.
        for term in &mut terms { term.scoring_order = 0; }
        assert_same(&terms);
        prepare_scoring_order(&mut terms);
        if count > 2 { assert!(terms.iter().all(|p|p.scoring_order == u8::MAX)); }
        for _ in 0..count {
            terms.rotate_left(1);
            assert_same(&terms);
        }
        terms[0].scoring_order = u8::MAX;
        assert_same(&terms);
    }
}
