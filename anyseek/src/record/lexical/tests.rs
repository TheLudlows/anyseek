use super::*;
use tantivy::query::{AllQuery, BooleanQuery, BoostQuery, Occur, PhraseQuery, TermQuery};
use tantivy::schema::Document as _;

const LIMIT: u64 = 32 * 1024 * 1024;
const CUT: Cut = Cut {
    sequence: 17,
    evaluation_time_ms: 1000,
};

pub(super) struct Temp(pub(super) std::path::PathBuf);
impl Temp {
    pub(super) fn new() -> Self {
        Self(std::env::temp_dir().join(format!("anyseek-tantivy-{:032x}", rand::random::<u128>())))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        if self.0.exists() {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
}
fn identity(id: &str, revision: u64) -> Identity {
    Identity {
        id: id.into(),
        version: 1,
        revision,
    }
}
fn input(id: &str, revision: u64, text: &[&str]) -> Input {
    Input {
        identity: identity(id, revision),
        fields: text
            .iter()
            .map(|s| s.split_whitespace().map(String::from).collect())
            .collect(),
    }
}
fn term(field: Field, text: &str) -> TermQuery {
    TermQuery::new(
        Term::from_field_text(field, text),
        IndexRecordOption::WithFreqs,
    )
}
fn all(snapshot: &Snapshot) -> BTreeSet<Identity> {
    snapshot.identities.iter().cloned().collect()
}

#[test]
fn commit_open_is_immediately_visible_and_stores_no_body() {
    let root = Temp::new();
    let candidate = Candidate::build(
        &root.0,
        CUT,
        vec!["body".into()],
        vec![input("a", 1, &["alpha beta"])],
        LIMIT,
    )
    .unwrap();
    let snapshot = candidate.open(&root.0, CUT, LIMIT).unwrap();
    let active = snapshot.active(CUT, &all(&snapshot)).unwrap();
    let (count, hits) = active
        .bm25_probe(&term(snapshot.fields[0], "alpha"), 10, |_| true)
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(hits[0].identity, identity("a", 1));
    for segment in snapshot.searcher.segment_readers() {
        for doc in 0..segment.max_doc() {
            let stored: TantivyDocument = snapshot.searcher.doc(DocAddress::new(0, doc)).unwrap();
            assert_eq!(stored.iter_fields_and_values().count(), 0);
        }
    }
}

#[test]
fn topk_filters_hidden_revisions_before_truncation_and_uses_active_statistics() {
    let root = Temp::new();
    let mut inputs = (0..80)
        .map(|n| input(&format!("hidden-{n}"), 1, &["alpha alpha alpha alpha"]))
        .collect::<Vec<_>>();
    inputs.push(input("z-live", 1, &["alpha beta beta"]));
    inputs.push(input("z-live", 2, &["alpha beta"]));
    inputs.push(input("other", 1, &["gamma"]));
    let candidate = Candidate::build(&root.0, CUT, vec!["body".into()], inputs, LIMIT).unwrap();
    let snapshot = candidate.open(&root.0, CUT, LIMIT).unwrap();
    let live = BTreeSet::from([identity("z-live", 2), identity("other", 1)]);
    let active = snapshot.active(CUT, &live).unwrap();
    let query = term(snapshot.fields[0], "alpha");
    assert_eq!(active.total_num_docs().unwrap(), 2);
    assert_eq!(active.total_num_tokens(snapshot.fields[0]).unwrap(), 3);
    assert_eq!(
        active
            .doc_freq(&Term::from_field_text(snapshot.fields[0], "alpha"))
            .unwrap(),
        1
    );
    let (count, hits) = active.bm25_probe(&query, 1, |_| true).unwrap();
    assert_eq!(count, 1);
    assert_eq!(hits[0].identity, identity("z-live", 2));
    let clean = Temp::new();
    let clean_package = Candidate::build(
        &clean.0,
        CUT,
        vec!["body".into()],
        vec![
            input("z-live", 2, &["alpha beta"]),
            input("other", 1, &["gamma"]),
        ],
        LIMIT,
    )
    .unwrap();
    let clean_snapshot = clean_package.open(&clean.0, CUT, LIMIT).unwrap();
    let clean_view = clean_snapshot.active(CUT, &live).unwrap();
    assert_eq!(hits, clean_view.bm25_probe(&query, 1, |_| true).unwrap().1);
    // Metadata filtering changes matches, never the active corpus or score.
    assert_eq!(
        active.bm25_probe(&query, 1, |i| i.id != "z-live").unwrap(),
        (0, vec![])
    );
    assert_eq!(active.total_num_docs().unwrap(), 2);
}

#[test]
fn fixed_searcher_does_not_adopt_an_unselected_internal_commit() {
    let root = Temp::new();
    let package = Candidate::build(
        &root.0,
        CUT,
        vec!["body".into()],
        vec![input("a", 1, &["alpha"])],
        LIMIT,
    )
    .unwrap();
    let snapshot = package.open(&root.0, CUT, LIMIT).unwrap();
    let index = Index::open_in_dir(root.0.join("lexical")).unwrap();
    let mut writer = index
        .writer_with_num_threads::<TantivyDocument>(1, WRITER_BYTES)
        .unwrap();
    writer.set_merge_policy(Box::new(NoMergePolicy));
    let mut doc = TantivyDocument::default();
    doc.add_text(snapshot.fields[0], "alpha");
    writer.add_document(doc).unwrap();
    writer.commit().unwrap();
    writer.wait_merging_threads().unwrap();
    let active = snapshot.active(CUT, &all(&snapshot)).unwrap();
    assert_eq!(active.bm25_probe(&AllQuery, 10, |_| true).unwrap().0, 1);
    assert!(package.open(&root.0, CUT, LIMIT).is_err());
}

#[test]
fn field_boost_chinese_phrase_missing_term_and_stable_ties() {
    let root = Temp::new();
    let package = Candidate::build(
        &root.0,
        CUT,
        vec!["title".into(), "body".into()],
        vec![
            input("z", 1, &["", "北京 大学 alpha"]),
            input("a", 1, &["", "北京 大学 alpha"]),
            input("b", 1, &["北京 大学 alpha", ""]),
            input("c", 1, &["", "北京 城市 大学"]),
        ],
        LIMIT,
    )
    .unwrap();
    let snapshot = package.open(&root.0, CUT, LIMIT).unwrap();
    let active = snapshot.active(CUT, &all(&snapshot)).unwrap();
    let phrase = PhraseQuery::new(vec![
        Term::from_field_text(snapshot.fields[1], "北京"),
        Term::from_field_text(snapshot.fields[1], "大学"),
    ]);
    let (count, hits) = active.bm25_probe(&phrase, 1, |_| true).unwrap();
    assert_eq!(count, 2);
    assert_eq!(hits[0].identity.id, "a");
    let query = BooleanQuery::new(vec![
        (
            Occur::Should,
            Box::new(BoostQuery::new(
                Box::new(term(snapshot.fields[0], "alpha")),
                5.0,
            )),
        ),
        (Occur::Should, Box::new(term(snapshot.fields[1], "alpha"))),
    ]);
    assert_eq!(
        active.bm25_probe(&query, 1, |_| true).unwrap().1[0]
            .identity
            .id,
        "b"
    );
    assert_eq!(
        active
            .bm25_probe(&term(snapshot.fields[0], "absent"), 10, |_| true)
            .unwrap(),
        (0, vec![])
    );
    assert_eq!(
        active.bm25_probe(&AllQuery, 0, |_| true).unwrap(),
        (4, vec![])
    );
    assert!(
        active
            .bm25_probe(&AllQuery, MAX_DOCUMENTS + 1, |_| true)
            .is_err()
    );
}

#[test]
fn mismatched_cut_and_revision_fail_closed() {
    let root = Temp::new();
    let package = Candidate::build(
        &root.0,
        CUT,
        vec!["body".into()],
        vec![input("a", 1, &["alpha"]), input("a", 2, &["beta"])],
        LIMIT,
    )
    .unwrap();
    let snapshot = package.open(&root.0, CUT, LIMIT).unwrap();
    assert!(snapshot.active(CUT, &all(&snapshot)).is_err());
    assert!(
        snapshot
            .active(CUT, &BTreeSet::from([identity("a", 3)]))
            .is_err()
    );
    let wrong = Cut {
        evaluation_time_ms: CUT.evaluation_time_ms + 1,
        ..CUT
    };
    assert!(package.open(&root.0, wrong, LIMIT).is_err());
    assert!(snapshot.active(wrong, &BTreeSet::new()).is_err());
    assert_eq!(
        snapshot
            .active(CUT, &BTreeSet::new())
            .unwrap()
            .bm25_probe(&AllQuery, 1, |_| true)
            .unwrap(),
        (0, vec![])
    );
}

#[test]
fn copied_package_reopens_with_same_identity_and_scores() {
    let root = Temp::new();
    let target = Temp::new();
    let package = Candidate::build(
        &root.0,
        CUT,
        vec!["body".into()],
        vec![input("a", 7, &["alpha beta"])],
        LIMIT,
    )
    .unwrap();
    for file in &package.files {
        let to = target.0.join(&file.path);
        std::fs::create_dir_all(to.parent().unwrap()).unwrap();
        std::fs::copy(root.0.join(&file.path), to).unwrap();
    }
    let original = package.open(&root.0, CUT, LIMIT).unwrap();
    let restored = package.open(&target.0, CUT, LIMIT).unwrap();
    let query = term(original.fields[0], "alpha");
    let execute = |s: &Snapshot| {
        s.active(CUT, &all(s))
            .unwrap()
            .bm25_probe(&query, 10, |_| true)
            .unwrap()
    };
    assert_eq!(execute(&original), execute(&restored));
    // The full inventory also rejects extra files and trailing/mutated bytes.
    std::fs::write(target.0.join("unexpected"), b"not selected").unwrap();
    assert!(package.open(&target.0, CUT, LIMIT).is_err());
    std::fs::remove_file(target.0.join("unexpected")).unwrap();
    std::fs::write(target.0.join("package.json"), b"{}").unwrap();
    assert!(package.open(&target.0, CUT, LIMIT).is_err());
}

#[test]
fn rejected_input_does_not_create_candidate_directory() {
    let root = Temp::new();
    assert!(
        Candidate::build(
            &root.0,
            CUT,
            vec!["body".into()],
            vec![input("a", 1, &["alpha"]), input("a", 1, &["beta"])],
            LIMIT
        )
        .is_err()
    );
    assert!(!root.0.exists());
}

#[tokio::test]
async fn existing_analysis_contract_feeds_native_positions_without_legacy_index() {
    use crate::{
        index::{TextNormalization, TokenizerType},
        search::QueryType,
        tokenizer::{AnalysisContext, analyze},
    };
    let stop_words = ahash::AHashSet::new();
    let frequent_hashset = ahash::AHashSet::new();
    #[cfg(feature = "zh")]
    let mut chinese = crate::word_segmentation::WordSegmentationTM::new();
    #[cfg(feature = "zh")]
    chinese.load_dictionary(0, 1, true);
    let context = AnalysisContext {
        text_normalization: TextNormalization::OpenCcT2s1_1_9,
        stop_words: &stop_words,
        frequent_hashset: &frequent_hashset,
        #[cfg(feature = "zh")]
        word_segmentation_option: Some(&chinese),
    };
    let mut cases = vec![(
        TokenizerType::UnicodeAlphanumericFolded,
        "Café ＡＢＣ",
        "cafe abc",
    )];
    #[cfg(feature = "zh")]
    cases.push((
        TokenizerType::UnicodeAlphanumericZH,
        "北京大學 歡迎學生",
        "北京大学 欢迎学生",
    ));
    for (tokenizer, text, query) in cases {
        let mut terms = ahash::AHashMap::new();
        let mut count = 0;
        analyze(
            &context,
            text,
            &mut terms,
            &mut Vec::new(),
            tokenizer,
            0,
            &mut count,
            65536,
            65536,
            false,
            &mut QueryType::Union,
            0,
            0,
            1,
        )
        .await;
        let mut positioned = terms
            .values()
            .flat_map(|t| {
                t.field_positions_vec[0]
                    .iter()
                    .map(|p| (*p, t.term.clone()))
            })
            .collect::<Vec<_>>();
        positioned.sort();
        let words = positioned.into_iter().map(|(_, t)| t).collect();
        let root = Temp::new();
        let package = Candidate::build(
            &root.0,
            CUT,
            vec!["body".into()],
            vec![Input {
                identity: identity("a", 1),
                fields: vec![words],
            }],
            LIMIT,
        )
        .unwrap();
        let snapshot = package.open(&root.0, CUT, LIMIT).unwrap();
        let mut query_terms = Vec::new();
        analyze(
            &context,
            query,
            &mut ahash::AHashMap::new(),
            &mut query_terms,
            tokenizer,
            0,
            &mut 0,
            65536,
            65536,
            true,
            &mut QueryType::Phrase,
            0,
            0,
            1,
        )
        .await;
        assert!(query_terms.len() >= 2);
        let query = PhraseQuery::new(
            query_terms
                .into_iter()
                .map(|t| Term::from_field_text(snapshot.fields[0], &t.term))
                .collect(),
        );
        assert_eq!(
            snapshot
                .active(CUT, &all(&snapshot))
                .unwrap()
                .bm25_probe(&query, 1, |_| true)
                .unwrap()
                .0,
            1
        );
    }
}

#[tokio::test]
async fn native_unigram_bm25f_matches_record_scores_at_the_vector_cut() {
    use crate::record::tests::{config, write};
    use crate::{
        index::FieldLengthNormalization,
        record::{ExpectedVersion, RecordPartition, RecordQuery},
        search::{QueryType, ResultType, SearchMode},
        vector::Embedding,
        vector_similarity::AnnMode,
    };
    for normalization in [
        FieldLengthNormalization::Shared,
        FieldLengthNormalization::PerField,
    ] {
        let root = Temp::new();
        let package_root = Temp::new();
        let mut config = config();
        config.index.field_length_normalization = normalization.clone();
        config.schema[0].boost = 1.7;
        config.schema.push(serde_json::from_value(serde_json::json!({"field":"title","field_type":"Text","store":false,"index_lexical":true,"boost":3.0})).unwrap());
        let partition = RecordPartition::create(&root.0, config).await.unwrap();
        let long = format!("alpha {}", "beta ".repeat(130));
        for (id, body, title) in [
            ("a", "alpha alpha", "beta"),
            ("b", long.as_str(), ""),
            ("c", "gamma", "alpha beta"),
            ("d", "", ""),
        ] {
            let mut request = write(id, id, body, ExpectedVersion::Absent);
            request
                .document
                .as_mut()
                .unwrap()
                .insert("title".into(), title.into());
            partition.write(request).await.unwrap();
        }
        // Freeze business state and actual vector query under one partition guard.
        let inner = partition.inner.lock().await;
        let cut = Cut {
            sequence: inner.state.seq,
            evaluation_time_ms: inner.evaluation_time_ms,
        };
        let make_query = |mode, vector| RecordQuery {
            temporal: None,
            text: "alpha beta".into(),
            vector,
            mode,
            query_type: QueryType::Union,
            offset: 0,
            length: 10,
            result_type: ResultType::TopkCount,
            fields: vec![],
            filters: vec![],
            sort: vec![],
        };
        let expected = inner
            .query_view(make_query(SearchMode::Lexical, None))
            .await
            .unwrap();
        let vectors = inner
            .query_view(make_query(
                SearchMode::Vector {
                    similarity_threshold: None,
                    ann_mode: AnnMode::All,
                },
                Some(Embedding::F32(vec![1.0, 0.0])),
            ))
            .await
            .unwrap();
        assert_eq!(
            (vectors.visible_seq, vectors.evaluation_time_ms),
            (cut.sequence, cut.evaluation_time_ms)
        );
        let inputs = inner
            .state
            .records
            .values()
            .map(|r| Input {
                identity: Identity {
                    id: r.record_id.clone(),
                    version: r.version,
                    revision: cut.sequence,
                },
                fields: ["body", "title"]
                    .iter()
                    .map(|f| {
                        r.document.as_ref().unwrap()[*f]
                            .as_str()
                            .unwrap()
                            .split_whitespace()
                            .map(String::from)
                            .collect()
                    })
                    .collect(),
            })
            .collect();
        let package = Candidate::build(
            &package_root.0,
            cut,
            vec!["body".into(), "title".into()],
            inputs,
            LIMIT,
        )
        .unwrap();
        let snapshot = package.open(&package_root.0, cut, LIMIT).unwrap();
        let active = snapshot.active(cut, &all(&snapshot)).unwrap();
        let terms = vec!["alpha".to_string(), "beta".to_string()];
        let query = BooleanQuery::new(
            terms
                .iter()
                .flat_map(|word| {
                    snapshot.fields.iter().map(move |field| {
                        (
                            Occur::Should,
                            Box::new(term(*field, word)) as Box<dyn Query>,
                        )
                    })
                })
                .collect(),
        );
        let (count, hits) = active
            .bm25f_unigram_probe(&query, &terms, &[1.7, 3.0], normalization, 10, |_| true)
            .unwrap();
        assert_eq!(count, expected.count_total);
        let actual: Vec<_> = hits.into_iter().map(|h| (h.identity.id, h.score)).collect();
        let expected: Vec<_> = expected
            .hits
            .into_iter()
            .map(|h| (h.record.record_id, h.score))
            .collect();
        assert_eq!(actual, expected);
        assert_eq!(vectors.hits.len(), 4);
        drop(inner);
        drop(partition);
    }
}
