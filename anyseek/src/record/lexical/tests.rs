use super::*;
use tantivy::query::{AllQuery, BooleanQuery, BoostQuery, Occur, PhraseQuery, TermQuery};
use tantivy::schema::Document as _;

#[tokio::test]
async fn field_restricted_phrase_keeps_first_position_after_skipping_earlier_field() {
    use crate::record::tests::{config, write};
    use crate::{
        commit::Commit,
        record::{ExpectedVersion, RecordPartition, RecordQuery},
        search::{QueryType, ResultType, SearchMode},
    };
    let root = Temp::new();
    let mut config = config();
    config.schema.push(serde_json::from_value(serde_json::json!({"field":"title","field_type":"Text","store":false,"index_lexical":true})).unwrap());
    let partition = RecordPartition::create(&root.0, config).await.unwrap();
    for (id, body, title) in [
        (
            "a",
            "the of and the of and anchor of",
            "the of and anchor of",
        ),
        ("c", "the anchor of red apple fishing car", "the of anchor"),
    ] {
        let mut request = write(id, id, body, ExpectedVersion::Absent);
        request
            .document
            .as_mut()
            .unwrap()
            .insert("title".into(), title.into());
        partition.write(request).await.unwrap();
    }
    let inner = partition.inner.lock().await;
    for committed in [false, true] {
        if committed {
            inner.index.test_index().commit().await.unwrap();
        }
        let result = inner
            .query_view(RecordQuery {
                temporal: None,
                text: "\"the of\"".into(),
                vector: None,
                mode: SearchMode::Lexical,
                query_type: QueryType::Union,
                offset: 0,
                length: 10,
                result_type: ResultType::TopkCount,
                fields: vec!["title".into()],
                filters: vec![],
                sort: vec![],
            })
            .await
            .unwrap();
        let ids = result
            .hits
            .iter()
            .map(|h| h.record.record_id.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(ids, BTreeSet::from(["a", "c"]), "committed={committed}");
        assert_eq!(result.count_total, 2);
    }
    drop(inner);
    drop(partition);
}

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

#[tokio::test]
async fn native_query_contract_matches_before_and_after_commit() {
    use crate::commit::Commit;
    use crate::record::tests::{config, write};
    use crate::{
        index::{FieldLengthNormalization, FrequentwordType, LexicalSimilarity},
        record::{ExpectedVersion, RecordPartition, RecordQuery},
        search::{QueryType, ResultType, SearchMode},
    };
    let mut field_differences = Vec::new();
    let documents = [
        (
            "a",
            "the of and the of and anchor of",
            "the of and anchor of",
        ),
        ("b", "of neutral neutral neutral neutral", "and neutral"),
        ("c", "the anchor of red apple fishing car", "the of anchor"),
        ("d", "anchor the of red green apple", "the anchor"),
        ("e", "alpha beta alpha", "beta alpha"),
        ("f", "alpha", "beta"),
        ("g", "", ""),
    ];
    for normalization in [
        FieldLengthNormalization::Shared,
        FieldLengthNormalization::PerField,
    ] {
        for similarity in [LexicalSimilarity::Bm25f, LexicalSimilarity::Bm25fProximity] {
            for mask in [0, 1, 2, 4, 8, 16, 32, 64, 9, 127] {
                let root = Temp::new();
                let native = Temp::new();
                let mut config = config();
                config.index.field_length_normalization = normalization.clone();
                config.index.lexical_similarity = similarity.clone();
                config.index.frequent_words = FrequentwordType::English;
                config.index.ngram_indexing = mask;
                config.schema[0].boost = 2.0;
                config.schema.push(serde_json::from_value(serde_json::json!({"field":"title","field_type":"Text","store":false,"index_lexical":true,"boost":0.5})).unwrap());
                let analyzer = super::query::Analyzer::new(config.index.clone());
                let partition = RecordPartition::create(&root.0, config).await.unwrap();
                let mut inputs = Vec::new();
                for (id, body, title) in documents {
                    let mut request = write(id, id, body, ExpectedVersion::Absent);
                    request
                        .document
                        .as_mut()
                        .unwrap()
                        .insert("title".into(), title.into());
                    partition.write(request).await.unwrap();
                    inputs.push(AnalyzedInput {
                        identity: identity(id, 1),
                        fields: vec![analyzer.field(body).await, analyzer.field(title).await],
                    });
                }
                let inner = partition.inner.lock().await;
                let cut = Cut {
                    sequence: inner.state.seq,
                    evaluation_time_ms: inner.evaluation_time_ms,
                };
                let package = Candidate::build_analyzed(
                    &native.0,
                    cut,
                    vec!["body".into(), "title".into()],
                    inputs,
                    LIMIT,
                )
                .unwrap();
                let snapshot = package.open(&native.0, cut, LIMIT).unwrap();
                let active = snapshot.active(cut, &all(&snapshot)).unwrap();
                for committed in [false, true] {
                    if committed {
                        inner.index.test_index().commit().await.unwrap();
                    }
                    for field_choice in [None, Some(0usize), Some(1usize)] {
                        for mode in [QueryType::Union, QueryType::Intersection, QueryType::Phrase] {
                            for text in [
                                "",
                                " ",
                                "alpha beta",
                                "alpha missing",
                                "alpha alpha",
                                "\"alpha beta\"",
                                "\"alpha alpha\"",
                                "\"the of\"",
                                "\"the of and\"",
                                "\"the of and anchor\"",
                                "\"anchor the of\"",
                                "\"the of anchor\"",
                                "\"the anchor of\"",
                                "\"the anchor\"",
                                "\"anchor of\"",
                                "+red -green",
                                "red +apple",
                                "red -green",
                                "-red",
                                "applle",
                                "r*",
                                "FISHING",
                                "fish",
                                "automobile",
                                "\"red apple\"",
                                "\"red apple\" car",
                            ] {
                                let expected = inner
                                    .query_view(RecordQuery {
                                        temporal: None,
                                        text: text.into(),
                                        vector: None,
                                        mode: SearchMode::Lexical,
                                        query_type: mode.clone(),
                                        offset: 0,
                                        length: 20,
                                        result_type: ResultType::TopkCount,
                                        fields: field_choice
                                            .map(|f| vec![["body", "title"][f].to_string()])
                                            .unwrap_or_default(),
                                        filters: vec![],
                                        sort: vec![],
                                    })
                                    .await
                                    .unwrap();
                                let plan = analyzer
                                    .query_fields(
                                        text,
                                        mode.clone(),
                                        &snapshot.fields,
                                        &field_choice
                                            .map(|f| vec![snapshot.fields[f]])
                                            .unwrap_or_default(),
                                        true,
                                    )
                                    .await;
                                let (count, hits) = active
                                    .score_groups(
                                        plan.predicate.as_ref(),
                                        &plan.scores,
                                        &[2.0, 0.5],
                                        normalization.clone(),
                                        20,
                                        |_| true,
                                    )
                                    .unwrap();
                                let label = format!(
                                    "{normalization:?}/{similarity:?}/mask={mask}/{mode:?}/{text:?}/field={field_choice:?}/committed={committed}"
                                );
                                let actual_ids =
                                    hits.iter().map(|h| &h.identity.id).collect::<Vec<_>>();
                                let expected_ids = expected
                                    .hits
                                    .iter()
                                    .map(|h| &h.record.record_id)
                                    .collect::<Vec<_>>();
                                let equivalent = count == expected.count_total
                                    && actual_ids == expected_ids
                                    && hits.iter().zip(&expected.hits).all(|(a, b)| {
                                        (a.score - b.score).abs() <= 2e-5 * b.score.abs().max(1.0)
                                    });
                                if field_choice.is_some() && !equivalent {
                                    field_differences.push(serde_json::json!({"case":label,"native":hits.iter().map(|h| (&h.identity.id,h.score)).collect::<Vec<_>>(),
                                        "legacy":expected.hits.iter().map(|h| (&h.record.record_id,h.score)).collect::<Vec<_>>(), "native_count":count,"legacy_count":expected.count_total}));
                                    continue;
                                }
                                assert_eq!(
                                    count,
                                    expected.count_total,
                                    "{label}; native={:?}; old={:?}",
                                    hits.iter().map(|h| &h.identity.id).collect::<Vec<_>>(),
                                    expected
                                        .hits
                                        .iter()
                                        .map(|h| &h.record.record_id)
                                        .collect::<Vec<_>>()
                                );
                                assert_eq!(
                                    hits.iter().map(|h| &h.identity.id).collect::<Vec<_>>(),
                                    expected
                                        .hits
                                        .iter()
                                        .map(|h| &h.record.record_id)
                                        .collect::<Vec<_>>(),
                                    "{label}"
                                );
                                for (hit, expected) in hits.iter().zip(&expected.hits) {
                                    assert!(
                                        (hit.score - expected.score).abs()
                                            <= 2e-5 * expected.score.abs().max(1.0),
                                        "{label}: {} got {} expected {}",
                                        hit.identity.id,
                                        hit.score,
                                        expected.score
                                    );
                                }
                            }
                        }
                    }
                }
                drop(inner);
                drop(partition);
            }
        }
    }
    // These mismatches are evidence of an unresolved field restriction contract,
    // NOT accepted parity. Unrestricted cases above must all match exactly.
    assert!(field_differences.is_empty());
    if let Some(path) = std::env::var_os("ANYSEEK_TEST_CONTRACT_REPORT") {
        std::fs::write(path, serde_json::to_vec_pretty(&field_differences).unwrap()).unwrap();
    }
}

#[test]
fn native_corpus_uses_one_selected_population_across_frozen_and_tail() {
    use super::scoring::CorpusView;
    use crate::index::FieldLengthNormalization;
    let roots = [Temp::new(), Temp::new(), Temp::new()];
    let newer = Cut {
        sequence: CUT.sequence + 1,
        evaluation_time_ms: CUT.evaluation_time_ms + 1,
    };
    let frozen = Candidate::build(
        &roots[0].0,
        CUT,
        vec!["body".into()],
        vec![
            input("a", 1, &["alpha alpha"]),
            input("b", 1, &["alpha alpha alpha alpha"]),
        ],
        LIMIT,
    )
    .unwrap()
    .open(&roots[0].0, CUT, LIMIT)
    .unwrap();
    let tail = Candidate::build(
        &roots[1].0,
        newer,
        vec!["body".into()],
        vec![
            input("b", 2, &["beta"]),
            input("c", 1, &["alpha gamma gamma"]),
        ],
        LIMIT,
    )
    .unwrap()
    .open(&roots[1].0, newer, LIMIT)
    .unwrap();
    let clean = Candidate::build(
        &roots[2].0,
        newer,
        vec!["body".into()],
        vec![
            input("a", 1, &["alpha alpha"]),
            input("b", 2, &["beta"]),
            input("c", 1, &["alpha gamma gamma"]),
        ],
        LIMIT,
    )
    .unwrap()
    .open(&roots[2].0, newer, LIMIT)
    .unwrap();
    let selected = all(&clean);
    let corpus = CorpusView::select(&[&frozen, &tail], newer, &selected).unwrap();
    let oracle = clean.active(newer, &selected).unwrap();
    let groups = vec![super::query::ScoreGroup {
        matched_term: "alpha".into(),
        components: vec!["alpha".into()],
        match_fields: vec![],
    }];
    let query = term(clean.fields[0], "alpha");
    for normalization in [
        FieldLengthNormalization::Shared,
        FieldLengthNormalization::PerField,
    ] {
        for limit in [0, 1, 10] {
            for filtered in [false, true] {
                let accepts = |i: &Identity| !filtered || i.id != "a";
                assert_eq!(
                    corpus
                        .score_groups(
                            &query,
                            &groups,
                            &[1.0],
                            normalization.clone(),
                            limit,
                            accepts
                        )
                        .unwrap(),
                    oracle
                        .score_groups(
                            &query,
                            &groups,
                            &[1.0],
                            normalization.clone(),
                            limit,
                            accepts
                        )
                        .unwrap()
                );
            }
        }
    }
    assert!(CorpusView::select(&[&frozen, &tail], CUT, &selected).is_err());
    assert!(CorpusView::select(&[&frozen, &tail, &tail], newer, &selected).is_err());
    assert!(CorpusView::select(&[&frozen], newer, &selected).is_err());
    let mut ambiguous = selected.clone();
    ambiguous.insert(identity("b", 1));
    assert!(CorpusView::select(&[&frozen, &tail], newer, &ambiguous).is_err());
}

#[tokio::test]
async fn field_or_matches_selected_field_before_and_after_commit() {
    use crate::commit::Commit;
    use crate::record::tests::{config, write};
    use crate::{
        record::{ExpectedVersion, RecordPartition, RecordQuery},
        search::{QueryType, ResultType, SearchMode},
    };
    let root = Temp::new();
    let mut config = config();
    config.schema.push(serde_json::from_value(serde_json::json!({"field":"title","field_type":"Text","store":false,"index_lexical":true})).unwrap());
    let partition = RecordPartition::create(&root.0, config).await.unwrap();
    for (id, body, title) in [
        ("f", "alpha", "beta"),
        ("g", "beta", "alpha"),
        ("h", "gamma", "alpha"),
    ] {
        let mut request = write(id, id, body, ExpectedVersion::Absent);
        request
            .document
            .as_mut()
            .unwrap()
            .insert("title".into(), title.into());
        partition.write(request).await.unwrap();
    }
    let inner = partition.inner.lock().await;
    let mut before = Vec::new();
    for committed in [false, true] {
        if committed {
            inner.index.test_index().commit().await.unwrap();
        }
        for (offset, kind) in [
            (0, ResultType::TopkCount),
            (1, ResultType::TopkCount),
            (2, ResultType::TopkCount),
            (0, ResultType::Count),
        ] {
            let result = inner
                .query_view(RecordQuery {
                    temporal: None,
                    text: "alpha beta".into(),
                    vector: None,
                    mode: SearchMode::Lexical,
                    query_type: QueryType::Union,
                    offset,
                    length: 1,
                    result_type: kind.clone(),
                    fields: vec!["body".into()],
                    filters: vec![],
                    sort: vec![],
                })
                .await
                .unwrap();
            let ids: Vec<_> = result
                .hits
                .iter()
                .map(|h| h.record.record_id.as_str())
                .collect();
            assert_eq!(
                ids,
                if kind == ResultType::Count || offset == 2 {
                    vec![]
                } else if offset == 0 {
                    vec!["g"]
                } else {
                    vec!["f"]
                }
            );
            assert_eq!(result.count_total, 2);
            let scores: Vec<_> = result.hits.iter().map(|h| h.score).collect();
            if committed { assert_eq!(scores, before.remove(0)); } else { before.push(scores); }
        }
    }
}

#[tokio::test]
async fn native_reader_reservation_follows_snapshot_lifetime() {
    use crate::resources::{Governor, Lane};
    let root = Temp::new();
    let candidate = Candidate::build(
        &root.0,
        CUT,
        vec!["body".into()],
        vec![input("a", 1, &["alpha"])],
        LIMIT,
    )
    .unwrap();
    let governor = Arc::new(Governor::default());
    let scope = governor
        .admit("reader-owner".into(), Lane::Foreground, 0)
        .unwrap();
    let charge = candidate.files.iter().map(|f| f.bytes).sum::<u64>() * 8 + 64 * 1024;
    let cap = 256 * 1024 * 1024;
    let observer = scope.clone();
    let snapshot = scope
        .run(async { candidate.open(&root.0, CUT, LIMIT).unwrap() })
        .await;
    assert!(observer.bytes(cap).is_err());
    let remaining = observer.bytes(cap - charge).unwrap();
    assert!(observer.bytes(1).is_err());
    drop(remaining);
    drop(snapshot);
    drop(observer.bytes(cap).unwrap());
    // Rejection must release partial admission and leave the authenticated files intact.
    let blocked = observer.bytes(cap).unwrap();
    assert!(
        observer
            .clone()
            .run(async { candidate.open(&root.0, CUT, LIMIT) })
            .await
            .is_err()
    );
    drop(blocked);
    drop(
        observer
            .run(async { candidate.open(&root.0, CUT, LIMIT).unwrap() })
            .await,
    );
}
