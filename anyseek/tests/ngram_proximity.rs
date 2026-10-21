//! Independent literal ngram scoring across candidate and persistence states.
#[path = "common/ngram_oracle.rs"]
mod oracle;
#[tokio::test]
async fn whole_ngram_scores_match_literal_formula_in_all_states() {
    oracle::exercise(true).await;
}
