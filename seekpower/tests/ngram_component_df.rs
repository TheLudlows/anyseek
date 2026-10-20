//! Independent literal ngram scoring across candidate and persistence states.
#[path = "common/ngram_oracle.rs"]
mod oracle;
#[tokio::test]
async fn unequal_component_df_matches_literal_formula_in_all_states() {
    oracle::exercise(false).await;
}
