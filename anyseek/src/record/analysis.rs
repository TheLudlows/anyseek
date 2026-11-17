//! Content-free description of the analysis configuration used by native packages.
use super::{RecordConfig, RecordError};
use crate::index::StopwordType;
use serde::Serialize;
use sha2::{Digest, Sha256};

/// Exact persisted configuration identity, not an assertion of retrieval quality.
#[derive(Clone, Debug, Serialize)]
pub struct AnalysisContract {
    /// Contract describing the digest encoding.
    pub format: &'static str,
    /// SHA-256 of the configuration tuple checked by native lexical packages.
    pub fingerprint: String,
    /// Versioned tokenizer profile.
    pub tokenizer: String,
    /// Tokenizer implementation and dictionary identity.
    pub tokenizer_identity: &'static str,
    /// Stopword mode; custom dictionary contents are never disclosed here.
    pub stop_words: &'static str,
    /// Digest of the exact persisted stopword configuration.
    pub stop_words_fingerprint: String,
}
impl RecordConfig {
    /// Describe the exact configuration used by the native lexical package guard.
    /// Includes the persisted index/schema and versioned tokenizer identity.
    pub fn analysis_contract(&self) -> Result<AnalysisContract, RecordError> {
        let fingerprint = Sha256::digest(
            serde_json::to_vec(&(
                &self.index,
                &self.schema,
                crate::tokenizer::standard::identity(self.index.tokenizer),
            ))
            .map_err(super::storage)?,
        );
        let stop_words = match self.index.stop_words {
            StopwordType::None => "None",
            StopwordType::English => "English",
            StopwordType::German => "German",
            StopwordType::French => "French",
            StopwordType::Spanish => "Spanish",
            StopwordType::Custom { .. } => "Custom",
        };
        Ok(AnalysisContract {
            format: "native-package-analysis:v1",
            fingerprint: fingerprint.iter().map(|b| format!("{b:02x}")).collect(),
            tokenizer: self.index.tokenizer.to_string(),
            tokenizer_identity: crate::tokenizer::standard::identity(self.index.tokenizer),
            stop_words,
            stop_words_fingerprint: Sha256::digest(
                serde_json::to_vec(&self.index.stop_words).map_err(super::storage)?,
            )
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
        })
    }
}
