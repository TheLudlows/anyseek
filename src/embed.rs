use crate::error::Result;

/// Wrapper for embedding models used to generate vector representations of text.
///
/// When the `model2vec` feature is enabled and a model is loaded, `embed()` will
/// produce embeddings. Otherwise, documents with `Text` fields that have
/// `index_vector = true` will not have auto-generated embeddings -- users must
/// provide external vectors.
pub enum EmbeddingModel {
    #[cfg(feature = "model2vec")]
    Model2Vec(model2vec::Model),
    None,
}

impl EmbeddingModel {
    /// Load a Model2Vec model by name (e.g., "PotionBase4M").
    ///
    /// Returns `EmbeddingModel::None` when the `model2vec` feature is not enabled
    /// or when the model cannot be found.
    // TODO: integrate model2vec-rs when the dependency becomes available
    pub fn load(_model_name: &str) -> Result<Self> {
        Ok(EmbeddingModel::None)
    }

    /// Generate an embedding vector for the given text.
    ///
    /// Returns `None` if no model is loaded.
    pub fn embed(&self, _text: &str) -> Option<Vec<f32>> {
        match self {
            EmbeddingModel::None => None,
            #[cfg(feature = "model2vec")]
            EmbeddingModel::Model2Vec(model) => {
                Some(model.embed(_text).to_vec())
            }
        }
    }

    /// Returns `true` if a real embedding model is loaded.
    pub fn is_loaded(&self) -> bool {
        !matches!(self, EmbeddingModel::None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_returns_none_variant() {
        let model = EmbeddingModel::load("PotionBase4M").unwrap();
        assert!(matches!(model, EmbeddingModel::None));
    }

    #[test]
    fn embed_returns_none_when_no_model() {
        let model = EmbeddingModel::None;
        let result = model.embed("hello world");
        assert!(result.is_none());
    }

    #[test]
    fn is_loaded_returns_false_when_no_model() {
        let model = EmbeddingModel::None;
        assert!(!model.is_loaded());
    }
}
