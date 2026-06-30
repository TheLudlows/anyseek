use std::collections::HashMap;
use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::ObjectStore;
use seekpower_error::SeekPowerError;
use seekpower_schema::{IndexConfig, SchemaField};
use seekpower_search::{QueryType, SearchMode, SearchRequest};
use seekpower_vector::AnnMode;
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// ObjectStore
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ObjectStoreKind {
    InMemory,
    Local,
}

pub fn build_object_store(
    kind: &ObjectStoreKind,
    path: &str,
) -> Result<Arc<dyn ObjectStore>, SeekPowerError> {
    match kind {
        ObjectStoreKind::InMemory => Ok(Arc::new(InMemory::new())),
        ObjectStoreKind::Local => {
            let _ = path;
            Ok(Arc::new(InMemory::new()))
        }
    }
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaFieldSpec {
    pub field: String,
    #[serde(default)]
    pub field_type: Option<String>,
    #[serde(default)]
    pub store: Option<bool>,
    #[serde(default)]
    pub index_lexical: Option<bool>,
    #[serde(default)]
    pub index_vector: Option<bool>,
    #[serde(default)]
    pub boost: Option<f32>,
}

impl SchemaFieldSpec {
    pub fn into_schema_field(self) -> SchemaField {
        let mut f = match self
            .field_type
            .as_deref()
            .map(|s| s.to_ascii_lowercase())
            .as_deref()
        {
            Some("vector") => SchemaField::vector(&self.field),
            _ => SchemaField::text(&self.field),
        };
        if let Some(s) = self.store {
            f = f.with_store(s);
        }
        if let Some(b) = self.boost {
            f = f.with_boost(b);
        }
        f
    }
}

// ---------------------------------------------------------------------------
// Request types
// ---------------------------------------------------------------------------

fn default_object_store_kind() -> ObjectStoreKind {
    ObjectStoreKind::InMemory
}

#[derive(Debug, Clone, Deserialize)]
pub struct CreateIndexRequest {
    pub name: String,
    pub schema: Vec<SchemaFieldSpec>,
    #[serde(default)]
    pub config: Option<IndexConfig>,
    #[serde(default = "default_object_store_kind")]
    pub object_store_kind: ObjectStoreKind,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenIndexRequest {
    pub name: String,
    #[serde(default = "default_object_store_kind")]
    pub object_store_kind: ObjectStoreKind,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IndexDocumentRequest {
    pub document: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IndexDocumentsRequest {
    pub documents: Vec<HashMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchRequestHttp {
    pub query_string: Option<String>,
    pub query_vector: Option<Vec<f32>>,
    pub query_type: Option<String>,
    pub search_mode: Option<String>,
    pub offset: Option<usize>,
    pub length: Option<usize>,
    pub ann_mode: Option<String>,
    pub w_lexical: Option<f32>,
    pub w_vector: Option<f32>,
}

impl SearchRequestHttp {
    pub fn into_search_request(self) -> Result<SearchRequest, SeekPowerError> {
        let query_type = match self.query_type.as_deref() {
            Some("union") | None => QueryType::Union,
            Some("intersection") | Some("and") => QueryType::Intersection,
            Some("phrase") => QueryType::Phrase,
            Some(other) => {
                return Err(SeekPowerError::InvalidQuery(format!(
                    "unknown query_type: {other}"
                )));
            }
        };

        let ann_mode = match self.ann_mode.as_deref() {
            Some("all") | None => AnnMode::All,
            Some(s) if s.starts_with("nprobe:") => {
                let n = s[7..]
                    .parse::<usize>()
                    .map_err(|_| SeekPowerError::InvalidQuery(format!("invalid nprobe: {s}")))?;
                AnnMode::Nprobe(n)
            }
            Some(s) if s.starts_with("threshold:") => {
                let t = s[10..]
                    .parse::<f32>()
                    .map_err(|_| SeekPowerError::InvalidQuery(format!("invalid threshold: {s}")))?;
                AnnMode::SimilarityThreshold(t)
            }
            Some(other) => {
                return Err(SeekPowerError::InvalidQuery(format!(
                    "unknown ann_mode: {other}"
                )));
            }
        };

        let search_mode = match self.search_mode.as_deref() {
            Some("lexical") | None => SearchMode::Lexical,
            Some("vector") => SearchMode::Vector {
                ann_mode: ann_mode.clone(),
            },
            Some("hybrid") => SearchMode::Hybrid {
                ann_mode: ann_mode.clone(),
                w_lexical: self.w_lexical.unwrap_or(1.0),
                w_vector: self.w_vector.unwrap_or(1.0),
            },
            Some(other) => {
                return Err(SeekPowerError::InvalidQuery(format!(
                    "unknown search_mode: {other}"
                )));
            }
        };

        Ok(SearchRequest {
            query_string: self.query_string,
            query_vector: self.query_vector,
            search_mode,
            query_type,
            ann_mode,
            offset: self.offset.unwrap_or(0),
            length: self.length.unwrap_or(10),
            field_filter: None,
        })
    }
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct CreateIndexResponse {
    pub name: String,
    pub field_count: usize,
}

#[derive(Debug, Serialize)]
pub struct IndexDocumentResponse {
    pub doc_id: u64,
}

#[derive(Debug, Serialize)]
pub struct IndexDocumentsResponse {
    pub doc_ids: Vec<u64>,
}

#[derive(Debug, Serialize)]
pub struct GetDocumentResponse {
    pub doc_id: u64,
    pub document: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}
