//! Default reliable service. No route delegates to the legacy per-key index map.
mod takeover;
use crate::api_types::{CreateIndexRequest, FieldRequest, SearchRequestObject};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use seekpower::{
    catalog::{
        Catalog, CatalogCommand, CatalogError, CatalogRequest, CollectionEntry, OutputPermit,
    },
    index::{AccessType, FrequentwordType, IndexMetaObject, LexicalSimilarity},
    index::{Clustering, DocumentCompression},
    record::{
        OperationId, RecordConfig, RecordCursor, RecordError, RecordPartition, RecordQuery,
        RecordWrite, DataOutputPermit,
    },
    vector::Inference,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path as FsPath, PathBuf},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::{Mutex, RwLock, Semaphore};

const MAX_BODY: usize = 2 * 1024 * 1024;
const MAX_RESPONSE: usize = 16 * 1024 * 1024;
#[derive(Debug)]
struct Error(StatusCode, &'static str, String);
impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"code":self.1,"message":self.2}))).into_response()
    }
}
impl From<CatalogError> for Error {
    fn from(e: CatalogError) -> Self {
        let (status, code) = match &e {
            CatalogError::Busy => (StatusCode::TOO_MANY_REQUESTS, "busy"),
            CatalogError::Unauthorized => (StatusCode::FORBIDDEN, "unauthorized"),
            CatalogError::Rejected(_) => (StatusCode::BAD_REQUEST, "rejected"),
            CatalogError::Conflict(_) => (StatusCode::CONFLICT, "conflict"),
            CatalogError::OperationExpired => (StatusCode::GONE, "operation_expired"),
            CatalogError::OutcomeUnknown(_) => (StatusCode::SERVICE_UNAVAILABLE, "outcome_unknown"),
            CatalogError::RecoveryRequired => {
                (StatusCode::SERVICE_UNAVAILABLE, "recovery_required")
            }
            CatalogError::Storage(_) => (StatusCode::SERVICE_UNAVAILABLE, "storage_error"),
        };
        Self(status, code, e.to_string())
    }
}
impl From<RecordError> for Error {
    fn from(e: RecordError) -> Self {
        let (status, code) = match &e {
            RecordError::Busy => (StatusCode::TOO_MANY_REQUESTS, "busy"),
            RecordError::Rejected(_) => (StatusCode::BAD_REQUEST, "rejected"),
            RecordError::Conflict(_) => (StatusCode::CONFLICT, "conflict"),
            RecordError::OperationExpired => (StatusCode::GONE, "operation_expired"),
            RecordError::DurablePending(_) => (StatusCode::SERVICE_UNAVAILABLE, "durable_pending"),
            RecordError::OutcomeUnknown(_) => (StatusCode::SERVICE_UNAVAILABLE, "outcome_unknown"),
            RecordError::RecoveryRequired => (StatusCode::SERVICE_UNAVAILABLE, "recovery_required"),
            RecordError::Storage(_) => (StatusCode::SERVICE_UNAVAILABLE, "storage_error"),
        };
        Self(status, code, e.to_string())
    }
}
fn bad(message: impl ToString) -> Error {
    Error(StatusCode::BAD_REQUEST, "rejected", message.to_string())
}
fn storage(message: impl ToString) -> Error {
    Error(
        StatusCode::SERVICE_UNAVAILABLE,
        "storage_error",
        message.to_string(),
    )
}
fn not_found() -> Error {
    Error(
        StatusCode::NOT_FOUND,
        "not_found",
        "record or collection not found".into(),
    )
}
fn parse<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, Error> {
    serde_json::from_slice(bytes).map_err(bad)
}
fn verifier(raw: &[u8]) -> String {
    Sha256::digest(raw)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
fn key_bytes(text: &str) -> Result<Vec<u8>, Error> {
    let bytes = STANDARD
        .decode(text)
        .map_err(|_| bad("invalid API key encoding"))?;
    if bytes.len() != 32 {
        return Err(bad("API key must contain 32 random bytes"));
    }
    Ok(bytes)
}
fn master_verifier() -> String {
    verifier(&Sha256::digest(crate::MASTER_KEY_SECRET.as_bytes()))
}
fn control_id(id: &OperationId) -> Result<(), Error> {
    if id.nonce.is_empty() || id.nonce.len() > 256 {
        return Err(bad("operation nonce length must be 1..256 bytes"));
    }
    if id.nonce.starts_with("sys:") {
        return Err(bad("reserved operation namespace"));
    }
    Ok(())
}
fn internal_id(nonce: String) -> OperationId {
    OperationId::new(nonce)
}
async fn owned<T: Send + 'static>(
    work: impl Future<Output = Result<T, Error>> + Send + 'static,
) -> Result<T, Error> {
    tokio::spawn(work).await.map_err(|e| {
        Error(
            StatusCode::SERVICE_UNAVAILABLE,
            "outcome_unknown",
            e.to_string(),
        )
    })?
}

/// The frame handoff, not JSON construction, is the authorization linearization
/// boundary. Bytes already handed to the transport cannot be recalled.
struct AuthorizedBody {
    bytes: Option<Bytes>,
    permit: OutputPermit,
    data: Option<DataOutputPermit>,
}
impl http_body::Body for AuthorizedBody {
    type Data = Bytes;
    type Error = std::io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, std::io::Error>>> {
        if self.bytes.is_none() {
            return Poll::Ready(None);
        }
        let permit = self.permit.clone();
        let data = self.data.clone();
        Poll::Ready(
            match deliver(&permit, data.as_ref(), || self.bytes.take().map(http_body::Frame::data)) {
                Ok(frame) => frame.map(Ok),
                Err(e) => {
                    self.bytes = None;
                    Some(Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        e.2,
                    )))
                }
            },
        )
    }
}
fn response<T: Serialize>(value: T, permit: OutputPermit) -> Result<Response, Error> {
    protected(Ok(value), permit)
}
fn deliver<T>(permit: &OutputPermit, data: Option<&DataOutputPermit>, handoff: impl FnOnce() -> T) -> Result<T, Error> {
    // Lock order is catalog output -> data output, never a partition mutex.
    permit.deliver(|| match data {
        Some(data) => data.deliver(handoff).map_err(Error::from),
        None => Ok(handoff()),
    }).map_err(Error::from)?
}
fn protected<T: Serialize>(result: Result<T, Error>, permit: OutputPermit) -> Result<Response, Error> {
    protected_data(result, permit, None)
}
fn protected_data<T: Serialize>(
    result: Result<T, Error>, permit: OutputPermit, data: Option<DataOutputPermit>,
) -> Result<Response, Error> {
    let encoded = match result {
        Ok(value) => serde_json::to_vec(&value)
            .map(|bytes| (StatusCode::OK, bytes))
            .map_err(|_| storage("response serialization failed")),
        Err(error) => Err(error),
    };
    let encoded = encoded.and_then(|(status, bytes)| {
        if bytes.len() > MAX_RESPONSE {
            Err(bad("response budget exceeded; reduce page size"))
        } else {
            Ok((status, bytes))
        }
    });
    let (status, bytes) = match encoded {
        Ok(value) => value,
        Err(error) => (
            error.0,
            json!({"code":error.1,"message":error.2})
                .to_string()
                .into_bytes(),
        ),
    };
    deliver(&permit, data.as_ref(), || ())?;
    let mut response = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::new(AuthorizedBody {
            bytes: Some(bytes.into()),
            permit: permit.clone(),
            data: data.clone(),
        }))
        .unwrap();
    response.extensions_mut().insert(permit);
    if let Some(data) = data { response.extensions_mut().insert(data); }
    Ok(response)
}

pub(crate) struct Service {
    root: PathBuf,
    catalog: Catalog,
    records: RwLock<BTreeMap<String, RecordPartition>>,
    control: Mutex<()>,
    control_admission: Arc<Semaphore>,
    requests: Arc<Semaphore>,
    default_nprobe: std::num::NonZeroUsize,
    // Drop after catalog, partitions and mappings: stable across root repair.
    _root_owner: std::fs::File,
}
impl Service {
    pub(crate) async fn open(
        root: &FsPath,
        default_nprobe: std::num::NonZeroUsize,
    ) -> Result<Arc<Self>, String> {
        let root = root.to_owned();
        tokio::spawn(async move { Self::open_owned(&root, default_nprobe).await })
            .await
            .map_err(|e| e.to_string())?
    }
    async fn open_owned(
        root: &FsPath,
        default_nprobe: std::num::NonZeroUsize,
    ) -> Result<Arc<Self>, String> {
        let parent = root.parent().ok_or("service root requires parent")?;
        let name = root
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or("invalid root name")?;
        let root_owner = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(parent.join(format!(".seekpower-service-{name}.lock")))
            .map_err(|e| e.to_string())?;
        root_owner
            .try_lock()
            .map_err(|e| format!("service root is already owned: {e}"))?;
        Catalog::resume_initialization(root)
            .await
            .map_err(|e| e.to_string())?;
        let existed = root.exists();
        let catalog = if existed {
            Catalog::open(root).await
        } else {
            Catalog::create_with_key(root, "master".into(), master_verifier()).await
        }
        .map_err(|e| e.to_string())?;
        catalog
            .require_service_completion(root)
            .await
            .map_err(|e| e.to_string())?;
        // Never silently reset a missing/mismatched master or legacy directory.
        if catalog
            .authenticate(&master_verifier())
            .await
            .map_err(|e| e.to_string())?
            != "master"
        {
            return Err("master identity mismatch".into());
        }
        let service = Arc::new(Self {
            root: root.into(),
            catalog,
            records: RwLock::new(BTreeMap::new()),
            control: Mutex::new(()),
            control_admission: Arc::new(Semaphore::new(16)),
            requests: Arc::new(Semaphore::new(64)),
            default_nprobe,
            _root_owner: root_owner,
        });
        // Only unaddressable preparation names inside this exclusively owned
        // service root are reclaimable. Never scan sibling service roots.
        for row in std::fs::read_dir(root).map_err(|e| e.to_string())? {
            let row = row.map_err(|e| e.to_string())?;
            let name = row.file_name().to_string_lossy().into_owned();
            if name
                .strip_prefix(".partition-prepare-")
                .is_some_and(|suffix| {
                    suffix.len() == 32 && suffix.bytes().all(|b| b.is_ascii_hexdigit())
                })
                && row.file_type().map_err(|e| e.to_string())?.is_dir()
            {
                std::fs::remove_dir_all(row.path()).map_err(|e| e.to_string())?;
            }
        }
        for id in service
            .catalog
            .retired_backups()
            .await
            .map_err(|e| e.to_string())?
        {
            if let Err(error) =
                RecordPartition::retire_snapshot(&service.root.join(format!("backup-{id}"))).await
            {
                log::warn!("retired backup cleanup pending: {error}");
            }
        }
        for entry in service
            .catalog
            .collections()
            .await
            .map_err(|e| e.to_string())?
        {
            service.finish_collection(entry).await.map_err(|e| e.2)?;
        }
        Ok(service)
    }
    async fn finish_collection(&self, entry: CollectionEntry) -> Result<(), Error> {
        if entry.deleted || entry.takeover_target.is_some() {
            return Ok(());
        }
        if entry.takeover.as_ref().is_some_and(|t| t.completed.is_none()) {
            return self.finish_takeover(entry).await;
        }
        if self.records.read().await.contains_key(&entry.collection_id) {
            return Ok(());
        }
        let path = self.root.join(&entry.directory);
        if !entry.active {
            RecordPartition::resume_initialization(&path, entry.config.max_journal_bytes).await?;
        }
        let partition = if path.exists() {
            RecordPartition::open(&path, entry.config.max_journal_bytes).await?
        } else if !entry.active {
            if let Some(backup_id) = &entry.restore_from {
                let backup = RecordPartition::open(
                    &self.root.join(format!("backup-{backup_id}")),
                    entry.config.max_journal_bytes,
                )
                .await?;
                if serde_json::to_value(backup.configuration().await?).map_err(storage)?
                    != serde_json::to_value(&entry.config).map_err(storage)?
                {
                    return Err(storage(
                        "backup configuration differs from durable restore intent",
                    ));
                }
                backup.checkpoint(&path).await?;
                drop(backup);
                RecordPartition::open(&path, entry.config.max_journal_bytes).await?
            } else {
                RecordPartition::create(&path, entry.config.clone()).await?
            }
        } else {
            return Err(storage(
                "active collection data missing; refusing empty replacement",
            ));
        };
        if !entry.active {
            self.catalog
                .execute(CatalogRequest {
                    operation_id: internal_id(format!("sys:activate:{}", entry.directory)),
                    command: CatalogCommand::ActivateCollection {
                        collection_id: entry.collection_id.clone(),
                    },
                })
                .await?;
        }
        self.records
            .write()
            .await
            .insert(entry.collection_id, partition);
        Ok(())
    }
    async fn authenticate(&self, headers: &HeaderMap) -> Result<String, Error> {
        let value = headers
            .get("apikey")
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                Error(
                    StatusCode::UNAUTHORIZED,
                    "unauthorized",
                    "API key required".into(),
                )
            })?;
        self.catalog
            .authenticate(&verifier(&key_bytes(value)?))
            .await
            .map_err(Into::into)
    }
    async fn admin(&self, headers: &HeaderMap) -> Result<String, Error> {
        let key = self.authenticate(headers).await?;
        if key != "master" {
            return Err(Error(
                StatusCode::FORBIDDEN,
                "unauthorized",
                "master key required".into(),
            ));
        }
        Ok(key)
    }
    async fn admit_control(
        &self,
        headers: &HeaderMap,
        admin: bool,
    ) -> Result<tokio::sync::OwnedSemaphorePermit, Error> {
        if admin {
            self.admin(headers).await?;
        } else {
            self.authenticate(headers).await?;
        }
        self.control_admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                Error(
                    StatusCode::TOO_MANY_REQUESTS,
                    "busy",
                    "control queue full".into(),
                )
            })
    }
    async fn record(&self, key: &str, id: &str) -> Result<RecordPartition, Error> {
        self.catalog
            .authorized(key.into(), id.into(), |_| async {})
            .await?;
        self.records
            .read()
            .await
            .get(id)
            .cloned()
            .ok_or_else(not_found)
    }
    async fn permit(&self, key: &str) -> Result<OutputPermit, Error> {
        Ok(self.catalog.listing(key).await?.1)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyCreate {
    operation_id: OperationId,
    key_id: String,
    apikey_base64: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyRevoke {
    operation_id: OperationId,
    key_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantChange {
    operation_id: OperationId,
    key_id: String,
    allow: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CollectionCreate {
    operation_id: OperationId,
    collection_id: String,
    config: CreateIndexRequest,
    #[serde(default)]
    embedding_space_id: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadParams {
    #[serde(default)]
    fields: String,
}
fn record_config(
    request: CreateIndexRequest,
    embedding_space_id: Option<String>,
) -> Result<RecordConfig, Error> {
    if request.index_name.trim().is_empty() {
        return Err(bad("index_name must not be empty"));
    }
    let schema = request.schema.into_iter().map(Into::into).collect();
    let inference = request.vector.map(Inference::from).unwrap_or_default();
    Ok(RecordConfig {
        index: IndexMetaObject {
            id: 0,
            name: request.index_name,
            lexical_similarity: LexicalSimilarity::Bm25fProximity,
            tokenizer: request.tokenizer,
            text_normalization: request.text_normalization,
            field_length_normalization: request.field_length_normalization,
            stop_words: request.stop_words,
            frequent_words: FrequentwordType::English,
            ngram_indexing: 9,
            document_compression: DocumentCompression::Snappy,
            access_type: AccessType::Mmap,
            clustering: Clustering::Auto,
            inference,
        },
        schema,
        embedding_space_id,
        max_records: 10_000,
        max_record_bytes: 1024 * 1024,
        max_operations: 10_000,
        max_journal_bytes: 256 * 1024 * 1024,
        max_candidates: 10_000,
    })
}
fn index_info(entry: &CollectionEntry, stats: seekpower::record::RecordStatistics) -> Value {
    let schema: Vec<FieldRequest> = entry.config.schema.iter().map(FieldRequest::from).collect();
    json!({"id":entry.collection_id,"name":entry.config.index.name,"schema":schema,
        "text_normalization":entry.config.index.text_normalization,"field_length_normalization":entry.config.index.field_length_normalization,
        "indexed_doc_count":stats.active_records,"committed_doc_count":stats.active_records,"version":crate::VERSION,"statistics":stats,
        "embedding_space_id":entry.config.embedding_space_id,"evidence_quarantined":stats.evidence_quarantined})
}
async fn create_key(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<Response, Error> {
    let request: KeyCreate = parse(&bytes)?;
    control_id(&request.operation_id)?;
    let admission = s.admit_control(&headers, true).await?;
    owned(async move {
        let _admission = admission;
        let _control = s.control.lock().await;
        let key = s.admin(&headers).await?;
        if request.key_id == "master" {
            return Err(bad("master identity is reserved"));
        }
        let receipt = s
            .catalog
            .execute(CatalogRequest {
                operation_id: request.operation_id,
                command: CatalogCommand::AddKey {
                    key_id: request.key_id,
                    verifier: verifier(&key_bytes(&request.apikey_base64)?),
                },
            })
            .await?;
        response(receipt, s.permit(&key).await?)
    })
    .await
}
async fn revoke_key(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<Response, Error> {
    let request: KeyRevoke = parse(&bytes)?;
    control_id(&request.operation_id)?;
    let admission = s.admit_control(&headers, true).await?;
    owned(async move {
        let _admission = admission;
        let _control = s.control.lock().await;
        let key = s.admin(&headers).await?;
        if request.key_id == "master" {
            return Err(bad("master key cannot revoke itself"));
        }
        let receipt = s
            .catalog
            .execute(CatalogRequest {
                operation_id: request.operation_id,
                command: CatalogCommand::RevokeKey {
                    key_id: request.key_id,
                },
            })
            .await?;
        response(receipt, s.permit(&key).await?)
    })
    .await
}
async fn grants(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    bytes: Bytes,
) -> Result<Response, Error> {
    let request: GrantChange = parse(&bytes)?;
    control_id(&request.operation_id)?;
    let admission = s.admit_control(&headers, true).await?;
    owned(async move {
        let _admission = admission;
        let _control = s.control.lock().await;
        let key = s.admin(&headers).await?;
        let command = if request.allow {
            CatalogCommand::Grant {
                collection_id: id,
                key_id: request.key_id,
            }
        } else {
            CatalogCommand::RevokeGrant {
                collection_id: id,
                key_id: request.key_id,
            }
        };
        let receipt = s
            .catalog
            .execute(CatalogRequest {
                operation_id: request.operation_id,
                command,
            })
            .await?;
        response(receipt, s.permit(&key).await?)
    })
    .await
}
async fn create_collection(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<Response, Error> {
    let request: CollectionCreate =
        parse(&bytes).map_err(|e| Error(e.0, "creation_rejected", e.2))?;
    control_id(&request.operation_id)?;
    let admission = s.admit_control(&headers, false).await?;
    owned(async move {
        let _admission = admission;
        let _control = s.control.lock().await;
        let key = s.authenticate(&headers).await?;
        // Creation grants only a new collection to its creator; existing grants
        // can be changed exclusively through the master control plane.
        let operation_id = request.operation_id.clone();
        let id = request.collection_id.clone();
        let config = record_config(request.config, request.embedding_space_id)
            .map_err(|e| Error(e.0, "creation_rejected", e.2))?;
        seekpower::record::validate_config(&config)
            .map_err(|e| Error(StatusCode::BAD_REQUEST, "creation_rejected", e.to_string()))?;
        s.catalog
            .execute(CatalogRequest {
                operation_id: request.operation_id,
                command: CatalogCommand::PrepareCollection {
                    collection_id: id.clone(),
                    owner_key: key.clone(),
                    config,
                },
            })
            .await?;
        let entry = s
            .catalog
            .collections()
            .await?
            .into_iter()
            .find(|e| e.collection_id == id)
            .ok_or_else(not_found)?;
        s.finish_collection(entry).await?;
        let (_, permit) = s.catalog.authorized(key, id.clone(), |_| async {}).await?;
        response(
            json!({"collection_id":id,"operation_id":operation_id}),
            permit,
        )
    })
    .await
}
async fn list_collections(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
) -> Result<Response, Error> {
    let key = s.authenticate(&headers).await?;
    let (entries, permit) = s.catalog.listing(&key).await?;
    let mut info = vec![];
    let mut data = vec![];
    for entry in entries {
        let p = s.record(&key, &entry.collection_id).await?;
        data.push(p.output_permit()?);
        let id = entry.collection_id.clone();
        let (value, _) = s
            .catalog
            .authorized(
                key.clone(),
                id,
                move |_| async move { p.statistics().await },
            )
            .await?;
        match value {
            Ok(stats) => info.push(index_info(&entry, stats)),
            Err(e) => return protected_data::<Value>(Err(e.into()), permit, Some(DataOutputPermit::combine(data))),
        }
    }
    protected_data(Ok(info), permit, Some(DataOutputPermit::combine(data)))
}
async fn info(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, Error> {
    let key = s.authenticate(&headers).await?;
    let p = s.record(&key, &id).await?;
    let data = p.output_permit()?;
    let (value, permit) = s
        .catalog
        .authorized(key, id, move |entry| async move {
            p.statistics().await.map(|stats| index_info(&entry, stats))
        })
        .await?;
    protected_data(value.map_err(Into::into), permit, Some(data))
}
async fn write(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    bytes: Bytes,
) -> Result<Response, Error> {
    let value: Value = parse(&bytes)?;
    if !value
        .as_object()
        .is_some_and(|v| v.contains_key("document"))
    {
        return Err(bad("document must be explicit; null means deletion"));
    }
    let request: RecordWrite = serde_json::from_value(value).map_err(bad)?;
    let key = s.authenticate(&headers).await?;
    let p = s.record(&key, &id).await?;
    let operation_id = request.operation_id.clone();
    let (result, permit) = s
        .catalog
        .authorized(key, id, move |_| async move { p.write(request).await })
        .await?;
    protected(result.map(|receipt|json!({"receipt":receipt,"operation_id":operation_id,"expires_at":operation_id.expires_at_unix_seconds()})).map_err(Into::into),permit)
}
async fn record_status(
    State(s): State<Arc<Service>>, headers: HeaderMap, Path((id, record_id)): Path<(String, String)>,
) -> Result<Response, Error> {
    let key = s.authenticate(&headers).await?;
    let p = s.record(&key, &id).await?;
    let data = p.output_permit()?;
    let (result, permit) = s.catalog.authorized(key, id, move |_| async move { p.record_status(&record_id).await }).await?;
    protected_data(result.map_err(Error::from).and_then(|status| status.ok_or_else(not_found)), permit, Some(data))
}
async fn read(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    Path((id, record_id)): Path<(String, String)>,
    Query(params): Query<ReadParams>,
) -> Result<Response, Error> {
    let key = s.authenticate(&headers).await?;
    let p = s.record(&key, &id).await?;
    let data = p.output_permit()?;
    let (result, permit) = s
        .catalog
        .authorized(key, id, move |_| async move { p.get(&record_id).await })
        .await?;
    let value = result
        .map_err(Error::from)
        .and_then(|record| record.ok_or_else(not_found))
        .map(|mut record| {
            if !params.fields.is_empty() {
                let fields: Vec<_> = params.fields.split(',').collect();
                if let Some(document) = record.document.as_mut() {
                    document.retain(|field, _| fields.contains(&field.as_str()));
                }
            }
            record
        });
    protected_data(value, permit, Some(data))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PageRequest {
    #[serde(default)]
    cursor: Option<RecordCursor>,
    #[serde(default = "page_length")]
    length: usize,
    #[serde(default)]
    fields: Vec<String>,
}
fn page_length() -> usize {
    10
}
async fn iterate(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    bytes: Bytes,
) -> Result<Response, Error> {
    let request: PageRequest = parse(&bytes)?;
    let key = s.authenticate(&headers).await?;
    let p = s.record(&key, &id).await?;
    let data = p.output_permit()?;
    let (result, permit) = s
        .catalog
        .authorized(key, id, move |_| async move {
            let mut page = p.page(request.cursor, request.length).await?;
            if !request.fields.is_empty() {
                for record in &mut page.records {
                    if let Some(doc) = record.document.as_mut() {
                        doc.retain(|field, _| request.fields.contains(field));
                    }
                }
            }
            Ok::<_, RecordError>(page)
        })
        .await?;
    protected_data(result.map_err(Into::into), permit, Some(data))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchRequest {
    writes: Vec<RecordWrite>,
}
async fn batch(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    bytes: Bytes,
) -> Result<Response, Error> {
    // Null is a deliberate deletion. Missing document is never interpreted as one.
    let value: Value = parse(&bytes)?;
    if !value
        .get("writes")
        .and_then(Value::as_array)
        .is_some_and(|rows| {
            rows.iter()
                .all(|r| r.as_object().is_some_and(|o| o.contains_key("document")))
        })
    {
        return Err(bad("every write must contain explicit document"));
    }
    let request: BatchRequest = serde_json::from_value(value).map_err(bad)?;
    if request.writes.is_empty() || request.writes.len() > 100 {
        return Err(bad("batch requires 1..100 independent writes"));
    }
    let key = s.authenticate(&headers).await?;
    let p = s.record(&key, &id).await?;
    let (result, permit) = s.catalog.authorized(key, id, move |_| async move {
        let mut rows = Vec::new();
        let mut stopped = false;
        for write in request.writes {
            let operation_id = write.operation_id.clone();
            if stopped { rows.push(json!({"operation_id":operation_id,"code":"not_attempted"})); continue; }
            match p.write(write).await {
                Ok(receipt) => rows.push(json!({"operation_id":operation_id,"receipt":receipt,"code":"applied","expires_at":operation_id.expires_at_unix_seconds()})),
                Err(error) => {
                    stopped = matches!(error, RecordError::OutcomeUnknown(_) | RecordError::DurablePending(_) | RecordError::RecoveryRequired | RecordError::Storage(_));
                    let error = Error::from(error);
                    rows.push(json!({"operation_id":operation_id,"code":error.1,"message":error.2,"status":error.0.as_u16()}));
                }
            }
        }
        json!({"atomic":false,"results":rows})
    }).await?;
    response(result, permit)
}
#[derive(Deserialize)]
#[serde(tag = "select", rename_all = "snake_case", deny_unknown_fields)]
enum DeleteSelection {
    All,
    Ids { record_ids: Vec<String> },
    Query { query: SearchRequestObject },
}
async fn deletion_plan(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    bytes: Bytes,
) -> Result<Response, Error> {
    let selection: DeleteSelection = parse(&bytes)?;
    let default_nprobe = s.default_nprobe;
    let key = s.authenticate(&headers).await?;
    let p = s.record(&key, &id).await?;
    let data = p.output_permit()?;
    let (result, permit) = s.catalog.authorized(key, id, move |entry| async move {
        let (seq, records) = match selection {
            DeleteSelection::Query { query } => {
                if query.query_vector.is_some() || query.similarity_threshold.is_some() || query.query_string.trim().is_empty() {
                    return Err(bad("query deletion requires nonempty lexical text; use explicit all selection to clear"));
                }
                // Deletion selects every match, never the caller's display page or ANN sample.
                query.search_mode(default_nprobe).map_err(bad)?;
                let result = p.query(RecordQuery { text:query.query_string.clone(), vector:None, mode:seekpower::search::SearchMode::Lexical,
                    query_type:query.query_type(), offset:0, length:entry.config.max_candidates, result_type:seekpower::search::ResultType::TopkCount,
                    fields:query.field_filter, filters:query.facet_filter, sort:vec![] }).await?;
                if result.hits.len() != result.count_total { return Err(bad("deletion selection exceeds candidate budget")); }
                (result.visible_seq, result.hits.into_iter().map(|hit| hit.record).collect::<Vec<_>>())
            }
            selection => {
                let mut page = p.page(None, entry.config.max_candidates).await?;
                if page.next.is_some() { return Err(bad("deletion selection exceeds candidate budget")); }
                if let DeleteSelection::Ids { record_ids } = selection {
                    if record_ids.len() > entry.config.max_candidates { return Err(bad("identity selection exceeds budget")); }
                    page.records.retain(|record| record_ids.contains(&record.record_id));
                }
                (page.visible_seq, page.records)
            }
        };
        let targets: Vec<_> = records.into_iter().map(|r| json!({"record_id":r.record_id,"routing_key":r.routing_key,"expected":{"Match":r.version},"document":null})).collect();
        Ok::<_,Error>(json!({"visible_seq":seq,"targets":targets,"atomic":false,"instruction":"Persist targets with one original operation_id each before executing /batch; retries must reuse the saved requests, never regenerate the selection."}))
    }).await?;
    protected_data(result, permit, Some(data))
}

async fn operation(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    Path((id, epoch, nonce)): Path<(String, u64, String)>,
) -> Result<Response, Error> {
    let key = s.authenticate(&headers).await?;
    let p = s.record(&key, &id).await?;
    let operation_id = OperationId { epoch, nonce };
    let lookup = operation_id.clone();
    let (result, permit) = s
        .catalog
        .authorized(key, id, move |_| async move { p.operation(&lookup).await })
        .await?;
    protected(result.map(|receipt|json!({"operation_id":operation_id,"receipt":receipt,"expires_at":operation_id.expires_at_unix_seconds(),"absence_proves_failure":false})).map_err(Into::into),permit)
}
async fn catalog_operation(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    Path((epoch, nonce)): Path<(u64, String)>,
) -> Result<Response, Error> {
    let key = s.admin(&headers).await?;
    let id = OperationId { epoch, nonce };
    let result = s.catalog.operation(&id).await?;
    response(
        json!({"receipt":result,"absence_proves_failure":false}),
        s.permit(&key).await?,
    )
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BackupRequest {
    operation_id: OperationId,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RestoreRequest {
    operation_id: OperationId,
    collection_id: String,
    backup_id: String,
}
fn validate_backup_id(id: &str) -> Result<(), Error> {
    if id.len() != 64
        || !id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(bad("invalid backup ID"));
    }
    Ok(())
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ServiceRestoreRequest {
    operation_id: OperationId,
    backup_id: String,
}
fn service_budget(parent: &FsPath, prefix: &str, target: &FsPath) -> Result<(), Error> {
    if target.exists()
        || target
            .with_file_name(format!(
                ".{}.staging",
                target.file_name().unwrap().to_string_lossy()
            ))
            .exists()
    {
        return Ok(());
    }
    let count = std::fs::read_dir(parent)
        .map_err(storage)?
        .try_fold(0usize, |n, e| {
            let e = e?;
            let name = e.file_name().to_string_lossy().into_owned();
            Ok::<_, std::io::Error>(
                n + usize::from(
                    name.starts_with(prefix) || name.starts_with(&format!(".{prefix}")),
                ),
            )
        })
        .map_err(storage)?;
    if count >= 4 {
        return Err(bad(
            "service snapshot/root limit (4) reached; no automatic eviction",
        ));
    }
    Ok(())
}
async fn service_backup(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<Response, Error> {
    let request: BackupRequest = parse(&bytes)?;
    control_id(&request.operation_id)?;
    let admission = s.admit_control(&headers, true).await?;
    owned(async move {
        let _admission=admission;let _control=s.control.lock().await;
        let key=s.admin(&headers).await?;
        s.catalog.operation(&request.operation_id).await?; // epoch admission, never infer absence = failure
        let id=verifier(&serde_json::to_vec(&("service-backup",&request.operation_id)).map_err(bad)?);
        let target=s.root.join(format!("service-backup-{id}"));
        service_budget(&s.root,"service-backup-",&target)?;
        let records=s.records.read().await.clone();
        let receipt=s.catalog.backup_service(s.root.clone(),target,id.clone(),records).await?;
        response(json!({"backup_id":id,"snapshot":receipt,"operation_id":request.operation_id,"scope":"catalog, all retained collections and live collection backups; no recursive service archives"}),s.permit(&key).await?)
    }).await
}
async fn service_restore(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<Response, Error> {
    let request: ServiceRestoreRequest = parse(&bytes)?;
    control_id(&request.operation_id)?;
    validate_backup_id(&request.backup_id)?;
    let admission = s.admit_control(&headers, true).await?;
    owned(async move {
        let _admission=admission;let _control=s.control.lock().await;
        let key=s.admin(&headers).await?;s.catalog.operation(&request.operation_id).await?;
        let id=verifier(&serde_json::to_vec(&("service-restore",&request.operation_id)).map_err(bad)?);
        let parent=s.root.parent().ok_or_else(||bad("missing service parent"))?;
        let name=format!("restored-service-{id}");let target=parent.join(&name);
        service_budget(parent,"restored-service-",&target)?;
        if let Some(receipt)=Catalog::completed_service_restore(&target)? {
            if receipt.identity!=id || receipt.restored_from.as_ref()!=Some(&request.backup_id){return Err(bad("restore request identity reused"));}
            return response(json!({"snapshot":receipt,"target_root":target,"authorization":"current master only; historical keys disabled; historical control operations rejected","evidence_restore":"quarantined; deletion-history reconciliation not yet supported","started":false}),s.permit(&key).await?);
        }

        let guard=std::fs::OpenOptions::new().create(true).truncate(false).read(true).write(true).open(parent.join(format!(".seekpower-service-{name}.lock"))).map_err(storage)?;
        guard.try_lock().map_err(storage)?;
        let receipt=Catalog::restore_service(s.root.join(format!("service-backup-{}",request.backup_id)),target.clone(),id,master_verifier()).await?;
        response(json!({"snapshot":receipt,"target_root":target,"authorization":"current master only; historical keys disabled; historical control operations rejected","evidence_restore":"quarantined; deletion-history reconciliation not yet supported","started":false}),s.permit(&key).await?)
    }).await
}
async fn backup(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    bytes: Bytes,
) -> Result<Response, Error> {
    let request: BackupRequest = parse(&bytes)?;
    control_id(&request.operation_id)?;
    let admission = s.admit_control(&headers, false).await?;
    owned(async move {
        let _admission = admission;
        let _control = s.control.lock().await;
        let key = s.authenticate(&headers).await?;
        let p = s.record(&key, &id).await?;
        let identity = serde_json::to_vec(&(id.clone(), &request.operation_id)).map_err(bad)?;
        let backup_id = verifier(&identity);
        if s.catalog.retired_backups().await?.contains(&backup_id) {
            return Err(Error(StatusCode::GONE,"backup_retired","backup identity was permanently retired".into()));
        }
        let path = s.root.join(format!("backup-{backup_id}"));
        let root = s.root.clone();
        let oid = request.operation_id.clone();
        let (result, permit) = s.catalog.authorized(key, id, move |entry| async move {
            // The source epoch watermark is checked even for a replay. Backup IDs
            // are immutable; retrying an ID never takes a fresher snapshot.
            p.operation(&oid).await?;
            RecordPartition::resume_initialization(&path, entry.config.max_journal_bytes).await?;
            let seq = if path.exists() {
                let existing = RecordPartition::open(&path, entry.config.max_journal_bytes).await?;
                existing.statistics().await?.visible_seq
            } else {
                let count = std::fs::read_dir(&root).map_err(storage)?.try_fold(0usize, |count, row| {
                    let row = row?;
                    Ok::<_,std::io::Error>(count + usize::from(row.file_name().to_string_lossy().starts_with("backup-")))
                }).map_err(storage)?;
                if count >= 32 { return Err(bad("local backup budget exhausted (32); export/retirement tooling is pending")); }
                p.checkpoint(&path).await?
            };
            Ok::<_,Error>(json!({"backup_id":backup_id,"visible_seq":seq,"operation_id":request.operation_id,"expires_at":request.operation_id.expires_at_unix_seconds(),"scope":"collection data/configuration/versions/operation receipts; grants are not restored"}))
        }).await?;
        protected(result, permit)
    }).await
}
async fn list_backups(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
) -> Result<Response, Error> {
    let key = s.admin(&headers).await?;
    let _control = s.control.lock().await;
    s.admin(&headers).await?;
    let retired = s.catalog.retired_backups().await?;
    let mut ids = retired
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    for entry in std::fs::read_dir(&s.root).map_err(storage)? {
        let entry = entry.map_err(storage)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if let Some(id) = name.strip_prefix("backup-") {
            if validate_backup_id(id).is_ok() {
                ids.insert(id.to_owned());
            }
        }
    }
    response(ids.into_iter().map(|id|json!({"backup_id":id,"retired":retired.contains(&id),"on_disk":s.root.join(format!("backup-{id}")).exists()})).collect::<Vec<_>>(),s.permit(&key).await?)
}
async fn retire_backup(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    bytes: Bytes,
) -> Result<Response, Error> {
    validate_backup_id(&id)?;
    let request: BackupRequest = parse(&bytes)?;
    control_id(&request.operation_id)?;
    let admission = s.admit_control(&headers, true).await?;
    owned(async move {
        let _admission=admission;
        let _control=s.control.lock().await;
        let key=s.admin(&headers).await?;
        let receipt=s.catalog.execute(CatalogRequest { operation_id:request.operation_id, command:CatalogCommand::RetireBackup { backup_id:id.clone() } }).await?;
        let cleanup=RecordPartition::retire_snapshot(&s.root.join(format!("backup-{id}"))).await;
        response(json!({"receipt":receipt,"backup_id":id,"retired":true,"cleanup_pending":cleanup.is_err(),"cleanup_error":cleanup.err().map(|e|e.to_string())}),s.permit(&key).await?)
    }).await
}

async fn restore(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<Response, Error> {
    let request: RestoreRequest = parse(&bytes)?;
    control_id(&request.operation_id)?;
    validate_backup_id(&request.backup_id)?;
    let admission = s.admit_control(&headers, true).await?;
    owned(async move {
        let _admission = admission;
        let _control = s.control.lock().await;
        let key = s.admin(&headers).await?;
        let existing = s.catalog.collections().await?.into_iter().find(|e| e.collection_id==request.collection_id);
        let config = if let Some(entry) = existing {
            if entry.owner_key!=key || entry.restore_from.as_ref()!=Some(&request.backup_id) { return Err(bad("collection identity belongs to another creation intent")); }
            entry.config
        } else {
            if s.catalog.retired_backups().await?.contains(&request.backup_id) { return Err(Error(StatusCode::GONE,"backup_retired","backup identity was retired".into())); }
            let backup = RecordPartition::open(&s.root.join(format!("backup-{}",request.backup_id)),256*1024*1024).await?;
            let config = backup.configuration().await?;
            drop(backup);
            config
        };
        let operation_id = request.operation_id.clone();
        let id = request.collection_id;
        s.catalog.execute(CatalogRequest { operation_id:request.operation_id, command:CatalogCommand::PrepareRestore {
            collection_id:id.clone(), owner_key:key.clone(), config, backup_id:request.backup_id,
        }}).await?;
        let entry = s.catalog.collections().await?.into_iter().find(|e|e.collection_id==id).ok_or_else(not_found)?;
        if entry.deleted { return Err(bad("restored collection has since been deleted")); }
        s.finish_collection(entry).await?;
        let (_, permit) = s.catalog.authorized(key.clone(), id.clone(), |_|async {}).await?;
        let stats = s.record(&key, &id).await?.statistics().await?;
        response(json!({"collection_id":id,"operation_id":operation_id,"expires_at":operation_id.expires_at_unix_seconds(),"evidence_quarantined":stats.evidence_quarantined}), permit)
    }).await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteCollectionRequest {
    operation_id: OperationId,
}
async fn delete_collection(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    bytes: Bytes,
) -> Result<Response, Error> {
    let request: DeleteCollectionRequest = parse(&bytes)?;
    control_id(&request.operation_id)?;
    let admission = s.admit_control(&headers, true).await?;
    owned(async move {
        let _admission = admission;
        let _control = s.control.lock().await;
        let key = s.admin(&headers).await?;
        let receipt = s
            .catalog
            .execute(CatalogRequest {
                operation_id: request.operation_id,
                command: CatalogCommand::DeleteCollection {
                    collection_id: id.clone(),
                },
            })
            .await?;
        // No physical deletion while mmap owners may remain. The durable catalog
        // tombstone prevents serving or reopening this identity after a restart.
        s.records.write().await.remove(&id);
        response(
            json!({"receipt":receipt,"deleted":true,"physical_reclamation":"deferred"}),
            s.permit(&key).await?,
        )
    })
    .await
}

async fn checkpoint(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Response, Error> {
    let key = s.authenticate(&headers).await?;
    let p = s.record(&key, &id).await?;
    let (result, permit) = s
        .catalog
        .authorized(key, id, move |_| async move { p.compact_journal().await })
        .await?;
    protected(
        result
            .map(|seq| json!({"visible_seq":seq}))
            .map_err(Into::into),
        permit,
    )
}
async fn query(
    State(s): State<Arc<Service>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    bytes: Bytes,
) -> Result<Response, Error> {
    let request: SearchRequestObject = parse(&bytes)?;
    let key = s.authenticate(&headers).await?;
    let p = s.record(&key, &id).await?;
    let data = p.output_permit()?;
    let mode = request.search_mode(s.default_nprobe).map_err(bad)?;
    if request.query_vector.is_none() && request.similarity_threshold.is_some() {
        return Err(bad("similarity_threshold requires query_vector"));
    }
    let (result,permit)=s.catalog.authorized(key,id,move|entry|async move{
        let start=std::time::Instant::now();
        let vector=request.query_vector.as_ref().map(|value|seekpower::vector::parse_external_embedding(value,&entry.config.index.inference)).transpose().map_err(bad)?;
        let result=p.query(RecordQuery{text:request.query_string.clone(),vector,mode,query_type:request.query_type(),offset:request.offset,length:request.length,result_type:request.result_type(),fields:request.field_filter,filters:request.facet_filter,sort:request.result_sort}).await?;
        let hits:Vec<_>=result.hits.into_iter().map(|mut hit|{if let Some(doc)=hit.record.document.as_mut(){doc.retain(|field,_|entry.config.schema.iter().any(|f|f.field==*field&&f.store)&&(request.fields.is_empty()||request.fields.contains(field)));}hit}).collect();
        Ok::<_,Error>(json!({"original_query":request.query_string,"query":request.query_string,"time":start.elapsed().as_nanos(),"offset":request.offset,"length":request.length,"count":hits.len(),"count_total":request.count.then_some(result.count_total),"query_terms":result.query_terms,"hits":hits,"visible_seq":result.visible_seq,"statistics":result.statistics}))
    }).await?;
    protected_data(result, permit, Some(data))
}

// Last application handoff, covering status/headers as well as the later body
// frame handoff. No await is permitted after the final authorization check.
async fn delivery_gate(request: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let response = next.run(request).await;
    let data = response.extensions().get::<DataOutputPermit>().cloned();
    match response.extensions().get::<OutputPermit>().cloned() {
        Some(permit) => match deliver(&permit, data.as_ref(), || response) {
            Ok(response) => response,
            Err(e) => e.into_response(),
        },
        None => response,
    }
}
struct AdmissionBody {
    inner: Body,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl http_body::Body for AdmissionBody {
    type Data = Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        Pin::new(&mut self.inner).poll_frame(cx)
    }
}
async fn request_guard(
    State(service): State<Arc<Service>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let Ok(_permit) = service.requests.clone().try_acquire_owned() else {
        return Error(
            StatusCode::TOO_MANY_REQUESTS,
            "busy",
            "request queue full".into(),
        )
        .into_response();
    };
    if request.uri().path().ends_with("/query")
        && request.uri().query().is_some_and(|q| !q.is_empty())
    {
        return bad("query parameters belong in the JSON body").into_response();
    }
    let response =
        match tokio::time::timeout(std::time::Duration::from_secs(30), next.run(request)).await {
            Ok(response) => response,
            Err(_) => Error(
                StatusCode::SERVICE_UNAVAILABLE,
                "outcome_unknown",
                "request deadline exceeded; retry the original operation identity".into(),
            )
            .into_response(),
        };
    let (parts, body) = response.into_parts();
    Response::from_parts(
        parts,
        Body::new(AdmissionBody {
            inner: body,
            _permit,
        }),
    )
}

pub(crate) fn router(service: Arc<Service>) -> Router {
    let mut app = Router::new()
        .route("/api/v1/live", get(|| async { Json(crate::VERSION) }))
        .route(
            "/api/v1/apikey",
            get(list_collections).post(create_key).delete(revoke_key),
        )
        .route("/api/v1/index", post(create_collection))
        .route("/api/v1/restore", post(restore))
        .route("/api/v1/takeovers", post(takeover::takeover))
        .route("/api/v1/index/{id}/doc/{record_id}/status", get(record_status))
        .route("/api/v1/backups", get(list_backups))
        .route("/api/v1/service-backups", post(service_backup))
        .route("/api/v1/service-restores", post(service_restore))
        .route("/api/v1/backups/{id}", axum::routing::delete(retire_backup))
        .route("/api/v1/index/{id}/backup", post(backup))
        .route(
            "/api/v1/index/{id}",
            get(info).patch(checkpoint).delete(delete_collection),
        )
        .route("/api/v1/index/{id}/grants", post(grants))
        .route(
            "/api/v1/index/{id}/doc",
            post(write).patch(write).delete(write),
        )
        .route("/api/v1/index/{id}/doc/{record_id}", get(read))
        .route("/api/v1/index/{id}/query", post(query))
        .route("/api/v1/index/{id}/iterator", post(iterate))
        .route("/api/v1/index/{id}/batch", post(batch))
        .route("/api/v1/index/{id}/delete-plan", post(deletion_plan))
        .route(
            "/api/v1/index/{id}/operation/{epoch}/{nonce}",
            get(operation),
        )
        .route(
            "/api/v1/catalog/operation/{epoch}/{nonce}",
            get(catalog_operation),
        );
    for (path, content_type, bytes) in [
        (
            "/",
            "text/html; charset=utf-8",
            include_bytes!("../web/index.html").as_slice(),
        ),
        (
            "/js/master.js",
            "text/javascript",
            include_bytes!("../web/js/master.js").as_slice(),
        ),
        (
            "/js/setup.js",
            "text/javascript",
            include_bytes!("../web/js/setup.js").as_slice(),
        ),
        (
            "/js/documents.js",
            "text/javascript",
            include_bytes!("../web/js/documents.js").as_slice(),
        ),
        (
            "/js/jquery-3.7.1.min.js",
            "text/javascript",
            include_bytes!("../web/js/jquery-3.7.1.min.js").as_slice(),
        ),
        (
            "/css/master.css",
            "text/css",
            include_bytes!("../web/css/master.css").as_slice(),
        ),
        (
            "/css/flexboxgrid.min.css",
            "text/css",
            include_bytes!("../web/css/flexboxgrid.min.css").as_slice(),
        ),
        (
            "/svg/logo.svg",
            "image/svg+xml",
            include_bytes!("../web/svg/logo.svg").as_slice(),
        ),
    ] {
        app = app.route(
            path,
            get(move || async move { ([(header::CONTENT_TYPE, content_type)], bytes) }),
        );
    }
    app.fallback(|| async { not_found() })
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .layer(axum::middleware::from_fn_with_state(
            service.clone(),
            request_guard,
        ))
        .layer(axum::middleware::from_fn(delivery_gate))
        .with_state(service)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[test]
    fn admission_errors_do_not_claim_a_definitive_write_rejection() {
        for error in [Error::from(RecordError::Busy), Error::from(CatalogError::Busy)] {
            assert_eq!(error.0, StatusCode::TOO_MANY_REQUESTS);
            assert_eq!(error.1, "busy");
        }
    }
    fn path() -> PathBuf {
        let base = std::env::var_os("SEEKPOWER_TEST_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        std::fs::create_dir_all(&base).unwrap();
        base.join(format!("service-{:032x}", rand::random::<u128>()))
    }
    async fn setup() -> (Arc<Service>, PathBuf, HeaderMap) {
        let path = path();
        let service = Service::open(&path, std::num::NonZeroUsize::new(200).unwrap())
            .await
            .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            "apikey",
            STANDARD
                .encode(Sha256::digest(crate::MASTER_KEY_SECRET.as_bytes()))
                .parse()
                .unwrap(),
        );
        let create = json!({"operation_id":OperationId::new("create".into()),"collection_id":"c","config":{"index_name":"test","schema":[{"field":"body","field_type":"Text"}]}});
        create_collection(
            State(service.clone()),
            headers.clone(),
            serde_json::to_vec(&create).unwrap().into(),
        )
        .await
        .unwrap();
        service
            .catalog
            .execute(CatalogRequest {
                operation_id: OperationId::new("key".into()),
                command: CatalogCommand::AddKey {
                    key_id: "a".into(),
                    verifier: verifier(&[7; 32]),
                },
            })
            .await
            .unwrap();
        service
            .catalog
            .execute(CatalogRequest {
                operation_id: OperationId::new("grant".into()),
                command: CatalogCommand::Grant {
                    collection_id: "c".into(),
                    key_id: "a".into(),
                },
            })
            .await
            .unwrap();
        (service, path, headers)
    }
    async fn raw(address: std::net::SocketAddr, request: String) -> Vec<u8> {
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut bytes = vec![];
        let _ = stream.read_to_end(&mut bytes).await;
        bytes
    }
    #[tokio::test]
    async fn service_real_http_revocation_blocks_prepared_error_and_success_bodies() {
        for missing in [false, true] {
            let (service, path, headers) = setup().await;
            let doc = json!({"operation_id":OperationId::new("write".into()),"record_id":"r","routing_key":"r","expected":"Absent","document":{"body":"secret-original"}});
            write(
                State(service.clone()),
                headers.clone(),
                Path("c".into()),
                serde_json::to_vec(&doc).unwrap().into(),
            )
            .await
            .unwrap();
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let e = entered.clone();
            let r = release.clone();
            let app = router(service.clone())
                .layer(axum::middleware::from_fn(
                    move |request: axum::extract::Request, next: axum::middleware::Next| {
                        let e = e.clone();
                        let r = r.clone();
                        async move {
                            let hold = request.headers().contains_key("x-test-hold");
                            let result = next.run(request).await;
                            if hold {
                                e.notify_one();
                                r.notified().await;
                            }
                            result
                        }
                    },
                ))
                .layer(axum::middleware::from_fn(delivery_gate));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let record = if missing { "missing" } else { "r" };
            let key = STANDARD.encode([7; 32]);
            let pending = tokio::spawn(raw(
                address,
                format!(
                    "GET /api/v1/index/c/doc/{record} HTTP/1.1\r\nHost: localhost\r\napikey: {key}\r\nx-test-hold: yes\r\nConnection: close\r\n\r\n"
                ),
            ));
            entered.notified().await;
            let body =
                json!({"operation_id":OperationId::new("revoke".into()),"key_id":"a"}).to_string();
            let acknowledgment=raw(address,format!("DELETE /api/v1/apikey HTTP/1.1\r\nHost: localhost\r\napikey: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",headers["apikey"].to_str().unwrap(),body.len())).await;
            assert!(String::from_utf8_lossy(&acknowledgment).starts_with("HTTP/1.1 200"));
            release.notify_one();
            let received = pending.await.unwrap();
            let text = String::from_utf8_lossy(&received);
            assert!(
                !text.contains("secret-original") && !text.contains("not_found"),
                "protected body escaped revoke: {text}"
            );
            server.abort();
            let _ = server.await;
            drop(service);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
    #[tokio::test]
    async fn service_real_http_source_delete_blocks_prepared_error_and_success_bodies() {
        for target in ["r", "missing", "listing"] {
            let (service, path, headers) = setup().await;
            for doc in [
                json!({"operation_id":OperationId::new("source".into()),"record_id":"s","routing_key":"route","expected":"Absent","document":{"body":"secret-source"},"system":{"kind":"source"}}),
                json!({"operation_id":OperationId::new("derived".into()),"record_id":"r","routing_key":"route","expected":"Absent","document":{"body":"secret-original"},"system":{"kind":"derived","evidence_refs":[{"record_id":"s","routing_key":"route","version":1}]}}),
            ] {
                write(State(service.clone()), headers.clone(), Path("c".into()), serde_json::to_vec(&doc).unwrap().into()).await.unwrap();
            }
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let e = entered.clone();
            let r = release.clone();
            let app = router(service.clone())
                .layer(axum::middleware::from_fn(
                    move |request: axum::extract::Request, next: axum::middleware::Next| {
                        let e = e.clone();
                        let r = r.clone();
                        async move {
                            let hold = request.headers().contains_key("x-test-hold");
                            let result = next.run(request).await;
                            if hold {
                                e.notify_one();
                                r.notified().await;
                            }
                            result
                        }
                    },
                ))
                .layer(axum::middleware::from_fn(delivery_gate));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let uri = if target == "listing" { "/api/v1/apikey".to_string() } else { format!("/api/v1/index/c/doc/{target}") };
            let key = STANDARD.encode([7; 32]);
            let pending = tokio::spawn(raw(
                address,
                format!(
                    "GET {uri} HTTP/1.1\r\nHost: localhost\r\napikey: {key}\r\nx-test-hold: yes\r\nConnection: close\r\n\r\n"
                ),
            ));
            entered.notified().await;
            let body =
                json!({"operation_id":OperationId::new("delete-source".into()),"record_id":"s","routing_key":"route","expected":{"Match":1},"document":null}).to_string();
            let acknowledgment=raw(address,format!("DELETE /api/v1/index/c/doc HTTP/1.1\r\nHost: localhost\r\napikey: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",headers["apikey"].to_str().unwrap(),body.len())).await;
            assert!(String::from_utf8_lossy(&acknowledgment).starts_with("HTTP/1.1 200"));
            release.notify_one();
            let received = pending.await.unwrap();
            let text = String::from_utf8_lossy(&received);
            assert!(
                !text.contains("secret-original") && !text.contains("not_found") && !text.contains("indexed_doc_count"),
                "protected body escaped source deletion: {text}"
            );
            server.abort();
            let _ = server.await;
            drop(service);
            std::fs::remove_dir_all(path).unwrap();
        }
    }
    #[tokio::test]
    async fn service_source_delete_blocks_body_after_response_header_handoff() {
        use http_body::Body as _;
        let (service, path, headers) = setup().await;
        let source = json!({"operation_id":OperationId::new("source".into()),"record_id":"s","routing_key":"route","expected":"Absent","document":{"body":"secret-source"},"system":{"kind":"source"}});
        write(State(service.clone()), headers.clone(), Path("c".into()), serde_json::to_vec(&source).unwrap().into()).await.unwrap();
        let response = read(State(service.clone()), headers.clone(), Path(("c".into(), "s".into())), Query(ReadParams { fields: String::new() })).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let delete=json!({"operation_id":OperationId::new("delete".into()),"record_id":"s","routing_key":"route","expected":{"Match":1},"document":null});
        write(State(service.clone()), headers, Path("c".into()), serde_json::to_vec(&delete).unwrap().into()).await.unwrap();
        let mut body=response.into_body();
        let frame=std::future::poll_fn(|cx|Pin::new(&mut body).poll_frame(cx)).await;
        assert!(matches!(frame, Some(Err(_))));
        drop(body);drop(service);std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn service_unconsumed_bodies_retain_request_admission() {
        use tower::ServiceExt;
        let (service, path, _) = setup().await;
        let app = router(service.clone());
        let mut responses = Vec::new();
        for _ in 0..64 {
            let response = app
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .uri("/api/v1/live")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            responses.push(response);
        }
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/live")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        drop(response);
        drop(responses);
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/api/v1/live")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        drop(response);
        drop(app);
        drop(service);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[tokio::test]
    async fn service_oversize_and_serialization_errors_retain_output_barrier() {
        struct Unserializable;
        impl Serialize for Unserializable {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("injected"))
            }
        }
        use http_body::Body as _;
        let (service, path, _) = setup().await;
        let (_, permit) = service
            .catalog
            .authorized("a".into(), "c".into(), |_| async {})
            .await
            .unwrap();
        let mut responses = vec![
            protected(Ok("x".repeat(MAX_RESPONSE + 1)), permit.clone()).unwrap(),
            protected(Ok(Unserializable), permit).unwrap(),
        ];
        assert_eq!(responses[0].status(), StatusCode::BAD_REQUEST);
        service
            .catalog
            .execute(CatalogRequest {
                operation_id: OperationId::new("revoke-errors".into()),
                command: CatalogCommand::RevokeKey { key_id: "a".into() },
            })
            .await
            .unwrap();
        for response in responses.drain(..) {
            assert!(response.extensions().get::<OutputPermit>().is_some());
            let mut body = response.into_body();
            let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
            assert!(frame.unwrap().is_err());
        }
        drop(service);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[tokio::test]
    async fn service_takeover_blocks_old_body_and_supports_successive_recovery() {
        let (service, path, headers) = setup().await;
        let doc = json!({"operation_id":OperationId::new("initial".into()),"record_id":"r","routing_key":"r","expected":"Absent","document":{"body":"final acknowledged"}});
        write(State(service.clone()), headers.clone(), Path("c".into()), serde_json::to_vec(&doc).unwrap().into()).await.unwrap();
        let mut origin = "c".to_string();
        for target in ["next", "next-again"] {
            let response = backup(State(service.clone()), headers.clone(), Path(origin.clone()),
                serde_json::to_vec(&json!({"operation_id":OperationId::new(format!("backup-{target}"))})).unwrap().into()).await.unwrap();
            let archive: Value = serde_json::from_slice(&axum::body::to_bytes(response.into_body(), MAX_RESPONSE).await.unwrap()).unwrap();
            let pending = read(State(service.clone()), headers.clone(), Path((origin.clone(), "r".into())), Query(ReadParams { fields: String::new() })).await.unwrap();
            let old_owner = service.records.read().await[&origin].clone();
            let request = json!({"operation_id":OperationId::new(format!("take-{target}")),"source_collection_id":origin,"collection_id":target,"backup_id":archive["backup_id"]});
            takeover::takeover(State(service.clone()), headers.clone(), serde_json::to_vec(&request).unwrap().into()).await.unwrap();
            assert!(axum::body::to_bytes(pending.into_body(), MAX_RESPONSE).await.is_err());
            assert!(old_owner.output_permit().is_err());
            assert!(old_owner.get("r").await.is_err());
            assert!(service.record("a", target).await.is_err());
            let next = service.record("master", target).await.unwrap();
            assert_eq!(next.get("r").await.unwrap().unwrap().version, 1);
            origin = target.into();
        }
        drop(service);
        let service = Service::open(&path, std::num::NonZeroUsize::new(200).unwrap()).await.unwrap();
        assert!(service.record("master", "c").await.is_err());
        assert!(service.record("master", "next").await.is_err());
        assert!(service.record("master", "next-again").await.unwrap().get("r").await.unwrap().is_some());
        drop(service); std::fs::remove_dir_all(path).unwrap();
    }
    #[tokio::test]
    async fn service_internal_ids_cannot_be_reserved_by_clients_or_overflow() {
        let (service, path, headers) = setup().await;
        for (id, nonce) in [
            ("victim".to_string(), "activate:victim"),
            ("x".repeat(256), "long-id"),
        ] {
            let body = json!({"operation_id":OperationId::new(nonce.into()),"collection_id":id,"config":{"index_name":"test","schema":[{"field":"body","field_type":"Text"}]}});
            create_collection(
                State(service.clone()),
                headers.clone(),
                serde_json::to_vec(&body).unwrap().into(),
            )
            .await
            .unwrap();
        }
        let body = json!({"operation_id":OperationId::new("sys:reserved".into()),"collection_id":"reserved","config":{"index_name":"test","schema":[{"field":"body","field_type":"Text"}]}});
        assert_eq!(
            create_collection(
                State(service.clone()),
                headers,
                serde_json::to_vec(&body).unwrap().into()
            )
            .await
            .unwrap_err()
            .0,
            StatusCode::BAD_REQUEST
        );
        drop(service);
        let service = Service::open(&path, std::num::NonZeroUsize::new(200).unwrap())
            .await
            .unwrap();
        assert_eq!(service.records.read().await.len(), 3);
        drop(service);
        std::fs::remove_dir_all(path).unwrap();
    }
}

/// Generate reference for the actually mounted router, never the legacy routes.
pub(crate) fn generate_openapi() {
    use utoipa::OpenApi;
    #[derive(utoipa::OpenApi)]
    #[openapi(components(schemas(CreateIndexRequest, SearchRequestObject)))]
    struct Api;
    let mut doc = serde_json::to_value(Api::openapi()).unwrap();
    doc["openapi"] = json!("3.1.0");
    doc["info"] = json!({"title":"Seekpower Record service (bounded P1-A)","version":crate::VERSION,"description":"Default Record/catalog routes. Writes require stable operation and record identities. Batch writes are explicitly non-atomic; deletion plans require saved per-target operation IDs. Collection restore and full-service recovery into a new stopped root do not restore old grants. See docs/implementation-57.md for acceptance limits."});
    let schemas = doc["components"]["schemas"].as_object_mut().unwrap();
    schemas.insert("OperationId".into(),json!({"type":"object","required":["epoch","nonce"],"properties":{"epoch":{"type":"integer","minimum":0},"nonce":{"type":"string"}}}));
    schemas.insert("RecordWrite".into(),json!({"type":"object","additionalProperties":false,"required":["operation_id","record_id","routing_key","expected","document"],"properties":{"operation_id":{"$ref":"#/components/schemas/OperationId"},"record_id":{"type":"string"},"routing_key":{"type":"string"},"expected":{"oneOf":[{"type":"string","enum":["Absent","Any"]},{"type":"object","required":["Match"],"properties":{"Match":{"type":"integer","minimum":0}},"additionalProperties":false}]},"document":{"type":["object","null"]}}}));
    schemas.get_mut("RecordWrite").unwrap()["properties"]["system"] = json!({"$ref":"#/components/schemas/EvidenceMetadata"});
    schemas.insert("EvidenceRef".into(), json!({"type":"object","additionalProperties":false,"required":["record_id","routing_key","version"],"properties":{"record_id":{"type":"string"},"routing_key":{"type":"string"},"version":{"type":"integer","minimum":1}}}));
    schemas.insert("EvidenceMetadata".into(), json!({"type":"object","additionalProperties":false,"required":["kind"],"properties":{"kind":{"type":"string","enum":["source","derived"]},"evidence_refs":{"type":"array","maxItems":32,"items":{"$ref":"#/components/schemas/EvidenceRef"}}}}));
    schemas.insert("CollectionCreate".into(),json!({"type":"object","additionalProperties":false,"required":["operation_id","collection_id","config"],"properties":{"operation_id":{"$ref":"#/components/schemas/OperationId"},"collection_id":{"type":"string"},"config":{"$ref":"#/components/schemas/CreateIndexRequest"},"embedding_space_id":{"type":["string","null"]}}}));
    let mut paths = serde_json::Map::new();
    for (path, method, summary, schema) in [
        ("/api/v1/live", "get", "Server version", None),
        ("/api/v1/index/{id}/doc/{record_id}/status", "get", "Content-free visibility reason and exact current version; stale bodies remain hidden", None),
        ("/api/v1/apikey", "get", "List granted collections", None),
        (
            "/api/v1/apikey",
            "post",
            "Master: register a client-generated 32-byte API key",
            Some(json!({"type":"object","required":["operation_id","key_id","apikey_base64"]})),
        ),
        (
            "/api/v1/apikey",
            "delete",
            "Master: revoke a key without deleting data",
            Some(json!({"type":"object","required":["operation_id","key_id"]})),
        ),
        (
            "/api/v1/index",
            "post",
            "Create an independent collection idempotently",
            Some(json!({"$ref":"#/components/schemas/CollectionCreate"})),
        ),
        (
            "/api/v1/index/{id}",
            "get",
            "Collection metadata and active statistics",
            None,
        ),
        (
            "/api/v1/index/{id}",
            "patch",
            "Online full checkpoint and WAL retirement",
            None,
        ),
        (
            "/api/v1/index/{id}/grants",
            "post",
            "Master: grant/revoke unified access",
            Some(json!({"type":"object","required":["operation_id","key_id","allow"]})),
        ),
        (
            "/api/v1/index/{id}/doc",
            "post",
            "Durable conditional replacement or deletion",
            Some(json!({"$ref":"#/components/schemas/RecordWrite"})),
        ),
        (
            "/api/v1/index/{id}/doc",
            "patch",
            "Durable conditional replacement or deletion",
            Some(json!({"$ref":"#/components/schemas/RecordWrite"})),
        ),
        (
            "/api/v1/index/{id}/doc",
            "delete",
            "Durable conditional deletion (document:null)",
            Some(json!({"$ref":"#/components/schemas/RecordWrite"})),
        ),
        (
            "/api/v1/index/{id}/doc/{record_id}",
            "get",
            "Read original record envelope",
            None,
        ),
        (
            "/api/v1/index/{id}/query",
            "post",
            "Retrieve records and statistics from one view",
            Some(json!({"$ref":"#/components/schemas/SearchRequestObject"})),
        ),
        (
            "/api/v1/index/{id}/operation/{epoch}/{nonce}",
            "get",
            "Authorized data operation status",
            None,
        ),
        (
            "/api/v1/catalog/operation/{epoch}/{nonce}",
            "get",
            "Master: control-plane operation status",
            None,
        ),
        (
            "/api/v1/index/{id}/iterator",
            "post",
            "Stable identity page; cursor conflicts if view changed",
            Some(
                json!({"type":"object","properties":{"cursor":{"type":"object"},"length":{"type":"integer","minimum":1},"fields":{"type":"array","items":{"type":"string"}}}}),
            ),
        ),
        (
            "/api/v1/index/{id}/batch",
            "post",
            "1..100 independent writes; per-item outcomes; no batch atomicity",
            Some(
                json!({"type":"object","required":["writes"],"properties":{"writes":{"type":"array","minItems":1,"maxItems":100,"items":{"$ref":"#/components/schemas/RecordWrite"}}}}),
            ),
        ),
        (
            "/api/v1/index/{id}/delete-plan",
            "post",
            "Read-only all/ids/query deletion target selection; save plan and assign original operation IDs before batch execution",
            Some(
                json!({"type":"object","required":["select"],"properties":{"select":{"enum":["all","ids","query"]},"record_ids":{"type":"array","items":{"type":"string"}},"query":{"$ref":"#/components/schemas/SearchRequestObject"}}}),
            ),
        ),
        (
            "/api/v1/index/{id}/backup",
            "post",
            "Independent local collection checkpoint; retry same operation for same snapshot",
            Some(
                json!({"type":"object","required":["operation_id"],"properties":{"operation_id":{"$ref":"#/components/schemas/OperationId"}}}),
            ),
        ),
        (
            "/api/v1/backups",
            "get",
            "Master: list local backup identities and durable retirement state",
            None,
        ),
        (
            "/api/v1/backups/{id}",
            "delete",
            "Master: durably retire backup and reclaim files; unfinished restore pins reject retirement",
            Some(
                json!({"type":"object","required":["operation_id"],"properties":{"operation_id":{"$ref":"#/components/schemas/OperationId"}}}),
            ),
        ),
        (
            "/api/v1/takeovers", "post",
            "Master: seal current source and recover to its final waterline in a new collection; retry the same operation",
            Some(json!({"type":"object","additionalProperties":false,"required":["operation_id","source_collection_id","collection_id","backup_id"],"properties":{"operation_id":{"$ref":"#/components/schemas/OperationId"},"source_collection_id":{"type":"string"},"collection_id":{"type":"string"},"backup_id":{"type":"string"}}})),
        ),
        (
            "/api/v1/restore",
            "post",
            "Master: restore local backup into new collection; grants are not restored",
            Some(
                json!({"type":"object","required":["operation_id","backup_id","collection_id"],"properties":{"operation_id":{"$ref":"#/components/schemas/OperationId"},"backup_id":{"type":"string"},"collection_id":{"type":"string"}}}),
            ),
        ),
        (
            "/api/v1/index/{id}",
            "delete",
            "Master: permanently retire collection identity and revoke all grants; physical reclamation deferred",
            Some(
                json!({"type":"object","required":["operation_id"],"properties":{"operation_id":{"$ref":"#/components/schemas/OperationId"}}}),
            ),
        ),
        (
            "/api/v1/service-backups",
            "post",
            "Master: complete consistent local service snapshot",
            Some(
                json!({"type":"object","required":["operation_id"],"properties":{"operation_id":{"$ref":"#/components/schemas/OperationId"}}}),
            ),
        ),
        (
            "/api/v1/service-restores",
            "post",
            "Master: restore into new stopped root; historical authorization never revived",
            Some(
                json!({"type":"object","required":["operation_id","backup_id"],"properties":{"operation_id":{"$ref":"#/components/schemas/OperationId"},"backup_id":{"type":"string"}}}),
            ),
        ),
    ] {
        let mut operation = json!({"summary":summary,"responses":{"200":{"description":"Success; JSON response"},"default":{"description":"JSON code/message error; 409 conflict, 410 expired, 429 busy (admission before operation lookup; earlier attempts remain unresolved), 503 durable_pending/outcome_unknown/recovery_required"}}});
        let mut params = vec![
            json!({"name":"apikey","in":"header","required":path!="/api/v1/live","schema":{"type":"string"}}),
        ];
        for part in path.split('/') {
            if let Some(name) = part.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
                params.push(json!({"name":name,"in":"path","required":true,"schema":{"type":if name=="epoch"{"integer"}else{"string"}}}));
            }
        }
        operation["parameters"] = json!(params);
        if let Some(schema) = schema {
            operation["requestBody"] =
                json!({"required":true,"content":{"application/json":{"schema":schema}}});
        }
        paths.entry(path).or_insert(json!({}))[method] = operation;
    }
    doc["paths"] = Value::Object(paths);
    let text = serde_json::to_string_pretty(&doc).unwrap();
    println!("{text}");
    let parent = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    std::fs::write(parent.join("openapi.json"), &text).unwrap();
    // JSON is valid YAML 1.2; keep both existing output filenames consistent.
    std::fs::write(parent.join("openapi.yml"), text).unwrap();
}
