//! Direct local access to the same catalog and collection engine used by the server.
pub use super::QueryResponse;
use super::*;
use anyseek::resources::Lane;
use std::num::NonZeroUsize;

/// Safe classification of failures from the embedded engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalErrorKind {
    /// The tenant does not have this collection.
    NotFound,
    /// The input is invalid.
    Rejected,
    /// The operation conflicts with existing state.
    Conflict,
    /// The same operation identity is outside its retry window.
    Expired,
    /// Admission, storage, or shutdown currently prevents the operation.
    Unavailable,
}

/// A content-free local engine error.
#[derive(Debug)]
pub struct LocalError {
    kind: LocalErrorKind,
    code: &'static str,
}

impl LocalError {
    /// Machine-readable failure category.
    pub fn kind(&self) -> LocalErrorKind {
        self.kind
    }
    /// Static AnySeek error code.
    pub fn code(&self) -> &'static str {
        self.code
    }
    fn unavailable(code: &'static str) -> Self {
        Self {
            kind: LocalErrorKind::Unavailable,
            code,
        }
    }
}
impl std::fmt::Display for LocalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.code)
    }
}
impl std::error::Error for LocalError {}
impl From<Error> for LocalError {
    fn from(error: Error) -> Self {
        let kind = match error.0 {
            StatusCode::NOT_FOUND | StatusCode::FORBIDDEN => LocalErrorKind::NotFound,
            StatusCode::BAD_REQUEST => LocalErrorKind::Rejected,
            StatusCode::CONFLICT => LocalErrorKind::Conflict,
            StatusCode::GONE => LocalErrorKind::Expired,
            _ => LocalErrorKind::Unavailable,
        };
        Self {
            kind,
            code: error.1,
        }
    }
}

/// Exclusive owner of an AnySeek catalog backed by local disk. The caller
/// authorizes access to tenant and collection names before calling the SDK.
/// Hold one owner per directory and await shutdown before reopening it.
pub struct LocalEngine {
    state: std::sync::Mutex<Owner>,
}

enum Owner {
    Open(Arc<Service>),
    Closing(shutdown::Shutdown),
}

impl LocalEngine {
    /// Recover existing local collections, or create a catalog if allowed.
    /// The parent directory must exist. This opens a disk owner without a socket.
    pub async fn open(root: &FsPath, allow_create: bool) -> Result<Self, LocalError> {
        let open_error = |_| LocalError::unavailable("open_failed");
        let root = std::path::absolute(root).map_err(open_error)?;
        let root = match tokio::fs::canonicalize(&root).await {
            Ok(path) => path,
            Err(error) if allow_create && error.kind() == std::io::ErrorKind::NotFound => {
                let name = root
                    .file_name()
                    .ok_or_else(|| LocalError::unavailable("open_failed"))?;
                let parent = tokio::fs::canonicalize(root.parent().unwrap())
                    .await
                    .map_err(open_error)?;
                parent.join(name)
            }
            Err(error) => return Err(open_error(error)),
        };
        let nprobe = NonZeroUsize::new(super::super::api_types::DEFAULT_NPROBE).unwrap();
        let service = Service::open(&root, nprobe, None, allow_create)
            .await
            .map_err(|_| LocalError::unavailable("open_failed"))?;
        Service::start_reclamation(&service);
        Ok(Self {
            state: std::sync::Mutex::new(Owner::Open(service)),
        })
    }

    /// Report the engine version for a running catalog.
    pub fn version(&self) -> Result<&'static str, LocalError> {
        self.service()?;
        Ok(crate::VERSION)
    }

    fn service(&self) -> Result<Arc<Service>, LocalError> {
        let owner = self
            .state
            .lock()
            .map_err(|_| LocalError::unavailable("recovery_required"))?;
        match &*owner {
            Owner::Open(service) => Ok(service.clone()),
            Owner::Closing(_) => Err(LocalError::unavailable("shutting_down")),
        }
    }

    async fn run<T>(
        &self,
        tenant: &str,
        collection: &str,
        lane: Lane,
        operation: impl AsyncFnOnce(Arc<Service>, String, String) -> Result<T, Error>,
    ) -> Result<T, LocalError> {
        anyseek::catalog::validate_tenant(tenant).map_err(Error::from)?;
        anyseek::catalog::validate_name(collection).map_err(Error::from)?;
        let service = self.service()?;
        let activity = service
            .lifecycle
            .enter()
            .ok_or_else(|| LocalError::unavailable("shutting_down"))?;
        let scope = service
            .resources
            .admit(
                format!("collection:{tenant}/{collection}"),
                lane,
                (MAX_BODY + MAX_RESPONSE) as u64,
            )
            .map_err(|_| LocalError::unavailable("busy"))?;
        Ok(activity
            .run(scope.run(operation(service, tenant.to_owned(), collection.to_owned())))
            .await?)
    }

    /// Create a collection using a stable operation identity.
    pub async fn create_collection(
        &self,
        tenant: &str,
        collection: &str,
        operation_id: OperationId,
        config: CreateIndexRequest,
        embedding_space_id: Option<String>,
    ) -> Result<(), LocalError> {
        control_id(&operation_id)?;
        self.run(
            tenant,
            collection,
            Lane::Recovery,
            async move |service, tenant, collection| {
                owned(async move {
                    let _control = service.control.lock().await;
                    let _writes = service.write_gate.write().await;
                    create_collection_locked(
                        &service,
                        tenant,
                        CollectionCreate {
                            operation_id,
                            collection_id: collection,
                            config,
                            embedding_space_id,
                        },
                    )
                    .await?;
                    Ok(())
                })
                .await
            },
        )
        .await
    }

    /// Read the admitted schema, limits, and analysis identity for a collection.
    pub async fn collection_info(
        &self,
        tenant: &str,
        collection: &str,
    ) -> Result<Value, LocalError> {
        self.run(
            tenant,
            collection,
            Lane::Foreground,
            async |s, tenant, id| {
                let p = s.record(&tenant, &id).await?;
                let data = p.output_permit()?;
                let result = s
                    .catalog
                    .authorized_generation(tenant, id, p.generation, async move |entry| {
                        p.statistics()
                            .await
                            .and_then(|stats| index_info(&entry, stats))
                    })
                    .await??;
                Ok(data.deliver(|| result)?)
            },
        )
        .await
    }

    /// Read a live record by its exact collection identity.
    pub async fn get(
        &self,
        tenant: &str,
        collection: &str,
        record: &str,
    ) -> Result<Option<anyseek::record::Record>, LocalError> {
        let record = record.to_owned();
        self.run(tenant, collection, Lane::Foreground, async move |service, tenant, collection| {
            let (record, permit) = read_collection(&service, tenant, collection, record).await?;
            Ok(permit.deliver_after_publication(|| record).await?)
        }).await
    }

    /// Query a collection in process and return the admitted record view.
    pub async fn query(
        &self,
        tenant: &str,
        collection: &str,
        query: SearchRequestObject,
    ) -> Result<QueryResponse, LocalError> {
        self.run(
            tenant,
            collection,
            Lane::Foreground,
            async move |service, tenant, collection| {
                let (result, data) =
                    query_collection(&service, tenant, collection, query, diagnostics::new_id())
                        .await?;
                Ok(data.deliver_after_publication(|| result).await??)
            },
        )
        .await
    }

    /// Read the current version of a publication group if present.
    pub async fn group_version(
        &self,
        tenant: &str,
        collection: &str,
        group: &str,
    ) -> Result<Option<u64>, LocalError> {
        let group = group.to_owned();
        self.run(
            tenant,
            collection,
            Lane::Foreground,
            async move |s, tenant, id| {
                let p = s.record(&tenant, &id).await?;
                let data = p.output_permit()?;
                let result = s
                    .catalog
                    .authorized_generation(tenant, id, p.generation, async move |_| {
                        p.group_version(&group).await
                    })
                    .await??;
                Ok(data.deliver(|| result)?)
            },
        )
        .await
    }

    /// Atomically replace a group, preserving accepted writes across waiter cancellation.
    pub async fn replace_group(
        &self,
        tenant: &str,
        collection: &str,
        request: GroupReplace,
    ) -> Result<anyseek::record::GroupReceipt, LocalError> {
        self.run(
            tenant,
            collection,
            Lane::Foreground,
            async move |service, tenant, collection| {
                owned(async move {
                    replace_collection_group(&service, tenant, collection, request).await
                })
                .await
            },
        )
        .await
    }

    /// Stop admitting calls, drain accepted work, and close the catalog. A
    /// cancelled wait can be retried without restarting the shutdown.
    pub async fn shutdown(&self) -> Result<(), LocalError> {
        let mut handle = {
            let mut owner = self
                .state
                .lock()
                .map_err(|_| LocalError::unavailable("recovery_required"))?;
            match &*owner {
                Owner::Closing(handle) => handle.clone(),
                Owner::Open(service) => {
                    let handle = service.clone().shutdown();
                    *owner = Owner::Closing(handle.clone());
                    handle
                }
            }
        };
        handle
            .wait()
            .await
            .map_err(|_| LocalError::unavailable("shutdown_failed"))
    }
}
