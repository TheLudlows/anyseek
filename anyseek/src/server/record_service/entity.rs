//! Entity transport and the generation-scoped read shared with the local SDK.
use super::*;
use crate::{
    catalog::CatalogFrameGeneration,
    record::{EntityCandidatePaths, EntityDirection, EntityFrameOutput},
};
use tokio::time::{Instant, timeout_at};

pub(super) const QUERY_PATH: &str = "/api/v1/index/{id}/entity/query";
pub(super) const CHECKPOINT_PATH: &str = "/api/v1/entity-checkpoints";

#[derive(Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct EntityPathRequest {
    direction: EntityDirection,
    /// Namespaced entity ID, 1 to 256 UTF-8 bytes.
    anchor: String,
    /// Edge type, 1 to 128 UTF-8 bytes.
    kind: String,
    #[schema(minimum = 1, maximum = 256)]
    visit_limit: usize,
}

#[derive(Deserialize, utoipa::ToSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct EntityCheckpointRequest {
    tenant_id: String,
    collection_id: String,
}

pub(super) struct Output {
    record: EntityFrameOutput,
    catalog: CatalogFrameGeneration,
    _residency: ResidencyPin,
    deadline: Instant,
}

impl Output {
    pub(super) fn deliver<T>(&self, handoff: impl FnOnce() -> T) -> Result<T, Error> {
        let record = self.record.try_pin()?.ok_or(RecordError::Busy)?;
        let _catalog = self.catalog.try_pin()?.ok_or(CatalogError::Busy)?;
        Ok(record.deliver(handoff)?)
    }

    pub(super) async fn deliver_after_publication<T>(
        &self,
        handoff: impl FnOnce() -> T,
    ) -> Result<T, Error> {
        loop {
            let record = self.record.pin().await?;
            if let Some(_catalog) = self.catalog.try_pin()? {
                return Ok(record.deliver(handoff)?);
            }
            // No Record or Catalog lock is kept while waiting for the other.
            drop(record);
            if Instant::now() >= self.deadline {
                return Err(RecordError::Busy.into());
            }
            timeout_at(self.deadline, self.catalog.wait_active())
                .await
                .map_err(|_| RecordError::Busy)??;
        }
    }
}

pub(super) async fn query_collection(
    service: &Arc<Service>,
    tenant: String,
    id: String,
    direction: EntityDirection,
    keys: (&str, &str),
    visit_limit: usize,
    deadline: Instant,
) -> Result<(EntityCandidatePaths, Output), Error> {
    if Instant::now() >= deadline {
        return Err(RecordError::Busy.into());
    }
    let partition = service.record(&tenant, &id).await?;
    let generation = partition.generation;
    let for_read = partition.clone();
    let (anchor, kind) = keys;
    // Validate borrowed keys before copying. Report input errors only inside
    // the authorization guard so a retired generation stays hidden.
    let keys = RecordPartition::validate_entity_path_keys(anchor, kind)
        .map(|()| (anchor.to_owned(), kind.to_owned()));
    let found = service
        .catalog
        .authorized_generation(
            tenant.clone(),
            id.clone(),
            generation,
            move |_| async move {
                let (anchor, kind) = keys?;
                for_read
                    .prepare_entity_paths(direction, &anchor, &kind, visit_limit, deadline)
                    .await
            },
        )
        .await??;
    #[cfg(test)]
    service.cold.test.entity_handoff.wait().await;
    let catalog = service.catalog.frame_generation(&tenant, &id, generation)?;
    let residency = partition.residency.clone();
    let (paths, record) = service
        .catalog
        .authorized_generation(tenant, id, generation, move |_| async move {
            found.deliver(&partition).await
        })
        .await??;
    Ok((
        paths,
        Output {
            record,
            catalog,
            _residency: residency,
            deadline,
        },
    ))
}

pub(super) async fn query(
    State(service): State<Arc<Service>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    bytes: Bytes,
) -> Result<Response, Error> {
    let deadline = REQUEST_DEADLINE.with(|deadline| *deadline);
    if Instant::now() >= deadline {
        return Err(RecordError::Busy.into());
    }
    let tenant = service.authenticate(&headers).await?;
    let request: EntityPathRequest = parse(&bytes)?;
    let (paths, output) = query_collection(
        &service,
        tenant,
        id,
        request.direction,
        (&request.anchor, &request.kind),
        request.visit_limit,
        deadline,
    )
    .await?;
    encode_data_response(Ok(paths), Some(TransportData::Entity(Arc::new(output))))
}

pub(super) async fn checkpoint(
    State(service): State<Arc<Service>>,
    Extension(admission): Extension<ProjectionAdmission>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<Response, Error> {
    service.admin(&headers).await?;
    let request: EntityCheckpointRequest = parse(&bytes)?;
    let deadline = REQUEST_DEADLINE.with(|deadline| *deadline);
    let (seq, pin) = build_projection_collection(
        &service,
        request.tenant_id,
        request.collection_id,
        deadline,
        admission,
        RecordPartition::start_entity_checkpoint,
    )
    .await?;
    protected_data(
        Ok(json!({"indexed_seq":seq})),
        Some(DataOutputPermit::residency_only(pin)),
    )
}

#[cfg(test)]
#[path = "entity_http_tests.rs"]
mod tests;
