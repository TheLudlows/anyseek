//! Same-service recovery orchestration. No cross-root or forced activation path.
use super::*;
use anyseek::record::SealRequest;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TakeoverRequest {
    operation_id: OperationId,
    source_collection_id: String,
    collection_id: String,
    backup_id: String,
}
fn fault(stage: &str) {
    #[cfg(feature = "fault-injection")]
    if std::env::var("ANYSEEK_TAKEOVER_FAULT").as_deref() == Ok(stage) {
        std::process::exit(89);
    }
    let _ = stage;
}
impl Service {
    pub(super) async fn finish_takeover(&self, entry: CollectionEntry) -> Result<(), Error> {
        let intent = entry.takeover.as_ref().ok_or_else(|| storage("missing takeover intent"))?;
        if intent.completed.is_some() || entry.active || entry.deleted {
            return Err(storage("invalid pending takeover lifecycle"));
        }
        let source_entry = self.catalog.collections().await?.into_iter()
            .find(|c| c.collection_id == intent.source_collection_id)
            .ok_or_else(|| storage("takeover source absent"))?;
        if source_entry.deleted || source_entry.active
            || source_entry.takeover_target.as_ref() != Some(&entry.collection_id) {
            return Err(storage("takeover source is not exclusively reserved"));
        }
        let source = match self.records.write().await.remove(&source_entry.collection_id) {
            Some(p) => p,
            None => RecordPartition::open(&self.root.join(&source_entry.directory), source_entry.config.max_journal_bytes).await?,
        };
        let identity = source.recovery_identity().await?;
        if identity.lineage != intent.lineage || identity.independent_snapshot || identity.quarantined {
            return Err(storage("takeover needs current live deletion authority; archive cannot substitute"));
        }
        let seal = source.seal_for_takeover(intent.binding.clone()).await?;
        fault("sealed");
        let target = self.root.join(&entry.directory);
        RecordPartition::resume_initialization(&target, entry.config.max_journal_bytes).await?;
        if !target.exists() {
            let backup_id = entry.restore_from.as_ref().ok_or_else(|| storage("takeover backup missing"))?;
            let backup = RecordPartition::open(&self.root.join(format!("backup-{backup_id}")), entry.config.max_journal_bytes).await?;
            source.create_takeover_successor(&backup, &target, intent.binding.clone()).await?;
        }
        fault("target_created");
        // Opening drains all index construction before certification/activation.
        let successor = RecordPartition::open(&target, entry.config.max_journal_bytes).await?;
        successor.certify_takeover_successor(&seal).await?;
        fault("certified");
        self.catalog.execute(CatalogRequest {
            operation_id: internal_id(format!("sys:takeover:{}", entry.directory)),
            command: CatalogCommand::FinalizeTakeover { collection_id: entry.collection_id.clone(), certificate: seal },
        }).await?;
        fault("finalized");
        self.records.write().await.insert(entry.collection_id, successor);
        Ok(())
    }
}

pub(super) async fn takeover(
    State(s): State<Arc<Service>>, headers: HeaderMap, bytes: Bytes,
) -> Result<Response, Error> {
    let request: TakeoverRequest = parse(&bytes)?;
    control_id(&request.operation_id)?;
    validate_backup_id(&request.backup_id)?;
    let admission = s.admit_control(&headers, true).await?;
    owned(async move {
        let _admission = admission;
        let _control = s.control.lock().await;
        let key = s.admin(&headers).await?;
        let existing = s.catalog.collections().await?.into_iter().find(|c| c.collection_id == request.collection_id);
        let entry = if let Some(entry) = existing {
            let intent = entry.takeover.as_ref().ok_or_else(|| bad("target identity retained by another operation"))?;
            if intent.operation_id != request.operation_id || intent.source_collection_id != request.source_collection_id
                || entry.restore_from.as_ref() != Some(&request.backup_id) || entry.owner_key != key || entry.deleted {
                return Err(bad("takeover identity reused or retired"));
            }
            entry
        } else {
            if s.catalog.retired_backups().await?.contains(&request.backup_id) { return Err(bad("backup retired")); }
            let source = s.record(&key, &request.source_collection_id).await?;
            let identity = source.recovery_identity().await?;
            if identity.independent_snapshot || identity.quarantined || identity.seal.is_some() {
                return Err(bad("source is not current live deletion authority"));
            }
            let backup = RecordPartition::open(&s.root.join(format!("backup-{}",request.backup_id)),256*1024*1024).await?;
            let archive = backup.recovery_identity().await?;
            if !archive.independent_snapshot || archive.lineage != identity.lineage || archive.seq > identity.seq {
                return Err(bad("backup does not belong to current source lineage"));
            }
            let binding = SealRequest {
                intent_id: verifier(&serde_json::to_vec(&("takeover", &request.operation_id, &request.source_collection_id,
                    &request.collection_id, &request.backup_id, &archive.digest)).map_err(bad)?),
                target_collection_id: request.collection_id.clone(), backup_digest: archive.digest,
            };
            let config = s.catalog.collections().await?.into_iter()
                .find(|c| c.collection_id == request.source_collection_id)
                .ok_or_else(|| storage("source intent absent"))?.config;
            drop(backup);
            // Catalog serializes this closure with entire authorized writes and
            // removes old grants before Record sealing begins. No reverse lock.
            s.catalog.prepare_takeover(CatalogRequest { operation_id: request.operation_id.clone(),
                command: CatalogCommand::PrepareTakeover {
                    collection_id: request.collection_id.clone(), source_collection_id: request.source_collection_id.clone(),
                    owner_key: key.clone(), config, backup_id: request.backup_id.clone(), lineage: identity.lineage, binding,
                },
            }, source).await?;
            fault("prepared");
            s.catalog.collections().await?.into_iter().find(|c| c.collection_id == request.collection_id)
                .ok_or_else(|| storage("takeover intent disappeared"))?
        };
        if !entry.active { s.finish_takeover(entry).await?; }
        let complete = s.catalog.collections().await?.into_iter().find(|c| c.collection_id == request.collection_id)
            .ok_or_else(|| storage("takeover target disappeared"))?;
        response(json!({"collection_id":request.collection_id,"source_collection_id":request.source_collection_id,
            "operation_id":request.operation_id,"status":"active","recovery":"final_sealed_waterline",
            "certificate":complete.takeover.and_then(|t|t.completed),"old_grants_restored":false}),s.permit(&key).await?)
    }).await
}
