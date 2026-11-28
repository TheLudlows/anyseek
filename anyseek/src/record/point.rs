//! Exact Record reads use the certified directory and its committed overrides.
use super::*;
use state_values::{CurrentRow, RecordLookup, RecordSource};

pub(super) struct PointView {
    records: RecordLookup,
    clock: Arc<dyn temporal::Clock>,
    observed: temporal::ObservedTime,
    floor: u64,
    cut: u64,
    owner: Arc<std::fs::File>,
}

impl PointView {
    fn evaluation(&self) -> Result<u64, RecordError> {
        Ok(self
            .observed
            .advance(temporal::checked_time(self.clock.as_ref(), self.floor)?))
    }
}

impl Inner {
    // Called with Inner at a complete publication boundary. Private replay and
    // checkpoint candidates have their own recent map and are never selected here.
    pub(super) fn publish_point_view(&self, fenced: Option<bool>) -> Result<(), RecordError> {
        let view = PointView {
            records: self.state.records.lookup(),
            clock: self.clock.clone(),
            observed: self.observed_time.clone(),
            floor: self.state.time_watermark_ms,
            cut: self.state.seq,
            owner: self.journal.owner_guard(),
        };
        let mut output = self
            .output
            .write()
            .map_err(|_| RecordError::RecoveryRequired)?;
        output.point = Some(view);
        output.point_pending = false;
        if let Some(fenced) = fenced {
            output.fenced = fenced;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn set_clock(&mut self, clock: Arc<dyn temporal::Clock>) {
        self.clock = clock.clone();
        self.output.write().unwrap().point.as_mut().unwrap().clock = clock;
    }
}

struct Captured {
    source: Option<RecordSource>,
    evaluation: u64,
    cut: u64,
    owner: Arc<std::fs::File>,
    output: DataOutputPermit,
}

impl RecordPartition {
    async fn capture_point(&self, id: &str) -> Result<Captured, RecordError> {
        let capture = || {
            let state = self
                .output
                .read()
                .map_err(|_| RecordError::RecoveryRequired)?;
            if state.fenced || state.point_pending {
                return Ok(None);
            }
            let view = state.point.as_ref().ok_or(RecordError::RecoveryRequired)?;
            Ok::<_, RecordError>(Some(Captured {
                source: view.records.source(id)?,
                evaluation: view.evaluation()?,
                cut: view.cut,
                owner: view.owner.clone(),
                output: DataOutputPermit::new(self.output.clone(), state.revision)
                    .for_point(Arc::downgrade(&self.inner)),
            }))
        };
        if let Some(view) = capture()? {
            return Ok(view);
        }
        // A real publication may be in progress. Wait for its existing
        // failure fence to settle, as transport does for other record data.
        let _publication = self.inner.lock().await;
        capture()?.ok_or(RecordError::RecoveryRequired)
    }

    /// Read the exact live record and keep the failure fence for its delivery.
    #[doc(hidden)]
    pub(crate) async fn get_with_output(
        &self,
        id: &str,
    ) -> Result<(Option<Record>, DataOutputPermit), RecordError> {
        let _admission = self
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| RecordError::Busy)?;
        let Captured {
            source,
            evaluation,
            cut,
            owner,
            output,
        } = self.capture_point(id).await?;
        let record = match source {
            None => None,
            Some(RecordSource::Recent(row)) => {
                temporal::visible(&row, evaluation).then(|| (*row).clone())
            }
            Some(source @ RecordSource::Sealed(_)) => {
                let key_memory = crate::storage::build_budget::reserve_memory(id.len() as u64)
                    .map_err(memory_error)?;
                let key = id.to_owned();
                let (source, record) = crate::resources::spawn_blocking(move || {
                    let (_owner, _key_memory) = (owner, key_memory);
                    let record = match source.find(&key)? {
                        Some(CurrentRow::Sealed(row)) if row.metadata.visible(evaluation) => {
                            state_rows::decode(&key, row, |record| Ok(Some(record)))?
                        }
                        _ => None,
                    };
                    Ok::<_, RecordError>((source, record))
                })
                .await
                .map_err(storage)??;
                let current = self.capture_point(id).await?;
                if current.cut != cut
                    && !current
                        .source
                        .is_some_and(|current| source.same_source(&current))
                {
                    return Err(RecordError::Busy);
                }
                record.filter(|row| temporal::visible(row, current.evaluation))
            }
        };
        output.deliver_after_publication(|| ()).await?;
        Ok((record, output))
    }
}
