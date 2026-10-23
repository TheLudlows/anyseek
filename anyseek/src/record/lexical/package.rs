//! Native implementation of the production immutable-package boundary. No
//! package selection or second head is performed here; the authority owns that.
use super::*;
use crate::record::{Document, RecordConfig};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativePackage {
    pub(crate) closure: segment::Package,
    cut: Cut,
    analysis: [u8; 32],
}
fn fingerprint(config: &RecordConfig) -> Result<[u8; 32], RecordError> {
    Ok(
        Sha256::digest(serde_json::to_vec(&(&config.index, &config.schema)).map_err(storage)?)
            .into(),
    )
}
impl NativePackage {
    pub(crate) async fn prepare(
        root: &Path,
        config: &RecordConfig,
        cut: Cut,
        records: Vec<(Identity, Document)>,
        limit: u64,
    ) -> Result<Self, RecordError> {
        if records.len() > config.max_records || records.len() > MAX_DOCUMENTS {
            return Err(RecordError::Rejected(
                "native package record budget exceeded".into(),
            ));
        }
        let analysis = fingerprint(config)?;
        let analyzer = super::query::Analyzer::new(config.index.clone());
        let fields: Vec<_> = config.schema.iter().filter(|f| f.index_lexical).collect();
        let names: Vec<String> = fields.iter().map(|f| f.field.clone()).collect();
        schema(&names)?;
        let mut inputs = Vec::new();
        let estimate = records.iter().try_fold(0u64, |sum, (_, doc)| {
            sum.checked_add(crate::record::build_budget::document_estimate(
                doc,
                &config.schema,
            )?)
            .ok_or_else(|| storage("native analysis estimate overflow"))
        })?;
        let memory = crate::storage::build_budget::reserve_memory(estimate).map_err(storage)?;
        let scope = crate::resources::Scope::current();
        let buffer = scope
            .as_ref()
            .map(|s| s.bytes(estimate))
            .transpose()
            .map_err(storage)?;
        for (identity, document) in records {
            let mut analyzed = Vec::new();
            for field in &fields {
                let text = match document.get(&field.field) {
                    None => String::new(),
                    Some(value) => match field.field_type {
                        crate::index::FieldType::Json if value.is_object() => {
                            let mut strings = Vec::new();
                            crate::index::object_values_to_string_vec_recursive(
                                value,
                                &mut strings,
                            );
                            strings.join(" ")
                        }
                        crate::index::FieldType::Text
                        | crate::index::FieldType::Json
                        | crate::index::FieldType::String16
                        | crate::index::FieldType::String32 => value
                            .as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(|| value.to_string()),
                        _ => value.to_string(),
                    },
                };
                analyzed.push(analyzer.field(&text).await);
            }
            inputs.push(AnalyzedInput {
                identity,
                fields: analyzed,
            });
        }
        let root = root.to_owned();
        let handle = tokio::runtime::Handle::current();
        // Cancellation of the awaiting request does not cancel or disown the
        // synchronous native writer. Its scope and leases live in this worker.
        tokio::task::spawn_blocking(move || {
            let work = async move {
                let (_memory, _buffer) = (memory, buffer);
                let _disk = crate::storage::build_budget::ensure_scope(&root, limit).map_err(storage)?;
                let id = format!("{:032x}", rand::random::<u128>());
                let prepared = root.join(format!(".segment-prepare-{id}"));
                // The worker exclusively owns this randomly named candidate.
                // Errors remove only its private directory, after native drain.
                struct Private(std::path::PathBuf);
                impl Drop for Private {
                    fn drop(&mut self) {
                        if self.0.exists() {
                            let _ = std::fs::remove_dir_all(&self.0);
                        }
                    }
                }
                let _private = Private(prepared.clone());
                Candidate::build_analyzed(&prepared, cut, names, inputs, limit)?;
                segment::write_json(&prepared.join("analysis.json"), &analysis)?;
                let closure = segment::Package::seal(&root, &prepared, id, limit)?;
                Ok(Self {
                    closure,
                    cut,
                    analysis,
                })
            };
            handle.block_on(async move {
                match scope {
                    Some(scope) => scope.run(work).await,
                    None => work.await,
                }
            })
        })
        .await
        .map_err(storage)?
    }
    pub(crate) fn load(
        &self,
        root: &Path,
        config: &RecordConfig,
        limit: u64,
    ) -> Result<Snapshot, RecordError> {
        if self.analysis != fingerprint(config)? {
            return Err(storage("native analysis configuration mismatch"));
        }
        let path = self.closure.verify(root, limit)?;
        let stored: [u8; 32] =
            serde_json::from_slice(&segment::read_bounded(&path.join("analysis.json"), 1024)?)
                .map_err(storage)?;
        if stored != self.analysis {
            return Err(storage("native analysis certificate mismatch"));
        }
        Candidate {
            files: self.closure.files.clone(),
        }
        .open(&path, self.cut, limit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn sealed_native_package_reopens_from_selected_descriptor_and_rejects_configuration_drift()
     {
        let root = super::super::tests::Temp::new();
        std::fs::create_dir(&root.0).unwrap();
        let config = crate::record::tests::config();
        let cut = Cut {
            sequence: 7,
            evaluation_time_ms: 100,
        };
        let identity = Identity {
            id: "a".into(),
            version: 2,
            revision: 9,
        };
        let package = NativePackage::prepare(
            &root.0,
            &config,
            cut,
            vec![(
                identity.clone(),
                serde_json::from_value(serde_json::json!({"body":"alpha beta"})).unwrap(),
            )],
            32 * 1024 * 1024,
        )
        .await
        .unwrap();
        let reference: NativePackage =
            serde_json::from_slice(&serde_json::to_vec(&package).unwrap()).unwrap();
        let snapshot = reference.load(&root.0, &config, 32 * 1024 * 1024).unwrap();
        assert_eq!(&*snapshot.identities, &[identity]);
        assert_eq!(snapshot.cut, cut);
        let mut changed = config.clone();
        changed.index.ngram_indexing ^= 1;
        assert!(reference.load(&root.0, &changed, 32 * 1024 * 1024).is_err());
        drop(snapshot);
        std::fs::write(
            reference.closure.path(&root.0).unwrap().join("extra"),
            b"unselected",
        )
        .unwrap();
        assert!(reference.load(&root.0, &config, 32 * 1024 * 1024).is_err());
    }
}
