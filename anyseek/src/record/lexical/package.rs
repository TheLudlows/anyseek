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
    #[serde(skip)]
    pin: Option<Arc<crate::record::package_pins::Pin>>,
}
fn fingerprint(config: &RecordConfig) -> Result<[u8; 32], RecordError> {
    Ok(
        Sha256::digest(serde_json::to_vec(&(&config.index, &config.schema, crate::tokenizer::standard::identity(config.index.tokenizer))).map_err(storage)?)
            .into(),
    )
}
impl NativePackage {
    pub(crate) async fn prepare(
        owner: Arc<tokio::sync::Mutex<crate::record::Inner>>,
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
        let fields: Vec<_> = config.schema.iter().filter(|f| f.index_lexical).collect();
        let names: Vec<String> = fields.iter().map(|f| f.field.clone()).collect();
        schema(&names)?;
        let mut inputs = Vec::new();
        let analysis_base = 64 * 1024u64;
        let estimate = records.iter().try_fold(analysis_base, |sum, (_, doc)| {
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
        // Dictionary construction and token buffers start only after admission.
        let analyzer = super::query::Analyzer::new(config.index.clone())?;
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
        let root = owner.lock().await.journal.root().to_owned();
        let id = format!("{:032x}", rand::random::<u128>());
        let prepared = root.join(format!(".segment-prepare-{id}"));
        let pin = crate::record::package_pins::Pin::acquire(&[
            prepared.clone(),
            root.join(format!("segment-{id}")),
        ])?;
        let handle = tokio::runtime::Handle::current();
        // Cancellation of the awaiting request does not cancel or disown the
        // synchronous native writer. Its scope and leases live in this worker.
        tokio::task::spawn_blocking(move || {
            let work = async move {
                let (_owner, _memory, _buffer) = (owner, memory, buffer);
                let _disk =
                    crate::storage::build_budget::ensure_scope(&root, limit).map_err(storage)?;
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
                #[cfg(test)]
                if root.join(".test-native-pause").exists() {
                    std::fs::write(root.join(".test-native-entered"), b"ready").map_err(storage)?;
                    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
                    while !root.join(".test-native-release").exists() {
                        if std::time::Instant::now() >= deadline {
                            return Err(storage("native test gate timeout"));
                        }
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                }
                Candidate::build_analyzed(&prepared, cut, names, inputs, limit)?;
                segment::write_json(&prepared.join("analysis.json"), &analysis)?;
                let closure = segment::Package::seal(&root, &prepared, id, limit)?;
                Ok(Self {
                    closure,
                    cut,
                    analysis,
                    pin: Some(pin),
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
        let pin = crate::record::package_pins::Pin::acquire(&[self.closure.path(root)?])?;
        let path = self.closure.verify(root, limit)?;
        let stored: [u8; 32] =
            serde_json::from_slice(&segment::read_bounded(&path.join("analysis.json"), 1024)?)
                .map_err(storage)?;
        if stored != self.analysis {
            return Err(storage("native analysis certificate mismatch"));
        }
        let mut snapshot = Candidate {
            files: self.closure.files.clone(),
        }
        .open(&path, self.cut, limit)?;
        snapshot._pin = Some(pin);
        Ok(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn sealed_native_package_reopens_from_selected_descriptor_and_rejects_configuration_drift()
     {
        let root = super::super::tests::Temp::new();
        let config = crate::record::tests::config();
        let partition = crate::record::RecordPartition::create(&root.0, config.clone())
            .await
            .unwrap();
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
            partition.inner.clone(),
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

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    #[tokio::test]
    async fn production_gc_retains_candidate_and_fixed_reader_until_last_pin_drops() {
        let root = super::super::tests::Temp::new();
        let config = crate::record::tests::config();
        let partition = crate::record::RecordPartition::create(&root.0, config.clone())
            .await
            .unwrap();
        let package = NativePackage::prepare(
            partition.inner.clone(),
            &config,
            Cut {
                sequence: 0,
                evaluation_time_ms: 0,
            },
            vec![],
            32 * 1024 * 1024,
        )
        .await
        .unwrap();
        let path = package.closure.path(&root.0).unwrap();
        partition
            .inner
            .lock()
            .await
            .reclaim_anchor_objects()
            .await
            .unwrap();
        assert!(path.exists(), "unselected candidate owns a build pin");
        let reference: NativePackage =
            serde_json::from_slice(&serde_json::to_vec(&package).unwrap()).unwrap();
        let snapshot = reference.load(&root.0, &config, 32 * 1024 * 1024).unwrap();
        drop(package);
        partition
            .inner
            .lock()
            .await
            .reclaim_anchor_objects()
            .await
            .unwrap();
        assert!(path.exists(), "fixed reader owns a separate pin");
        drop(snapshot);
        partition
            .inner
            .lock()
            .await
            .reclaim_anchor_objects()
            .await
            .unwrap();
        assert!(
            !path.exists(),
            "unselected package is reclaimable after last pin"
        );
        assert!(reference.load(&root.0, &config, 32 * 1024 * 1024).is_err());
    }
    #[tokio::test]
    async fn cancelled_native_builder_retains_partition_owner_until_worker_drains() {
        let root = super::super::tests::Temp::new();
        let config = crate::record::tests::config();
        let partition = crate::record::RecordPartition::create(&root.0, config.clone())
            .await
            .unwrap();
        std::fs::write(root.0.join(".test-native-pause"), b"pause").unwrap();
        let owner = partition.inner.clone();
        let worker_config = config.clone();
        let request = tokio::spawn(async move {
            NativePackage::prepare(
                owner,
                &worker_config,
                Cut {
                    sequence: 0,
                    evaluation_time_ms: 0,
                },
                vec![],
                32 * 1024 * 1024,
            )
            .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !root.0.join(".test-native-entered").exists() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        drop(partition);
        assert!(
            crate::record::RecordPartition::open(&root.0, config.max_journal_bytes)
                .await
                .is_err(),
            "cancelled request must not release journal owner before native worker"
        );
        std::fs::write(root.0.join(".test-native-release"), b"release").unwrap();
        let recovered = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Ok(partition) =
                    crate::record::RecordPartition::open(&root.0, config.max_journal_bytes).await
                {
                    break partition;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        recovered
            .inner
            .lock()
            .await
            .reclaim_anchor_objects()
            .await
            .unwrap();
        assert!(!std::fs::read_dir(&root.0).unwrap().any(|e| {
            e.unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("segment-")
        }));
    }
}
