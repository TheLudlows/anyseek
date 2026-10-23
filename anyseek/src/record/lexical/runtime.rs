//! Record's native lexical component. The companion IndexArc owns only vectors
//! and metadata. Tail commits are private until Record's publication guard opens.
use super::*;
use crate::record::{Document, RecordConfig};
use tantivy::{IndexReader, IndexWriter};
use tokio::sync::Mutex;

pub(crate) struct Runtime {
    pub(crate) config: RecordConfig,
    path: std::path::PathBuf,
    data: Mutex<Data>,
    pin: Arc<crate::record::package_pins::Pin>,
}
struct Data {
    index: Index,
    reader: IndexReader,
    writer: Option<IndexWriter<TantivyDocument>>,
    identities: Vec<Identity>,
    snapshot: Option<Arc<Snapshot>>,
    cut: Cut,
}
impl Drop for Runtime {
    fn drop(&mut self) {
        if let Some(writer) = self.data.get_mut().writer.take() {
            let _ = writer.wait_merging_threads();
        }
    }
}
impl Runtime {
    pub(crate) fn create(path: &Path, config: &RecordConfig) -> Result<Arc<Self>, RecordError> {
        let pin = crate::record::package_pins::Pin::acquire(&[path
            .parent()
            .ok_or_else(|| storage("native package parent missing"))?
            .to_owned()])?;
        std::fs::create_dir(path).map_err(storage)?;
        let names = config
            .schema
            .iter()
            .filter(|f| f.index_lexical)
            .map(|f| f.field.clone())
            .collect::<Vec<_>>();
        let (schema, _) = schema(&names)?;
        let limit = config
            .max_journal_bytes
            .checked_mul(64)
            .and_then(|n| n.checked_add(32 * 1024 * 1024))
            .ok_or_else(|| storage("native disk budget overflow"))?;
        let directory =
            super::directory::BudgetDirectory::persistent(path, limit, 65536, WRITER_BYTES as u64)?;
        let index = Index::create(directory, schema, Default::default()).map_err(storage)?;
        let writer = index
            .writer_with_options(
                tantivy::indexer::IndexWriterOptions::builder()
                    .num_worker_threads(1)
                    .num_merge_threads(1)
                    .memory_budget_per_thread(WRITER_BYTES)
                    .build(),
            )
            .map_err(storage)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(storage)?;
        Ok(Arc::new(Self {
            config: config.clone(),
            path: path.to_owned(),
            data: Mutex::new(Data {
                index,
                reader,
                writer: Some(writer),
                identities: Vec::new(),
                snapshot: None,
                cut: Cut {
                    sequence: 0,
                    evaluation_time_ms: 0,
                },
            }),
            pin,
        }))
    }
    pub(crate) fn open(path: &Path, config: &RecordConfig) -> Result<Arc<Self>, RecordError> {
        let pin = crate::record::package_pins::Pin::acquire(&[path
            .parent()
            .ok_or_else(|| storage("native package parent missing"))?
            .to_owned()])?;
        let bytes =
            segment::read_bounded(&path.join("record-native.json"), config.max_journal_bytes)?;
        let (manifest, stored): (Manifest, serde_json::Value) =
            serde_json::from_slice(&bytes).map_err(storage)?;
        if manifest.format != FORMAT
            || manifest.engine != ENGINE
            || stored != serde_json::to_value((&config.index, &config.schema)).map_err(storage)?
        {
            return Err(storage("native Record configuration/format mismatch"));
        }
        let index = Index::open_in_dir(path).map_err(storage)?;
        let reader: IndexReader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(storage)?;
        let mut snapshot = capture(reader.searcher(), &manifest)?;
        snapshot._pin = Some(pin.clone());
        Ok(Arc::new(Self {
            config: config.clone(),
            path: path.to_owned(),
            data: Mutex::new(Data {
                index,
                reader,
                writer: None,
                identities: manifest.identities,
                snapshot: Some(Arc::new(snapshot)),
                cut: manifest.cut,
            }),
            pin,
        }))
    }
    pub(crate) async fn append(
        &self,
        identity: Identity,
        document: &Document,
        publish: bool,
    ) -> Result<(), RecordError> {
        let estimate =
            crate::record::build_budget::document_estimate(document, &self.config.schema)?;
        let _analysis = crate::storage::build_budget::reserve_memory(estimate).map_err(storage)?;
        let _buffer = crate::resources::Scope::current()
            .map(|s| s.bytes(estimate))
            .transpose()
            .map_err(storage)?;
        let analyzer = super::query::Analyzer::new(self.config.index.clone());
        let mut fields = Vec::new();
        for field in self.config.schema.iter().filter(|f| f.index_lexical) {
            let text = match document.get(&field.field) {
                None => String::new(),
                Some(value)
                    if field.field_type == crate::index::FieldType::Json && value.is_object() =>
                {
                    let mut strings = Vec::new();
                    crate::index::object_values_to_string_vec_recursive(value, &mut strings);
                    strings.join(" ")
                }
                Some(value) => value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string()),
            };
            fields.push(analyzer.field(&text).await);
        }
        let mut data = self.data.lock().await;
        let mut doc = TantivyDocument::new();
        let native_schema = data.index.schema();
        doc.add_u64(
            native_schema.get_field("identity").map_err(storage)?,
            data.identities.len() as u64,
        );
        for (i, field) in fields.into_iter().enumerate() {
            doc.add_pre_tokenized_text(
                native_schema
                    .get_field(&format!("text_{i}"))
                    .map_err(storage)?,
                PreTokenizedString {
                    text: field.text,
                    tokens: field.tokens,
                },
            );
            doc.add_u64(
                native_schema
                    .get_field(&format!("length_{i}"))
                    .map_err(storage)?,
                field.length as u64,
            );
        }
        data.writer
            .as_ref()
            .ok_or_else(|| storage("immutable lexical package"))?
            .add_document(doc)
            .map_err(storage)?;
        data.cut.sequence = data.cut.sequence.max(identity.revision);
        data.identities.push(identity);
        if publish {
            self.flush_locked(&mut data)?;
        }
        Ok(())
    }
    fn flush_locked(&self, data: &mut Data) -> Result<(), RecordError> {
        data.writer
            .as_mut()
            .ok_or_else(|| storage("immutable lexical package"))?
            .commit()
            .map_err(storage)?;
        data.reader.reload().map_err(storage)?;
        let manifest = Manifest {
            format: FORMAT,
            engine: ENGINE.into(),
            cut: data.cut,
            fields: self
                .config
                .schema
                .iter()
                .filter(|f| f.index_lexical)
                .map(|f| f.field.clone())
                .collect(),
            identities: data.identities.clone(),
        };
        let mut snapshot = capture(data.reader.searcher(), &manifest)?;
        snapshot._pin = Some(self.pin.clone());
        data.snapshot = Some(Arc::new(snapshot));
        let bytes = serde_json::to_vec(&(
            manifest,
            serde_json::to_value((&self.config.index, &self.config.schema)).map_err(storage)?,
        ))
        .map_err(storage)?;
        if bytes.len() as u64 > self.config.max_journal_bytes {
            return Err(storage("native identity manifest budget exceeded"));
        }
        use std::io::Write;
        let mut file =
            std::fs::File::create(self.path.join("record-native.json")).map_err(storage)?;
        file.write_all(&bytes).map_err(storage)?;
        file.sync_all().map_err(storage)?;
        Ok(())
    }
    pub(crate) async fn flush(&self) -> Result<(), RecordError> {
        self.flush_locked(&mut *self.data.lock().await)
    }
    pub(crate) async fn snapshot(&self) -> Result<Arc<Snapshot>, RecordError> {
        self.data
            .lock()
            .await
            .snapshot
            .clone()
            .ok_or_else(|| storage("unpublished native tail"))
    }
}
fn capture(searcher: Searcher, manifest: &Manifest) -> Result<Snapshot, RecordError> {
    let (expected, fields) = schema(&manifest.fields)?;
    if *searcher.schema() != expected {
        return Err(storage("native runtime schema mismatch"));
    }
    let estimate = (manifest.identities.len() as u64)
        .checked_mul(128 + fields.len() as u64 * 32)
        .and_then(|n| n.checked_add(65536))
        .ok_or_else(|| storage("native view memory overflow"))?;
    let memory = crate::storage::build_budget::reserve_memory(estimate).map_err(storage)?;
    let buffer = crate::resources::Scope::current()
        .map(|s| s.retained_io().bytes(estimate))
        .transpose()
        .map_err(storage)?;
    let mut seen = BTreeSet::new();
    let mut addresses = Vec::new();
    let mut lengths = Vec::new();
    for segment in searcher.segment_readers() {
        if segment.num_docs() != segment.max_doc() {
            return Err(storage("native physical deletion unsupported"));
        }
        let column = segment.fast_fields().u64("identity").map_err(storage)?;
        let columns = (0..fields.len())
            .map(|i| segment.fast_fields().u64(&format!("length_{i}")))
            .collect::<tantivy::Result<Vec<_>>>()
            .map_err(storage)?;
        let mut mapping = Vec::new();
        let mut rows = Vec::new();
        for doc in 0..segment.max_doc() {
            let ids = column.values_for_doc(doc).collect::<Vec<_>>();
            if ids.len() != 1 || ids[0] >= manifest.identities.len() as u64 || !seen.insert(ids[0])
            {
                return Err(storage("native runtime identity mismatch"));
            }
            mapping.push(ids[0] as usize);
            let mut row = Vec::new();
            for column in &columns {
                let values = column.values_for_doc(doc).collect::<Vec<_>>();
                if values.len() != 1 {
                    return Err(storage("native runtime length mismatch"));
                }
                row.push(values[0]);
            }
            rows.push(row);
        }
        addresses.push(mapping);
        lengths.push(rows);
    }
    if seen.len() != manifest.identities.len() {
        return Err(storage("native runtime incomplete commit"));
    }
    Ok(Snapshot {
        searcher,
        fields,
        names: manifest.fields.clone(),
        cut: manifest.cut,
        identities: Arc::new(manifest.identities.clone()),
        addresses,
        lengths,
        _memory: memory,
        _buffer: buffer,
        _pin: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::tests::{config, query, write};
    use crate::record::{ExpectedVersion, RecordPartition};
    use crate::search::{QueryType, ResultType, Search, SearchMode};
    #[tokio::test]
    async fn record_native_write_query_checkpoint_reopen_has_no_legacy_lexical_postings() {
        let root = super::super::tests::Temp::new();
        let config = config();
        let partition = RecordPartition::create(&root.0, config.clone())
            .await
            .unwrap();
        partition
            .write(write("a", "a", "alpha beta", ExpectedVersion::Absent))
            .await
            .unwrap();
        assert_eq!(query(&partition, SearchMode::Lexical).await.count_total, 1);
        let index = partition.inner.lock().await.index.test_index().clone();
        assert!(!index.read().await.schema_map["body"].index_lexical);
        assert_eq!(index.read().await.record_lexical.len(), 1);
        let old = index
            .search(
                "alpha".into(),
                None,
                QueryType::Union,
                SearchMode::Lexical,
                true,
                0,
                10,
                ResultType::TopkCount,
                true,
                vec![],
                vec![],
                vec![],
            )
            .await;
        assert_eq!(
            old.result_count_total, 0,
            "legacy lexical executor must not supply the hit"
        );
        drop(index);
        partition.compact_journal().await.unwrap();
        assert_eq!(query(&partition, SearchMode::Lexical).await.count_total, 1);
        drop(partition);
        let reopened = RecordPartition::open(&root.0, config.max_journal_bytes)
            .await
            .unwrap();
        assert_eq!(query(&reopened, SearchMode::Lexical).await.count_total, 1);
        reopened
            .write(write("b", "b", "alpha gamma", ExpectedVersion::Absent))
            .await
            .unwrap();
        assert_eq!(query(&reopened, SearchMode::Lexical).await.count_total, 2);
    }
    #[tokio::test]
    async fn retained_native_writer_releases_request_slot() {
        let root = super::super::tests::Temp::new();
        let governor = Arc::new(crate::resources::Governor::default());
        let scope = governor
            .admit(
                "native-retained".into(),
                crate::resources::Lane::Foreground,
                0,
            )
            .unwrap();
        let partition = scope
            .run(RecordPartition::create(&root.0, config()))
            .await
            .unwrap();
        let slots = (0..4)
            .map(|_| {
                governor
                    .admit(
                        "native-retained".into(),
                        crate::resources::Lane::Foreground,
                        0,
                    )
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert!(
            governor
                .admit(
                    "native-retained".into(),
                    crate::resources::Lane::Foreground,
                    0
                )
                .is_err()
        );
        drop(slots);
        drop(partition);
    }
}
