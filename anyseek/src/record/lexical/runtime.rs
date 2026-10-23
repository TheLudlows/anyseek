//! Record's native lexical component. The companion IndexArc owns only vectors
//! and metadata. Tail commits are private until Record's publication guard opens.
use super::*;
use crate::record::{Document, RecordConfig};
use tantivy::{IndexReader, IndexWriter};
use tokio::sync::Mutex;

struct Charge {
    memory: crate::storage::build_budget::MemoryLease,
    buffer: Option<crate::resources::Lease>,
}
impl Charge {
    fn new(bytes: u64) -> Result<Self, RecordError> {
        let memory = crate::storage::build_budget::reserve_memory(bytes).map_err(storage)?;
        let buffer = crate::resources::Scope::current().map(|s| s.retained_io().bytes(bytes))
            .transpose().map_err(storage)?;
        Ok(Self { memory, buffer })
    }
    fn grow(&mut self, bytes: u64) -> Result<(), RecordError> {
        self.memory.grow(bytes).map_err(storage)?;
        if let Some(buffer) = &mut self.buffer { buffer.grow_bytes(bytes).map_err(storage)?; }
        Ok(())
    }
}

pub(crate) struct Runtime {
    pub(crate) config: RecordConfig,
    pub(super) analyzer: super::query::Analyzer,
    data: Mutex<Data>,
    directory: Option<super::directory::BudgetDirectory>,
    pin: Arc<crate::record::package_pins::Pin>,
}
struct Data {
    index: Index,
    reader: IndexReader,
    writer: Option<IndexWriter<TantivyDocument>>,
    identities: Vec<Identity>,
    identity_charge: Charge,
    published_updates: usize,
    snapshot: Option<Arc<Snapshot>>,
    cut: Cut,
}
impl Drop for Runtime {
    fn drop(&mut self) {
        if let Some(writer) = self.data.get_mut().writer.take() {
            if writer.wait_merging_threads().is_ok() {
                if let Some(directory)=&self.directory {directory.release_joined_writer();}
            }
            // On join failure Directory ownership conservatively retains the
            // charge until the remaining workers and snapshots release it.
        }
    }
}
impl Runtime {
    pub(crate) fn create(path: &Path, config: &RecordConfig) -> Result<Arc<Self>, RecordError> {
        let analyzer = super::query::Analyzer::admitted(config.index.clone())?;
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
            super::directory::BudgetDirectory::persistent(path, limit, 65536, 0)?;
        directory.retain_pin(pin.clone())?;
        let index = Index::create(directory.clone(), schema, Default::default()).map_err(storage)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(storage)?;
        Ok(Arc::new(Self {
            config: config.clone(),
            analyzer,
            directory: Some(directory),
            data: Mutex::new(Data {
                index,
                reader,
                writer: None,
                published_updates: 0,
                identities: Vec::new(),
                identity_charge: Charge::new(0)?,
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
        let analyzer = super::query::Analyzer::admitted(config.index.clone())?;
        let pin = crate::record::package_pins::Pin::acquire(&[path
            .parent()
            .ok_or_else(|| storage("native package parent missing"))?
            .to_owned()])?;
        let manifest_path = path.join("record-native.json");
        let length = std::fs::metadata(&manifest_path).map_err(storage)?.len();
        if length > config.max_journal_bytes { return Err(storage("native manifest budget exceeded")); }
        // Retain conservative JSON/identity storage admission before reading/parsing.
        let identity_charge = Charge::new(length.checked_mul(8).and_then(|n| n.checked_add(65536))
            .ok_or_else(|| storage("native manifest memory overflow"))?)?;
        let bytes = segment::read_bounded(&manifest_path, config.max_journal_bytes)?;
        let (manifest, stored): (Manifest, serde_json::Value) =
            serde_json::from_slice(&bytes).map_err(storage)?;
        if manifest.format != FORMAT
            || manifest.engine != ENGINE
            || stored != serde_json::to_value((&config.index, &config.schema)).map_err(storage)?
        {
            return Err(storage("native Record configuration/format mismatch"));
        }
        let limit=config.max_journal_bytes.checked_mul(64).and_then(|n|n.checked_add(32*1024*1024))
            .ok_or_else(||storage("native selected byte budget overflow"))?;
        let directory=super::directory::BudgetDirectory::open_selected(path,limit)?;
        directory.retain_pin(pin.clone())?;
        let index = Index::open(directory).map_err(storage)?;
        let reader: IndexReader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(storage)?;
        let mut snapshot = capture(reader.searcher(), &manifest)?;
        snapshot._pin = Some(pin.clone());
        Ok(Arc::new(Self {
            config: config.clone(),
            analyzer,
            directory: None,
            data: Mutex::new(Data {
                index,
                reader,
                writer: None,
                published_updates: 0,
                identities: manifest.identities,
                identity_charge,
                snapshot: Some(Arc::new(snapshot)),
                cut: manifest.cut,
            }),
            pin,
        }))
    }
    /// Bound native segment fanout and cumulative metadata rewrite traffic.
    /// Record certifies a replacement before accepting another durable operation.
    /// Bulk rebuilds publish once and do not consume this mutable-tail allowance.
    pub(crate) async fn needs_rotation(&self, incoming: usize) -> bool {
        self.data.lock().await.published_updates.saturating_add(incoming) > 128
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
        let analyzer = &self.analyzer;
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
        if data.writer.is_none() {
            let directory=self.directory.as_ref().ok_or_else(||storage("immutable lexical package"))?;
            directory.admit_writer(WRITER_BYTES as u64)?;
            let writer=data.index.writer_with_options(
                tantivy::indexer::IndexWriterOptions::builder().num_worker_threads(1)
                    .num_merge_threads(1).memory_budget_per_thread(WRITER_BYTES).build()
            ).map_err(storage)?;
            // Record's owned compaction controls merge allocation and publication.
            writer.set_merge_policy(Box::new(tantivy::merge_policy::NoMergePolicy));
            data.writer=Some(writer);
        }
        data.identity_charge.grow(256 + identity.id.len() as u64)?;
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
            data.published_updates += 1;
        }
        Ok(())
    }
    fn flush_locked(&self, data: &mut Data) -> Result<(), RecordError> {
        if self.directory.is_none() {return Err(storage("immutable lexical package"));}
        if let Some(writer)=data.writer.as_mut() {
            writer.commit().map_err(storage)?;
        }
        data.reader.reload().map_err(storage)?;
        // Manifest cloning and serialization coexist with the retained identities.
        let temporary_bytes = data.identities.iter().try_fold(65536u64, |n, id| {
            n.checked_add(256 + id.id.len() as u64 * 8)
        }).ok_or_else(|| storage("native manifest memory overflow"))?;
        let _temporary = Charge::new(temporary_bytes)?;
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
        use tantivy::directory::Directory;
        let directory = self.directory.as_ref().ok_or_else(|| storage("immutable lexical package"))?;
        directory.atomic_write(Path::new("record-native.json"), &bytes).map_err(storage)?;
        directory.sync_directory().map_err(storage)?;
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
        .and_then(|n| manifest.identities.iter().try_fold(n, |n, id| n.checked_add(id.id.len() as u64)))
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
                if values.len() != 1 || values[0] > MAX_TOKENS as u64 {
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
    async fn joined_writer_releases_buffer_while_fixed_searcher_retains_pin() {
        let root=tempfile::tempdir().unwrap();let package=root.path().join("package");
        std::fs::create_dir(&package).unwrap();
        let governor=Arc::new(crate::resources::Governor::default());
        let scope=governor.admit("joined-writer".into(),crate::resources::Lane::Foreground,0).unwrap();
        let accounting=scope.clone().in_lane(crate::resources::Lane::Background);
        let native=scope.run(async {Runtime::create(&package.join("native"),&config()).unwrap()}).await;
        native.append(Identity{id:"a".into(),version:1,revision:1},&serde_json::from_str(r#"{"body":"alpha"}"#).unwrap(),true).await.unwrap();
        let held=native.snapshot().await.unwrap();
        assert!(accounting.bytes(375*1024*1024).is_err(),"live native writer retains its buffer");
        drop(native);
        let capacity=accounting.bytes(375*1024*1024).expect("joined writer must release buffer even with fixed reader");
        assert_eq!(held.identities.len(),1);
        assert!(!crate::record::package_pins::retire_if_unpinned(&package,||{panic!("live Searcher must protect package")}).unwrap());
        drop(held);drop(capacity);
        assert!(crate::record::package_pins::retire_if_unpinned(&package,||Ok(())).unwrap());
    }

    #[tokio::test]
    async fn complete_generation_is_admitted_before_rotation_and_recovers_atomically() {
        use crate::record::tests::{generation_write,stage_generation,source_write};
        use crate::record::GenerationCommand;
        for fail in [false,true] {
            let root=tempfile::tempdir().unwrap();let path=root.path().join("partition");
            let mut settings=config();settings.max_records=512;settings.max_operations=1024;settings.max_candidates=512;settings.max_record_bytes=64*1024;settings.max_journal_bytes=16*1024*1024;
            let p=RecordPartition::create(&path,settings.clone()).await.unwrap();
            p.write(source_write("s","s",&[])).await.unwrap();
            for i in 0..126 {
                p.write(write(&format!("o-{i}"),&format!("r-{i}"),"alpha",ExpectedVersion::Absent)).await.unwrap();
            }
            let members=(0..100).map(|i|format!("member-{i}")).collect::<Vec<_>>();
            p.write(generation_write("stage",ExpectedVersion::Absent,stage_generation("g",&members.iter().map(String::as_str).collect::<Vec<_>>()))).await.unwrap();
            assert!(p.inner.lock().await.anchor.is_none());
            assert_eq!(p.generation_status("doc").await.unwrap().unwrap().visible_members,0);
            if fail {p.inner.lock().await.fail_publication_stage=1;}
            let request=generation_write("publish",ExpectedVersion::Match(1),GenerationCommand::Publish{generation_id:"g".into()});
            let result=p.write(request.clone()).await;
            assert!(p.inner.lock().await.anchor.is_some(),"127 + 100 must rotate before the durable publish");
            if fail {
                assert_eq!(result.unwrap_err(),crate::record::RecordError::DurablePending(129));
                assert!(p.generation_status("doc").await.is_err());
                assert!(p.search("generationword".into(),None,SearchMode::Lexical,vec![],10).await.is_err());
            } else {
                assert_eq!(result.unwrap().version,2);
                assert_eq!(p.generation_status("doc").await.unwrap().unwrap().visible_members,100);
            }
            drop(p);
            let p=RecordPartition::open(&path,settings.max_journal_bytes).await.unwrap();
            let receipt=p.write(request.clone()).await.unwrap();assert_eq!(receipt.seq,129);assert_eq!(receipt.version,2);
            assert_eq!(p.write(request).await.unwrap(),receipt);
            assert_eq!(p.generation_status("doc").await.unwrap().unwrap().visible_members,100);
            assert_eq!(p.search("generationword".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().count_total,100);
        }
    }

    #[tokio::test]
    async fn automatic_tail_rotation_preserves_durable_retry_and_fixed_searcher() {
        let root=tempfile::tempdir().unwrap();
        let path=root.path().join("partition");
        let mut settings=config(); settings.max_records=512;settings.max_operations=1024;settings.max_candidates=512;
        let partition=RecordPartition::create(&path,settings.clone()).await.unwrap();
        for i in 0..128 {
            partition.write(write(&format!("op-{i}"),&format!("r-{i}"),"alpha",ExpectedVersion::Absent)).await.unwrap();
        }
        let held=partition.inner.lock().await.index.test_index().read().await.record_lexical[0].snapshot().await.unwrap();
        assert_eq!(held.identities.len(),128);
        partition.inner.lock().await.fail_publication_stage=1;
        let request=write("rotate-and-retry","r-new","alpha",ExpectedVersion::Absent);
        assert_eq!(partition.write(request.clone()).await.unwrap_err(),crate::record::RecordError::DurablePending(129));
        assert!(partition.inner.lock().await.anchor.is_some(),"rotation must certify before WAL append");
        assert_eq!(held.identities.len(),128,"captured Searcher remains at its original cut");
        drop(partition);
        let partition=RecordPartition::open(&path,settings.max_journal_bytes).await.unwrap();
        let receipt=partition.write(request.clone()).await.unwrap();
        assert_eq!(receipt.seq,129);assert_eq!(receipt.version,1);
        assert_eq!(partition.write(request).await.unwrap(),receipt);
        assert_eq!(partition.search("alpha".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().count_total,129);
    }

    // Keep this direct-native boundary test: Record must maintain its tail before
    // this finite traffic budget is exhausted; commit is not business authority.
    #[tokio::test]
    async fn repeated_native_commits_expose_finite_traffic_budget() {
        let root=tempfile::tempdir().unwrap();
        let mut settings=config(); settings.max_journal_bytes=1024*1024;
        let native=Runtime::create(&root.path().join("native"),&settings).unwrap();
        let doc=serde_json::from_str(r#"{"body":"common half"}"#).unwrap();
        for i in 0..1000 {
            let result=native.append(Identity{id:format!("{i:020}"),version:1,revision:i+1},&doc,true).await;
            if let Err(error)=result {
                log::info!("first native failure after {i} visible writes: {error:?}");
                assert!(format!("{error:?}").contains("native writer byte budget exceeded"),"{error:?}");
                return;
            }
        }
        panic!("expected bounded native traffic admission to reject repeated commits");
    }

    #[tokio::test]
    async fn idle_collections_leave_native_publication_headroom() {
        let root=tempfile::tempdir().unwrap();
        let governor=Arc::new(crate::resources::Governor::default());
        let mut owners=Vec::new();
        let mut settings=config();settings.index.inference=crate::vector::Inference::None;
        settings.embedding_space_id=None;settings.schema.retain(|f|f.index_lexical);
        for i in 0..65 {
            let scope=governor.admit(format!("collection-{i}"),crate::resources::Lane::Foreground,0).unwrap();
            let partition=scope.run(RecordPartition::create(&root.path().join(i.to_string()),settings.clone())).await.unwrap();
            owners.push(partition);
        }
        let scope=governor.admit("collection-0".into(),crate::resources::Lane::Foreground,0).unwrap();
        scope.run(async {
            owners[0].write(write("first","r","alpha",ExpectedVersion::Absent)).await.unwrap();
            owners[0].compact_journal().await.unwrap();
            assert_eq!(owners[0].search("alpha".into(),None,SearchMode::Lexical,vec![],10).await.unwrap().hits.len(),1);
        }).await;
    }

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
        assert!(old.error.is_some(), "private vector component rejects lexical execution");
        let component_path=std::path::PathBuf::from(&index.read().await.index_path_string);
        assert!(!component_path.join("index.bin").exists());
        assert!(!component_path.join("docstore.bin").exists());
        assert!(!component_path.join("shards").exists());
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
    async fn metadata_component_does_not_build_or_warm_lexical_dictionaries() {
        let root = super::super::tests::Temp::new();
        let mut cfg = config();
        cfg.index.frequent_words = crate::index::FrequentwordType::English;
        cfg.index.ngram_indexing = 9;
        let p = RecordPartition::create(&root.0, cfg.clone()).await.unwrap();
        p.write(write("a", "a", "alpha in the world", ExpectedVersion::Absent)).await.unwrap();
        p.compact_journal().await.unwrap();
        drop(p);
        let p = RecordPartition::open(&root.0, cfg.max_journal_bytes).await.unwrap();
        assert_eq!(query(&p, SearchMode::Lexical).await.count_total, 1);
        let inner = p.inner.lock().await;
        let index = inner.index.test_index().read().await;
        for shard in &index.shard_vec {
            let shard = shard.read().await;
            assert!(shard.indexed_schema_vec.iter().all(|field| !field.index_lexical));
        }
    }

    #[tokio::test]
    async fn hybrid_total_preserves_per_component_maximum() {
        let root = super::super::tests::Temp::new();
        let cfg = config();
        let p = RecordPartition::create(&root.0, cfg.clone()).await.unwrap();
        let mut lexical_only = write("a", "a", "alpha", ExpectedVersion::Absent);
        lexical_only.document.as_mut().unwrap().remove("vector");
        p.write(lexical_only).await.unwrap();
        p.compact_journal().await.unwrap();
        p.write(write("b", "b", "beta", ExpectedVersion::Absent)).await.unwrap();
        let mode = SearchMode::Hybrid { similarity_threshold: None, ann_mode: crate::vector_similarity::AnnMode::All };
        let result = query(&p, mode.clone()).await;
        assert_eq!(result.count_total, 2, "lexical-only frozen row plus vector-only tail row");
        assert_eq!(result.hits.len(), 2);
        drop(p);
        let p = RecordPartition::open(&root.0, cfg.max_journal_bytes).await.unwrap();
        assert_eq!(query(&p, mode).await.count_total, 2);
    }

    #[tokio::test]
    async fn zero_weight_vector_preserves_count_before_and_after_checkpoint() {
        let root = super::super::tests::Temp::new();
        let cfg = config();
        let p = RecordPartition::create(&root.0, cfg.clone()).await.unwrap();
        for (id, body) in [("a", "alpha"), ("b", "beta")] {
            p.write(write(id, id, body, ExpectedVersion::Absent)).await.unwrap();
        }
        let mode = SearchMode::WeightedHybrid {
            similarity_threshold: None, ann_mode: crate::vector_similarity::AnnMode::All,
            rrf: crate::search::RrfOptions { vector_weight: 0.0, ..Default::default() },
        };
        for checkpoint in [false, true] {
            if checkpoint { p.compact_journal().await.unwrap(); }
            let result = query(&p, mode.clone()).await;
            assert_eq!(result.count_total, 2);
            assert_eq!(result.hits.len(), 1);
            assert_eq!(result.hits[0].record.record_id, "a");
        }
        drop(p);
        let p = RecordPartition::open(&root.0, cfg.max_journal_bytes).await.unwrap();
        assert_eq!(query(&p, mode).await.count_total, 2);
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
    #[tokio::test]
    async fn text_only_record_uses_zero_field_metadata_component_and_recovers() {
        let root=super::super::tests::Temp::new();
        let mut cfg=config();cfg.schema.retain(|field|field.field=="body");
        cfg.index.inference=crate::vector::Inference::None;cfg.embedding_space_id=None;
        let p=RecordPartition::create(&root.0,cfg.clone()).await.unwrap();
        let mut request=write("a","a","alpha",ExpectedVersion::Absent);
        request.document.as_mut().unwrap().remove("vector");
        p.write(request).await.unwrap();
        assert_eq!(query(&p,SearchMode::Lexical).await.count_total,1);
        p.compact_journal().await.unwrap();drop(p);
        let p=RecordPartition::open(&root.0,cfg.max_journal_bytes).await.unwrap();
        assert_eq!(query(&p,SearchMode::Lexical).await.count_total,1);
    }

}
