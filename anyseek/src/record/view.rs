//! Retrieval capabilities owned by one partition view. No IndexArc escapes to
//! production callers: writes target the tail, searches target the selected view.
use super::*;

pub(super) struct RotationBudget {
    pub max_segments: usize,
    pub max_address_slots: usize,
}

pub(super) struct RetrievalView {
    query: IndexArc,
    tail: IndexArc,
    // Ordered like query components after the mutable tail, before certified packages.
    pub(super) pending_seals: Vec<segment::Package>,
    // Intake buffers native inputs while Record/vector materialization owns
    // the partition guard; publication then runs in an independent consumer.
    batch: bool,
    batch_inputs: Vec<index_consumers::Input>,
    batch_vectors: Vec<index_consumers::Input>,
    vector: Option<index_consumers::Consumer>,
    reserved_rows: Option<usize>,
    component_bytes: u64,
    retired: Vec<Arc<crate::active::Retired>>,
    lexical: Option<index_consumers::Consumer>,
    cursors: Option<Arc<index_consumers::Cursors>>,
    owner: std::sync::Weak<Mutex<Inner>>,
}
impl RetrievalView {
    pub(super) async fn capture_query(&self,length:usize)->Result<super::query::Handle,RecordError> {
        super::query::Handle::capture(&self.query,length).await
    }
    pub(super) async fn components(&self)->Vec<crate::component::ShardArc> {
        self.query.read().await.shard_vec.clone()
    }
    pub(super) async fn matches(&self,ordinal:usize,component:&crate::component::ShardArc,local:usize)->bool {
        let root=self.query.read().await;
        let n=root.shard_number;
        ordinal/n==local && Arc::ptr_eq(&root.shard_vec[ordinal%n],component)
    }
    pub(super) async fn can_admit(&self,bytes:u64,native_updates:usize)->bool {
        let root=self.tail.read().await;
        for native in &root.record_lexical {
            if native.needs_rotation(native_updates).await {return false;}
        }
        if self.reserved_rows.is_some() {
            return self.component_bytes.checked_add(bytes).is_some_and(|n|crate::storage::build_budget::validate_single(n).is_ok());
        }
        let shard=root.shard_vec[0].read().await;
        shard.build_reservation.as_ref().is_some_and(|r|r.can_grow(bytes))
    }

    pub(super) fn single(index: IndexArc) -> Self {
        Self {
            query: index.clone(),
            tail: index,
            pending_seals: Vec::new(),
            batch: false,
            batch_inputs: Vec::new(),
            batch_vectors:Vec::new(), vector:None, reserved_rows:None, component_bytes:0, retired:Vec::new(),
            lexical: None,
            cursors: None,
            owner:Default::default(),
        }
    }
    /// Install a new empty tail under the exclusive partition publication guard.
    /// The persistence orchestrator must certify the old tail's segment before
    /// invoking this transition; it grants no publication authority itself.
    pub(super) async fn rotate_tail(
        &mut self,
        new_tail: IndexArc,
        ordinals: &mut Vec<String>,
        locations: &mut BTreeMap<String, usize>,
        budget: RotationBudget,
    ) -> Result<(), RecordError> {
        let tail_root = new_tail.read().await;
        if Arc::ptr_eq(&self.query, &new_tail)
            || tail_root.shard_number != 1
            || tail_root.shard_vec.len() != 1
        {
            return Err(storage("replacement must be a separate single-shard tail"));
        }
        let native_tail = tail_root.record_lexical.first().cloned().ok_or_else(|| storage("missing native tail"))?;
        let tail_shard = tail_root.shard_vec[0].clone();
        let tail_meta = serde_json::to_value(&tail_root.meta).map_err(storage)?;
        let tail_schema = serde_json::to_value(&tail_root.schema_map).map_err(storage)?;
        drop(tail_root);
        let mut root = self.query.write().await;
        if tail_meta != serde_json::to_value(&root.meta).map_err(storage)?
            || tail_schema != serde_json::to_value(&root.schema_map).map_err(storage)?
        {
            return Err(storage("replacement tail configuration mismatch"));
        }
        let previous = root.shard_number;
        if previous == 0
            || root.shard_vec.len() != previous
            || root.shard_vec.iter().any(|s| Arc::ptr_eq(s, &tail_shard))
        {
            return Err(storage("invalid retrieval view topology"));
        }
        let next = previous
            .checked_add(1)
            .ok_or_else(|| storage("segment count overflow"))?;
        // Admission is checked before allocation or changes to the live view.
        if next > budget.max_segments {
            return Err(RecordError::Rejected("segment budget exceeded".into()));
        }
        let remap = |physical: usize| {
            (physical / previous)
                .checked_mul(next)
                .and_then(|v| v.checked_add(physical % previous + 1))
                .filter(|v| *v < budget.max_address_slots)
                .ok_or_else(|| RecordError::Rejected("segment address budget exceeded".into()))
        };
        let mut mapped = Vec::new();
        for (physical, key) in ordinals
            .iter()
            .enumerate()
            .filter(|(_, key)| !key.is_empty())
        {
            let address = remap(physical)?;
            if mapped.len() <= address {
                mapped
                    .try_reserve(address + 1 - mapped.len())
                    .map_err(storage)?;
                mapped.resize(address + 1, String::new());
            }
            mapped[address] = key.clone();
        }
        let mut next_locations = BTreeMap::new();
        for (key, address) in locations.iter() {
            if ordinals.get(*address) != Some(key) {
                return Err(storage("inconsistent live record location"));
            }
            next_locations.insert(key.clone(), remap(*address)?);
        }
        // Lock every component before modifying anything. Cancellation while
        // acquiring locks leaves the current context and addresses unchanged.
        root.shard_vec.try_reserve(1).map_err(storage)?;
        let mut shards = futures::future::join_all(root.shard_vec.iter().map(|s| s.write())).await;
        let mut tail = tail_shard.write().await;
        let local = tail
            .active
            .as_ref()
            .ok_or_else(|| storage("missing tail statistics"))?;
        if !local.keys.is_empty() {
            return Err(storage("replacement tail is not empty"));
        }
        let mut statistics = Vec::new();
        statistics.try_reserve(next).map_err(storage)?;
        let mut new_stats = local.clone_local();
        statistics.push(new_stats);
        for shard in &shards {
            let mut local = shard
                .active
                .as_ref()
                .ok_or_else(|| storage("missing segment statistics"))?
                .clone_local();
            statistics.push(local);
        }
        let ids = (0..next)
            .map(u64::try_from)
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        // No await or fallible work after this point. Old statistics point only
        // at the discarded corpus; Drop does not retire their contributions.
        let mut statistics = statistics.into_iter();
        tail.active = statistics.next();
        tail.meta.id = ids[0];
        for ((shard, statistics), id) in shards
            .iter_mut()
            .zip(statistics)
            .zip(ids.into_iter().skip(1))
        {
            shard.active = Some(statistics);
            shard.meta.id = id;
        }
        drop(tail);
        drop(shards);
        root.record_lexical.insert(0, native_tail);
        root.shard_vec.insert(0, tail_shard);
        root.shard_number = next;
        *ordinals = mapped;
        *locations = next_locations;
        self.tail = new_tail;
        self.lexical = None; // predecessor was drained before seal
        self.vector=None; self.reserved_rows=None; self.retired.clear();
        Ok(())
    }

    pub(super) async fn stable_tail_path(&self, root: &Path) -> Option<PathBuf> {
        let path = PathBuf::from(&self.tail.read().await.index_path_string);
        let name = path.file_name()?.to_str()?;
        let id = name.strip_prefix("segment-")?;
        (path.parent() == Some(root) && id.len() == 32
            && id.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)))
            .then_some(path)
    }
    /// Caller owns the partition and has already admitted the successor. Any
    /// failure after entry requires recovery fencing; it must not reuse a
    /// possibly sealed sole writer. Successful handoff remains private until
    /// the Record certificate selects these packages.
    pub(super) async fn seal_into_successor(
        &mut self, root: &Path, new_tail: IndexArc, ordinals: &mut Vec<String>,
        locations: &mut BTreeMap<String,usize>, budget: RotationBudget, limit: u64,
    ) -> Result<(),RecordError> {
        self.drain_indexes().await?;
        let path=self.stable_tail_path(root).await.ok_or_else(||storage("tail lacks stable package identity"))?;
        let id=path.file_name().unwrap().to_str().unwrap().strip_prefix("segment-").unwrap().to_owned();
        self.pending_seals.try_reserve(1).map_err(storage)?;
        self.tail.commit().await.map_err(storage)?;
        let native=self.tail.read().await.record_lexical[0].clone();
        native.seal().await?;
        let package=segment::Package::seal(root,&path,id,limit)?;
        self.rotate_tail(new_tail,ordinals,locations,budget).await?;
        // No fallible step after handoff. Native runtime pins in the query view
        // retain every private predecessor until replacement or recovery.
        self.pending_seals.insert(0,package);
        Ok(())
    }

    pub(super) async fn append(
        &mut self,
        key: String,
        version: u64,
        revision: u64,
        document: Document,
    ) -> Result<usize, RecordError> {
        if self.batch {
            self.initialize_async().await?;
            let local=self.reserved_rows.ok_or(RecordError::RecoveryRequired)?;
            let native=self.tail.read().await.record_lexical[0].clone();
            let identity=lexical::Identity { id:key.clone(),version,revision };
            let vector=index_consumers::Input::vector(&native.config,identity.clone(),local,&document)?;
            let lexical=index_consumers::Input::admitted(&native,identity,&document)?;
            let schema=retrieval_schema(&native.config);
            let growth=build_budget::component_estimate(&document,schema.iter().filter(|f|f.index_vector||f.facet).map(|f|f.field.as_str()))?
                .checked_add(build_budget::identity_estimate(&key)?).ok_or_else(||storage("queued component growth overflow"))?;
            self.component_bytes=self.component_bytes.checked_add(growth).ok_or_else(||storage("component admission overflow"))?;
            self.reserved_rows=Some(local.checked_add(1).ok_or_else(||storage("component ordinal overflow"))?);
            self.batch_inputs.push(lexical); self.batch_vectors.push(vector);
            return local.checked_mul(self.query.read().await.shard_number).ok_or_else(||storage("candidate address overflow"));
        }
        let shard = self.tail.read().await.shard_vec[0].clone();
        {
            let mut shard=shard.write().await;
            if let Some(memory)=&mut shard.build_reservation {
                memory.grow(build_budget::identity_estimate(&key)?).map_err(storage)?;
            }
        }
        let key_for_native = key.clone();
        let local = {
            let mut shard = shard.write().await;
            let active = shard
                .active
                .as_mut()
                .ok_or_else(|| storage("missing tail statistics"))?;
            let local = active.keys.len();
            active.keys.push(key);
            local
        };
        let native = self.tail.read().await.record_lexical[0].clone();
        let identity = lexical::Identity { id:key_for_native, version, revision };
        native.append(identity, &document, true).await?;
        self.tail
            .index_documents(vec![document])
            .await
            .map_err(storage)?;
        if self.reserved_rows.is_some() {
            self.reserved_rows=Some(local+1);
            self.component_bytes=shard.read().await.build_reservation.as_ref().map_or(0, |r|r.bytes());
        }
        local
            .checked_mul(self.query.read().await.shard_number)
            .ok_or_else(|| storage("candidate address overflow"))
    }
    async fn initialize_async(&mut self) -> Result<(),RecordError> {
        if self.reserved_rows.is_some() {return Ok(());}
        let cursors=self.cursors.clone().ok_or(RecordError::RecoveryRequired)?;
        let root=self.query.read().await;
        self.retired.clear();
        for component in &root.shard_vec {
            let mut shard=component.write().await;
            let active=shard.active.as_mut().ok_or_else(||storage("component statistics missing"))?;
            let mask=active.retired.get_or_insert_with(Default::default).clone();
            self.retired.push(mask);
        }
        drop(root);
        let root=self.tail.read().await;
        let shard=root.shard_vec[0].read().await;
        self.reserved_rows=Some(shard.indexed_doc_count);
        self.component_bytes=shard.build_reservation.as_ref().map_or(0,|r|r.bytes());
        let path=Path::new(&root.index_path_string).parent().ok_or_else(||storage("component parent missing"))?.to_owned();
        self.lexical=Some(index_consumers::Consumer::new_lexical(root.record_lexical[0].clone(),cursors.clone(),path.clone(),self.owner.clone()));
        self.vector=Some(index_consumers::Consumer::new_vector(self.tail.clone(),root.record_lexical[0].clone(),cursors,path,self.owner.clone()));
        Ok(())
    }
    pub(super) async fn begin_batch(&mut self,cursors:Arc<index_consumers::Cursors>,owner:std::sync::Weak<Mutex<Inner>>) -> Result<(),RecordError> {
        debug_assert!(!self.batch);
        if cursors.error().is_some() {return Err(RecordError::RecoveryRequired);}
        self.cursors=Some(cursors);
        self.owner=owner;
        self.initialize_async().await?;
        self.batch=true; self.batch_inputs.clear(); self.batch_vectors.clear();
        Ok(())
    }
    pub(super) fn abort_batch(&mut self) {
        self.batch=false; self.batch_inputs.clear(); self.batch_vectors.clear();
    }
    pub(super) async fn finish_batch(&mut self,sequence:u64,_cursors:Arc<index_consumers::Cursors>) -> Result<(),RecordError> {
        self.batch=false;
        self.publish_batch_prefix(sequence).await
    }
    pub(super) async fn publish_batch_prefix(&mut self,sequence:u64) -> Result<(),RecordError> {
        self.initialize_async().await?;
        self.lexical.as_ref().ok_or(RecordError::RecoveryRequired)?.submit(std::mem::take(&mut self.batch_inputs),sequence)?;
        self.vector.as_ref().ok_or(RecordError::RecoveryRequired)?.submit(std::mem::take(&mut self.batch_vectors),sequence)
    }
    pub(super) async fn drain_indexes(&self) -> Result<(),RecordError> {
        // Metadata catches up before native lag checks become strict again.
        let vector=if let Some(vector)=&self.vector {vector.drain().await}else{Ok(())};
        let lexical=if let Some(lexical)=&self.lexical {lexical.drain().await}else{Ok(())};
        vector.and(lexical)
    }
    pub(super) async fn drain_batch_prefix(&mut self,sequence:u64) -> Result<(),RecordError> {
        if self.batch && (!self.batch_inputs.is_empty() || !self.batch_vectors.is_empty()) {self.publish_batch_prefix(sequence).await?;}
        self.drain_indexes().await
    }
    pub(super) async fn retire(&self,ordinal:usize) -> Result<(),RecordError> {
        if !self.retired.is_empty() {
            return self.retired[ordinal%self.retired.len()].insert(ordinal/self.retired.len()).map_err(storage);
        }
        let root=self.query.read().await;
        let n=root.shard_number;
        let mut shard=root.shard_vec[ordinal%n].write().await;
        let local=ordinal/n;
        shard.delete_hashset.insert(local);
        shard.active.as_mut().ok_or_else(||storage("missing view statistics"))?.retire(local);
        Ok(())
    }
    #[cfg(test)]
    pub(super) async fn search(
        &self,
        text: String,
        vector: Option<Embedding>,
        query_type: QueryType,
        mode: SearchMode,
        offset: usize,
        length: usize,
        result_type: ResultType,
        fields: Vec<String>,
        filters: Vec<FacetFilter>,
        sort: Vec<ResultSort>,
        cut: lexical::Cut,
    ) -> crate::search::ResultObject {
        match lexical::runtime_query::search(&self.query,text,vector,mode,query_type,offset,length,result_type,fields,filters,sort,cut).await {
            Ok(result)=>result,
            Err(error)=>crate::search::ResultObject{error:Some(error.to_string()),..Default::default()},
        }
    }
    pub(super) async fn local_statistics(&self)->Result<Vec<crate::active::ActiveStatistics>,RecordError>{
        let root=self.query.read().await;
        let mut result=Vec::new();
        for shard in &root.shard_vec {
            result.push(shard.read().await.active.as_ref().ok_or_else(||storage("missing view statistics"))?.clone_local());
        }
        Ok(result)
    }
    pub(super) async fn statistics_wire_length(&self)->Result<usize,RecordError>{
        let root=self.query.read().await;let mut bytes=2usize;
        for shard in &root.shard_vec {
            let shard=shard.read().await;
            let active=shard.active.as_ref().ok_or_else(||storage("missing view statistics"))?;
            bytes=bytes.checked_add(checkpoint_codec::json_length(active)?).and_then(|n|n.checked_add(1)).ok_or_else(||storage("statistics size overflow"))?;
        }
        Ok(bytes)
    }
    pub(super) async fn storage_paths(&self)->Vec<PathBuf>{
        vec![PathBuf::from(&self.query.read().await.index_path_string),PathBuf::from(&self.tail.read().await.index_path_string)]
    }
    #[cfg(test)]
    pub(super) fn test_index(&self) -> &IndexArc {
        &self.query
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn cancelled_rotation_lock_wait_preserves_view_and_can_retry() {
        use super::super::tests::{config, path, query, write};
        let root = path();
        let partition = RecordPartition::create(&root, config()).await.unwrap();
        partition
            .write(write("a", "a", "alpha", ExpectedVersion::Absent))
            .await
            .unwrap();
        let expected = query(&partition, SearchMode::Lexical).await.hits[0].score;
        {
            let mut inner = partition.inner.lock().await;
            let target = root.join(format!("cache-{:032x}", rand::random::<u128>()));
            let (tail, _) = build_at(&root, &target, &inner.config, &State::default())
                .await
                .unwrap();
            let tail_shard = tail.read().await.shard_vec[0].clone();
            let held = tail_shard.write().await;
            let Inner {
                index,
                ordinals,
                locations,
                ..
            } = &mut *inner;
            let original = ordinals.clone();
            let original_locations = locations.clone();
            {
                let pending = index.rotate_tail(
                    tail.clone(),
                    ordinals,
                    locations,
                    RotationBudget {
                        max_segments: 4,
                        max_address_slots: 100,
                    },
                );
                tokio::pin!(pending);
                assert!(futures::poll!(pending.as_mut()).is_pending());
                // Drop the suspended publication before its final swap.
            }
            drop(held);
            assert_eq!(*ordinals, original);
            assert_eq!(*locations, original_locations);
            assert_eq!(index.query.read().await.shard_number, 1);
            index
                .rotate_tail(
                    tail,
                    ordinals,
                    locations,
                    RotationBudget {
                        max_segments: 4,
                        max_address_slots: 100,
                    },
                )
                .await
                .unwrap();
        }
        assert_eq!(
            query(&partition, SearchMode::Lexical).await.hits[0].score,
            expected
        );
        drop(partition);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn rejected_rotation_preserves_live_addresses_and_statistics() {
        use super::super::tests::{config, path, query, write};
        let root = path();
        let partition = RecordPartition::create(&root, config()).await.unwrap();
        partition
            .write(write("a", "a", "alpha", ExpectedVersion::Absent))
            .await
            .unwrap();
        let before = query(&partition, SearchMode::Lexical).await;
        {
            let mut inner = partition.inner.lock().await;
            let target = root.join(format!("cache-{:032x}", rand::random::<u128>()));
            let (tail, _) = build_at(&root, &target, &inner.config, &State::default())
                .await
                .unwrap();
            let Inner {
                index,
                ordinals,
                locations,
                ..
            } = &mut *inner;
            let old_ordinals = ordinals.clone();
            let old_locations = locations.clone();
            for budget in [
                RotationBudget {
                    max_segments: 1,
                    max_address_slots: 100,
                },
                RotationBudget {
                    max_segments: 4,
                    max_address_slots: 1,
                },
            ] {
                assert!(matches!(
                    index
                        .rotate_tail(tail.clone(), ordinals, locations, budget)
                        .await,
                    Err(RecordError::Rejected(_))
                ));
                assert_eq!(*ordinals, old_ordinals);
                assert_eq!(*locations, old_locations);
                assert_eq!(index.query.read().await.shard_number, 1);
            }
            index
                .rotate_tail(
                    tail,
                    ordinals,
                    locations,
                    RotationBudget {
                        max_segments: 4,
                        max_address_slots: 100,
                    },
                )
                .await
                .unwrap();
        }
        let after = query(&partition, SearchMode::Lexical).await;
        assert_eq!(before.hits[0].score, after.hits[0].score);
        partition
            .write(write("b", "b", "alpha beta", ExpectedVersion::Absent))
            .await
            .unwrap();
        assert_eq!(query(&partition, SearchMode::Lexical).await.hits.len(), 2);
        drop(partition);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn native_snapshot_scores_survive_empty_tail_rotation() {
        use super::super::tests::{config, path, query, write};
        let root = path();
        let partition = RecordPartition::create(&root, config()).await.unwrap();
        partition
            .write(write("a", "a", "alpha", ExpectedVersion::Absent))
            .await
            .unwrap();
        let expected = query(&partition, SearchMode::Lexical).await;
        {
            let mut inner = partition.inner.lock().await;
            inner.index.test_index().commit().await.unwrap();
            let target = root.join(format!("cache-{:032x}", rand::random::<u128>()));
            let (tail, _) = build_at(&root, &target, &inner.config, &State::default())
                .await
                .unwrap();
            let Inner {
                index,
                ordinals,
                locations,
                ..
            } = &mut *inner;
            index
                .rotate_tail(
                    tail,
                    ordinals,
                    locations,
                    RotationBudget {
                        max_segments: 4,
                        max_address_slots: 100,
                    },
                )
                .await
                .unwrap();
        }
        let actual = query(&partition, SearchMode::Lexical).await;
        assert_eq!(actual.hits.len(), expected.hits.len());
        assert_eq!(actual.hits[0].score, expected.hits[0].score);
        partition
            .write(write("b", "b", "alpha beta", ExpectedVersion::Absent))
            .await
            .unwrap();
        assert_eq!(query(&partition, SearchMode::Lexical).await.hits.len(), 2);
        drop(partition);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn real_record_writes_and_source_barriers_span_two_frozen_shards_and_tail() {
        use super::super::tests::{config, path, query, source_write, write};
        let root = path();
        let reference = path();
        let partition = RecordPartition::create(&root, config()).await.unwrap();
        let oracle = RecordPartition::create(&reference, config()).await.unwrap();
        async fn both(partition: &RecordPartition, oracle: &RecordPartition, request: RecordWrite) {
            partition.write(request.clone()).await.unwrap();
            oracle.write(request).await.unwrap();
        }
        async fn rotate(partition: &RecordPartition, root: &Path) {
            let mut inner = partition.inner.lock().await;
            let target = root.join(format!("cache-{:032x}", rand::random::<u128>()));
            let (tail, _) = build_at(root, &target, &inner.config, &State::default())
                .await
                .unwrap();
            let Inner {
                index,
                ordinals,
                locations,
                ..
            } = &mut *inner;
            index
                .rotate_tail(
                    tail,
                    ordinals,
                    locations,
                    RotationBudget {
                        max_segments: 8,
                        max_address_slots: 10000,
                    },
                )
                .await
                .unwrap();
        }
        both(&partition, &oracle, source_write("source", "s", &[])).await;
        both(
            &partition,
            &oracle,
            source_write("derived", "d", &[("s", 1)]),
        )
        .await;
        both(
            &partition,
            &oracle,
            write("first", "a", "alpha alpha", ExpectedVersion::Absent),
        )
        .await;
        rotate(&partition, &root).await;
        both(
            &partition,
            &oracle,
            write("second", "b", "alpha beta", ExpectedVersion::Absent),
        )
        .await;
        both(
            &partition,
            &oracle,
            write("updated", "a", "alpha new", ExpectedVersion::Match(1)),
        )
        .await;
        rotate(&partition, &root).await;
        both(
            &partition,
            &oracle,
            write("third", "c", "alpha omega", ExpectedVersion::Absent),
        )
        .await;
        let mut delete = write("source-delete", "s", "", ExpectedVersion::Match(1));
        delete.document = None;
        both(&partition, &oracle, delete).await;
        assert!(partition.get("d").await.unwrap().is_none());
        for mode in [
            SearchMode::Lexical,
            SearchMode::Vector {
                similarity_threshold: None,
                ann_mode: crate::vector_similarity::AnnMode::All,
            },
            SearchMode::Hybrid {
                similarity_threshold: None,
                ann_mode: crate::vector_similarity::AnnMode::All,
            },
        ] {
            let actual = query(&partition, mode.clone()).await;
            let expected = query(&oracle, mode).await;
            let hits = |result: RecordSearch| {
                result
                    .hits
                    .into_iter()
                    .map(|hit| (hit.record.record_id, hit.record.version, hit.score))
                    .collect::<Vec<_>>()
            };
            assert_eq!(hits(actual), hits(expected));
        }
        assert_eq!(
            partition.statistics().await.unwrap().active_records,
            oracle.statistics().await.unwrap().active_records
        );
        assert_eq!(
            partition.statistics().await.unwrap().physical_versions,
            oracle.statistics().await.unwrap().physical_versions
        );
        drop(partition);
        drop(oracle);
        std::fs::remove_dir_all(root).unwrap();
        std::fs::remove_dir_all(reference).unwrap();
    }
}
