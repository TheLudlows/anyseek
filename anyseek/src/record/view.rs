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
}
impl RetrievalView {
    pub(super) async fn can_admit(&self,bytes:u64,native_updates:usize)->bool {
        let root=self.tail.read().await;
        for native in &root.record_lexical {
            if native.needs_rotation(native_updates).await {return false;}
        }
        let shard=root.shard_vec[0].read().await;
        shard.build_reservation.as_ref().is_some_and(|r|r.can_grow(bytes))
    }

    pub(super) fn single(index: IndexArc) -> Self {
        Self {
            query: index.clone(),
            tail: index,
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
        Ok(())
    }

    pub(super) async fn append(
        &self,
        key: String,
        version: u64,
        revision: u64,
        document: Document,
    ) -> Result<usize, RecordError> {
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
        native.append(lexical::Identity { id:key_for_native, version, revision }, &document, true).await?;
        self.tail
            .index_documents(vec![document])
            .await
            .map_err(storage)?;
        local
            .checked_mul(self.query.read().await.shard_number)
            .ok_or_else(|| storage("candidate address overflow"))
    }
    pub(super) async fn retire(&self, ordinal: usize) -> Result<(), RecordError> {
        let root = self.query.read().await;
        let n = root.shard_number;
        let mut shard = root.shard_vec[ordinal % n].write().await;
        let local = ordinal / n;
        shard.delete_hashset.insert(local);
        shard
            .active
            .as_mut()
            .ok_or_else(|| storage("missing view statistics"))?
            .retire(local);
        Ok(())
    }
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
    pub(super) async fn with_filter<T>(
        &self,
        locations: &BTreeMap<String, usize>,
        filters: &[FacetFilter],
        action: impl FnOnce(&dyn Fn(&str) -> bool) -> Result<T, RecordError>,
    ) -> Result<T, RecordError> {
        let root = self.query.read().await;
        let shards =
            futures::future::join_all(root.shard_vec.iter().map(|shard| shard.read())).await;
        let compiled = shards
            .iter()
            .map(|shard| {
                crate::metadata_filter::compile_filters(shard, filters)
                    .map_err(RecordError::Rejected)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let n = shards.len();
        action(&|id| {
            locations.get(id).is_some_and(|ordinal| {
                !crate::metadata_filter::is_facet_filter(
                    &shards[*ordinal % n],
                    &compiled[*ordinal % n],
                    *ordinal / n,
                )
            })
        })
    }
    pub(super) async fn local_statistics(&self)->Result<Vec<crate::active::ActiveStatistics>,RecordError>{
        let root=self.query.read().await;
        let mut result=Vec::new();
        for shard in &root.shard_vec {
            result.push(shard.read().await.active.as_ref().ok_or_else(||storage("missing view statistics"))?.clone_local());
        }
        Ok(result)
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
