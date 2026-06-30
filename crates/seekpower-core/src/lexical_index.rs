use ahash::{AHashMap, AHashSet};
use seekpower_error::Result;
use seekpower_posting::{PostingCodec, PostingEntry, PostingListBlock};
use seekpower_ranking::{compute_idf, Bm25fScorer, ResultType, SearchResult};
use seekpower_schema::{make_key, PREFIX_DELL, PREFIX_IDX, PREFIX_LEN, FieldType, IndexConfig, IndexStats, SchemaMap};
use seekpower_storage::Cache;
use seekpower_tokenizer::{TermObject, TokenizerRegistry};

use crate::write_buffer::WriteBuffer;

type ReadCache = Cache;

pub struct LexicalIndex {
    db: crate::index::PrefixedKvStore,
    config: IndexConfig,
    schema: SchemaMap,
    field_names: Vec<String>,
    buffer: tokio::sync::RwLock<WriteBuffer>,
    cache: ReadCache,
    delete_set: tokio::sync::RwLock<AHashSet<u64>>,
}

impl LexicalIndex {
    pub fn new(
        db: crate::index::PrefixedKvStore,
        config: IndexConfig,
        schema: SchemaMap,
        field_names: Vec<String>,
        cache: ReadCache,
        delete_set: AHashSet<u64>,
    ) -> Self {
        Self {
            db,
            config,
            schema,
            field_names,
            buffer: tokio::sync::RwLock::new(WriteBuffer::default()),
            cache,
            delete_set: tokio::sync::RwLock::new(delete_set),
        }
    }

    pub fn add_posting(&self, _term_hash: u64, _entry: PostingEntry) {
        // Intentionally left empty — use buffer_write() + index_text_field() instead.
    }

    /// Acquire a write lock on the internal buffer.
    pub async fn buffer_write(&self) -> tokio::sync::RwLockWriteGuard<'_, WriteBuffer> {
        self.buffer.write().await
    }

    /// Tokenize a text field and add the resulting postings to the buffer.
    /// Returns the token count for this field.
    pub fn index_text_field(
        &self,
        text: &str,
        field_id: u16,
        doc_id: u64,
        buffer: &mut WriteBuffer,
    ) -> u16 {
        let registry = TokenizerRegistry::global();
        let registry_guard = registry.read().unwrap();

        let tokenizer_name = match &self.config.tokenizer {
            seekpower_schema::TokenizerType::Simple => "simple",
            seekpower_schema::TokenizerType::Unicode => "unicode",
            seekpower_schema::TokenizerType::Jieba => "jieba",
            seekpower_schema::TokenizerType::Custom(name) => name.as_str(),
        };

        if registry_guard.get(tokenizer_name).is_none() {
            return 0;
        }

        let terms = seekpower_tokenizer::tokenize_index(
            text, field_id, &self.config, &registry_guard,
        );

        let mut token_count: u16 = 0;
        for term in terms {
            let entry = buffer.postings.entry(term.key_hash).or_default();
            entry.push(PostingEntry {
                doc_id,
                field_id,
                positions: term.positions.clone(),
            });
            token_count += term.positions.len() as u16;
        }
        token_count
    }

    /// Mark a document as deleted.
    pub async fn delete_document(&self, doc_id: u64) {
        self.buffer.write().await.deletions.insert(doc_id);
        self.delete_set.write().await.insert(doc_id);
    }

    /// Get the current delete set.
    pub async fn delete_set(&self) -> AHashSet<u64> {
        self.delete_set.read().await.clone()
    }

    /// Check if the buffer has any postings (used to short-circuit empty searches).
    pub async fn has_buffer_postings(&self) -> bool {
        !self.buffer.read().await.postings.is_empty()
    }

    /// Get a read lock on the buffer postings.
    pub async fn buffer_postings(&self) -> tokio::sync::RwLockReadGuard<'_, WriteBuffer> {
        self.buffer.read().await
    }

    /// Take the entire write buffer atomically (for commit).
    pub async fn take_buffer(&self) -> WriteBuffer {
        let mut buf = self.buffer.write().await;
        std::mem::take(&mut *buf)
    }

    /// Flush buffered postings, deletions, and doc lengths to the KvStore.
    pub async fn flush(&self, old_buffer: &WriteBuffer, stats: &mut IndexStats) -> Result<()> {
        // Write deletion markers.
        for &doc_id in &old_buffer.deletions {
            let key = make_key(&[PREFIX_DELL, b":", &doc_id.to_le_bytes()]);
            self.db.put(&key, b"1").await?;
        }

        // Write posting lists: read-modify-write.
        for (&term_hash, new_entries) in &old_buffer.postings {
            let key = make_key(&[PREFIX_IDX, b":", &term_hash.to_le_bytes()]);

            let existing_block = match self.cache.get(&key) {
                Some(data) => Some(PostingCodec::deserialize(&data)?),
                None => match self.db.get(&key).await? {
                    Some(data) => {
                        let block = PostingCodec::deserialize(&data)?;
                        self.cache.insert(&key, data);
                        Some(block)
                    }
                    None => None,
                }
            };

            let block = match existing_block {
                Some(existing) => seekpower_posting::merge_postings(&existing, new_entries),
                None => PostingCodec::encode(new_entries),
            };

            let serialized = PostingCodec::serialize(&block);
            self.cache.insert(&key, serialized.clone());
            self.db.put(&key, &serialized).await?;
        }

        // Write document lengths.
        for (&doc_id, lengths) in &old_buffer.doc_lengths {
            for &(field_id, len) in lengths {
                let key = make_key(&[PREFIX_LEN, b":doc_len:", &doc_id.to_le_bytes(), b":", &field_id.to_le_bytes()]);
                self.db.put(&key, &[len as u8]).await?;
            }
        }

        // Update stats with doc lengths.
        let mut total_len = 0u64;
        for lengths in old_buffer.doc_lengths.values() {
            for (_, len) in lengths {
                total_len += *len as u64;
            }
        }
        stats.total_doc_length += total_len;
        if stats.doc_count > 0 {
            stats.avg_doc_length = stats.total_doc_length as f32 / stats.doc_count as f32;
        }

        Ok(())
    }

    /// Load a posting list from cache or KV store.
    pub async fn load_posting(&self, key: &[u8]) -> Result<Option<PostingListBlock>> {
        if let Some(data) = self.cache.get(key) {
            return Ok(Some(PostingCodec::deserialize(&data)?));
        }
        if let Some(data) = self.db.get(key).await? {
            let block = PostingCodec::deserialize(&data)?;
            self.cache.insert(key, data);
            return Ok(Some(block));
        }
        Ok(None)
    }

    /// Compute key_hash for a term in a specific field.
    pub fn compute_key_hash(field_id: u16, term: &str) -> u64 {
        use std::hash::Hasher;
        let mut hasher = ahash::AHasher::default();
        hasher.write(&field_id.to_le_bytes());
        hasher.write(term.as_bytes());
        hasher.finish()
    }

    /// Get field boost for a field_id.
    pub fn field_boost(&self, field_id: u16) -> f32 {
        self.field_names
            .get(field_id as usize)
            .and_then(|name| self.schema.get(name))
            .map(|s| s.boost)
            .unwrap_or(1.0)
    }

    /// Get average doc length from stats.
    pub fn avg_doc_length(stats: &IndexStats, _field_id: u16) -> f32 {
        if stats.doc_count == 0 {
            return 1.0;
        }
        let avg = stats.total_doc_length as f32 / stats.doc_count as f32;
        if avg < 1.0 { 1.0 } else { avg }
    }

    /// Perform a lexical search across all fields with BM25F scoring.
    pub async fn search(
        &self,
        query_terms: &[TermObject],
        query_type: seekpower_search::QueryType,
        stats: &IndexStats,
        k: usize,
    ) -> Result<Vec<SearchResult>> {
        let total_docs = stats.doc_count;
        let scorer = Bm25fScorer::new();

        if total_docs == 0 && !self.has_buffer_postings().await {
            return Ok(Vec::new());
        }

        let delete_set = self.delete_set.read().await.clone();

        // Collect postings per term, per field
        let mut term_field_postings: Vec<AHashMap<u16, Vec<PostingEntry>>> = Vec::new();

        for term in query_terms {
            let mut field_postings: AHashMap<u16, Vec<PostingEntry>> = AHashMap::new();

            for (field_id, field_name) in self.field_names.iter().enumerate() {
                let field_schema = match self.schema.get(field_name) {
                    Some(s) => s,
                    None => continue,
                };
                if !field_schema.index_lexical || field_schema.field_type != FieldType::Text {
                    continue;
                }

                let fid = field_id as u16;
                let key_hash = Self::compute_key_hash(fid, &term.term);
                let key = make_key(&[PREFIX_IDX, b":", &key_hash.to_le_bytes()]);

                if let Some(block) = self.load_posting(&key).await? {
                    let entries = PostingCodec::decode(&block);
                    field_postings.entry(fid).or_default().extend(entries);
                }
            }

            // Also check buffer postings
            {
                let buffer = self.buffer.read().await;
                for (bh, entries) in &buffer.postings {
                    for (field_id, field_name) in self.field_names.iter().enumerate() {
                        let field_schema = match self.schema.get(field_name) {
                            Some(s) => s,
                            None => continue,
                        };
                        if !field_schema.index_lexical || field_schema.field_type != FieldType::Text {
                            continue;
                        }
                        let fid = field_id as u16;
                        let key_hash = Self::compute_key_hash(fid, &term.term);
                        if *bh == key_hash {
                            field_postings.entry(fid).or_default().extend(entries.iter().cloned());
                        }
                    }
                }
            }

            term_field_postings.push(field_postings);
        }

        match query_type {
            seekpower_search::QueryType::Union => {
                let mut doc_scores: AHashMap<u64, f32> = AHashMap::new();

                for field_postings in &term_field_postings {
                    for (field_id, entries) in field_postings {
                        let boost = self.field_boost(*field_id);
                        let avgdl = Self::avg_doc_length(stats, *field_id);
                        let df = entries.len() as u32;
                        let idf = compute_idf(total_docs, df);

                        for entry in entries {
                            if delete_set.contains(&entry.doc_id) { continue; }
                            let tf = entry.positions.len() as u32;
                            let score = scorer.score_field(tf, avgdl, avgdl, boost, idf);
                            *doc_scores.entry(entry.doc_id).or_insert(0.0) += score;
                        }
                    }
                }

                let mut results: Vec<SearchResult> = doc_scores
                    .into_iter()
                    .map(|(doc_id, score)| SearchResult {
                        doc_id,
                        score,
                        result_type: ResultType::Lexical,
                    })
                    .collect();
                results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
                results.truncate(k);
                Ok(results)
            }

            seekpower_search::QueryType::Intersection => {
                if term_field_postings.is_empty() {
                    return Ok(Vec::new());
                }

                let term_doc_sets = self.collect_term_doc_sets(&term_field_postings, &delete_set);

                let mut candidates: AHashSet<u64> = term_doc_sets[0].clone();
                for set in &term_doc_sets[1..] {
                    candidates = candidates.intersection(set).copied().collect();
                }

                let results = self.score_candidates(
                    &candidates, &term_field_postings, stats, &scorer, total_docs,
                );
                Ok(results)
            }

            seekpower_search::QueryType::Phrase => {
                if term_field_postings.is_empty() {
                    return Ok(Vec::new());
                }

                let term_doc_sets = self.collect_term_doc_sets(&term_field_postings, &delete_set);

                let mut candidates: AHashSet<u64> = term_doc_sets[0].clone();
                for set in &term_doc_sets[1..] {
                    candidates = candidates.intersection(set).copied().collect();
                }

                let mut results: Vec<SearchResult> = Vec::new();
                for doc_id in candidates {
                    if self.check_phrase_match(&term_field_postings, doc_id) {
                        let scored = self.score_single(
                            doc_id, &term_field_postings, stats, &scorer, total_docs,
                        );
                        results.push(scored);
                    }
                }

                results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
                results.truncate(k);
                Ok(results)
            }
        }
    }

    fn collect_term_doc_sets(
        &self,
        term_field_postings: &[AHashMap<u16, Vec<PostingEntry>>],
        delete_set: &AHashSet<u64>,
    ) -> Vec<AHashSet<u64>> {
        term_field_postings
            .iter()
            .map(|field_postings| {
                let mut doc_ids = AHashSet::new();
                for entries in field_postings.values() {
                    for entry in entries {
                        if !delete_set.contains(&entry.doc_id) {
                            doc_ids.insert(entry.doc_id);
                        }
                    }
                }
                doc_ids
            })
            .collect()
    }

    fn score_candidates(
        &self,
        candidates: &AHashSet<u64>,
        term_field_postings: &[AHashMap<u16, Vec<PostingEntry>>],
        stats: &IndexStats,
        scorer: &Bm25fScorer,
        total_docs: u64,
    ) -> Vec<SearchResult> {
        let mut results: Vec<SearchResult> = candidates
            .iter()
            .map(|&doc_id| self.score_single(doc_id, term_field_postings, stats, scorer, total_docs))
            .collect();
        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    fn score_single(
        &self,
        doc_id: u64,
        term_field_postings: &[AHashMap<u16, Vec<PostingEntry>>],
        stats: &IndexStats,
        scorer: &Bm25fScorer,
        total_docs: u64,
    ) -> SearchResult {
        let mut total_score = 0.0f32;
        for field_postings in term_field_postings {
            for (field_id, entries) in field_postings {
                let boost = self.field_boost(*field_id);
                let avgdl = Self::avg_doc_length(stats, *field_id);
                let df = entries.len() as u32;
                let idf = compute_idf(total_docs, df);

                if let Some(entry) = entries.iter().find(|e| e.doc_id == doc_id) {
                    let tf = entry.positions.len() as u32;
                    total_score += scorer.score_field(tf, avgdl, avgdl, boost, idf);
                }
            }
        }
        SearchResult {
            doc_id,
            score: total_score,
            result_type: ResultType::Lexical,
        }
    }

    fn check_phrase_match(
        &self,
        term_field_postings: &[AHashMap<u16, Vec<PostingEntry>>],
        doc_id: u64,
    ) -> bool {
        for (field_id, _field_name) in self.field_names.iter().enumerate() {
            let fid = field_id as u16;

            let mut position_lists: Vec<Vec<u16>> = Vec::new();
            for field_postings in term_field_postings {
                if let Some(entries) = field_postings.get(&fid) {
                    if let Some(entry) = entries.iter().find(|e| e.doc_id == doc_id) {
                        position_lists.push(entry.positions.clone());
                    } else {
                        position_lists.push(Vec::new());
                    }
                } else {
                    position_lists.push(Vec::new());
                }
            }

            if position_lists.iter().any(|p| p.is_empty()) {
                continue;
            }

            let first_positions = &position_lists[0];
            'outer: for &start_pos in first_positions {
                for (offset, pos_list) in position_lists.iter().enumerate().skip(1) {
                    if !pos_list.contains(&(start_pos + offset as u16)) {
                        continue 'outer;
                    }
                }
                return true;
            }
        }
        false
    }
}
