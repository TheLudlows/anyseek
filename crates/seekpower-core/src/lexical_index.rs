use ahash::{AHashMap, AHashSet};
use seekpower_error::Result;
use seekpower_posting::{PostingCodec, PostingEntry, PostingListBlock};
use seekpower_ranking::{compute_idf, Bm25fScorer, ResultType, SearchResult};
use seekpower_schema::{FieldType, IndexConfig, IndexStats, SchemaMap, TYPE_DICT, TYPE_IDX, TYPE_DELL, TYPE_LEN};
use seekpower_storage::{KvStore, TermDictionary, TermId};
use seekpower_tokenizer::{TermObject, TokenizerRegistry};

use crate::write_buffer::WriteBuffer;

/// Business key for term dictionary: term_id (u32 LE)
fn term_dict_key(term_id: TermId) -> Vec<u8> {
    term_id.to_le_bytes().to_vec()
}

/// Business key for posting list: term_id (u32 LE)
fn posting_key(term_id: TermId) -> Vec<u8> {
    let key = term_id.to_le_bytes();
    key.to_vec()
}

/// Key for storing the next_term_id in the dictionary
const NEXT_TERM_ID_KEY: &[u8] = b"__next_term_id__";

/// Load next_term_id from storage
async fn load_next_term_id(db: &KvStore, index_id: u32) -> TermId {
    match db.prefixed_get(index_id, TYPE_DICT, NEXT_TERM_ID_KEY).await {
        Ok(Some(bytes)) if bytes.len() == 4 => {
            let arr: [u8; 4] = bytes.try_into().unwrap_or([0u8; 4]);
            TermId::from_le_bytes(arr)
        }
        _ => 0,
    }
}

/// Save next_term_id to storage
async fn save_next_term_id(db: &KvStore, index_id: u32, next_id: TermId) -> Result<()> {
    db.prefixed_put(index_id, TYPE_DICT, NEXT_TERM_ID_KEY, &next_id.to_le_bytes()).await
}

pub struct LexicalIndex {
    db: KvStore,
    index_id: u32,
    config: IndexConfig,
    schema: SchemaMap,
    field_names: Vec<String>,
    term_dictionary: tokio::sync::RwLock<TermDictionary>,
    buffer: tokio::sync::RwLock<WriteBuffer>,
    delete_set: tokio::sync::RwLock<AHashSet<u64>>,
}

/// Load term dictionary from storage.
/// Scans all TYPE_DICT entries and reconstructs the term <-> term_id mapping.
/// Uses persisted next_term_id to continue allocation.
async fn load_term_dictionary(db: &KvStore, index_id: u32) -> Result<TermDictionary> {
    let mut dict = TermDictionary::default();
    let entries = db.prefixed_scan_all(index_id, TYPE_DICT).await?;

    // Load persisted next_term_id first
    dict.next_id = load_next_term_id(db, index_id).await;

    // Key format: term_id (u32 LE) for term entries
    // Value format: term bytes
    for (key, term) in entries {
        // Skip the special next_term_id key
        if key == NEXT_TERM_ID_KEY {
            continue;
        }

        if key.len() == 4 {
            let term_id_bytes: [u8; 4] = key.as_slice().try_into()
                .map_err(|_| seekpower_error::SeekPowerError::StorageError(
                    format!("Invalid term_id key length: {}", key.len())
                ))?;
            let term_id = TermId::from_le_bytes(term_id_bytes);

            // Insert mappings
            dict.term_to_id.insert(term.clone(), term_id);
            dict.id_to_term.insert(term_id, term);
        }
    }

    Ok(dict)
}

impl LexicalIndex {
    /// Create a new LexicalIndex with an empty term dictionary.
    pub fn new(
        db: KvStore,
        index_id: u32,
        config: IndexConfig,
        schema: SchemaMap,
        field_names: Vec<String>,
    ) -> Self {
        let term_dictionary = tokio::sync::RwLock::new(TermDictionary::default());
        Self {
            db,
            index_id,
            config,
            schema,
            field_names,
            term_dictionary,
            buffer: tokio::sync::RwLock::new(WriteBuffer::default()),
            delete_set: tokio::sync::RwLock::new(AHashSet::new()),
        }
    }

    /// Open an existing LexicalIndex, loading the term dictionary from storage.
    pub async fn open(
        db: KvStore,
        index_id: u32,
        config: IndexConfig,
        schema: SchemaMap,
        field_names: Vec<String>,
    ) -> Result<Self> {
        let term_dictionary = load_term_dictionary(&db, index_id).await?;
        Ok(Self {
            db,
            index_id,
            config,
            schema,
            field_names,
            term_dictionary: tokio::sync::RwLock::new(term_dictionary),
            buffer: tokio::sync::RwLock::new(WriteBuffer::default()),
            delete_set: tokio::sync::RwLock::new(AHashSet::new()),
        })
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
    pub async fn index_text_field(
        &self,
        text: &str,
        field_id: u16,
        doc_id: u64,
        buffer: &mut WriteBuffer,
    ) -> Result<u16> {
        let registry = TokenizerRegistry::global();
        let registry_guard = registry.read().unwrap();

        let tokenizer_name = match &self.config.tokenizer {
            seekpower_schema::TokenizerType::Simple => "simple",
            seekpower_schema::TokenizerType::Unicode => "unicode",
            seekpower_schema::TokenizerType::Jieba => "jieba",
            seekpower_schema::TokenizerType::Custom(name) => name.as_str(),
        };

        if registry_guard.get(tokenizer_name).is_none() {
            return Ok(0);
        }

        let terms = seekpower_tokenizer::tokenize_index(
            text, field_id, &self.config, &registry_guard,
        );

        let mut token_count: u16 = 0;
        let mut dict = self.term_dictionary.write().await;
        for term in terms {
            let term_id = dict.get_or_create(term.term.as_bytes())
                .map_err(|e| seekpower_error::SeekPowerError::StorageError(format!("Failed to create term_id: {}", e)))?;
            let entry = buffer.postings.entry(term_id).or_default();
            entry.push(PostingEntry {
                doc_id,
                field_id,
                positions: term.positions.clone(),
            });
            token_count += term.positions.len() as u16;
        }
        Ok(token_count)
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
        let id = self.index_id;

        // First, flush term dictionary (save any new terms added)
        // For now, we'll write the dictionary entries individually
        // TODO: Batch write all dictionary entries
        let dict = self.term_dictionary.read().await;
        for (term, term_id) in &dict.term_to_id {
            let key = term_dict_key(*term_id);
            self.db.prefixed_put(id, TYPE_DICT, &key, term).await?;
        }

        // Write deletion markers.
        for &doc_id in &old_buffer.deletions {
            self.db.prefixed_put(id, TYPE_DELL, &doc_id.to_le_bytes(), b"1").await?;
        }

        // Write posting lists by term_id instead of term_hash
        for (&term_id, new_entries) in &old_buffer.postings {
            let key = posting_key(term_id);

            let existing_block = match self.db.prefixed_get(id, TYPE_IDX, &key).await? {
                Some(data) => Some(PostingCodec::deserialize(&data)?),
                None => None,
            };

            let block = match existing_block {
                Some(existing) => seekpower_posting::merge_postings(&existing, new_entries),
                None => PostingCodec::encode(new_entries),
            };

            let serialized = PostingCodec::serialize(&block);
            self.db.prefixed_put(id, TYPE_IDX, &key, &serialized).await?;
        }

        // Write document lengths.
        for (&doc_id, lengths) in &old_buffer.doc_lengths {
            for &(field_id, len) in lengths {
                let key = seekpower_schema::make_key(&[&doc_id.to_le_bytes(), &field_id.to_le_bytes()]);
                self.db.prefixed_put(id, TYPE_LEN, &key, &[len as u8]).await?;
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

        // Persist next_term_id for recovery
        save_next_term_id(&self.db, id, dict.next_id).await?;

        Ok(())
    }

    /// Load a posting list from the KV store by term_id.
    pub async fn load_posting_by_term_id(&self, term_id: TermId) -> Result<Option<PostingListBlock>> {
        let key = posting_key(term_id);
        if let Some(data) = self.db.prefixed_get(self.index_id, TYPE_IDX, &key).await? {
            let block = PostingCodec::deserialize(&data)?;
            return Ok(Some(block));
        }
        Ok(None)
    }

    /// Compute key_hash for a term in a specific field (kept for compatibility, but not used with term_id).
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

    /// Get term_id for a term, creating a new mapping if not exists.
    async fn get_term_id(&self, term: &[u8]) -> Result<TermId> {
        let mut dict = self.term_dictionary.write().await;
        dict.get_or_create(term)
            .map_err(|e| seekpower_error::SeekPowerError::StorageError(format!("Failed to create term_id: {}", e)))
    }

    /// Get term for a term_id if exists.
    async fn get_term(&self, term_id: TermId) -> Option<Vec<u8>> {
        let dict = self.term_dictionary.read().await;
        dict.get_term(term_id).map(|v| v.to_vec())
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
            let term_id = match self.get_term_id(term.term.as_bytes()).await {
                Ok(id) => id,
                Err(_) => {
                    term_field_postings.push(field_postings);
                    continue;
                }
            };

            for (field_id, field_name) in self.field_names.iter().enumerate() {
                let field_schema = match self.schema.get(field_name) {
                    Some(s) => s,
                    None => continue,
                };
                if !field_schema.index_lexical || field_schema.field_type != FieldType::Text {
                    continue;
                }

                let fid = field_id as u16;
                if let Some(block) = self.load_posting_by_term_id(term_id).await? {
                    let entries = PostingCodec::decode(&block);
                    // Filter entries by field_id
                    let filtered: Vec<_> = entries.into_iter().filter(|e| e.field_id == fid).collect();
                    field_postings.entry(fid).or_default().extend(filtered);
                }
            }

            // Also check buffer postings
            {
                let buffer = self.buffer.read().await;
                if let Some(entries) = buffer.postings.get(&term_id) {
                    for (field_id, field_name) in self.field_names.iter().enumerate() {
                        let field_schema = match self.schema.get(field_name) {
                            Some(s) => s,
                            None => continue,
                        };
                        if !field_schema.index_lexical || field_schema.field_type != FieldType::Text {
                            continue;
                        }
                        let fid = field_id as u16;
                        // Filter buffer entries by field_id
                        let filtered: Vec<_> = entries.iter().filter(|e| e.field_id == fid).cloned().collect();
                        field_postings.entry(fid).or_default().extend(filtered);
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
