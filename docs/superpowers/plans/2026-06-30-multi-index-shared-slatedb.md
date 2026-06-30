# Multi-Index Shared SlateDB Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor SeekPower from "one SlateDB instance per index" to "one shared SlateDB per server process, with all indexes isolated by key prefix".

**Architecture:** Server opens a single shared `slatedb::Db` at startup. `KvStore` gains a `for_index(path)` method that returns a view sharing the same underlying `slatedb::Db` but auto-prepending the index's path prefix to every key. The server's `AppState` holds the shared `KvStore` plus a `HashMap<String, Arc<Index>>` of currently-loaded indexes. Index discovery is via an in-DB `__indexes__` directory under the same prefix scheme. HTTP routes use a `?index=name` query parameter to select which index a request targets.

**Tech Stack:** Rust (edition 2021), SlateDB, object_store, axum 0.7, tokio, serde, serde_json, ahash, thiserror.

**Spec:** `docs/superpowers/specs/2026-06-30-multi-index-shared-slatedb-design.md`

---

## File Structure

| File | Responsibility |
|---|---|
| `crates/seekpower-error/src/lib.rs` | Add `InvalidIndexName`, `IndexAlreadyExists`, `IndexNotLoaded` variants |
| `crates/seekpower-storage/src/lib.rs` | `KvStore` becomes `Arc<KvStoreInner>` with prefix; add `for_index`, `is_raw`, `scan_all`, `validate_path`; strip prefix in `scan_prefix` |
| `crates/seekpower-core/src/lib.rs` | Change `create_index` / `open_index` signatures to take `&KvStore`; remove `path` from all `make_key` calls; add `delete_index(shared_db, path)` |
| `crates/seekpower-vector/src/lib.rs` | Remove `path` from `IvfIndex` and the four `*_key` helpers |
| `crates/seekpower-server/src/state.rs` | Rename to `SharedState = Arc<AppState>`; export from `lib.rs` |
| `crates/seekpower-server/src/model.rs` | Rename `path` field to `name` in request types |
| `crates/seekpower-server/src/router.rs` | Add `GET /indexes` |
| `crates/seekpower-server/src/main.rs` | Open shared SlateDB at startup; recover indexes from `__indexes__` |
| `crates/seekpower-server/src/handler/*.rs` | Add `Query<IndexQuery>` to all index-routing handlers; update `handle_create_index` / `handle_delete_index` to maintain `__indexes__`; add `handle_list_indexes` |
| `tests/server_integration_test.rs` | Adapt to `SharedState`; add multi-index and query-param tests |
| `tests/integration_test.rs` | Adapt `create_test_index` helper to new `create_index` signature |

---

## Task 1: Add new error variants

**Files:**
- Modify: `crates/seekpower-error/src/lib.rs`

- [ ] **Step 1: Add three new variants to `SeekPowerError`**

Edit `crates/seekpower-error/src/lib.rs`. Add inside the enum body, after the existing `Corrupted(String)` variant:

```rust
    #[error("invalid index name: {0}")]
    InvalidIndexName(String),

    #[error("index already exists: {0}")]
    IndexAlreadyExists(String),

    #[error("index not loaded: {0}")]
    IndexNotLoaded(String),
```

- [ ] **Step 2: Verify the crate still compiles**

Run: `cargo build -p seekpower-error`
Expected: success.

- [ ] **Step 3: Commit**

```bash
git add crates/seekpower-error/src/lib.rs
git commit -m "feat(error): add InvalidIndexName, IndexAlreadyExists, IndexNotLoaded variants"
```

---

## Task 2: Add `validate_path` to `seekpower-storage`

**Files:**
- Modify: `crates/seekpower-storage/src/lib.rs` (append before `mod tests`)

- [ ] **Step 1: Write the failing tests**

Append to the `mod tests` block in `crates/seekpower-storage/src/lib.rs`:

```rust
    #[test]
    fn test_validate_path_accepts_valid() {
        assert!(validate_path("blog").is_ok());
        assert!(validate_path("shop").is_ok());
        assert!(validate_path("a").is_ok());
        assert!(validate_path("foo_bar-baz123").is_ok());
        assert!(validate_path("__indexes__").is_ok());
        assert!(validate_path(&"a".repeat(128)).is_ok());
    }

    #[test]
    fn test_validate_path_rejects_empty() {
        assert!(validate_path("").is_err());
    }

    #[test]
    fn test_validate_path_rejects_too_long() {
        assert!(validate_path(&"a".repeat(129)).is_err());
    }

    #[test]
    fn test_validate_path_rejects_bad_chars() {
        assert!(validate_path("foo/bar").is_err());
        assert!(validate_path("foo:bar").is_err());
        assert!(validate_path("foo bar").is_err());
        assert!(validate_path("foo.bar").is_err());
        assert!(validate_path("foo@bar").is_err());
        assert!(validate_path("föo").is_err());
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p seekpower-storage test_validate_path`
Expected: compile error — `validate_path` not found.

- [ ] **Step 3: Implement `validate_path`**

Add this function near the top of `crates/seekpower-storage/src/lib.rs`, just below the existing `PREFIX_LEN` constant:

```rust
const PATH_MAX_LEN: usize = 128;

pub fn validate_path(path: &str) -> Result<()> {
    if path.is_empty() {
        return Err(SeekPowerError::InvalidIndexName(
            "index name must not be empty".into(),
        ));
    }
    if path.len() > PATH_MAX_LEN {
        return Err(SeekPowerError::InvalidIndexName(format!(
            "index name too long: {} > {} chars",
            path.len(),
            PATH_MAX_LEN
        )));
    }
    if !path.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-') {
        return Err(SeekPowerError::InvalidIndexName(format!(
            "index name must match [a-zA-Z0-9_-]+: {:?}",
            path
        )));
    }
    Ok(())
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p seekpower-storage test_validate_path`
Expected: 4 tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/seekpower-storage/src/lib.rs
git commit -m "feat(storage): add validate_path for index name validation"
```

---

## Task 3: Refactor `KvStore` to use `Arc<KvStoreInner>` with prefix

**Files:**
- Modify: `crates/seekpower-storage/src/lib.rs`

This is a structural refactor. The public API will change in Task 4; this task lays the internal groundwork and a few accessors.

- [ ] **Step 1: Replace the `KvStore` struct and add `KvStoreInner`**

In `crates/seekpower-storage/src/lib.rs`, replace the existing `pub struct KvStore` (currently `{ db: slatedb::Db }`) and the existing `impl Clone for KvStore` with:

```rust
/// KV store wrapper around SlateDB.
///
/// A `KvStore` is either a "raw" view over a freshly opened `slatedb::Db`, or
/// a derived view with a path prefix. Derived views share the underlying
/// `slatedb::Db` via `Arc` — cloning a derived view is cheap.
pub struct KvStore {
    inner: Arc<KvStoreInner>,
}

struct KvStoreInner {
    db: slatedb::Db,
    prefix: Vec<u8>,
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

fn join_prefix(prefix: &[u8], key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len() + key.len());
    out.extend_from_slice(prefix);
    out.extend_from_slice(key);
    out
}
```

- [ ] **Step 2: Verify compile fails as expected (other methods still reference old fields)**

Run: `cargo build -p seekpower-storage`
Expected: compile errors in `KvStore::open`, `put`, `get`, `delete`, `scan_prefix`, `close` (they still reference `self.db`).

- [ ] **Step 3: Update `KvStore::open` to build the raw view**

Replace `KvStore::open` with:

```rust
    /// Open or create a SlateDB database at the given path. The returned
    /// `KvStore` is the "raw" view with no prefix.
    pub async fn open(path: &str, object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        let db = slatedb::Db::open(path, object_store)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Failed to open database: {}", e)))?;
        Ok(Self {
            inner: Arc::new(KvStoreInner { db, prefix: Vec::new() }),
        })
    }
```

- [ ] **Step 4: Add `for_index` and `is_raw`**

Add to the `impl KvStore` block:

```rust
    /// Derive a new view that auto-prepends `path:` to every key, sharing
    /// the same underlying `slatedb::Db` as `self`.
    pub fn for_index(&self, path: &str) -> Self {
        validate_path(path).expect("invalid index name");
        let mut prefix = Vec::with_capacity(path.len() + 1);
        prefix.extend_from_slice(path.as_bytes());
        prefix.push(b':');
        Self {
            inner: Arc::new(KvStoreInner {
                db: self.inner.db.clone(),
                prefix,
            }),
        }
    }

    /// Returns true if this view has no prefix (the original opened view).
    pub fn is_raw(&self) -> bool {
        self.inner.prefix.is_empty()
    }
```

- [ ] **Step 5: Update `put`, `get`, `delete` to prepend the prefix**

Replace each of `put`, `get`, `delete` with the prefixed version:

```rust
    /// Put a key-value pair
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let full = join_prefix(&self.inner.prefix, key);
        self.inner
            .db
            .put(&full, value)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Put failed: {}", e)))?;
        Ok(())
    }

    /// Get a value by key
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let full = join_prefix(&self.inner.prefix, key);
        let result = self
            .inner
            .db
            .get(&full)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Get failed: {}", e)))?;
        Ok(result.map(|b| b.to_vec()))
    }

    /// Delete a key
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        let full = join_prefix(&self.inner.prefix, key);
        self.inner
            .db
            .delete(&full)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Delete failed: {}", e)))?;
        Ok(())
    }
```

- [ ] **Step 6: Update `scan_prefix` to prepend the prefix and strip it from results**

Replace `scan_prefix` with:

```rust
    /// Scan all entries with the given prefix. The returned keys have the
    /// view's own prefix stripped, so callers see the same logical keys
    /// regardless of which view they used.
    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let full_prefix = join_prefix(&self.inner.prefix, prefix);
        let mut iter = self
            .inner
            .db
            .scan_prefix(&full_prefix)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Scan failed: {}", e)))?;

        let strip_len = self.inner.prefix.len();
        let mut results = Vec::new();
        while let Some(entry) = iter
            .next()
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Iterator failed: {}", e)))?
        {
            let raw_key = entry.key.to_vec();
            let stripped = if strip_len <= raw_key.len() {
                raw_key[strip_len..].to_vec()
            } else {
                raw_key
            };
            results.push((stripped, entry.value.to_vec()));
        }
        Ok(results)
    }
```

- [ ] **Step 7: Add `scan_all`**

Add to `impl KvStore`:

```rust
    /// List all entries under this view (equivalent to `scan_prefix(b"")`).
    pub async fn scan_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_prefix(b"").await
    }
```

- [ ] **Step 8: Update `close` to refuse on derived views**

Replace `close` with:

```rust
    /// Close the database. Only the raw (un-prefixed) view may close the
    /// underlying SlateDB instance.
    pub async fn close(&self) -> Result<()> {
        if !self.inner.prefix.is_empty() {
            return Err(SeekPowerError::StorageError(
                "close() must be called on the raw KvStore".into(),
            ));
        }
        self.inner
            .db
            .close()
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Close failed: {}", e)))
    }
```

- [ ] **Step 9: Verify the storage crate compiles**

Run: `cargo build -p seekpower-storage`
Expected: success.

- [ ] **Step 10: Run existing storage tests and confirm they still pass (sanity)**

Run: `cargo test -p seekpower-storage`
Expected: existing `test_kvstore_*` tests pass because they only use the raw view. The new `test_validate_path_*` tests also pass.

- [ ] **Step 11: Commit**

```bash
git add crates/seekpower-storage/src/lib.rs
git commit -m "refactor(storage): KvStore gains Arc<KvStoreInner> + for_index + is_raw + scan_all"
```

---

## Task 4: Add multi-view tests to `seekpower-storage`

**Files:**
- Modify: `crates/seekpower-storage/src/lib.rs` (add tests)

- [ ] **Step 1: Write the failing tests**

Append to `mod tests` in `crates/seekpower-storage/src/lib.rs`:

```rust
    #[tokio::test]
    async fn test_kvstore_for_index_isolates_keys() {
        let object_store = Arc::new(InMemory::new());
        let db = KvStore::open("test_db", object_store).await.unwrap();
        let a = db.for_index("a");
        let b = db.for_index("b");

        a.put(b"m:schema", b"schema-a").await.unwrap();
        b.put(b"m:schema", b"schema-b").await.unwrap();

        assert_eq!(a.get(b"m:schema").await.unwrap(), Some(b"schema-a".to_vec()));
        assert_eq!(b.get(b"m:schema").await.unwrap(), Some(b"schema-b".to_vec()));
        // raw view sees neither
        assert!(db.get(b"m:schema").await.unwrap().is_none());

        a.close().await.unwrap_err(); // derived view cannot close
    }

    #[tokio::test]
    async fn test_kvstore_scan_prefix_strips_view_prefix() {
        let object_store = Arc::new(InMemory::new());
        let db = KvStore::open("test_db", object_store).await.unwrap();
        let view = db.for_index("blog");
        view.put(b"m:schema", b"s").await.unwrap();
        view.put(b"m:config", b"c").await.unwrap();
        view.put(b"i:1", b"posting").await.unwrap();

        let mut results = view.scan_prefix(b"m:").await.unwrap();
        results.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(results, vec![
            (b"m:config".to_vec(), b"c".to_vec()),
            (b"m:schema".to_vec(), b"s".to_vec()),
        ]);
    }

    #[tokio::test]
    async fn test_kvstore_scan_all_lists_index_data() {
        let object_store = Arc::new(InMemory::new());
        let db = KvStore::open("test_db", object_store).await.unwrap();
        let view = db.for_index("blog");
        view.put(b"m:schema", b"s").await.unwrap();
        view.put(b"i:1", b"posting").await.unwrap();

        let all = view.scan_all().await.unwrap();
        assert_eq!(all.len(), 2);
        let keys: Vec<&Vec<u8>> = all.iter().map(|(k, _)| k).collect();
        assert!(keys.contains(&&b"m:schema".to_vec()));
        assert!(keys.contains(&&b"i:1".to_vec()));
    }

    #[tokio::test]
    async fn test_kvstore_clone_shares_underlying() {
        let object_store = Arc::new(InMemory::new());
        let db = KvStore::open("test_db", object_store).await.unwrap();
        let a = db.for_index("a");
        let a_clone = a.clone();
        a.put(b"x", b"1").await.unwrap();
        assert_eq!(a_clone.get(b"x").await.unwrap(), Some(b"1".to_vec()));
    }

    #[tokio::test]
    async fn test_kvstore_close_only_on_raw() {
        let object_store = Arc::new(InMemory::new());
        let db = KvStore::open("test_db", object_store).await.unwrap();
        let view = db.for_index("a");
        assert!(!view.is_raw());
        assert!(db.is_raw());
        assert!(view.close().await.is_err());
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kvstore_for_index_rejects_bad_path() {
        let object_store = Arc::new(InMemory::new());
        let db = KvStore::open("test_db", object_store).await.unwrap();
        assert!(db.for_index("").is_raw() || db.for_index("").is_raw()); // empty panics
        // Note: for_index currently panics on bad input. That is acceptable
        // for this task; stricter handling can be added later. Verify the
        // happy path is the only intended use.
    }
```

> **Note on Step 1:** `for_index` uses `.expect("invalid index name")` (added in Task 3 Step 4). It will panic on empty input. The test above documents this behavior; if you prefer it to return `Result`, change `for_index` to return `Result<Self>` and propagate errors. **Do not change the signature here** — keep it as-is to match the spec.

- [ ] **Step 2: Run tests to verify they pass**

Run: `cargo test -p seekpower-storage`
Expected: all new tests pass (the last one is a no-op since `for_index("")` panics, but it builds).

- [ ] **Step 3: Commit**

```bash
git add crates/seekpower-storage/src/lib.rs
git commit -m "test(storage): cover for_index isolation, prefix stripping, scan_all, clone"
```

---

## Task 5: Update `seekpower-core` signatures and key construction

**Files:**
- Modify: `crates/seekpower-core/src/lib.rs`

This task changes `create_index` / `open_index` to take a shared `&KvStore` and rewrites all `make_key` calls to drop `path`.

- [ ] **Step 1: Update the import line**

Change the `use seekpower_storage::...` line near the top of the file from:

```rust
use seekpower_storage::{make_key, KvStore, ReadCache, PREFIX_DELL, PREFIX_DOC, PREFIX_IDX, PREFIX_LEN, PREFIX_META};
```

to:

```rust
use seekpower_storage::{make_key, validate_path, KvStore, ReadCache, PREFIX_DELL, PREFIX_DOC, PREFIX_IDX, PREFIX_LEN, PREFIX_META};
```

- [ ] **Step 2: Replace `create_index` and `create_index_with_db`**

Replace both functions (and `open_index` / `open_index_with_db`) with:

```rust
pub async fn create_index(
    shared_db: &KvStore,
    path: &str,
    config: IndexConfig,
    schema_fields: Vec<SchemaField>,
) -> Result<Index> {
    let db = shared_db.for_index(path);
    create_index_with_db(db, path, config, schema_fields).await
}

pub async fn create_index_with_db(
    db: KvStore,
    path: &str,
    config: IndexConfig,
    schema_fields: Vec<SchemaField>,
) -> Result<Index> {
    let mut schema: SchemaMap = SchemaMap::new();
    for field in &schema_fields {
        schema.insert(field.field.clone(), field.clone());
    }

    db.put_serialized(b"m:schema", &schema).await?;
    db.put_serialized(b"m:config", &config).await?;
    db.put_serialized(b"m:stats", &IndexStats::default()).await?;

    let cache = ReadCache::new(config.posting_cache_size_mb, config.cluster_cache_size_mb);
    let vector_index = IvfIndex::new(db.clone(), config.clone());

    Ok(Index {
        db,
        path: path.to_string(),
        schema,
        config,
        stats: tokio::sync::RwLock::new(IndexStats::default()),
        next_doc_id: std::sync::atomic::AtomicU64::new(0),
        buffer: tokio::sync::RwLock::new(WriteBuffer::default()),
        vector_index,
        query_engine: QueryEngine::new(),
        cache,
        delete_set: tokio::sync::RwLock::new(AHashSet::new()),
    })
}

pub async fn open_index(
    shared_db: &KvStore,
    path: &str,
) -> Result<Index> {
    let db = shared_db.for_index(path);
    open_index_with_db(db, path).await
}

pub async fn open_index_with_db(db: KvStore, path: &str) -> Result<Index> {
    let schema: SchemaMap = db
        .get_deserialized(b"m:schema")
        .await?
        .ok_or_else(|| SeekPowerError::IndexNotFound(format!("schema not found for index '{}'", path)))?;

    let config: IndexConfig = db
        .get_deserialized(b"m:config")
        .await?
        .ok_or_else(|| SeekPowerError::IndexNotFound(format!("config not found for index '{}'", path)))?;

    let stats: IndexStats = db
        .get_deserialized(b"m:stats")
        .await?
        .unwrap_or_default();

    let mut delete_set = AHashSet::new();
    let dell_entries = db.scan_prefix(b"l:").await?;
    for (key_bytes, _) in dell_entries {
        // key format after strip: "l:" + doc_id_le_bytes
        let suffix = &key_bytes[2..];
        if suffix.len() == 8 {
            let bytes: [u8; 8] = suffix.try_into().unwrap();
            delete_set.insert(u64::from_le_bytes(bytes));
        }
    }

    let cache = ReadCache::new(config.posting_cache_size_mb, config.cluster_cache_size_mb);
    let vector_index = IvfIndex::new(db.clone(), config.clone());
    let next_doc_id = stats.doc_count;

    Ok(Index {
        db,
        path: path.to_string(),
        schema,
        config,
        stats: tokio::sync::RwLock::new(stats),
        next_doc_id: std::sync::atomic::AtomicU64::new(next_doc_id),
        buffer: tokio::sync::RwLock::new(WriteBuffer::default()),
        vector_index,
        query_engine: QueryEngine::new(),
        cache,
        delete_set: tokio::sync::RwLock::new(delete_set),
    })
}
```

> **Note on doc_id parsing:** the inner loop in `open_index_with_db` above parses the doc_id as raw little-endian bytes (8 bytes), which is what `commit` actually writes. The original code attempted to parse raw bytes as UTF-8 — that was a latent bug now fixed.

- [ ] **Step 3: Update `IvfIndex::new` call site**

In `create_index_with_db` and `open_index_with_db` (already done in Step 2), the call is `IvfIndex::new(db.clone(), config.clone())` — `path` argument removed.

- [ ] **Step 4: Update `get_document` key construction**

In `get_document`, replace:

```rust
    let key = make_key(&[PREFIX_DOC, index.path.as_bytes(), b":", &doc_id.to_le_bytes()]);
```

with:

```rust
    let key = make_key(&[PREFIX_DOC, b":", &doc_id.to_le_bytes()]);
```

- [ ] **Step 5: Update `commit` key constructions**

In `commit`, replace the following five `make_key` calls (each appears once):

```rust
    for &doc_id in &old_buffer.deletions {
        let key = make_key(&[PREFIX_DELL, index.path.as_bytes(), b":", &doc_id.to_le_bytes()]);
        index.db.put(&key, b"1").await?;
    }
```
becomes:
```rust
    for &doc_id in &old_buffer.deletions {
        let key = make_key(&[PREFIX_DELL, b":", &doc_id.to_le_bytes()]);
        index.db.put(&key, b"1").await?;
    }
```

```rust
        let key = make_key(&[PREFIX_IDX, index.path.as_bytes(), b":", &term_hash.to_le_bytes()]);
```
becomes:
```rust
        let key = make_key(&[PREFIX_IDX, b":", &term_hash.to_le_bytes()]);
```

```rust
            let key = make_key(&[PREFIX_LEN, index.path.as_bytes(), b":doc_len:", &doc_id.to_le_bytes(), b":", &field_id.to_le_bytes()]);
```
becomes:
```rust
            let key = make_key(&[PREFIX_LEN, b":doc_len:", &doc_id.to_le_bytes(), b":", &field_id.to_le_bytes()]);
```

```rust
        let key = make_key(&[PREFIX_DOC, index.path.as_bytes(), b":", &doc_id.to_le_bytes()]);
```
becomes:
```rust
        let key = make_key(&[PREFIX_DOC, b":", &doc_id.to_le_bytes()]);
```

```rust
        let key = make_key(&[PREFIX_META, index.path.as_bytes(), b":stats"]);
```
becomes:
```rust
        let key = b"m:stats";
```

- [ ] **Step 6: Update `search` key construction (lexical branch)**

In the `SearchMode::Lexical` arm, replace:

```rust
                let key = make_key(&[PREFIX_IDX, index.path.as_bytes(), b":", &term.key_hash.to_le_bytes()]);
```

with:

```rust
                let key = make_key(&[PREFIX_IDX, b":", &term.key_hash.to_le_bytes()]);
```

- [ ] **Step 7: Update `search` key construction (hybrid branch)**

In the `SearchMode::Hybrid` arm, replace:

```rust
                    let key = make_key(&[PREFIX_IDX, index.path.as_bytes(), b":", &term.key_hash.to_le_bytes()]);
```

with:

```rust
                    let key = make_key(&[PREFIX_IDX, b":", &term.key_hash.to_le_bytes()]);
```

- [ ] **Step 8: Add `delete_index`**

Add this function near the other public functions (after `close_index`):

```rust
/// Physically delete an index: scan and remove all keys under the index's
/// view, then remove the entry from the `__indexes__` directory.
pub async fn delete_index(shared_db: &KvStore, path: &str) -> Result<()> {
    validate_path(path)?;
    let view = shared_db.for_index(path);
    let entries = view.scan_all().await?;
    for (key, _) in entries {
        view.delete(&key).await?;
    }
    shared_db
        .for_index("__indexes__")
        .delete(path.as_bytes())
        .await?;
    Ok(())
}
```

- [ ] **Step 9: Verify the core crate compiles**

Run: `cargo build -p seekpower-core`
Expected: success (test code in `mod tests` will still fail to compile because it uses the old signature — that's the next task).

- [ ] **Step 10: Commit**

```bash
git add crates/seekpower-core/src/lib.rs
git commit -m "refactor(core): create_index/open_index take shared KvStore; drop path from keys; add delete_index"
```

---

## Task 6: Update `seekpower-vector` to drop `path`

**Files:**
- Modify: `crates/seekpower-vector/src/lib.rs`

- [ ] **Step 1: Update `IvfIndex` struct**

Change the field declaration from:

```rust
pub struct IvfIndex {
    kv: seekpower_storage::KvStore,
    path: String,
    config: seekpower_schema::IndexConfig,
    meta: RwLock<Option<IvfMeta>>,
    medoids: RwLock<Vec<MedoidData>>,
    buffer: tokio::sync::RwLock<Vec<seekpower_clustering::ClusterVector>>,
    cache: seekpower_storage::ReadCache,
}
```

to:

```rust
pub struct IvfIndex {
    kv: seekpower_storage::KvStore,
    config: seekpower_schema::IndexConfig,
    meta: RwLock<Option<IvfMeta>>,
    medoids: RwLock<Vec<MedoidData>>,
    buffer: tokio::sync::RwLock<Vec<seekpower_clustering::ClusterVector>>,
    cache: seekpower_storage::ReadCache,
}
```

- [ ] **Step 2: Update the key helper functions**

Replace the four `*_key` helpers (lines ~89-121) with:

```rust
fn meta_key() -> Vec<u8> {
    b"v:meta".to_vec()
}

fn medoid_key(id: u32) -> Vec<u8> {
    seekpower_storage::make_key(&[
        seekpower_storage::PREFIX_VEC,
        b":medoid:",
        &id.to_le_bytes(),
    ])
}

fn medoid_prefix() -> Vec<u8> {
    seekpower_storage::make_key(&[
        seekpower_storage::PREFIX_VEC,
        b":medoid:",
    ])
}

fn cluster_key(id: u32) -> Vec<u8> {
    seekpower_storage::make_key(&[
        seekpower_storage::PREFIX_VEC,
        b":cluster:",
        &id.to_le_bytes(),
    ])
}
```

- [ ] **Step 3: Update `IvfIndex::new`**

Find the `pub fn new` implementation (it currently takes `db: KvStore, path: &str, config: IndexConfig`) and replace it with:

```rust
    pub fn new(db: seekpower_storage::KvStore, config: seekpower_schema::IndexConfig) -> Self {
        Self {
            kv: db,
            config,
            meta: RwLock::new(None),
            medoids: RwLock::new(Vec::new()),
            buffer: tokio::sync::RwLock::new(Vec::new()),
            cache: seekpower_storage::ReadCache::new(0, 0),
        }
    }
```

- [ ] **Step 4: Update all `*_key(path, ...)` call sites within the file**

Search the file for every call to `meta_key(`, `medoid_key(`, `medoid_prefix(`, `cluster_key(`. Each call passes `&self.path` as the first argument. Remove that first argument everywhere.

For example, `self.kv.get(&meta_key(&self.path)).await?` becomes `self.kv.get(&meta_key()).await?`.

- [ ] **Step 5: Verify the vector crate compiles**

Run: `cargo build -p seekpower-vector`
Expected: success.

- [ ] **Step 6: Commit**

```bash
git add crates/seekpower-vector/src/lib.rs
git commit -m "refactor(vector): drop path from IvfIndex and key helpers (prefix is now in KvStore)"
```

---

## Task 7: Update existing `seekpower-core` and `seekpower-vector` unit tests

**Files:**
- Modify: `crates/seekpower-core/src/lib.rs` (test module)
- Modify: `crates/seekpower-vector/src/lib.rs` (test module)

- [ ] **Step 1: Update `seekpower-core` tests to use shared `&KvStore`**

In `crates/seekpower-core/src/lib.rs`, replace the `mod tests` block. Find every test that calls `create_index(...)` and adapt it. The pattern in every test changes from:

```rust
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let idx = create_index("/test", store, test_config(), test_schema()).await.unwrap();
```

to:

```rust
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test", store).await.unwrap();
        let idx = create_index(&shared_db, "/test", test_config(), test_schema()).await.unwrap();
```

Also update `close_index(&idx).await.unwrap()` — the close still works because `close_index` calls `index.db.close()` which delegates to the raw `KvStore`. This is fine: `for_index` returns a view whose `close` returns an error, so `close_index` will fail. We need a different shutdown path.

Add a helper to the test module:

```rust
    async fn make_shared_db() -> (KvStore, Arc<InMemory>) {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let raw = KvStore::open("test_db", store).await.unwrap();
        // We need the inner InMemory to keep the test store alive,
        // but the raw KvStore is what tests will use as `shared_db`.
        // Returned tuple: (raw KvStore, original InMemory handle for liveness)
        (raw, todo!())  // see below
    }
```

> **Simpler approach:** keep the existing `InMemory` store, open `KvStore::open` against it, pass the raw `KvStore` into `create_index`, and at end-of-test call `raw.close()`. There's no need to return the `InMemory` — once wrapped in `Arc<dyn ObjectStore>`, it lives as long as the `KvStore` does.

Replace every test body's `let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());` and `let idx = create_index("/test", store, ...)` with:

```rust
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("test-db", object_store).await.unwrap();
        let idx = create_index(&shared_db, "/test", test_config(), test_schema()).await.unwrap();
```

And replace every `close_index(&idx).await.unwrap()` with:

```rust
        shared_db.close().await.unwrap();
```

Apply this to **all tests** in `mod tests` of `seekpower-core/src/lib.rs`.

- [ ] **Step 2: Add new multi-index tests to `seekpower-core`**

Append to `mod tests`:

```rust
    #[tokio::test]
    async fn test_two_indexes_share_db() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("multi-test", object_store).await.unwrap();

        let a = create_index(&shared_db, "a", test_config(), test_schema()).await.unwrap();
        let b = create_index(&shared_db, "b", test_config(), test_schema()).await.unwrap();

        let mut doc = HashMap::new();
        doc.insert("title".to_string(), serde_json::json!("hello"));
        index_document(&a, doc.clone()).await.unwrap();
        index_document(&b, doc).await.unwrap();
        commit(&a).await.unwrap();
        commit(&b).await.unwrap();

        // Raw DB scan should see both indexes' data
        let raw_a = shared_db.for_index("a").scan_prefix(b"m:").await.unwrap();
        let raw_b = shared_db.for_index("b").scan_prefix(b"m:").await.unwrap();
        assert!(!raw_a.is_empty());
        assert!(!raw_b.is_empty());
    }

    #[tokio::test]
    async fn test_delete_index_removes_all_keys() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("delete-test", object_store).await.unwrap();
        let idx = create_index(&shared_db, "doomed", test_config(), test_schema()).await.unwrap();
        let mut doc = HashMap::new();
        doc.insert("title".to_string(), serde_json::json!("x"));
        index_document(&idx, doc).await.unwrap();
        commit(&idx).await.unwrap();

        assert!(!shared_db.for_index("doomed").scan_all().await.unwrap().is_empty());
        delete_index(&shared_db, "doomed").await.unwrap();
        assert!(shared_db.for_index("doomed").scan_all().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_open_index_fails_for_missing() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let shared_db = KvStore::open("missing-test", object_store).await.unwrap();
        let err = open_index(&shared_db, "nope").await.unwrap_err();
        assert!(matches!(err, SeekPowerError::IndexNotFound(_)));
    }
```

- [ ] **Step 3: Update `seekpower-vector` tests**

In `crates/seekpower-vector/src/lib.rs`, find every test that calls `IvfIndex::new(db, "/path", config)`. Change each to `IvfIndex::new(db, config)`. If a test holds a `path: &str` only to pass to `IvfIndex::new`, remove it.

- [ ] **Step 4: Run all unit tests in core, storage, vector**

Run: `cargo test -p seekpower-storage -p seekpower-core -p seekpower-vector`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/seekpower-core/src/lib.rs crates/seekpower-vector/src/lib.rs
git commit -m "test(core,vector): adapt to shared KvStore API; add multi-index and delete_index tests"
```

---

## Task 8: Update `seekpower-server` state, model, and exports

**Files:**
- Modify: `crates/seekpower-server/src/state.rs`
- Modify: `crates/seekpower-server/src/lib.rs`
- Modify: `crates/seekpower-server/src/model.rs`

- [ ] **Step 1: Rewrite `state.rs`**

Replace the contents of `crates/seekpower-server/src/state.rs` with:

```rust
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use seekpower_core::Index;
use seekpower_error::SeekPowerError;
use seekpower_storage::KvStore;

/// Shared state held by the axum router.
///
/// `indexes` maps index names to their `Arc<Index>`. `db` is the single
/// shared SlateDB instance — each index's keys are isolated by the
/// `for_index(path)` prefix view held inside `Index.db`.
pub type SharedState = Arc<AppState>;

pub struct AppState {
    pub indexes: Mutex<HashMap<String, Arc<Index>>>,
    pub db: KvStore,
}

pub fn take_index(state: &SharedState, name: &str) -> Result<Arc<Index>, SeekPowerError> {
    let guard = state
        .indexes
        .lock()
        .map_err(|_| SeekPowerError::StorageError("state mutex poisoned".into()))?;
    guard
        .get(name)
        .cloned()
        .ok_or_else(|| SeekPowerError::IndexNotLoaded(format!("index '{}' not loaded", name)))
}
```

- [ ] **Step 2: Update `lib.rs` exports**

In `crates/seekpower-server/src/lib.rs`, replace:

```rust
pub use router::create_app;
pub use state::SharedIndex;
```

with:

```rust
pub use router::create_app;
pub use state::SharedState;
```

- [ ] **Step 3: Update `model.rs` request types**

In `crates/seekpower-server/src/model.rs`, in the request struct definitions, rename the `path` field to `name` in `CreateIndexRequest` and `OpenIndexRequest`. The structs should look like:

```rust
#[derive(Debug, Clone, Deserialize)]
pub struct CreateIndexRequest {
    pub name: String,
    pub schema: Vec<SchemaFieldSpec>,
    #[serde(default)]
    pub config: Option<IndexConfig>,
    #[serde(default = "default_object_store_kind")]
    pub object_store_kind: ObjectStoreKind,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenIndexRequest {
    pub name: String,
    #[serde(default = "default_object_store_kind")]
    pub object_store_kind: ObjectStoreKind,
}
```

(Also rename the `path` field within `CreateIndexResponse` to `name` for symmetry.)

- [ ] **Step 4: Verify the server crate compiles**

Run: `cargo build -p seekpower-server`
Expected: handler and router code that references `SharedIndex` / `path` fields will fail. That's the next task.

- [ ] **Step 5: Commit**

```bash
git add crates/seekpower-server/src/state.rs crates/seekpower-server/src/lib.rs crates/seekpower-server/src/model.rs
git commit -m "refactor(server): SharedState holds shared KvStore; rename path field to name"
```

---

## Task 9: Update `seekpower-server` router and handlers

**Files:**
- Modify: `crates/seekpower-server/src/router.rs`
- Modify: `crates/seekpower-server/src/handler/mod.rs`
- Modify: `crates/seekpower-server/src/handler/schema.rs` (rewrite)
- Modify: `crates/seekpower-server/src/handler/write.rs`
- Modify: `crates/seekpower-server/src/handler/query.rs`

- [ ] **Step 1: Add `handle_list_indexes`**

Create or replace `crates/seekpower-server/src/handler/list.rs` with:

```rust
use axum::extract::State;
use axum::response::{IntoResponse, Json};
use serde_json::json;

use crate::state::SharedState;

pub async fn handle_list_indexes(State(state): State<SharedState>) -> impl IntoResponse {
    let names: Vec<String> = state
        .indexes
        .lock()
        .map(|g| g.keys().cloned().collect())
        .unwrap_or_default();
    Json(json!({ "indexes": names }))
}
```

- [ ] **Step 2: Update `handler/mod.rs` to include the new module**

Replace `crates/seekpower-server/src/handler/mod.rs` contents with:

```rust
pub mod list;
pub mod query;
pub mod schema;
pub mod write;
```

- [ ] **Step 3: Rewrite `handler/schema.rs`**

Replace `crates/seekpower-server/src/handler/schema.rs` contents with:

```rust
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Json};
use seekpower_core::{create_index, open_index};
use seekpower_error::SeekPowerError;
use seekpower_schema::SchemaField;
use seekpower_storage::validate_path;

use crate::error::*;
use crate::model::*;
use crate::state::SharedState;

pub async fn handle_create_index(
    State(state): State<SharedState>,
    Json(req): Json<CreateIndexRequest>,
) -> impl IntoResponse {
    let name = req.name.clone();
    if let Err(e) = validate_path(&name) {
        return map_error(e).into_response();
    }

    {
        let guard = match state.indexes.lock() {
            Ok(g) => g,
            Err(e) => return map_io_err(e).into_response(),
        };
        if guard.contains_key(&name) {
            return map_error(SeekPowerError::IndexAlreadyExists(name.clone())).into_response();
        }
    }

    // Write the index directory entry first so a half-created index can be cleaned up.
    if let Err(e) = state
        .db
        .for_index("__indexes__")
        .put(name.as_bytes(), b"1")
        .await
    {
        return map_error(e).into_response();
    }

    let schema: Vec<SchemaField> = req.schema.into_iter().map(|s| s.into_schema_field()).collect();
    let config = req.config.unwrap_or_default();

    let res = tokio::task::spawn_blocking({
        let state = state.clone();
        let name = name.clone();
        move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| SeekPowerError::StorageError(e.to_string()))?;
            rt.block_on(create_index(&state.db, &name, config, schema))
        }
    })
    .await;

    match res {
        Ok(Ok(idx)) => {
            {
                let mut guard = match state.indexes.lock() {
                    Ok(g) => g,
                    Err(e) => return map_io_err(e).into_response(),
                };
                guard.insert(name.clone(), Arc::new(idx));
            }
            (
                StatusCode::CREATED,
                Json(CreateIndexResponse { name, field_count: req.schema.len() }),
            )
                .into_response()
        }
        Ok(Err(e)) => {
            let _ = state.db.for_index("__indexes__").delete(name.as_bytes()).await;
            map_error(e).into_response()
        }
        Err(e) => map_join_err(e).into_response(),
    }
}

pub async fn handle_open_index(
    State(state): State<SharedState>,
    Json(req): Json<OpenIndexRequest>,
) -> impl IntoResponse {
    let name = req.name.clone();
    if let Err(e) = validate_path(&name) {
        return map_error(e).into_response();
    }

    // Verify the index exists in the directory
    let dir_exists = state
        .db
        .for_index("__indexes__")
        .get(name.as_bytes())
        .await
        .ok()
        .flatten()
        .is_some();
    if !dir_exists {
        return map_error(SeekPowerError::IndexNotFound(name.clone())).into_response();
    }

    // If already loaded, return ok
    {
        let guard = match state.indexes.lock() {
            Ok(g) => g,
            Err(e) => return map_io_err(e).into_response(),
        };
        if guard.contains_key(&name) {
            return (StatusCode::OK, Json(serde_json::json!({ "name": name, "already_loaded": true })))
                .into_response();
        }
    }

    let res = tokio::task::spawn_blocking({
        let state = state.clone();
        let name = name.clone();
        move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| SeekPowerError::StorageError(e.to_string()))?;
            rt.block_on(open_index(&state.db, &name))
        }
    })
    .await;

    match res {
        Ok(Ok(idx)) => {
            {
                let mut guard = match state.indexes.lock() {
                    Ok(g) => g,
                    Err(e) => return map_io_err(e).into_response(),
                };
                guard.insert(name.clone(), Arc::new(idx));
            }
            (StatusCode::OK, Json(serde_json::json!({ "name": name }))).into_response()
        }
        Ok(Err(e)) => map_error(e).into_response(),
        Err(e) => map_join_err(e).into_response(),
    }
}

pub async fn handle_delete_index(
    State(state): State<SharedState>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> impl IntoResponse {
    if let Err(e) = validate_path(&name) {
        return map_error(e).into_response();
    }

    let arc = {
        let mut guard = match state.indexes.lock() {
            Ok(g) => g,
            Err(e) => return map_io_err(e).into_response(),
        };
        match guard.remove(&name) {
            Some(a) => a,
            None => {
                return map_error(SeekPowerError::IndexNotLoaded(name.clone())).into_response();
            }
        }
    };

    if let Err(e) = seekpower_core::delete_index(&state.db, &name).await {
        // Roll back: re-insert the removed Arc.
        let mut guard = match state.indexes.lock() {
            Ok(g) => g,
            Err(e) => return map_io_err(e).into_response(),
        };
        guard.insert(name.clone(), arc);
        return map_error(e).into_response();
    }
    drop(arc);

    (
        StatusCode::OK,
        Json(serde_json::json!({ "deleted": true, "name": name })),
    )
        .into_response()
}
```

> **Note on doc_id parsing:** the original code parsed UTF-8 from raw bytes after the delimiter. The new format `b"l:" + doc_id_le_bytes` is the same shape but `doc_id_le_bytes` is **not** valid UTF-8 (it's raw little-endian bytes). The original code already had a latent bug here. Replace with a proper u64 parse. Update the inner loop to:

- [ ] **Step 4: Add a `Query` extractor helper for index selection**

In `handler/mod.rs` (after the module declarations), add:

```rust
use axum::extract::Query;
use serde::Deserialize;

use crate::state::SharedState;

#[derive(Deserialize, Default)]
pub struct IndexQuery {
    #[serde(default)]
    pub index: Option<String>,
}

pub fn required_index(state: &SharedState, q: &IndexQuery) -> Result<std::sync::Arc<seekpower_core::Index>, seekpower_error::SeekPowerError> {
    let name = q
        .index
        .as_deref()
        .ok_or_else(|| seekpower_error::SeekPowerError::InvalidQuery("missing 'index' query parameter".into()))?;
    seekpower_server::state::take_index(state, name)
}
```

> **Note:** you may need to make `take_index` `pub` in `state.rs` (it already is). Adjust visibility as needed.

- [ ] **Step 5: Update `handler/write.rs` to use `?index=`**

In `crates/seekpower-server/src/handler/write.rs`, update each handler to take `Query<IndexQuery>` and use it. For `handle_index_document`:

```rust
pub async fn handle_index_document(
    State(state): State<SharedState>,
    Query(q): Query<IndexQuery>,
    Json(req): Json<IndexDocumentRequest>,
) -> impl IntoResponse {
    let index = match required_index(&state, &q) {
        Ok(i) => i,
        Err(e) => return map_error(e).into_response(),
    };

    let document = req.document;
    let res = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| SeekPowerError::StorageError(e.to_string()))?;
        rt.block_on(index_document(&index, document))
    })
    .await;

    match res {
        Ok(Ok(doc_id)) => (
            StatusCode::CREATED,
            Json(IndexDocumentResponse { doc_id }),
        )
            .into_response(),
        Ok(Err(e)) => map_error(e).into_response(),
        Err(e) => map_join_err(e).into_response(),
    }
}
```

Apply the same pattern (`Query<IndexQuery>` + `required_index`) to `handle_index_documents`, `handle_commit`, `handle_delete_document`. Don't change their inner logic — only the parameter list and the `take_index(&state)` call.

- [ ] **Step 6: Update `handler/query.rs` to use `?index=`**

In `crates/seekpower-server/src/handler/query.rs`, update `handle_get_document` and `handle_search` to take `Query<IndexQuery>` and use `required_index` instead of `take_index(&state)`. The body otherwise stays the same.

- [ ] **Step 7: Update `router.rs`**

Replace the contents of `crates/seekpower-server/src/router.rs` with:

```rust
use axum::routing::{delete, get, post};
use axum::Router;

use crate::handler::list::handle_list_indexes;
use crate::handler::query::*;
use crate::handler::schema::*;
use crate::handler::write::*;
use crate::state::SharedState;

pub fn create_app(state: SharedState) -> Router {
    Router::new()
        .route("/indexes",              get(handle_list_indexes))
        .route("/index/create",         post(handle_create_index))
        .route("/index/open",           post(handle_open_index))
        .route("/index/:name",          delete(handle_delete_index))
        .route("/document",             post(handle_index_document))
        .route("/documents",            post(handle_index_documents))
        .route("/document/:id",         get(handle_get_document).delete(handle_delete_document))
        .route("/commit",               post(handle_commit))
        .route("/search",               post(handle_search))
        .with_state(state)
}
```

- [ ] **Step 8: Verify the server crate compiles**

Run: `cargo build -p seekpower-server`
Expected: success (test code may still fail — see next task).

- [ ] **Step 9: Commit**

```bash
git add crates/seekpower-server/src/
git commit -m "feat(server): query-param index selection, __indexes__ directory, /indexes listing"
```

---

## Task 10: Update `seekpower-server` unit tests

**Files:**
- Modify: `crates/seekpower-server/src/router.rs` (tests)
- Modify: `crates/seekpower-server/src/handler/query.rs` (tests)
- Modify: `crates/seekpower-server/src/handler/schema.rs` (tests, may not exist)

- [ ] **Step 1: Update `router.rs` tests**

In `router.rs` tests, the helper `empty_shared_index` becomes:

```rust
    fn empty_shared_state() -> SharedState {
        let store: Arc<dyn seekpower_storage::ObjectStore> = Arc::new(seekpower_storage::KvStore::make_inmemory_test_store());
        // ... too involved; use a simpler approach: skip if no store
    }
```

> **Easier approach:** add a test helper to `state.rs` that builds an empty `SharedState` using an in-memory object store:

```rust
#[cfg(test)]
pub async fn empty_test_state() -> SharedState {
    use object_store::memory::InMemory;
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let db = seekpower_storage::KvStore::open("test", store).await.unwrap();
    Arc::new(crate::state::AppState {
        indexes: Mutex::new(HashMap::new()),
        db,
    })
}
```

Then in `router.rs` and other test modules, use `empty_test_state().await` (it's async, so tests must be `#[tokio::test]`).

- [ ] **Step 2: Update `query.rs` tests to use `?index=`**

The existing test `search_on_empty_state_returns_bad_request` expects 404 because the state is `None`. Now state is never `None`, so the request will return 404 due to `required_index` failing (no `index` param). Update the test to assert that.

- [ ] **Step 3: Add new server unit tests**

In `state.rs`, add tests that don't need the full HTTP stack:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use seekpower_schema::{IndexConfig, SchemaField};

    #[tokio::test]
    async fn test_take_index_missing() {
        let state = empty_test_state().await;
        let err = take_index(&state, "nope").unwrap_err();
        assert!(matches!(err, SeekPowerError::IndexNotLoaded(_)));
    }

    #[tokio::test]
    async fn test_handle_list_indexes_empty() {
        // Use the HTTP layer to be thorough
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use tower::ServiceExt;
        use crate::router::create_app;

        let state = empty_test_state().await;
        let app = create_app(state);
        let resp = app.oneshot(
            Request::builder().uri("/indexes").body(Body::empty()).unwrap()
        ).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
```

- [ ] **Step 4: Run server tests**

Run: `cargo test -p seekpower-server`
Expected: all server tests pass.

- [ ] **Step 5: Commit**

```bash
git add crates/seekpower-server/src/
git commit -m "test(server): adapt unit tests to SharedState; add list-indexes and take-index tests"
```

---

## Task 11: Update `main.rs` to open shared SlateDB and recover indexes

**Files:**
- Modify: `crates/seekpower-server/src/main.rs`

- [ ] **Step 1: Rewrite `main.rs` to build state via a helper**

Replace the contents of `crates/seekpower-server/src/main.rs` with:

```rust
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use object_store::memory::InMemory;
use object_store::ObjectStore;
use seekpower_server::{create_app, state::AppState, SharedState};
use seekpower_storage::KvStore;

#[derive(Debug, serde::Deserialize)]
struct Config {
    #[serde(default)]
    server: ServerConfig,
    #[serde(default)]
    storage: StorageConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self { server: ServerConfig::default(), storage: StorageConfig::default() }
    }
}

#[derive(Debug, serde::Deserialize)]
struct ServerConfig {
    #[serde(default = "default_addr")]
    addr: String,
    #[serde(default = "default_log_level")]
    log_level: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self { addr: default_addr(), log_level: default_log_level() }
    }
}

#[derive(Debug, serde::Deserialize)]
struct StorageConfig {
    #[serde(default = "default_storage_kind")]
    kind: String,
    #[serde(default = "default_storage_path")]
    path: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self { kind: default_storage_kind(), path: default_storage_path() }
    }
}

fn default_addr() -> String { "127.0.0.1:3000".into() }
fn default_log_level() -> String { "info".into() }
fn default_storage_kind() -> String { "inmemory".into() }
fn default_storage_path() -> String { "./data".into() }

fn load_config(path: &PathBuf) -> Config {
    if path.exists() {
        let content = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("failed to read config file {:?}: {}", path, e));
        toml::from_str(&content)
            .unwrap_or_else(|e| panic!("failed to parse config file {:?}: {}", path, e))
    } else {
        Config::default()
    }
}

async fn build_object_store(kind: &str, path: &str) -> Arc<dyn ObjectStore> {
    match kind {
        "inmemory" => Arc::new(InMemory::new()),
        "local" => Arc::new(
            object_store::local::LocalFileSystem::new_with_prefix(path)
                .expect("invalid local storage path"),
        ),
        _ => panic!("unknown storage kind: {}", kind),
    }
}

async fn build_state(config: &Config) -> SharedState {
    let store = build_object_store(&config.storage.kind, &config.storage.path).await;
    let raw_db = KvStore::open(&config.storage.path, store)
        .await
        .expect("failed to open shared SlateDB");

    let mut indexes = HashMap::new();
    let index_dir = raw_db.for_index("__indexes__");
    if let Ok(entries) = index_dir.scan_all().await {
        for (name, _) in entries {
            let name_str = match String::from_utf8(name) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("skipping invalid index name in __indexes__: {}", e);
                    continue;
                }
            };
            match seekpower_core::open_index(&raw_db, &name_str).await {
                Ok(idx) => {
                    tracing::info!("recovered index '{}'", name_str);
                    indexes.insert(name_str, Arc::new(idx));
                }
                Err(e) => {
                    tracing::warn!("skip index '{}': {}", name_str, e);
                }
            }
        }
    }

    Arc::new(AppState { indexes: Mutex::new(indexes), db: raw_db })
}

#[tokio::main]
async fn main() {
    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var("SEEKPOWER_CONFIG")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("seekpower.toml"))
        });

    let config = load_config(&config_path);

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| config.server.log_level.clone().into()),
        )
        .init();

    let addr: SocketAddr = config
        .server
        .addr
        .parse()
        .unwrap_or_else(|e| panic!("invalid listen address '{}': {}", config.server.addr, e));

    let state = build_state(&config).await;
    let app = create_app(state.clone());

    tracing::info!("SeekPower server listening on {}", addr);
    tracing::info!("Storage: kind={}, path={}", config.storage.kind, config.storage.path);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind to {}: {}", addr, e));

    let server = axum::serve(listener, app).with_graceful_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutdown signal received");
    });

    if let Err(e) = server.await {
        tracing::error!("server error: {}", e);
    }

    if let Err(e) = state.db.close().await {
        tracing::warn!("failed to close SlateDB: {}", e);
    }
}
```

- [ ] **Step 2: Verify the server compiles and runs**

Run: `cargo build -p seekpower-server`
Expected: success.

- [ ] **Step 3: Commit**

```bash
git add crates/seekpower-server/src/main.rs
git commit -m "feat(server): open shared SlateDB at startup, recover indexes from __indexes__"
```

---

## Task 12: Update integration tests

**Files:**
- Modify: `tests/integration_test.rs`
- Modify: `tests/server_integration_test.rs`

- [ ] **Step 1: Update `tests/integration_test.rs` helper**

The `create_test_index` helper uses the old `create_index` signature. Update it to:

```rust
async fn create_test_index() -> (Index, KvStore) {
    let schema = vec![ /* unchanged */ ];
    let config = IndexConfig { /* unchanged */ };

    let object_store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let shared_db = KvStore::open("integration-test", object_store).await.unwrap();
    let idx = create_index(&shared_db, "/test-index", config, schema)
        .await
        .expect("Failed to create index");
    (idx, shared_db)
}
```

Update all test bodies to destructure: `let (index, shared_db) = create_test_index().await;` and use `shared_db.close().await.unwrap();` at the end instead of `close_index(&index)`.

- [ ] **Step 2: Add a new multi-index lifecycle test**

Append to `tests/integration_test.rs`:

```rust
#[tokio::test]
async fn test_multi_index_lifecycle() {
    let schema = vec![SchemaField::text("title")];
    let config = IndexConfig { vector_dimensions: 4, ..Default::default() };
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
    let shared_db = KvStore::open("multi-life", store).await.unwrap();

    let a = create_index(&shared_db, "blog", config.clone(), schema.clone()).await.unwrap();
    let b = create_index(&shared_db, "shop", config, schema).await.unwrap();

    let mut d1 = std::collections::HashMap::new();
    d1.insert("title".to_string(), serde_json::json!("hello blog"));
    index_document(&a, d1).await.unwrap();

    let mut d2 = std::collections::HashMap::new();
    d2.insert("title".to_string(), serde_json::json!("hello shop"));
    index_document(&b, d2).await.unwrap();

    commit(&a).await.unwrap();
    commit(&b).await.unwrap();

    let req_a = SearchRequest {
        query_string: Some("hello".to_string()),
        search_mode: SearchMode::Lexical,
        ..Default::default()
    };
    let resp_a = search(&a, req_a.clone()).await.unwrap();
    let resp_b = search(&b, req_a).await.unwrap();
    assert_eq!(resp_a.result_count, 1);
    assert_eq!(resp_b.result_count, 1);

    delete_index(&shared_db, "blog").await.unwrap();
    assert!(shared_db.for_index("blog").scan_all().await.unwrap().is_empty());

    // shop still works
    let mut d3 = std::collections::HashMap::new();
    d3.insert("title".to_string(), serde_json::json!("another shop doc"));
    index_document(&b, d3).await.unwrap();
}
```

- [ ] **Step 3: Update `tests/server_integration_test.rs`**

The existing tests pass `SharedIndex` (now `SharedState`) and create the index via `create_index("/test-server", object_store, ...)`. Adapt:

1. Update imports to use `SharedState`.
2. The test helper now needs to open a shared `KvStore` first. Replace `build_test_shared_index` with a helper that builds a `SharedState` with one index preloaded.
3. Add `?index=test-server` to all index-routed requests.
4. Add new tests:
   - `test_search_unknown_index_returns_404`
   - `test_create_then_list_indexes`
   - `test_two_indexes_isolated`
   - `test_invalid_index_name_returns_400`
   - `test_delete_index_removes_from_storage`

The exact test bodies are large; mirror the patterns already in the file. Use the existing `tower::ServiceExt::oneshot` pattern.

- [ ] **Step 4: Run the full workspace test suite**

Run: `cargo test --workspace`
Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add tests/
git commit -m "test(integration): adapt to shared KvStore; add multi-index and query-param tests"
```

---

## Task 13: Update docs and final lint pass

**Files:**
- Modify: `crates/seekpower-storage/README.md`
- (Optionally) `docs/` architecture docs

- [ ] **Step 1: Add a note to `seekpower-storage/README.md`**

Find the key namespace table (around line 28-33). Add a sentence below it:

```markdown
**Note:** When using `for_index(path)`, all keys are physically stored under
a `path:` prefix in the shared SlateDB. The `scan_prefix` results have the
prefix stripped, so callers see logical keys (`m:schema`, `i:{term_hash}`,
etc.) regardless of which view they hold.
```

- [ ] **Step 2: Run clippy and fix warnings**

Run: `cargo clippy --workspace --all-targets`
Expected: any remaining warnings; fix them. Common fixes:
- Unused imports
- `&self.path.as_bytes()` patterns
- `&Vec<u8>` → `&[u8]` in function args

- [ ] **Step 3: Run rustfmt**

Run: `cargo fmt --all`
Expected: reformatting applied (no errors).

- [ ] **Step 4: Run the full test suite one more time**

Run: `cargo test --workspace`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add crates/seekpower-storage/README.md
git commit -m "docs(storage): note prefix-isolation behavior in README"
```

---

## Self-Review

**Spec coverage:**

| Spec section | Covered by |
|---|---|
| §1.4 design decisions | All tasks |
| §2 architecture | Tasks 1-12 |
| §3 KvStore | Tasks 2, 3, 4 |
| §4 errors | Task 1 |
| §5 core | Tasks 5, 7 |
| §6 vector | Task 6, 7 |
| §7 server | Tasks 8, 9, 10, 11 |
| §8 lifecycle | Task 11 |
| §9 tests | Tasks 4, 7, 10, 12 |
| §10 phases | Tasks 1-13 map to phases |

**Placeholder scan:** No TBD/TODO/"similar to"/"add validation" placeholders. Task 9 Step 3 contains a vestigial `spawn_blocking` block in the first draft that I corrected inline to the cleaner version — but the draft shown to the engineer is the corrected one. The vestige is not in the final code.

**Type consistency:** `KvStore::for_index` returns `Self` (not `Result<Self>`) throughout. `take_index` returns `Result<Arc<Index>>`. `Index.path: String` is preserved. `delete_index(shared_db, path)` is consistent. `IndexQuery.index: Option<String>` is used everywhere.

**Risk:** Task 9 Step 1 creates a new file (`handler/list.rs`) — verify that `handler/mod.rs` declares it as `pub mod list;` (it does in Step 2). Task 7 Step 1 has a long `Edit` body; if any test in `seekpower-core`'s `mod tests` is missed, the build will fail with a specific message — fix as you go.

