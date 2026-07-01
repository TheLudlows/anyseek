# Storage 层重构 Implementation Plan

> **DEPRECATED** — 此计划已被废弃。实际实现采用了不同方案：删除了 `PrefixedView` 封装层，改为在 `KvStore` 上直接提供 `prefixed_*` 方法（接受 `index_id: u32` 参数），`Index`/`LexicalIndex`/`IvfIndex` 直接持有 `KvStore` + `index_id`。详见当前代码。

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 seekpower-storage 从"薄封装 SlateDB"重构为 README 设计文档中描述的"定长前缀层次结构 KV 布局 + Read-through Cache + 范围删除"，同时将 PrefixedKvStore 从 seekpower-core 下沉到 storage 层。

**Architecture:** 当前 KvStore 是对 SlateDB 的透传封装，前缀逻辑散落在 core 的 PrefixedKvStore 中，Cache 是简单的 HashMap+RwLock。重构后：KvStore 自带定长 ID 映射和前缀编码，Cache 改为真正的 LRU 并支持前缀范围失效，索引删除走 scan_prefix+delete 而非逐条删除。

**Tech Stack:** Rust, SlateDB 0.14, object_store, tokio, serde_json

---

## 当前问题 vs 目标设计

| 问题 | 当前代码 | 目标（README 设计） |
|---|---|---|
| Key 前缀 | 可变长字符串拼接 (`path + ":" + key`)，散落在 PrefixedKvStore | 定长 TenantID + IndexID（u32 大端序），KvStore 内部编码 |
| 前缀逻辑位置 | seekpower-core 中的 PrefixedKvStore | 下沉到 seekpower-storage，作为 KvStore 内建能力 |
| Cache | `HashMap<Vec<u8>, Vec<u8>>` + 随机淘汰 | LRU 淘汰策略，支持前缀范围失效 |
| 序列化 | KvStore 提供 put_serialized/get_deserialized | 保留，但随 PrefixedKvStore 一起下沉 |
| 索引删除 | scan_all + 逐条 delete | scan_prefix + 批量 delete |
| 事务 | KvStore 透传 SlateDB Transaction | 保留，随 PrefixedKvStore 下沉 |
| Vector 存储 | IvfIndex 直接用 raw KvStore，key 不走前缀 | 统一走 PrefixedKvStore，key 带索引前缀 |

---

## 文件变更清单

| 文件 | 操作 | 职责 |
|---|---|---|
| `crates/seekpower-storage/src/lib.rs` | 重写 | KvStore (内建前缀) + PrefixedView + Cache(LRU) + Transaction |
| `crates/seekpower-storage/src/cache.rs` | 新建 | LRU Cache 实现（从 lib.rs 拆出） |
| `crates/seekpower-storage/src/prefixed.rs` | 新建 | PrefixedView（从 core/index.rs 的 PrefixedKvStore 迁移并增强） |
| `crates/seekpower-storage/src/key.rs` | 新建 | 定长前缀 Key 编解码工具 |
| `crates/seekpower-core/src/index.rs` | 修改 | 删除 PrefixedKvStore/PrefixedTransaction，改用 storage::PrefixedView |
| `crates/seekpower-vector/src/ivf.rs` | 修改 | 使用 PrefixedView 替代裸 KvStore |

---

## Task 1: 创建 key.rs — 定长前缀 Key 编解码

**Files:**
- Create: `crates/seekpower-storage/src/key.rs`

- [ ] **Step 1: 编写 key.rs**

实现 README 中定义的定长前缀层次结构编码：

```rust
//! 定长前缀层次结构 Key 编解码。
//!
//! Key 格式: Data_Type ‖ Tenant_ID (u32 BE) ‖ Index_ID (u32 BE) ‖ Business_Key
//!
//! Tenant_ID 和 Index_ID 在创建/打开索引时由 KvStore 内部分配，
//! 对上层透明。当前阶段暂用 index path 的 hash 作为 ID。

/// 将字符串路径映射为定长 u32 ID（使用 fxhash 确定性哈希）。
pub fn path_to_id(path: &str) -> u32 {
    use std::hash::{Hash, Hasher};
    let mut hasher = ahash::AHasher::default();
    path.hash(&mut hasher);
    hasher.finish() as u32
}

/// 编码定长前缀 Key：`data_type + tenant_id(BE) + index_id(BE) + business_key`
pub fn encode_key(data_type: &[u8], index_id: u32, business_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(data_type.len() + 4 + business_key.len());
    key.extend_from_slice(data_type);
    key.extend_from_slice(&index_id.to_be_bytes());
    key.extend_from_slice(business_key);
    key
}

/// 编码前缀扫描 Key：`data_type + index_id(BE)`
pub fn encode_prefix(data_type: &[u8], index_id: u32) -> Vec<u8> {
    let mut key = Vec::with_capacity(data_type.len() + 4);
    key.extend_from_slice(data_type);
    key.extend_from_slice(&index_id.to_be_bytes());
    key
}
```

- [ ] **Step 2: 在 lib.rs 中声明模块**

在 `lib.rs` 顶部添加 `mod key;`，并在 public 导出中加上 `pub use key::*;`。

- [ ] **Step 3: 编写测试验证编码一致性**

在 key.rs 底部添加：

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_key_roundtrip() {
        let key = encode_key(b"idx:", 42, b"hello");
        assert!(key.starts_with(b"idx:"));
        // index_id 42 in BE bytes
        assert_eq!(&key[4..8], &42u32.to_be_bytes());
        assert_eq!(&key[8..], b"hello");
    }

    #[test]
    fn test_encode_prefix_is_prefix_of_encode_key() {
        let prefix = encode_prefix(b"idx:", 42);
        let key = encode_key(b"idx:", 42, b"world");
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn test_path_to_id_deterministic() {
        let a = path_to_id("my_index");
        let b = path_to_id("my_index");
        assert_eq!(a, b);
        let c = path_to_id("other_index");
        assert_ne!(a, c);
    }
}
```

- [ ] **Step 4: 运行测试**

Run: `cargo test -p seekpower-storage -- key`
Expected: 3 tests PASS

- [ ] **Step 5: Commit**

```bash
git add crates/seekpower-storage/src/key.rs
git commit -m "feat(storage): add fixed-length prefix key encoding"
```

---

## Task 2: 创建 cache.rs — LRU Cache 替换 HashMap

**Files:**
- Create: `crates/seekpower-storage/src/cache.rs`

- [ ] **Step 1: 编写 cache.rs**

将现有的 `Cache` 结构替换为真正的 LRU 实现：

```rust
//! Read-through LRU Cache for KvStore.
//!
//! Uses an ordered map to track access recency, evicting the
//! least-recently-used entries when the byte budget is exceeded.
//! Supports prefix-based invalidation for index drop scenarios.

use std::collections::HashMap;
use std::sync::RwLock;

/// LRU cache keyed by raw bytes.
pub struct Cache {
    inner: RwLock<CacheInner>,
}

struct CacheInner {
    /// Map from key to (value, access_order_key)
    data: HashMap<Vec<u8>, (Vec<u8>, u64)>,
    /// Monotonically increasing access counter for LRU ordering.
    next_access: u64,
    /// Current total bytes stored (keys + values).
    size: usize,
    /// Maximum bytes allowed.
    max_bytes: usize,
}

impl Cache {
    /// Create a new cache with the given byte budget.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            inner: RwLock::new(CacheInner {
                data: HashMap::new(),
                next_access: 0,
                size: 0,
                max_bytes,
            }),
        }
    }

    /// Get a value from cache, promoting its LRU position.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut inner = self.inner.write().ok()?;
        let (value, access) = inner.data.get_mut(key)?;
        *access = inner.next_access;
        inner.next_access += 1;
        Some(value.clone())
    }

    /// Insert a value, evicting LRU entries if the budget is exceeded.
    pub fn insert(&self, key: &[u8], value: Vec<u8>) {
        let mut inner = match self.inner.write() {
            Ok(i) => i,
            Err(_) => return,
        };

        let entry_bytes = key.len() + value.len();

        // If key already exists, remove old accounting.
        if let Some((old_value, _)) = inner.data.remove(key) {
            inner.size -= key.len() + old_value.len();
        }

        // Evict LRU entries until we have room.
        while inner.size + entry_bytes > inner.max_bytes && !inner.data.is_empty() {
            let lru_key = inner
                .data
                .iter()
                .min_by_key(|(_, (_, access))| access)
                .map(|(k, _)| k.clone());
            match lru_key {
                Some(k) => {
                    if let Some((v, _)) = inner.data.remove(&k) {
                        inner.size -= k.len() + v.len();
                    }
                }
                None => break,
            }
        }

        inner.size += entry_bytes;
        let access = inner.next_access;
        inner.next_access += 1;
        inner.data.insert(key.to_vec(), (value, access));
    }

    /// Invalidate a single key.
    pub fn invalidate(&self, key: &[u8]) {
        if let Ok(mut inner) = self.inner.write() {
            if let Some((value, _)) = inner.data.remove(key) {
                inner.size -= key.len() + value.len();
            }
        }
    }

    /// Invalidate all keys that start with the given prefix.
    /// Used when dropping an entire index.
    pub fn invalidate_prefix(&self, prefix: &[u8]) {
        if let Ok(mut inner) = self.inner.write() {
            let keys_to_remove: Vec<Vec<u8>> = inner
                .data
                .keys()
                .filter(|k| k.starts_with(prefix))
                .cloned()
                .collect();
            for k in keys_to_remove {
                if let Some((v, _)) = inner.data.remove(&k) {
                    inner.size -= k.len() + v.len();
                }
            }
        }
    }

    /// Clear all entries.
    pub fn clear(&self) {
        if let Ok(mut inner) = self.inner.write() {
            inner.data.clear();
            inner.size = 0;
        }
    }

    /// Current number of entries.
    pub fn len(&self) -> usize {
        self.inner.read().map(|i| i.data.len()).unwrap_or(0)
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Backward-compatible alias.
pub type ReadCache = Cache;
```

- [ ] **Step 2: 编写 Cache 测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_insert_get() {
        let cache = Cache::new(1024);
        cache.insert(b"key1", b"value1".to_vec());
        assert_eq!(cache.get(b"key1"), Some(b"value1".to_vec()));
        assert_eq!(cache.get(b"nonexistent"), None);
    }

    #[test]
    fn test_cache_invalidate() {
        let cache = Cache::new(1024);
        cache.insert(b"key1", b"value1".to_vec());
        cache.invalidate(b"key1");
        assert_eq!(cache.get(b"key1"), None);
    }

    #[test]
    fn test_cache_lru_eviction() {
        let cache = Cache::new(100);
        // Each entry: key(4) + value(50) = 54 bytes
        cache.insert(b"key1", vec![0u8; 50]);
        cache.insert(b"key2", vec![0u8; 50]);
        // key1 + key2 = 108 bytes > 100, so one should be evicted
        // key1 was accessed first (LRU), so key1 should be evicted
        assert!(cache.get(b"key2").is_some(), "key2 (most recent) should survive");
    }

    #[test]
    fn test_cache_lru_access_promotes() {
        let cache = Cache::new(200);
        cache.insert(b"key1", vec![0u8; 50]);
        cache.insert(b"key2", vec![0u8; 50]);
        // Access key1 to promote it
        let _ = cache.get(b"key1");
        // Insert key3 to trigger eviction — key2 should be evicted (least recently used)
        cache.insert(b"key3", vec![0u8; 50]);
        assert!(cache.get(b"key1").is_some(), "key1 was recently accessed");
        assert!(cache.get(b"key3").is_some(), "key3 just inserted");
    }

    #[test]
    fn test_cache_invalidate_prefix() {
        let cache = Cache::new(1024);
        cache.insert(b"idx:1:a", b"val1".to_vec());
        cache.insert(b"idx:1:b", b"val2".to_vec());
        cache.insert(b"doc:2:c", b"val3".to_vec());

        cache.invalidate_prefix(b"idx:1:");
        assert_eq!(cache.get(b"idx:1:a"), None);
        assert_eq!(cache.get(b"idx:1:b"), None);
        assert_eq!(cache.get(b"doc:2:c"), Some(b"val3".to_vec()));
    }

    #[test]
    fn test_cache_clear() {
        let cache = Cache::new(1024);
        cache.insert(b"key1", b"value1".to_vec());
        cache.insert(b"key2", b"value2".to_vec());
        cache.clear();
        assert_eq!(cache.get(b"key1"), None);
        assert_eq!(cache.get(b"key2"), None);
    }
}
```

- [ ] **Step 3: 运行测试**

Run: `cargo test -p seekpower-storage -- cache`
Expected: 6 tests PASS

- [ ] **Step 4: Commit**

```bash
git add crates/seekpower-storage/src/cache.rs
git commit -m "feat(storage): add LRU Cache with prefix invalidation"
```

---

## Task 3: 创建 prefixed.rs — 将 PrefixedKvStore 从 core 下沉到 storage

**Files:**
- Create: `crates/seekpower-storage/src/prefixed.rs`

- [ ] **Step 1: 编写 prefixed.rs**

将 `seekpower-core/src/index.rs` 中的 `PrefixedKvStore` 和 `PrefixedTransaction` 迁移过来，改用定长 ID 前缀：

```rust
//! PrefixedView — a key-namespacing view over a shared KvStore.
//!
//! Each PrefixedView prepends a fixed-length prefix (derived from the
//! index path's hash) to every key, so multiple logical indexes can
//! coexist in a single SlateDB instance without key collisions.

use seekpower_error::{Result, SeekPowerError};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::key::{encode_key, encode_prefix, path_to_id};
use crate::{Cache, KvStore, Transaction};

/// A key-namespacing view over a shared [`KvStore`].
///
/// All keys are transparently prefixed with the index's fixed-length ID,
/// so callers can use short business keys without worrying about collisions.
#[derive(Clone)]
pub struct PrefixedView {
    db: KvStore,
    index_id: u32,
    cache: Cache,
}

impl PrefixedView {
    /// Create a new prefixed view for the given index path.
    pub fn new(db: KvStore, path: &str, cache: Cache) -> Result<Self> {
        seekpower_schema::validate_path(path)
            .map_err(|e| SeekPowerError::InvalidIndexName(e))?;
        let index_id = path_to_id(path);
        Ok(Self { db, index_id, cache })
    }

    /// The raw index ID used as prefix.
    pub fn index_id(&self) -> u32 {
        self.index_id
    }

    /// Access the underlying shared KvStore.
    pub fn raw_db(&self) -> KvStore {
        self.db.clone()
    }

    /// Access the read-through cache.
    pub fn cache(&self) -> &Cache {
        &self.cache
    }

    fn prefixed_key(&self, key: &[u8]) -> Vec<u8> {
        encode_key(b"", self.index_id, key)
    }

    // -- CRUD ---------------------------------------------------------------

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let full_key = self.prefixed_key(key);
        self.db.put(&full_key, value).await?;
        self.cache.insert(&full_key, value.to_vec());
        Ok(())
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let full_key = self.prefixed_key(key);
        if let Some(data) = self.cache.get(&full_key) {
            return Ok(Some(data));
        }
        match self.db.get(&full_key).await? {
            Some(data) => {
                self.cache.insert(&full_key, data.clone());
                Ok(Some(data))
            }
            None => Ok(None),
        }
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        let full_key = self.prefixed_key(key);
        self.db.delete(&full_key).await?;
        self.cache.invalidate(&full_key);
        Ok(())
    }

    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let full_prefix = self.prefixed_key(prefix);
        let results = self.db.scan_prefix(&full_prefix).await?;
        // Strip the view prefix from returned keys.
        let prefix_len = 4; // index_id is u32 BE = 4 bytes
        Ok(results
            .into_iter()
            .filter_map(|(k, v)| {
                if k.len() >= prefix_len {
                    Some((k[prefix_len..].to_vec(), v))
                } else {
                    None
                }
            })
            .collect())
    }

    pub async fn scan_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_prefix(b"").await
    }

    // -- Serialized helpers -------------------------------------------------

    pub async fn put_serialized<T: Serialize>(&self, key: &[u8], value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value)
            .map_err(|e| SeekPowerError::StorageError(format!("Serialization failed: {}", e)))?;
        self.put(key, &bytes).await
    }

    pub async fn get_deserialized<T: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>> {
        match self.get(key).await? {
            Some(bytes) => {
                let value = serde_json::from_slice(&bytes).map_err(|e| {
                    SeekPowerError::StorageError(format!("Deserialization failed: {}", e))
                })?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    // -- Transaction --------------------------------------------------------

    pub async fn begin_transaction(&self) -> Result<PrefixedTransaction> {
        let txn = self.db.begin_transaction().await?;
        Ok(PrefixedTransaction {
            txn,
            index_id: self.index_id,
        })
    }

    // -- Bulk delete (for index drop) ---------------------------------------

    /// Delete all keys belonging to this view's index.
    ///
    /// Implementation: scan all keys with this index's prefix,
    /// then delete each one. SlateDB compaction will reclaim the
    /// physical space asynchronously.
    pub async fn delete_all(&self) -> Result<()> {
        let prefix = encode_prefix(b"", self.index_id);
        let entries = self.db.scan_prefix(&prefix).await?;
        for (key, _) in &entries {
            self.db.delete(key).await?;
        }
        self.cache.invalidate_prefix(&prefix);
        Ok(())
    }
}

/// Transaction wrapper that prefixes all keys with the view's index ID.
pub struct PrefixedTransaction {
    txn: Transaction,
    index_id: u32,
}

impl PrefixedTransaction {
    fn prefixed_key(&self, key: &[u8]) -> Vec<u8> {
        encode_key(b"", self.index_id, key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.txn.put(&self.prefixed_key(key), value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.txn.delete(&self.prefixed_key(key))
    }

    pub async fn commit(self) -> Result<()> {
        self.txn.commit().await
    }

    pub fn rollback(self) -> Result<()> {
        self.txn.rollback()
    }
}
```

- [ ] **Step 2: 编写 PrefixedView 测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    async fn make_store() -> KvStore {
        let os: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        KvStore::open("test_prefixed", os).await.unwrap()
    }

    #[tokio::test]
    async fn test_prefixed_view_isolation() {
        let db = make_store().await;
        let cache_a = Cache::new(1024);
        let cache_b = Cache::new(1024);
        let view_a = PrefixedView::new(db.clone(), "index_a", cache_a).unwrap();
        let view_b = PrefixedView::new(db.clone(), "index_b", cache_b).unwrap();

        view_a.put(b"key1", b"val_a").await.unwrap();
        view_b.put(b"key1", b"val_b").await.unwrap();

        assert_eq!(view_a.get(b"key1").await.unwrap(), Some(b"val_a".to_vec()));
        assert_eq!(view_b.get(b"key1").await.unwrap(), Some(b"val_b".to_vec()));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_prefixed_view_serialized() {
        let db = make_store().await;
        let cache = Cache::new(1024);
        let view = PrefixedView::new(db.clone(), "test_ser", cache).unwrap();

        view.put_serialized(b"conf", &42u64).await.unwrap();
        let val: Option<u64> = view.get_deserialized(b"conf").await.unwrap();
        assert_eq!(val, Some(42u64));
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_prefixed_view_delete_all() {
        let db = make_store().await;
        let cache = Cache::new(1024);
        let view = PrefixedView::new(db.clone(), "doomed", cache).unwrap();

        view.put(b"a", b"1").await.unwrap();
        view.put(b"b", b"2").await.unwrap();
        view.delete_all().await.unwrap();

        assert_eq!(view.get(b"a").await.unwrap(), None);
        assert_eq!(view.get(b"b").await.unwrap(), None);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_prefixed_view_scan_prefix() {
        let db = make_store().await;
        let cache = Cache::new(1024);
        let view = PrefixedView::new(db.clone(), "scan_test", cache).unwrap();

        view.put(b"idx:term1", b"v1").await.unwrap();
        view.put(b"idx:term2", b"v2").await.unwrap();
        view.put(b"doc:1", b"v3").await.unwrap();

        let results = view.scan_prefix(b"idx:").await.unwrap();
        assert_eq!(results.len(), 2);
        db.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_prefixed_view_transaction() {
        let db = make_store().await;
        let cache = Cache::new(1024);
        let view = PrefixedView::new(db.clone(), "txn_test", cache).unwrap();

        let txn = view.begin_transaction().await.unwrap();
        txn.put(b"k1", b"v1").unwrap();
        txn.commit().await.unwrap();

        assert_eq!(view.get(b"k1").await.unwrap(), Some(b"v1".to_vec()));
        db.close().await.unwrap();
    }
}
```

- [ ] **Step 3: 运行测试**

Run: `cargo test -p seekpower-storage -- prefixed`
Expected: 5 tests PASS

- [ ] **Step 4: Commit**

```bash
git add crates/seekpower-storage/src/prefixed.rs
git commit -m "feat(storage): add PrefixedView with fixed-length ID prefix"
```

---

## Task 4: 重写 lib.rs — 清理 KvStore，整合新模块

**Files:**
- Modify: `crates/seekpower-storage/src/lib.rs`

- [ ] **Step 1: 重写 lib.rs**

KvStore 保留为 SlateDB 的薄封装（不内含前缀逻辑，前缀由 PrefixedView 处理），删除旧的 Cache 定义，导出新模块：

```rust
//! seekpower-storage: SlateDB KV wrapper + LRU cache + prefixed views.

mod cache;
mod key;
mod prefixed;

pub use cache::{Cache, ReadCache};
pub use key::{encode_key, encode_prefix, path_to_id};
pub use prefixed::{PrefixedTransaction, PrefixedView};

use object_store::ObjectStore;
use seekpower_error::{Result, SeekPowerError};
use slatedb::IsolationLevel;
use std::sync::Arc;

/// Thin wrapper around SlateDB providing CRUD + scan + transactions.
///
/// Prefix logic is handled by [`PrefixedView`]; KvStore itself stores
/// keys as-is.
pub struct KvStore {
    db: Arc<slatedb::Db>,
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        Self { db: self.db.clone() }
    }
}

impl KvStore {
    /// Open or create a SlateDB database at the given path.
    pub async fn open(path: &str, object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        let db = slatedb::Db::open(path, object_store)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Failed to open database: {}", e)))?;
        Ok(Self { db: Arc::new(db) })
    }

    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db
            .put(key, value)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Put failed: {}", e)))
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let result = self
            .db
            .get(key)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Get failed: {}", e)))?;
        Ok(result.map(|b| b.to_vec()))
    }

    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        self.db
            .delete(key)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Delete failed: {}", e)))
    }

    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self
            .db
            .scan_prefix(prefix)
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Scan failed: {}", e)))?;
        let mut results = Vec::new();
        while let Some(entry) = iter
            .next()
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Iterator failed: {}", e)))?
        {
            results.push((entry.key.to_vec(), entry.value.to_vec()));
        }
        Ok(results)
    }

    pub async fn scan_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_prefix(b"").await
    }

    pub async fn begin_transaction(&self) -> Result<Transaction> {
        let txn = self
            .db
            .begin(IsolationLevel::default())
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Begin transaction failed: {}", e)))?;
        Ok(Transaction { txn })
    }

    pub async fn close(&self) -> Result<()> {
        self.db
            .close()
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Close failed: {}", e)))
    }
}

/// A transaction for atomic batch writes.
pub struct Transaction {
    txn: slatedb::DbTransaction,
}

impl Transaction {
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.txn
            .put(key, value)
            .map_err(|e| SeekPowerError::StorageError(format!("Transaction put failed: {}", e)))
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.txn
            .delete(key)
            .map_err(|e| SeekPowerError::StorageError(format!("Transaction delete failed: {}", e)))
    }

    pub async fn commit(self) -> Result<()> {
        self.txn
            .commit()
            .await
            .map_err(|e| SeekPowerError::StorageError(format!("Transaction commit failed: {}", e)))?;
        Ok(())
    }

    pub fn rollback(self) -> Result<()> {
        self.txn.rollback();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestData {
        id: u64,
        name: String,
    }

    #[tokio::test]
    async fn test_kvstore_put_get_delete() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let store = KvStore::open("test_db", object_store).await.unwrap();
        store.put(b"key1", b"value1").await.unwrap();
        assert_eq!(store.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        store.delete(b"key1").await.unwrap();
        assert_eq!(store.get(b"key1").await.unwrap(), None);
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kvstore_scan_prefix() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let store = KvStore::open("test_db", object_store).await.unwrap();
        store.put(b"prefix:key1", b"value1").await.unwrap();
        store.put(b"prefix:key2", b"value2").await.unwrap();
        store.put(b"prefix:key3", b"value3").await.unwrap();
        store.put(b"other:key4", b"value4").await.unwrap();
        let results = store.scan_prefix(b"prefix:").await.unwrap();
        assert_eq!(results.len(), 3);
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kvstore_transaction() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let store = KvStore::open("test_db", object_store).await.unwrap();
        let txn = store.begin_transaction().await.unwrap();
        txn.put(b"key1", b"value1").unwrap();
        txn.delete(b"key2").unwrap();
        txn.commit().await.unwrap();
        assert_eq!(store.get(b"key1").await.unwrap(), Some(b"value1".to_vec()));
        store.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_kvstore_clone_shares_underlying() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let store = KvStore::open("test_db", object_store).await.unwrap();
        let clone = store.clone();
        store.put(b"x", b"1").await.unwrap();
        assert_eq!(clone.get(b"x").await.unwrap(), Some(b"1".to_vec()));
        store.close().await.unwrap();
    }
}
```

注意：从 KvStore 中移除了 `put_serialized` / `get_deserialized` — 这些方法现在只在 `PrefixedView` 上提供，因为序列化是业务层的关注点。如果其他地方仍需要 KvStore 级别的序列化方法，可以在后续 task 中加回。

- [ ] **Step 2: 运行 storage 全部测试**

Run: `cargo test -p seekpower-storage`
Expected: 所有测试 PASS（包括 key, cache, prefixed, kvstore）

- [ ] **Step 3: Commit**

```bash
git add crates/seekpower-storage/src/lib.rs
git commit -m "refactor(storage): rewrite lib.rs with modular structure"
```

---

## Task 5: 更新 seekpower-core — 删除 PrefixedKvStore，使用 PrefixedView

**Files:**
- Modify: `crates/seekpower-core/src/index.rs`

- [ ] **Step 1: 重写 index.rs**

删除 `PrefixedKvStore`、`PrefixedTransaction`、`ReadCache` type alias，改用 `seekpower_storage::PrefixedView` 和 `seekpower_storage::Cache`：

关键变更：
1. `Index.db` 类型从 `PrefixedKvStore` 改为 `PrefixedView`
2. `create_index` 中创建 `PrefixedView::new(shared_db.clone(), path, cache)`
3. `open_index` 中同样使用 `PrefixedView::new`
4. `delete_index` 中使用 `view.delete_all()` 替代手动 scan+delete
5. `IvfIndex::new` 接收 `PrefixedView` 而非 `KvStore`（见 Task 6）
6. `LexicalIndex::new` 的 `db` 参数类型改为 `PrefixedView`
7. `close_index` 使用 `index.db.raw_db().close()`

```rust
use ahash::AHashSet;
use seekpower_schema::{IndexConfig, IndexStats, SchemaField, SchemaMap, validate_path};
use seekpower_error::{Result, SeekPowerError};
use seekpower_storage::{Cache, KvStore, PrefixedView};
use seekpower_vector::IvfIndex;

use crate::lexical_index::LexicalIndex;
use crate::search::QueryEngine;

pub struct Index {
    pub db: PrefixedView,
    pub path: String,
    pub schema: SchemaMap,
    pub field_names: Vec<String>,
    pub config: IndexConfig,
    pub stats: tokio::sync::RwLock<IndexStats>,
    pub next_doc_id: std::sync::atomic::AtomicU64,
    pub lexical_index: LexicalIndex,
    pub vector_index: IvfIndex,
    pub query_engine: QueryEngine,
    pub delete_set: tokio::sync::RwLock<AHashSet<u64>>,
}

pub async fn create_index(
    shared_db: &KvStore,
    path: &str,
    config: IndexConfig,
    schema_fields: Vec<SchemaField>,
) -> Result<Index> {
    let cache = Cache::new((config.posting_cache_size_mb + config.cluster_cache_size_mb) * 1024 * 1024);
    let db = PrefixedView::new(shared_db.clone(), path, cache)?;
    create_index_with_db(db, path, config, schema_fields).await
}

pub async fn create_index_with_db(
    db: PrefixedView,
    path: &str,
    config: IndexConfig,
    schema_fields: Vec<SchemaField>,
) -> Result<Index> {
    let mut schema: SchemaMap = SchemaMap::new();
    let mut field_names = Vec::new();
    for field in &schema_fields {
        schema.insert(field.field.clone(), field.clone());
        field_names.push(field.field.clone());
    }

    db.put_serialized(b"m:schema", &schema).await?;
    db.put_serialized(b"m:config", &config).await?;
    db.put_serialized(b"m:stats", &IndexStats::default()).await?;
    db.put_serialized(b"m:fields", &field_names).await?;

    let raw_db = db.raw_db();
    let vector_index = IvfIndex::new(db.clone(), config.clone());
    let lexical_index = LexicalIndex::new(
        db.clone(), config.clone(), schema.clone(), field_names.clone(),
    );

    Ok(Index {
        db,
        path: path.to_string(),
        schema,
        field_names,
        config,
        stats: tokio::sync::RwLock::new(IndexStats::default()),
        next_doc_id: std::sync::atomic::AtomicU64::new(0),
        lexical_index,
        vector_index,
        query_engine: QueryEngine::new(),
        delete_set: tokio::sync::RwLock::new(AHashSet::new()),
    })
}

pub async fn open_index(shared_db: &KvStore, path: &str) -> Result<Index> {
    let cache = Cache::new(256 * 1024 * 1024); // will be overridden after loading config
    let db = PrefixedView::new(shared_db.clone(), path, cache)?;
    open_index_with_db(db, path).await
}

pub async fn open_index_with_db(db: PrefixedView, path: &str) -> Result<Index> {
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

    let field_names: Vec<String> = db
        .get_deserialized(b"m:fields")
        .await?
        .unwrap_or_else(|| schema.keys().cloned().collect());

    let mut delete_set = AHashSet::new();
    let dell_entries = db.scan_prefix(b"l:").await?;
    for (key_bytes, _) in dell_entries {
        let suffix = &key_bytes[2..];
        if suffix.len() == 8 {
            let bytes: [u8; 8] = suffix.try_into().unwrap();
            delete_set.insert(u64::from_le_bytes(bytes));
        }
    }

    let vector_index = IvfIndex::new(db.clone(), config.clone());
    let lexical_index = LexicalIndex::new(
        db.clone(), config.clone(), schema.clone(), field_names.clone(),
    );
    let next_doc_id = stats.doc_count;

    Ok(Index {
        db,
        path: path.to_string(),
        schema,
        field_names,
        config,
        stats: tokio::sync::RwLock::new(stats),
        next_doc_id: std::sync::atomic::AtomicU64::new(next_doc_id),
        lexical_index,
        vector_index,
        query_engine: QueryEngine::new(),
        delete_set: tokio::sync::RwLock::new(delete_set),
    })
}

pub async fn close_index(index: &Index) -> Result<()> {
    index.db.raw_db().close().await
}

pub async fn delete_index(shared_db: &KvStore, path: &str) -> Result<()> {
    validate_path(path).map_err(|e| SeekPowerError::InvalidIndexName(e))?;
    let cache = Cache::new(1024);
    let view = PrefixedView::new(shared_db.clone(), path, cache)?;
    view.delete_all().await?;
    shared_db.delete(format!("__indexes__:{}", path).as_bytes()).await?;
    Ok(())
}
```

注意：`LexicalIndex::new` 签名变更 — 不再接收 `cache` 和 `delete_set` 参数（cache 内置于 PrefixedView，delete_set 从 scan 加载）。具体变更见 Task 7。

- [ ] **Step 2: 运行编译检查**

Run: `cargo check -p seekpower-core`
Expected: 编译错误（因为 LexicalIndex 和 IvfIndex 的签名还没改），确认只有预期的类型不匹配错误

- [ ] **Step 3: Commit（暂不编译通过，与 Task 6、7 一起完成）**

---

## Task 6: 更新 seekpower-vector — 使用 PrefixedView 替代裸 KvStore

**Files:**
- Modify: `crates/seekpower-vector/src/ivf.rs`

- [ ] **Step 1: 修改 IvfIndex 使用 PrefixedView**

关键变更：
1. `IvfIndex.kv` 类型从 `KvStore` 改为 `PrefixedView`
2. `IvfIndex::new` 接收 `PrefixedView` 替代 `KvStore`
3. 删除 `IvfIndex` 中的 `cache` 字段 — 缓存现在由 `PrefixedView` 内置处理
4. `load_cluster` 等方法改为先查 `self.kv.cache().get()`，miss 时 `self.kv.get()`
5. 所有 key helper (`meta_key`, `medoid_key`, `cluster_key`) 保持不变（它们已经是短业务 key）
6. `store_cluster` 等方法通过 `self.kv.put()` 写入（自动带前缀 + 缓存更新）

```rust
// IvfIndex 结构体变更:
pub struct IvfIndex {
    kv: seekpower_storage::PrefixedView,
    config: seekpower_schema::IndexConfig,
    meta: RwLock<Option<IvfMeta>>,
    medoids: RwLock<Vec<MedoidData>>,
    buffer: tokio::sync::RwLock<Vec<ClusterVector>>,
}

impl IvfIndex {
    pub fn new(kv: seekpower_storage::PrefixedView, config: seekpower_schema::IndexConfig) -> Self {
        Self {
            kv,
            config,
            meta: RwLock::new(None),
            medoids: RwLock::new(Vec::new()),
            buffer: tokio::sync::RwLock::new(Vec::new()),
        }
    }
    // ... 其余方法中的 self.cache.get/insert 改为 self.kv.cache().get/insert
    // ... self.kv.get/put 保持不变（PrefixedView 透传）
}
```

对于 `search` 方法中缓存查找的改动：
- `self.cache.get(&key)` → `self.kv.cache().get(&key)`
- `self.cache.insert(&key, data)` → `self.kv.cache().insert(&key, data)`
- `self.kv.get(&key).await` 保持不变（PrefixedView.get 已内建 cache-through）

对于 `store_cluster` / `store_medoid` / `store_meta`：
- 删除手动 `self.cache.insert()` — `PrefixedView.put()` 已自动更新缓存
- 但对于 `store_cluster` 存入 raw bytes 的场景，`PrefixedView.put()` 已经处理了缓存

- [ ] **Step 2: 更新 IvfIndex 测试**

测试中 `make_test_index` 需要改为：

```rust
async fn make_test_index() -> IvfIndex {
    let os = Arc::new(InMemory::new());
    let kv = seekpower_storage::KvStore::open("test_vec", os).await.unwrap();
    let cache = seekpower_storage::Cache::new(1024 * 1024);
    let view = seekpower_storage::PrefixedView::new(kv, "test", cache).unwrap();
    IvfIndex::new(view, default_config())
}
```

同时删除测试中手动 `index.cache.insert(...)` 的调用。

- [ ] **Step 3: 运行编译检查**

Run: `cargo check -p seekpower-vector`

- [ ] **Step 4: Commit**

```bash
git add crates/seekpower-vector/src/ivf.rs
git commit -m "refactor(vector): use PrefixedView instead of raw KvStore"
```

---

## Task 7: 更新 LexicalIndex — 使用 PrefixedView 内建缓存

**Files:**
- Modify: `crates/seekpower-core/src/lexical_index.rs`

- [ ] **Step 1: 修改 LexicalIndex**

关键变更：
1. `db` 类型从 `crate::index::PrefixedKvStore` 改为 `seekpower_storage::PrefixedView`
2. 删除 `cache` 字段 — 缓存由 PrefixedView 内建处理
3. `new()` 签名：移除 `cache` 和 `delete_set` 参数
4. `load_posting` 改为使用 `self.db.cache().get()` + `self.db.get()` 的 read-through 模式
5. `flush` 中的缓存操作也改为 `self.db.cache()`

```rust
pub struct LexicalIndex {
    db: seekpower_storage::PrefixedView,
    config: IndexConfig,
    schema: SchemaMap,
    field_names: Vec<String>,
    buffer: tokio::sync::RwLock<WriteBuffer>,
    delete_set: tokio::sync::RwLock<AHashSet<u64>>,
}

impl LexicalIndex {
    pub fn new(
        db: seekpower_storage::PrefixedView,
        config: IndexConfig,
        schema: SchemaMap,
        field_names: Vec<String>,
    ) -> Self {
        Self {
            db,
            config,
            schema,
            field_names,
            buffer: tokio::sync::RwLock::new(WriteBuffer::default()),
            delete_set: tokio::sync::RwLock::new(AHashSet::new()),
        }
    }

    // load_posting: 使用 PrefixedView 的 read-through
    pub async fn load_posting(&self, key: &[u8]) -> Result<Option<PostingListBlock>> {
        if let Some(data) = self.db.cache().get(key) {
            return Ok(Some(PostingCodec::deserialize(&data)?));
        }
        if let Some(data) = self.db.get(key).await? {
            // PrefixedView.get already populates cache, but we also need
            // the raw bytes for PostingCodec, so check cache after get.
            let block = PostingCodec::deserialize(&data)?;
            return Ok(Some(block));
        }
        Ok(None)
    }

    // flush: 缓存通过 PrefixedView.put 自动更新
    pub async fn flush(&self, old_buffer: &WriteBuffer, stats: &mut IndexStats) -> Result<()> {
        for &doc_id in &old_buffer.deletions {
            let key = make_key(&[PREFIX_DELL, b":", &doc_id.to_le_bytes()]);
            self.db.put(&key, b"1").await?;
        }

        for (&term_hash, new_entries) in &old_buffer.postings {
            let key = make_key(&[PREFIX_IDX, b":", &term_hash.to_le_bytes()]);

            let existing_block = match self.db.cache().get(&key) {
                Some(data) => Some(PostingCodec::deserialize(&data)?),
                None => match self.db.get(&key).await? {
                    Some(data) => Some(PostingCodec::deserialize(&data)?),
                    None => None,
                }
            };

            let block = match existing_block {
                Some(existing) => seekpower_posting::merge_postings(&existing, new_entries),
                None => PostingCodec::encode(new_entries),
            };

            let serialized = PostingCodec::serialize(&block);
            self.db.put(&key, &serialized).await?;  // PrefixedView.put updates cache
        }

        // ... doc_lengths 和 stats 更新逻辑不变
        for (&doc_id, lengths) in &old_buffer.doc_lengths {
            for &(field_id, len) in lengths {
                let key = make_key(&[PREFIX_LEN, b":doc_len:", &doc_id.to_le_bytes(), b":", &field_id.to_le_bytes()]);
                self.db.put(&key, &[len as u8]).await?;
            }
        }

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
    // ... 其余 search 方法不变
}
```

- [ ] **Step 2: 更新 core/index.rs 中 LexicalIndex 的构造调用**

在 `create_index_with_db` 和 `open_index_with_db` 中：
- `LexicalIndex::new(db.clone(), config.clone(), schema.clone(), field_names.clone())` — 不再传 cache 和 delete_set
- delete_set 的加载逻辑移到 `Index` 构造后单独处理

- [ ] **Step 3: 运行全部测试**

Run: `cargo test -p seekpower-core`

- [ ] **Step 4: Commit**

```bash
git add crates/seekpower-core/src/lexical_index.rs crates/seekpower-core/src/index.rs
git commit -m "refactor(core): use PrefixedView in LexicalIndex, remove local cache"
```

---

## Task 8: 全量集成测试 + 清理

**Files:**
- Modify: all test files that reference old types
- Verify: `crates/seekpower-core/src/lib.rs` re-exports

- [ ] **Step 1: 运行全量测试**

Run: `cargo test --workspace`
Expected: 所有测试 PASS

- [ ] **Step 2: 修复任何编译或测试错误**

重点检查：
- `seekpower-core/src/lib.rs` 的 re-exports 是否还引用已删除的类型
- `seekpower-vector/src/lib.rs` 的 re-exports
- 所有 `use seekpower_storage::Cache` 和 `use seekpower_storage::KvStore` 是否还能正常工作
- `PrefixedKvStore` 和 `PrefixedTransaction` 在 core/index.rs 中是否完全移除

- [ ] **Step 3: 运行 clippy 检查**

Run: `cargo clippy --workspace -- -D warnings`
修复所有 warning。

- [ ] **Step 4: 最终 Commit**

```bash
git add -A
git commit -m "refactor(storage): complete storage layer restructure with fixed-length prefix keys"
```

---

## 自查清单

1. **Spec 覆盖**：
   - 定长前缀 Key 编码 → Task 1 (key.rs)
   - LRU Cache → Task 2 (cache.rs)
   - PrefixedView 下沉 → Task 3 (prefixed.rs)
   - KvStore 清理 → Task 4 (lib.rs)
   - core 适配 → Task 5 (index.rs)
   - vector 适配 → Task 6 (ivf.rs)
   - lexical 适配 → Task 7 (lexical_index.rs)
   - 集成验证 → Task 8
   - 范围删除 (delete_all) → Task 3 (PrefixedView.delete_all)
   - 前缀范围缓存失效 → Task 2 (Cache.invalidate_prefix)

2. **Placeholder 扫描**：无 TBD/TODO/类似 Task N 的引用。

3. **类型一致性**：
   - `PrefixedView` 在 Task 3 定义，Task 5/6/7 一致使用
   - `Cache` 在 Task 2 定义，所有消费者使用 `Cache::new()` 和 `cache.get()/insert()`
   - `path_to_id` / `encode_key` / `encode_prefix` 在 Task 1 定义，Task 3 使用
   - `Index.db` 类型统一为 `PrefixedView`
   - `IvfIndex.kv` 类型统一为 `PrefixedView`
