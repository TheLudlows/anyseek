# SeekPower 多索引共享 SlateDB 设计文档

> 日期: 2026-06-30
> 版本: 1.0
> 状态: 待审批

---

## 1. 项目概述

### 1.1 目标

将 SeekPower 从"每索引 = 一个 SlateDB 实例"改为"单服务器进程 = 一个 SlateDB 实例，所有索引通过 key 前缀隔离"。服务器进程内可以同时加载、查询、删除任意数量的索引。

### 1.2 现状（待改造）

- `KvStore::open(path, store)` 当前在内部调用 `slatedb::Db::open(path, store)`，把 `path` 作为 SlateDB 物理存储位置
- 每次 `create_index` / `open_index` 都会新开一个 SlateDB 实例
- key 格式（`m:{path}:schema`、`i:{path}:{term_hash}` 等）已经包含 path，但 path 在 SlateDB 层就被隔离了，所以 key 里的 path 是冗余的
- `seekpower-server` 的 `state.rs` 已经定义了 `AppState { indexes: HashMap, db: KvStore }`，但 handler 还在用旧的单索引 `SharedIndex = Mutex<Option<Index>>`

### 1.3 改造后

- 服务器启动时打开**一个**共享 SlateDB（物理位置 = 配置项 `data_dir`）
- 每个索引通过 `KvStore::for_index(path)` 派生一个带前缀的视图，共享同一底层 SlateDB
- 所有 key 物理上混在一个 SlateDB 里，按 `path:` 前缀隔离
- 服务端 API 用 query 参数 `?index=...` 选索引；新增 `GET /indexes` 列出全部索引

### 1.4 设计决策摘要

| 决策 | 选择 |
|---|---|
| 多索引拓扑 | **共享 SlateDB + 前缀隔离**（方案 A） |
| 索引名字符集 | `[a-zA-Z0-9_-]+`，长度 1..=128 |
| 保留前缀 | `__indexes__` 等系统目录用 `__` 前缀；用户可用任何合法 path 名（无字符级保留），但建议避免与系统目录冲突 |
| SlateDB 位置 | 服务器启动时由 `data_dir` 配置决定，全进程唯一 |
| 路由选索引 | HTTP query 参数 `?index=...` |
| 索引发现 | DB 内 `__indexes__` 前缀维护目录，启动时扫描恢复 |
| 索引删除 | 物理删除该索引全部 key + 删 `__indexes__` 目录项 |
| 错误处理 | 新增 `InvalidIndexName`、`IndexAlreadyExists`、`IndexNotLoaded` 变体 |
| 测试范围 | 单元 + 集成测试都要更新 |

---

## 2. 整体架构

### 2.1 架构图

```
┌──────────────────────────────────────────────────────────────────┐
│                          seekpower-server                        │
│  ┌─────────────────┐                                             │
│  │ AppState        │                                             │
│  │  ├─ db: KvStore │ ← for_index("__indexes__") 维护索引目录    │
│  │  └─ indexes:    │                                             │
│  │      HashMap<   │                                             │
│  │        String,  │                                             │
│  │        Arc<Idx> │                                             │
│  │      >          │                                             │
│  └─────────────────┘                                             │
│                                                                  │
│   indexes["blog"]: Arc<Index>     indexes["shop"]: Arc<Index>   │
│   ├─ db: KvStore ──┐              ├─ db: KvStore ──┐             │
│   ├─ path:"blog"   │              ├─ path:"shop"   │             │
│   └─ cache, vec..  │              └─ cache, vec..  │             │
│                    ▼                              ▼              │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ KvStore::open("./data", object_store) — 共享 SlateDB     │    │
│  │  物理上一个 SlateDB 实例，所有 key 混在一起               │    │
│  │  按 path 前缀隔离: m:blog:schema / m:shop:schema / ...   │    │
│  └─────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────┘
```

### 2.2 数据流

**创建索引**：
1. HTTP POST `/index/create` 携带 `name`、schema、config
2. handler 校验 `name`（validate_path）
3. 写 `__indexes__:<name> = b"1"`（即使后续失败也能被 open 跳过/重试）
4. `create_index(&state.db, &name, config, schema)` 内部调 `state.db.for_index(&name)` 派生带前缀视图，写 `m:schema`、`m:config`、`m:stats`
5. 成功后把 `Arc<Index>` 插 HashMap

**搜索**：
1. HTTP POST `/search?index=blog&...`
2. handler 从 query 取 `index` 名字
3. `take_index(&state, "blog")` 查 HashMap
4. `search(&index, request)` —— 内部所有 `db.put/get` 经 `for_index("blog")` 自动加 `blog:` 前缀

**删除**：
1. HTTP DELETE `/index/blog`
2. HashMap 中移除
3. `delete_index(&state.db, "blog")` → `for_index("blog").scan_all()` → 批量 delete → 删 `__indexes__:blog`

---

## 3. `KvStore` 改造

### 3.1 数据结构

```rust
pub struct KvStore {
    inner: Arc<KvStoreInner>,
}

struct KvStoreInner {
    db: slatedb::Db,        // 共享的 SlateDB 实例，Arc 计数
    prefix: Vec<u8>,        // 该视图的前缀（空 = 原始视图）
}
```

`Clone` 克隆 Arc，开销极低（几十纳秒），可放心到处传。

### 3.2 API

```rust
impl KvStore {
    /// 打开共享 SlateDB（服务器启动时调用一次）
    pub async fn open(path: &str, object_store: Arc<dyn ObjectStore>) -> Result<Self>;

    /// 派生带前缀的视图，共享底层 SlateDB
    pub fn for_index(&self, path: &str) -> Self;

    /// 当前视图是否原始（无前缀）
    pub fn is_raw(&self) -> bool;

    // put / get / delete / scan_prefix —— 内部自动拼/剥前缀
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    pub async fn delete(&self, key: &[u8]) -> Result<()>;
    pub async fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    // 列出该视图下所有 key（prefix = ""）
    pub async fn scan_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;

    // 序列化版本（不变）
    pub async fn put_serialized<T: Serialize>(&self, key: &[u8], value: &T) -> Result<()>;
    pub async fn get_deserialized<T: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>>;

    /// 关闭底层 SlateDB。仅原始视图有效，派生视图返回错误
    pub async fn close(&self) -> Result<()>;
}
```

### 3.3 前缀语义

`for_index("blog")` 生成的前缀是 `b"blog:"`。所以：
- `view.put(b"m:schema", val)` 实际写入 SlateDB 的 key 是 `b"blog:m:schema"`
- `view.scan_prefix(b"m:")` 返回的 key 是 `b"m:schema"`（已剥前缀）—— 上层不感知 path
- `view.scan_all()` 等价于 `scan_prefix(b"")`，返回该 view 前缀下所有业务 key

### 3.4 path 校验

```rust
pub fn validate_path(path: &str) -> Result<()>;
```

规则：
- 非空
- 长度 ≤ 128
- 所有字节 ∈ `[a-zA-Z0-9_-]`

失败时返回 `SeekPowerError::InvalidIndexName(String)`。

`KvStore::for_index` 入口调用 `validate_path`。系统目录（如 `__indexes__`）也是合法 path 名字，仅靠命名约定避免与用户索引冲突。

---

## 4. 错误处理

### 4.1 新增错误变体

```rust
// crates/seekpower-error/src/lib.rs
pub enum SeekPowerError {
    // ... 已有变体 ...
    InvalidIndexName(String),
    IndexAlreadyExists(String),
    IndexNotLoaded(String),
}
```

### 4.2 HTTP 映射

| 错误 | HTTP 状态 |
|---|---|
| `InvalidIndexName` | 400 Bad Request |
| `IndexAlreadyExists` | 409 Conflict |
| `IndexNotFound` / `IndexNotLoaded` | 404 Not Found |
| `StorageError` | 500 Internal Server Error |
| `InvalidQuery` | 400 Bad Request |

---

## 5. `seekpower-core` 改造

### 5.1 函数签名变更

调用方传入的 `shared_db: &KvStore` 是**服务器启动时打开的共享 raw 视图**；`create_index` 内部用 `shared_db.for_index(path)` 派生一个带前缀的 owned `KvStore` 视图赋给 `Index.db`。

```rust
// 改前
pub async fn create_index(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    config: IndexConfig,
    schema_fields: Vec<SchemaField>,
) -> Result<Index>;

pub async fn open_index(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Index>;

// 改后
pub async fn create_index(
    shared_db: &KvStore,
    path: &str,
    config: IndexConfig,
    schema_fields: Vec<SchemaField>,
) -> Result<Index>;

pub async fn open_index(
    shared_db: &KvStore,
    path: &str,
) -> Result<Index>;
```

`create_index_with_db` / `open_index_with_db` 接受带前缀的 `KvStore` 视图，函数体不变（除了 key 构造，见 5.2）。

### 5.2 key 构造改造

`seekpower-core/src/lib.rs` 里所有 `make_key(&[PREFIX_X, path, ...])` 调用删掉 path：

| 改前 | 改后 |
|---|---|
| `make_key(&[PREFIX_META, path.as_bytes(), b":schema"])` | `b"m:schema"` |
| `make_key(&[PREFIX_META, path.as_bytes(), b":config"])` | `b"m:config"` |
| `make_key(&[PREFIX_META, path.as_bytes(), b":stats"])` | `b"m:stats"` |
| `make_key(&[PREFIX_DELL, path.as_bytes(), b":"])` | `b"l:"` |
| `make_key(&[PREFIX_DOC, index.path.as_bytes(), b":", &doc_id.to_le_bytes()])` | `make_key(&[PREFIX_DOC, b":", &doc_id.to_le_bytes()])` |
| `make_key(&[PREFIX_DELL, index.path.as_bytes(), b":", &doc_id.to_le_bytes()])` | `make_key(&[PREFIX_DELL, b":", &doc_id.to_le_bytes()])` |
| `make_key(&[PREFIX_IDX, index.path.as_bytes(), b":", &term_hash.to_le_bytes()])` | `make_key(&[PREFIX_IDX, b":", &term_hash.to_le_bytes()])` |
| `make_key(&[PREFIX_LEN, index.path.as_bytes(), b":doc_len:", ...])` | `make_key(&[PREFIX_LEN, b":doc_len:", ...])` |
| `make_key(&[PREFIX_META, index.path.as_bytes(), b":stats"])` | `b"m:stats"` |

### 5.3 `Index` 结构体

保持现状。`db: KvStore` 现在是带前缀的视图，`path: String` 字段保留（用于日志和未来扩展）。

### 5.4 物理删除

新增 `delete_index`：

```rust
pub async fn delete_index(shared_db: &KvStore, path: &str) -> Result<()> {
    seekpower_storage::validate_path(path)?;
    let view = shared_db.for_index(path);
    let entries = view.scan_all().await?;
    for (key, _) in entries {
        view.delete(&key).await?;
    }
    shared_db.for_index("__indexes__").delete(path.as_bytes()).await?;
    Ok(())
}
```

---

## 6. `seekpower-vector` 改造

### 6.1 key helper 去 path

`crates/seekpower-vector/src/lib.rs` 的 4 个函数：

```rust
// 改前
fn meta_key(path: &str) -> Vec<u8>;
fn medoid_key(path: &str, id: u32) -> Vec<u8>;
fn medoid_prefix(path: &str) -> Vec<u8>;
fn cluster_key(path: &str, id: u32) -> Vec<u8>;

// 改后
fn meta_key() -> Vec<u8> { b"v:meta".to_vec() }
fn medoid_key(id: u32) -> Vec<u8> { make_key(&[PREFIX_VEC, b":medoid:", &id.to_le_bytes()]) }
fn medoid_prefix() -> Vec<u8> { make_key(&[PREFIX_VEC, b":medoid:"]) }
fn cluster_key(id: u32) -> Vec<u8> { make_key(&[PREFIX_VEC, b":cluster:", &id.to_le_bytes()]) }
```

### 6.2 `IvfIndex::new`

```rust
// 改前
pub fn new(db: KvStore, path: &str, config: IndexConfig) -> Self;

// 改后
pub fn new(db: KvStore, config: IndexConfig) -> Self;
```

调用方同步更新（`seekpower-core` 的 `create_index_with_db` / `open_index_with_db`）。

---

## 7. `seekpower-server` 改造

### 7.1 state.rs

```rust
pub type SharedState = Arc<AppState>;

pub struct AppState {
    pub indexes: Mutex<HashMap<String, Arc<Index>>>,
    pub db: KvStore,
}

pub fn take_index(state: &SharedState, name: &str) -> Result<Arc<Index>, SeekPowerError>;
```

`lib.rs` 导出 `SharedState`（不再导出 `SharedIndex`）。

### 7.2 启动流程（main.rs）

```rust
async fn build_state(config: &Config) -> SharedState {
    let store: Arc<dyn ObjectStore> = match config.storage.kind.as_str() {
        "local" => Arc::new(object_store::local::LocalFileSystem::new_with_prefix(&config.storage.path).unwrap()),
        _ => Arc::new(object_store::memory::InMemory::new()),
    };

    let raw_db = KvStore::open(&config.storage.path, store).await
        .expect("failed to open shared SlateDB");

    let mut indexes = HashMap::new();
    let index_dir = raw_db.for_index("__indexes__");
    if let Ok(entries) = index_dir.scan_all().await {
        for (name, _) in entries {
            let name_str = String::from_utf8(name).expect("invalid index name in __indexes__");
            match seekpower_core::open_index(&raw_db, &name_str).await {
                Ok(idx) => { indexes.insert(name_str, Arc::new(idx)); }
                Err(e) => tracing::warn!("skip index {}: {}", name_str, e),
            }
        }
    }

    Arc::new(AppState { indexes: Mutex::new(indexes), db: raw_db })
}
```

### 7.3 路由

```rust
pub fn create_app(state: SharedState) -> Router {
    Router::new()
        .route("/indexes",               get(handle_list_indexes))
        .route("/index/create",          post(handle_create_index))
        .route("/index/open",            post(handle_open_index))
        .route("/index/:path",           delete(handle_delete_index))
        .route("/document",              post(handle_index_document))
        .route("/documents",             post(handle_index_documents))
        .route("/document/:id",          get(handle_get_document).delete(handle_delete_document))
        .route("/commit",                post(handle_commit))
        .route("/search",                post(handle_search))
        .with_state(state)
}
```

### 7.4 query 参数选索引

```rust
#[derive(Deserialize)]
struct IndexQuery {
    #[serde(default)]
    index: Option<String>,
}
```

所有需要索引的 handler 提取 `Query<IndexQuery>`，从中拿 `name`，再 `take_index(&state, &name)?`。

### 7.5 model.rs

`CreateIndexRequest` / `OpenIndexRequest` 字段名 `path` → `name`。`object_store_kind` 字段保留以兼容客户端，handler 忽略其值。

### 7.6 handler 行为

**`handle_create_index`**：
1. `validate_path(&name)` → 400 if fail
2. 检查 HashMap 是否有同名 → 409 if exists
3. 写 `state.db.for_index("__indexes__").put(name.as_bytes(), b"1")` → 500 if fail
4. `create_index(&state.db, &name, config, schema)` → 失败则回滚 `__indexes__` 条目并返回错误
5. 插 HashMap → 201

**`handle_open_index`**：
1. 校验 name
2. 检查 `__indexes__` 目录 → 404 if missing
3. 检查 HashMap → 已存在则直接返回 200
4. `open_index(&state.db, &name)` → 插 HashMap → 200

**`handle_delete_index`**：
1. HashMap 移除（不存在则 404）—— 这一步与其它请求互斥
2. 同步 `delete_index(&state.db, &name)` —— 物理删 + 删 `__indexes__` 条目
3. 失败回滚（把已删除的 Arc 重新插入 HashMap）
4. 200

**`GET /indexes`**：返回 HashMap 里的所有名字。

**其它 handler（search、document 等）**：从 query 取 index 名字，`take_index`。

---

## 8. 生命周期与并发

### 8.1 启动
1. 打开共享 SlateDB
2. 扫 `__indexes__` 自动恢复所有索引
3. 启动 HTTP

### 8.2 运行
- `create_index` 先写 `__indexes__` 再调 core，失败回滚目录项
- `open_index` 把已存在索引从"DB-only"状态加载进 HashMap
- `delete_index` 严格同步执行三步：(1) HashMap 移除；(2) 物理删 DB；(3) 删 `__indexes__` 条目
  任何一步失败都返回 5xx；前序步骤已成功的副作用不回滚，靠人工重试（或启动时扫到孤儿 `__indexes__` 条目时清理）

### 8.3 关闭
- `axum::serve` + graceful shutdown（`tokio::signal::ctrl_c`）
- 关停后调 `state.db.close().await`
- HashMap 里的 `Arc<Index>` 随 `AppState` drop 自动析构

### 8.4 并发
- `state.indexes: Mutex<HashMap>` —— 写少读多，持锁时间极短
- `state.db: KvStore` —— 内部 `Arc<KvStoreInner>`，clone 廉价
- 每个 `Index` 内部已有的 `RwLock<IndexStats>` / `RwLock<WriteBuffer>` / `RwLock<delete_set>` 保持不变
- `commit` 通过 `std::mem::take` 原子取出 WriteBuffer，避免持锁做 I/O

### 8.5 容错
- 启动时单索引 `open_index` 失败仅 warn，不阻塞其它
- `create_index` 失败时回滚 `__indexes__` 条目
- 重复创建返回 409

---

## 9. 测试策略

### 9.1 `seekpower-storage` 单元测试
- `test_kvstore_for_index_isolates_keys` —— 隔离性
- `test_kvstore_scan_prefix_strips_view_prefix` —— 前缀剥除
- `test_kvstore_scan_all_lists_index_data` —— 全量扫描
- `test_kvstore_clone_shares_underlying` —— Arc 共享
- `test_kvstore_close_only_on_raw` —— close 限制
- `test_kvstore_for_index_rejects_bad_path` —— path 校验
- `test_validate_path_*` —— 各种合法/非法 path
- 保留 `test_readcache_*`

### 9.2 `seekpower-core` 单元测试
- `test_two_indexes_share_db` —— 同一 db 上两索引互不可见
- `test_open_index_fails_for_missing` —— 不存在
- `test_delete_index_removes_all_keys` —— 删除彻底
- `test_index_data_survives_reopen` —— 持久化 + 重新打开
- 现有 `test_commit_persists_data` 等：内部 `create_index` 调用方式改为 `&KvStore`

### 9.3 `seekpower-vector` 单元测试
- 现有测试里 `IvfIndex::new` 去掉 path 参数

### 9.4 `seekpower-server` 单元测试
- `test_state_take_index` —— 取已存在/不存在的 index
- `test_indexes_listing` —— 列表
- `test_create_via_http_writes_index_directory` —— 写 `__indexes__` 目录

### 9.5 集成测试
- `test_multi_index_lifecycle` —— create A、create B、index doc 到 A、index doc 到 B、search A 拿不到 B 的 doc、delete A、B 仍可用
- `test_search_with_index_query_param` —— `POST /search?index=blog`
- `test_search_unknown_index_returns_404`
- `test_create_then_list_indexes`
- `test_delete_index_removes_from_storage` —— 删除后重启 SharedState 不再列出
- `test_two_indexes_isolated` —— 跨索引 search 互不干扰
- `test_invalid_index_name_returns_400` —— `?index=__reserved` 或 `?index=foo/bar`
- 现有 `test_search_on_empty_index_returns_empty_results` 改为传 `?index=test-server`

---

## 10. 实施步骤

按依赖顺序，最小可工作单元优先。

**Phase 0 — 准备**
- `seekpower-error` 加新变体
- `cargo build` 绿

**Phase 1 — `KvStore` 改造**
- 内部 `Arc<KvStoreInner>`
- `for_index` / `is_raw` / 前缀拼接到 put/get/delete
- `scan_prefix` 剥前缀
- `scan_all` 新增
- `close` 限制仅 raw
- `validate_path` 静态函数
- `seekpower-storage` 单测全绿

**Phase 2 — `seekpower-core` 适配**
- 改 `create_index` / `open_index` 签名
- 删掉 key 构造里的 `path` 重复
- 加 `delete_index(shared_db, path)`
- `seekpower-core` 单测绿

**Phase 3 — `seekpower-vector` 适配**
- 4 个 key helper 去 path
- `IvfIndex::new` 去 path 参数
- 单测绿

**Phase 4 — `seekpower-server` 改造**
- `state.rs` 切到 `SharedState`
- `main.rs` 启动时打开共享 SlateDB、扫 `__indexes__` 恢复
- `model.rs` 字段 `path` → `name`
- `router.rs` 新增 `GET /indexes`
- 所有 handler 用 `Query<IndexQuery>` 选 index
- `handle_create_index` / `handle_delete_index` 维护 `__indexes__` 目录
- 单测绿

**Phase 5 — 集成测试**
- 更新 `tests/server_integration_test.rs`
- 更新或新增 `tests/integration_test.rs`
- `cargo test --workspace` 全绿

**Phase 6 — 收尾**
- 更新 `seekpower-storage/README.md` key 命名空间说明
- 更新 `docs/` 架构文档（如有）
- `cargo clippy --workspace --all-targets` 修警告
- `cargo fmt --check` 修格式

**回滚策略**：每 Phase 独立 commit。如要回滚，`git revert` 对应 commit。

---

## 11. 不在本次 scope 内

- SlateDB 内部一致性（库的责任）
- `object_store` 行为（库的责任）
- 性能基准与调优
- 索引导入/导出
- 索引分片 / 多服务器节点
- 索引 ACL / 权限
- 索引 schema 演进
