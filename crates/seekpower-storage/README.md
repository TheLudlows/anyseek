# seekpower-storage

SlateDB KV 封装，内建定长前缀索引隔离与数据类型标签。

## 核心设计

所有索引共享同一个 SlateDB 实例，通过 `data_type`（u8，1 字节）+ `index_id`（u32 大端序，4 字节）实现键空间隔离与数据类型区分。

**物理键格式**: `data_type (1B) ‖ index_id (4B BE) ‖ business_key`

`data_type` 由 `seekpower_schema` 定义：

| 常量 | 值 | 用途 |
|------|-----|------|
| `TYPE_META` | 0 | 元数据 (schema, config, stats, fields) |
| `TYPE_DOC` | 1 | 文档原文 |
| `TYPE_IDX` | 2 | 倒排索引 (posting list) |
| `TYPE_VEC` | 3 | 向量索引 (meta, medoid, cluster) |
| `TYPE_DELL` | 4 | 删除标记 (tombstone) |
| `TYPE_LEN` | 5 | 文档长度 |

## KvStore

`KvStore` 是 SlateDB 的薄封装，提供两类方法：

- **裸操作** (`put`/`get`/`delete`/`scan_prefix`): 直接操作物理键，用于索引注册表等全局数据
- **带前缀操作** (`prefixed_put`/`prefixed_get`/`prefixed_delete`/`prefixed_scan_prefix`/`prefixed_scan_all`/`prefixed_put_serialized`/`prefixed_get_deserialized`/`prefixed_delete_all`): 自动附加/剥离 `data_type` + `index_id` 前缀，用于索引内数据

### 用法示例

```rust
use seekpower_storage::{KvStore, name_to_id};
use seekpower_schema::{TYPE_META, TYPE_IDX, TYPE_DOC, TYPE_VEC};

// 打开共享数据库
let db = KvStore::open("my_db", object_store).await?;

// 全局数据（不带前缀）
db.put(b"__indexes__:articles", b"1").await?;

// 索引内数据（带 index_id + data_type 前缀）
let index_id = name_to_id("articles");
db.prefixed_put(index_id, TYPE_META, b"schema", &schema_bytes).await?;
db.prefixed_put(index_id, TYPE_IDX, &term_hash.to_le_bytes(), &posting_bytes).await?;

// 读取时自动剥离前缀
let schema = db.prefixed_get(index_id, TYPE_META, b"schema").await?;

// 扫描索引内所有 posting list
let postings = db.prefixed_scan_all(index_id, TYPE_IDX).await?;

// 删除整个索引
db.prefixed_delete_all(index_id).await?;
```

## 键编解码工具

```rust
use seekpower_storage::{encode_key, encode_prefix, encode_index_prefix, name_to_id};

// 将索引名称映射为定长 u32 ID
let id = name_to_id("articles");

// 编码带前缀的键: [data_type 1B] [index_id BE 4B] [business_key]
let key = encode_key(id, TYPE_META, b"schema");

// 编码前缀扫描键: [data_type 1B] [index_id BE 4B]
let prefix = encode_prefix(id, TYPE_IDX);

// 编码仅 index_id 的前缀（用于 delete_all）
let idx_prefix = encode_index_prefix(id);
```

## 事务

`KvStore::begin_transaction()` 返回 `Transaction`，支持原子批量写入。事务内使用 `encode_key` 手动构建带前缀的键。

```rust
let txn = db.begin_transaction().await?;
let key = encode_key(index_id, TYPE_DOC, &doc_id.to_le_bytes());
txn.put(&key, b"value")?;
txn.commit().await?;
```

## 序列化

`KvStore` 同时提供裸和带前缀的序列化方法，使用 `serde_json` 编码：

- `put_serialized` / `get_deserialized` — 裸键
- `prefixed_put_serialized` / `prefixed_get_deserialized` — 带 `data_type` + `index_id` 前缀键
