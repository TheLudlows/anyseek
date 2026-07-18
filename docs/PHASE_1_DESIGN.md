# SeekPower v3 阶段一设计文档

> 基础架构搭建：对象存储层和基础框架
>
> **周期**：4 周
>
> **目标**：搭建对象存储层和基础框架，为后续功能奠定基础

---

## 目录

- [§1 阶段目标](#1-阶段目标)
- [§2 核心任务](#2-核心任务)
- [§3 技术架构](#3-技术架构)
- [§4 存储层设计](#4-存储层设计)
- [§5 ObjectStore Trait 设计](#5-objectstore-trait-设计)
- [§6 存储后端实现](#6-存储后端实现)
- [§7 装饰器栈设计](#7-装饰器栈设计)
- [§8 双层 WAL 设计](#8-双层-wal-设计)
- [§9 Manifest 管理器设计](#9-manifest-管理器设计)
- [§10 Builder 模式设计](#10-builder-模式设计)
- [§11 错误处理与重试](#11-错误处理与重试)
- [§12 目录结构](#12-目录结构)
- [§13 验收标准](#13-验收标准)

---

## §1 阶段目标

### 1.1 总体目标

搭建对象存储层和基础框架，实现存储抽象、多种存储后端、装饰器模式、持久化机制和错误处理。

### 1.2 关键成果

- 统一的对象存储抽象接口
- 两种存储后端实现（S3/FS）
- 基于 OpenDAL 的统一存储服务
- 可靠的持久化机制（双层 WAL + Manifest）
- 完善的错误处理体系和统一重试策略

---

## §2 核心任务

### 2.1 任务清单

1. 实现 ObjectStore Trait
2. 实现两种后端（S3/FS）基于 OpenDAL
3. 实现 Manifest 管理器
4. 实现双层 WAL 读写器
5. 实现基础错误处理和重试策略

### 2.2 任务依赖

```
ObjectStore Trait (任务1)
    ├── S3Store (任务2)
    ├── FsStore (任务2)
    ├── ManifestManager (任务3)
    └── WALManager (任务4)
```

---

## §3 技术架构

### 3.1 整体架构

```
┌─────────────────────────────────────────────────────────────────┐
│                         应用层                                │
│                    (后续阶段实现)                                │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      存储后端层（Builder模式）                   │
│                    │  ┌─────────┐  ┌─────────┐  │                │
│                    │  │ S3Store │  │ FsStore │  │                │
│                    │  │(OpenDAL)│ │(OpenDAL)│  │                │
│                    │  └─────────┘  └─────────┘  │                │
│                    └─────────────────────────────┘                │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                   OpenDAL 抽象层                                 │
│  ┌─────────────────┐  ┌─────────────────┐                     │
│  │  S3 Service     │  │  FS Service     │                     │
│  │(s3://bucket/)   │  │(file:///path/)  │                     │
│  └─────────────────┘  └─────────────────┘                     │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      持久化层（双层WAL）                         │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐            │
│  │本地WAL(快速)│  │  Manifest   │  │ S3 WAL(备份)│            │
│  └─────────────┘  └─────────────┘  └─────────────┘            │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                      错误处理层                                │
│          (统一错误类型、重试策略、日志记录)                      │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 技术选型

| 组件 | 技术方案 | 理由 |
|------|----------|------|
| 存储抽象 | Trait | 支持多种后端，易于扩展 |
| 存储服务 | OpenDAL | 统一抽象层，支持 S3/FS 等多种后端 |
| S3 服务 | opendal::services::s3 | OpenDAL 内置 S3 服务，处理签名/重试 |
| 文件服务 | opendal::services::fs | OpenDAL 内置 FS 服务，原子写入支持 |
| 序列化 | serde + bincode | 高效二进制序列化 |
| 日志 | tracing | 结构化日志 |

### 3.3 OpenDAL 优势

- **统一接口**：S3、本地文件使用相同 API
- **内置重试**：OpenDAL 内置指数退避重试策略
- **自动签名**：S3 自动处理 AWS Signature V4
- **内置缓存**：OpenDAL 内置缓存支持，无需额外层
- **高性能**：异步 I/O，连接池复用
- **易于扩展**：支持 100+ 存储服务

### 3.4 重试策略参数

- 最大重试次数：3 次（可配置）
- 初始延迟：100ms
- 退避因子：2（指数）
- 最大延迟：5s
- 可重试错误：超时、连接错误、5xx 服务错误

---

## §4 存储层设计

### 4.1 设计原则

- **抽象优先**：所有操作通过 Trait 定义，实现与接口分离
- **OpenDAL 核心**：使用 OpenDAL 作为底层存储服务
- **异步优先**：使用 tokio 异步运行时，避免阻塞
- **可组合**：装饰器模式支持缓存、监控等功能组合
- **容错设计**：支持重试、超时、错误恢复

### 4.2 存储模型

```
对象存储模型：
- Key: 字符串路径 (path: &str)
- Value: 字节数据 (Bytes)
- Metadata: 大小、ETag、修改时间
```

### 4.3 操作语义

| 操作 | 语义 | 错误处理 |
|------|------|----------|
| put | 覆盖写入 | 失败返回错误 |
| get | 读取 | 不存在返回 NotFound |
| delete | 删除 | 不存在也返回成功 |
| list | 列举前缀 | 空前缀返回空列表 |
| range | 范围读取 | 越界返回部分数据 |
| put_if_absent | 条件写入 | 已存在返回 false |
| put_if_match | 条件更新 | ETag 不匹配返回 false |
| metadata | 获取元数据 | 不存在返回 NotFound |

---

## §5 ObjectStore Trait 设计

### 5.1 核心方法

**基本操作**：
- put：存储对象（覆盖写入）
- get：获取对象
- delete：删除对象
- list：列举前缀下的所有对象

**高级操作**：
- range：范围读取（支持部分加载）
- put_if_absent：条件写入（不存在才写入）
- put_if_match：条件更新（ETag 匹配才更新）
- metadata：获取对象元数据

### 5.2 ObjectMetadata 结构

包含：
- size：对象大小（字节）
- etag：实体标签（用于 CAS）
- last_modified：最后修改时间（时间戳）

### 5.3 错误类型

定义统一的错误类型：
- NotFound：对象不存在
- AlreadyExists：对象已存在
- PermissionDenied：权限不足
- InvalidArgument：参数无效
- Internal：内部错误
- Unavailable：服务不可用

---

## §6 存储后端实现

### 6.1 S3Store

**特点**：
- 使用 OpenDAL S3 Service
- 自动处理 AWS 签名认证
- 支持条件操作（通过版本控制）
- 内置重试和指数退避
- 分片上传支持大文件

**实现要点**：
- 使用 opendal::Operator with S3 service
- OpenDAL 自动签名认证（AWS Signature V4）
- 支持多区域配置
- 连接池复用
- 内置重试策略

**OpenDAL S3 配置**：
```rust
use opendal::services::S3;
use opendal::Operator;

let mut builder = S3::default();
builder
    .bucket(&bucket)
    .region(&region)
    .root(&prefix);

let operator = Operator::new(builder)?.finish();
```

### 6.2 FsStore

**特点**：
- 本地文件系统存储
- 使用 OpenDAL FS Service
- OpenDAL 内置原子写入支持
- 跨平台兼容（Windows/Linux/macOS）

**实现要点**：
- 使用 opendal::Operator with FS service
- OpenDAL 内置原子写入（临时文件 + 重命名）
- 目录结构自动创建
- 路径规范化

**OpenDAL FS 配置**：
```rust
use opendal::services::Fs;
use opendal::Operator;

let mut builder = Fs::default();
builder.root(&root_path);

let operator = Operator::new(builder)?.finish();
```

---

## §8 双层 WAL 设计

### 8.1 WAL 概念

WAL（Write-Ahead Log）是预写日志，用于保证数据持久性和可恢复：
- 先写本地 WAL（快速，通过 OpenDAL FS）
- 后台异步备份到 S3（持久，通过 OpenDAL S3）
- 崩溃后重放本地 WAL 恢复
- 灾难时从 S3 恢复

### 8.2 本地 WAL（快速路径）

**特点**：
- 位置：`{data_dir}/wal/{seq}.wal`
- 写入：通过 OpenDAL FS Service，<1ms 延迟
- 清理：commit 后删除（保留最近 2 个用于快速恢复）
- 用途：日常崩溃恢复

**实现**：
```rust
use opendal::services::Fs;
use opendal::Operator;

let mut builder = Fs::default();
builder.root(&format!("{}/wal", data_dir));
let local_op = Operator::new(builder)?.finish();
```

### 8.3 S3 WAL（备份路径）

**特点**：
- 位置：`s3://{bucket}/{tenant_prefix}/wal/{seq}.wal`
- 写入：通过 OpenDAL S3 Service，异步上传（commit 后）
- 清理：保留策略（默认保留 7 天）
- 用途：灾难恢复

**实现**：
```rust
use opendal::services::S3;
use opendal::Operator;

let mut builder = S3::default();
builder.bucket(&bucket).region(&region).root(&format!("{}/wal", prefix));
let s3_op = Operator::new(builder)?.finish();
```

### 8.4 WAL 格式

**文件命名**：
- 前缀：wal/
- 格式：{seq}.wal
- 示例：wal/0000001.wal

**文件结构**：
```
Header (16 bytes):
  - magic: [u8; 4] = b"WLOG"
  - version: u32 = 1
  - checksum: u32

Records:
  For each record:
    - record_type: u8 (ADD/DELETE)
    - doc_id: u64
    - payload_len: u32
    - payload: [u8]
    - checksum: u32
```

### 8.5 WALManager 功能

- 追加：写入日志记录（通过 OpenDAL）
- 读取：读取日志文件（通过 OpenDAL）
- 截断：删除已提交的记录（通过 OpenDAL）
- 恢复：重放未提交的记录（通过 OpenDAL）
- 清理：删除过期的 WAL 文件（通过 OpenDAL）
- 上传：异步上传到 S3（通过 OpenDAL）

### 8.6 轮换策略

- 单个 WAL 文件 > 100MB 时创建新文件
- 序列号递增：0001.wal, 0002.wal, ...

### 8.7 恢复流程

1. 扫描本地 WAL 目录，找到所有 WAL 文件（通过 OpenDAL FS list）
2. 按 seq 顺序读取记录（通过 OpenDAL FS read）
3. 根据 Manifest 中的生成号确定起点
4. 重放未提交的记录
5. 重建内存索引
6. 如果本地 WAL 丢失，从 S3 下载（通过 OpenDAL S3 read）

---

## §9 Manifest 管理器设计

### 9.1 Manifest 概念

Manifest 是元数据文件，记录系统的状态和结构：
- 格式版本
- Schema 定义
- 活跃段列表
- 删除集
- 统计信息
- 生成号（每次更新递增）

### 9.2 Manifest 结构

```rust
pub struct Manifest {
    pub version: u64,              // 生成号，每次更新递增
    pub format_version: u32,       // 格式版本
    pub schema: Schema,            // Schema 定义
    pub segments: Vec<SegmentMeta>, // 活跃段列表
    pub delete_set: RoaringBitmap,  // 删除集
    pub stats: Stats,              // 统计信息
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

pub struct SegmentMeta {
    pub id: u64,
    pub doc_count: u64,
    pub doc_id_range: (u64, u64),
    pub size_bytes: u64,
    pub created_at: SystemTime,
}
```

### 9.3 版本化机制

**生成号**：每次 commit 递增
- 用于检测冲突
- 实现乐观并发控制
- 支持版本回滚

**原子更新**：
- 使用 CAS 操作（put_if_match）
- 更新失败时重试
- 记录历史版本（可选）

### 9.4 存储位置

- 本地：通过 OpenDAL FS Service (`{data_dir}/manifest.json`)
- S3：通过 OpenDAL S3 Service (`s3://{bucket}/{tenant_prefix}/manifest.json`)

### 9.5 ManifestManager 功能

- 读取：加载最新 manifest（通过 OpenDAL）
- 更新：原子更新 manifest（通过 OpenDAL CAS）
- 版本检查：验证生成号
- 回滚：恢复到历史版本（通过 OpenDAL）

---

## §10 Builder 模式设计

### 10.1 ObjectStoreBuilder

**定义**：
```rust
pub struct ObjectStoreBuilder {
    backend: BackendType,
}

pub enum BackendType {
    S3 { bucket: String, region: String, prefix: String },
    Fs { root: PathBuf },
}
```

### 10.2 Builder 方法

```rust
impl ObjectStoreBuilder {
    pub fn new(backend: BackendType) -> Self;

    pub fn build(self) -> Result<Arc<dyn ObjectStore>>;
}
```

### 10.3 使用示例

```rust
let store = ObjectStoreBuilder::new(BackendType::S3 {
    bucket: "my-bucket".to_string(),
    region: "us-east-1".to_string(),
    prefix: "tenant-123".to_string(),
})
.build()?;
```

---

## §11 错误处理与重试

### 11.1 错误类型体系

**存储错误**（StoreError）：
- NotFound
- AlreadyExists
- PermissionDenied
- InvalidArgument
- Internal
- Unavailable

**持久化错误**（PersistenceError）：
- SerializationError
- DeserializationError
- CorruptedData
- ChecksumMismatch

**并发错误**（ConcurrencyError）：
- Conflict（版本冲突）
- Timeout（操作超时）
- RetryExhausted（重试耗尽）

### 11.2 Result 类型

所有返回结果使用：
- Result<T, Error>
- Error 包含上下文信息
- 支持错误链

### 11.3 错误处理策略

- **重试**：对于临时错误（网络超时）自动重试
- **降级**：对于非关键错误降级处理
- **日志**：记录错误详情和上下文
- **监控**：统计错误率和类型

### 11.4 统一重试策略

**参数**：
- 最大重试次数：3 次（可配置）
- 初始延迟：100ms
- 退避因子：2（指数）
- 最大延迟：5s

**可重试错误**：
- 网络超时
- 连接错误
- 5xx 服务错误

**实现**：
```rust
async fn retry_with_backoff<F, T, E>(f: F) -> Result<T>
where
    F: Fn() -> Pin<Box<dyn Future<Output = Result<T, E>> + Send>>,
    E: Into<StoreError>;
```

**OpenDAL 内置重试**：
- OpenDAL Operator 内置重试机制
- 可通过 `RetryInterceptor` 配置
- 支持自定义重试策略

---

## §12 目录结构

```
seekpower/
├── Cargo.toml                         # Workspace 配置
├── crates/
│   ├── seekpower-error/               # 错误类型（共享）
│   │   └── src/lib.rs
│   │
│   ├── seekpower-storage/             # 存储模块（所有存储相关）
│   │   └── src/
│   │       ├── lib.rs                 # 重新导出
│   │       ├── object_store.rs        # ObjectStore trait + 元数据
│   │       ├── retry.rs               # 统一重试策略
│   │       ├── builder.rs             # Builder 模式
│   │       ├── s3_store.rs            # S3Store 实现 (OpenDAL)
│   │       ├── fs_store.rs            # FsStore 实现 (OpenDAL)
│   │       └── retry.rs               # 重试装饰器
│   │
│   └── seekpower-persistence/         # WAL + Manifest
│       └── src/
│           ├── lib.rs
│           ├── wal.rs                 # 双层 WAL (OpenDAL)
│           ├── local_wal.rs           # 本地 WAL (OpenDAL FS)
│           ├── s3_wal.rs              # S3 WAL (OpenDAL S3)
│           └── manifest.rs            # Manifest 管理 (OpenDAL)
│
└── docs/
    ├── PHASE_1_DESIGN.md
    └── PHASE_1_IMPLEMENTATION.md
```

**核心 Crate**：
1. `seekpower-error` - 统一错误
2. `seekpower-storage` - 所有存储相关（trait + 2个后端）
3. `seekpower-persistence` - WAL + Manifest

---

## §13 验收标准

### 13.1 存储后端测试

- [ ] S3Store 所有操作测试通过（使用 OpenDAL S3）
- [ ] FsStore 所有操作测试通过（使用 OpenDAL FS）
- [ ] 条件操作（CAS）正确实现
- [ ] 错误处理正确

### 13.2 Builder 测试

- [ ] Builder 正确构建 S3Store
- [ ] Builder 正确构建 FsStore

### 13.3 持久化测试

- [ ] 本地 WAL 写入正确（通过 OpenDAL FS）
- [ ] S3 WAL 上传正确（通过 OpenDAL S3）
- [ ] Manifest 版本化正常工作（通过 OpenDAL）
- [ ] Manifest 原子更新正确（通过 OpenDAL）
- [ ] Manifest CAS 操作正确（通过 OpenDAL）
- [ ] WAL 恢复功能验证

### 13.3 集成测试

- [ ] Manifest + WAL 集成测试
- [ ] 错误恢复测试
- [ ] OpenDAL 多后端切换测试

---

**文档版本**: v3.1
**创建时间**: 2026-07-17
**更新时间**: 2026-07-18
**作者**: Claude (Phase 1 Design - OpenDAL Refactoring)