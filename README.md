# SeekPower

高性能搜索引擎，支持分布式存储和实时索引。

## 项目结构

```
seekpower/
├── crates/
│   ├── seekpower-error/       # 错误类型定义
│   ├── seekpower-storage/     # 对象存储抽象（S3/FS）
│   └── seekpower-persistence/ # 持久化层（WAL + Manifest）
├── docs/                      # 设计文档
└── Cargo.toml                 # Workspace 配置
```

## 模块说明

### seekpower-error
统一的错误类型定义，提供清晰的错误分类和上下文。

### seekpower-storage
统一对象存储抽象，支持：
- S3 存储
- 文件系统存储
- 条件操作（CAS）
- 范围读取

### seekpower-persistence
持久化机制：
- 双层 WAL（本地 + S3 备份）
- Manifest 版本管理
- 崩溃恢复

## 快速开始

### 安装

```toml
[workspace.dependencies]
seekpower-storage = { path = "crates/seekpower-storage" }
seekpower-persistence = { path = "crates/seekpower-persistence" }
seekpower-error = { path = "crates/seekpower-error" }
```

### 使用存储

```rust
use seekpower_storage::{BackendType, ObjectStoreBuilder};
use bytes::Bytes;

let store = ObjectStoreBuilder::new(BackendType::s3(
    "my-bucket".to_string(),
    "us-east-1".to_string(),
    "tenant-123".to_string(),
))
.build()
.await?;

store.put("data.txt", Bytes::from("hello world")).await?;
```

## 文档

- [阶段一设计](docs/PHASE_1_DESIGN.md) - 存储层和基础框架设计
- [阶段二设计](docs/PHASE_2_DESIGN.md) - Schema-Free 自动推断设计
- [架构设计](docs/ARCHITECTURE_V3.md) - v3 整体架构设计

## 开发

```bash
# 检查代码
cargo check

# 运行测试
cargo test

# 运行特定 crate 测试
cargo test -p seekpower-storage
```

## 许可

MIT