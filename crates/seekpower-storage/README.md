# SeekPower Storage

统一对象存储抽象，支持 S3 和文件系统后端。

## 核心功能

- **统一抽象**：S3 和本地文件系统使用相同 API
- **内置重试**：自动指数退避重试策略
- **条件操作**：支持 CAS (put_if_match, put_if_absent)
- **范围读取**：支持部分数据加载

## 安装

```toml
[dependencies]
seekpower-storage = "0.1"
seekpower-error = "0.1"
```

## 快速开始

### S3 存储

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

// 写入
store.put("data.txt", Bytes::from("hello world")).await?;

// 读取
let data = store.get("data.txt").await?;

// 删除
store.delete("data.txt").await?;
```

### 文件系统存储

```rust
use seekpower_storage::{BackendType, ObjectStoreBuilder};

let store = ObjectStoreBuilder::new(BackendType::fs(
    std::path::PathBuf::from("/data/storage"),
))
.build()
.await?;
```

## API

### 基本操作

| 方法 | 说明 |
|------|------|
| `put(location, bytes)` | 写入对象（覆盖） |
| `get(location)` | 读取对象 |
| `delete(location)` | 删除对象 |
| `list(prefix)` | 列举前缀下的对象 |

### 高级操作

| 方法 | 说明 |
|------|------|
| `range(location, offset, len)` | 范围读取 |
| `put_if_absent(location, bytes)` | 不存在才写入 |
| `put_if_match(location, bytes, etag)` | ETag 匹配才更新 |
| `metadata(location)` | 获取元数据 |

## 错误处理

```rust
use seekpower_error::StoreError;

match store.get("data.txt").await {
    Ok(data) => println!("{:?}", data),
    Err(StoreError::NotFound(key)) => eprintln!("对象不存在: {}", key),
    Err(e) => eprintln!("错误: {}", e),
}
```

## 内置特性

- **自动重试**：指数退避策略
- **异步 I/O**：高性能并发

## 许可

MIT