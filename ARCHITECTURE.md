# 当前架构

Seekpower 保留一个 Rust 核心库和一个 HTTP/Web UI 服务。当前是单机实现，本机 shard/level 用于并行与增量索引，不代表分布式路由或副本。

## 模块边界

| 层 | 代码 | 职责 |
| --- | --- | --- |
| UI | `seekpower_server/web/` | 搜索、分页、原文/链接预览、日期交互 |
| 服务 | `http_server.rs`、`operations.rs`、`keys.rs`、`server.rs` | HTTP 参数、API key、索引作用域、启动、日志与退出 |
| 核心入口 | `index.rs`、`search.rs`、`iterator.rs` | schema、文档操作、三类查询与遍历 |
| 全文 | `tokenizer.rs`、`index_posting.rs`、`intersection*.rs`、`union.rs` | 分词、倒排、布尔/短语、BM25 与剪枝 |
| 向量 | `vector.rs`、`vector_similarity.rs`、`clustering.rs` | 外部向量校验、量化、相似度、聚类与 IVF 检索 |
| 存储 | `commit.rs`、`doc_store.rs`、`compress_postinglist.rs` | 本地提交、文档压缩、posting 编码 |
| 过滤 | `metadata_filter.rs` | 共享 schema 校验、每分片字典编译、候选元数据判定 |
| 统计 | `info.rs` | 收集索引信息；服务记录启动索引概况 |

源码路径分别相对于 [核心目录](seekpower/src) 或 [服务目录](seekpower_server/src)。HTTP 请求/响应对象位于服务的 `api_types.rs`，创建字段和向量配置在服务边界转换为核心类型；`api_schema.rs` 为复用的核心值提供 OpenAPI 描述。核心不再依赖 utoipa 或数字/终端格式化库。`info::collect_index_info` 返回数字、枚举和时间组成的 `IndexInfo`；服务输出普通日志，不再渲染终端表格。核心使用 `log` 门面，不安装日志输出器，默认静默；服务初始化日志输出并支持 `RUST_LOG`。模型运行时与文件解析器仍在应用侧。

## 查询执行

HTTP 根据文本与向量输入推导 Lexical、Vector 或 Hybrid；Rust 核心调用方仍显式选择。Lexical 使用倒排，Vector 使用外部查询向量；Hybrid 收集两路候选，汇总本机分片结果后按文档 ID 执行 RRF。已提供 weighted RRF、两路独立候选预算与权重；没有公开的多策略自动规划器。

词法路径支持元数据过滤、文档命中计数和字段排序；向量路径支持检索字段选择、聚类探测与相似度阈值。三种模式复用元数据过滤谓词；向量评分/Top-K 前排除不匹配及已删除记录。ANN 保留已有簇探测规则，可能因未探测簇中的有效记录而返回不足 K。Hybrid 默认按融合分数排序，也可按字段重排候选；候选截断后的重排不保证全库字段排序 Top-K。详见 [功能边界](FEATURES.md)。

## 数据与文件

服务目录按 API key、索引、本机 shard/level 组织。主要文件如下；实际文件位于索引或其 shard/level 下，以源码打开/提交逻辑为准。

| 文件 | 用途 |
| --- | --- |
| `apikey.json` | key 身份与目录关联 |
| `schema.json`、`index.json` | 字段定义、索引元数据与配置 |
| `index.bin` | 词项 posting、文档 ID 与位置编码 |
| `vector.bin` | 向量及聚类数据 |
| `docstore.bin` | 压缩的已存储文档字段 |
| `facet.bin`、`facet.json` | 过滤/排序值及词典（不维护统计聚合） |
| `delete.bin` | 删除标记 |

Rust 核心与 HTTP 均使用 Mmap 访问已提交的数据，由操作系统管理驻留页；未提交写入和计算缓存仍在内存。文档压缩可选 None、Snappy，默认 Snappy。分词、高频词、N-gram、SIMD 和 posting 压缩仍保留。

## 持久化边界

数据先进入内存，HTTP 搜索默认包含未提交数据，Rust 核心仍显式选择 realtime；显式提交与内部自动提交将数据写入本地索引。未提交数据在异常退出时可能丢失。已有磁盘重开能力不能等同于经过故障注入验证的 WAL、原子检查点或一致快照。

稳定业务 ID/版本、共同可见视图、可恢复日志、结构化错误、资源边界和分布式协议见 [后续计划](SEEKPOWER_PLAN.md)，尚未完成。当前文件布局与功能清单不能作为无限容量、固定延迟或高可用承诺。

本实现来源及许可见 [UPSTREAM.md](UPSTREAM.md) 和 [LICENSE](LICENSE)。



聚合查询类型、统计计算及输出已删除。`FacetField` 直接读写字段名及有序值字典，没有统计槽或兼容序列化器。索引格式为 7.0，根索引和分片均要求版本精确匹配；不读取或升级旧格式。量化内部数值范围仍保留。


## #57 本地实施状态

现有 Commit 调用链已传播文件写入、映射和同步错误，单条索引调用等待其任务完成。`record.rs` 的进行中原型以权威日志驱动同一检索内核，使用有界有效集合重建验证版本、统计和读取一致性。它尚未接入 HTTP/UI；现有服务不因此获得 durable 写入或共享 collection 授权保证。完整改动、平台验收和剩余门槛见 [实施记录](docs/implementation-57.md)。
