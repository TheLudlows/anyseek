# 当前架构

SeekPower 保留一个 Rust 核心库和一个 HTTP/Web UI 服务。当前是单机实现，本机 shard/level 用于并行与增量索引，不代表分布式路由或副本。

## 模块边界

| 层 | 代码 | 职责 |
| --- | --- | --- |
| UI | `seekstorm_server/web/` | 搜索、分页、原文/链接预览、日期交互 |
| 服务 | `http_server.rs`、`api_endpoints.rs`、`multi_tenancy.rs`、`server.rs` | HTTP 参数、API key、索引作用域、启动与终端展示 |
| 核心入口 | `index.rs`、`search.rs`、`iterator.rs` | schema、文档操作、三类查询与遍历 |
| 全文 | `tokenizer.rs`、`index_posting.rs`、`intersection*.rs`、`union.rs` | 分词、倒排、布尔/短语、BM25 与剪枝 |
| 向量 | `vector.rs`、`vector_similarity.rs`、`clustering.rs` | 外部向量校验、量化、相似度、聚类与 IVF 检索 |
| 存储 | `commit.rs`、`doc_store.rs`、`compress_postinglist.rs` | 本地提交、文档压缩、posting 编码 |
| 过滤 | `metadata_filter.rs` | 共享 schema 校验、每分片字典编译、候选元数据判定 |
| 统计 | `info.rs` | 收集索引信息；终端渲染位于服务 |

源码路径分别相对于 [核心目录](seekstorm/src) 或 [服务目录](seekstorm_server/src)。核心 OpenAPI derive 通过可选 `api-schema` 启用，核心不依赖终端表格、模型运行时或文件解析器。

## 查询执行

调用方显式选择 Lexical、Vector 或 Hybrid。Lexical 使用倒排，Vector 使用外部查询向量；Hybrid 收集两路候选，汇总本机分片结果后按文档 ID 执行 RRF。融合参数目前固定在代码中，没有公开的多策略自动规划器。

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
| `synonyms.json` | 同义词配置 |

Rust 核心支持 Ram 与 Mmap；HTTP 创建索引使用 Mmap。文档压缩可选 None、Lz4、Snappy、Zstd，默认 Snappy。分词、高频词、N-gram、SIMD 和 posting 压缩仍保留。

## 持久化边界

数据先进入内存，搜索可通过 realtime 包含未提交数据；显式提交与内部自动提交将数据写入本地索引。未提交数据在异常退出时可能丢失。已有磁盘重开能力不能等同于经过故障注入验证的 WAL、原子检查点或一致快照。

稳定业务 ID/版本、共同可见视图、可恢复日志、结构化错误、资源边界和分布式协议见 [后续计划](SEEKPOWER_PLAN.md)，尚未完成。当前文件布局与功能清单不能作为无限容量、固定延迟或高可用承诺。

本实现来源及许可见 [UPSTREAM.md](UPSTREAM.md) 和 [LICENSE](LICENSE)。



聚合查询类型、统计计算及输出已删除。`FacetField` 仅保留字段字典；磁盘旧统计槽由兼容序列化器写入 0/None，避免改变既有字典 ID 与文件布局。量化内部数值范围仍保留。
