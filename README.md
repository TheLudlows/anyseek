# anyseek

anyseek 是单机全文、向量与混合检索引擎。当前组成是 `anyseek` Rust 核心库、`anyseek_server` HTTP 服务和内嵌 Web UI。模型推理、文档解析与切块由调用方完成。

[完整功能清单](FEATURES.md) · [服务使用](anyseek_server/README.md) · [架构](ARCHITECTURE.md) · [开发与验证](docs/development.md) · [当前状态](STATUS.md) · [文档索引](docs/README.md)

## 构建与启动

从本仓库构建核心库和 HTTP 服务。

```sh
cargo build --workspace
export MASTER_KEY_SECRET='replace-with-your-private-secret'
cargo run -p anyseek_server -- local_ip=127.0.0.1 local_port=8080
```

打开 `http://127.0.0.1:8080/`，将启动日志中的 `Master API key` 填入连接表单，即可创建索引、写入和检索。主密钥由 `MASTER_KEY_SECRET` 派生，重启必须保持一致。首次服务目录必须不存在；多个 key 可共享 collection，撤销 key 不删除数据。完整流程见 [服务使用](anyseek_server/README.md)。

## 当前能力

| 领域 | 保留内容 |
| --- | --- |
| 全文 | BM25F/邻近度评分、布尔/短语、多字段、中文分词、停用词、N-gram |
| 向量 | 外部 JSON/base64 向量、多向量字段、F32/I8、三种距离、量化、全簇扫描与 IVF ANN |
| 混合 | 全文/向量及可选一跳关系在共同视图中 RRF 融合，返回逐命中通道解释 |
| 来源与整文 | 精确来源版本与定位、证据依赖、删除/stale 隐藏、多批 staging、整代发布、向量后补 |
| 时间与同步 | 固定 evaluation_time、不可逆 TTL、实体/一跳关系、至少七天变更流与 bootstrap |
| 元数据与结果 | 全文/向量/混合元数据过滤、多字段排序、计数、分页、字段投影、按稳定 ID 的游标遍历 |
| 数据与服务 | 稳定 Record ID/版本/条件写入、非原子 batch、WAL/checkpoint、共享 collection 授权及备份恢复 |
| Web UI | 密钥连接、索引创建/选择、字段 schema 配置、JSON 粘贴/文件写入与原请求重试、三种检索模式、外部向量输入、分页、原文/链接预览、日期输入过滤与排序 |

完整接口、配置、外围功能及限制见 [FEATURES.md](FEATURES.md)。特别注意：HTTP 向量/混合检索默认使用可配置的 Nprobe 预算，`all:true` 显式扫描全部簇；ANN 受簇探测范围影响；UI 尚无完整管理页面；HTTP 使用稳定 Record ID，成功写入要求 durable + visible。P1-A 与已界定的 P2 功能批次已合入，仍是有界单机实现：累计 Record 操作最多 10,000 项，checkpoint 单帧上限 16 MiB。系统断电未验收；持续回收、不可变 Segment、SQLite 云端协调及对象存储尚未实现。低层索引库的 DocID/commit API 与此服务契约不同。

## 外部向量示例

创建请求为 `{operation_id,collection_id,config,embedding_space_id}`。发送前保存完整请求，顶层 `embedding_space_id` 标识外部模型/空间。以下是其中的 `config`，完整可执行流程见[服务文档](anyseek_server/README.md)：

```json
{
  "index_name": "external_demo",
  "schema": [
    {"field":"body","field_type":"Text"},
    {"field":"vector","field_type":"Json","index_vector":true}
  ],
  "vector":{"dimensions":3}
}
```

写入 `/api/v1/index/{id}/doc` 使用 `{operation_id,record_id,routing_key,expected,document}`，其中 `document` 为 `{"body":"example text","vector":[1,0,0]}`，新记录 `expected:"Absent"`。成功即持久且可见，不需再提交。向 `/api/v1/index/{id}/query` POST：

```json
{"query":"example","query_vector":[1,0,0]}
```

仅文本执行全文检索，仅 `query_vector` 执行向量检索，两者都有执行混合检索。空对象 `{}` 浏览文档；查询返回 `hits[].record` 和共同 visible_seq；PATCH collection 仅执行在线 checkpoint。已配置向量可以缺席，正文可先发布；缺向量记录不参与对应向量召回。后补使用精确版本条件的 `vector_patch`，不填零向量。二进制向量字段仍以 base64 字符串放在 JSON 中，与已删除的 v2 二进制查询协议无关。

## 开发与文档

- [中文测试语料](miracl-corpus-v1.0-zh/README.md)：MIRACL 中文语料，包含 4,934,368 条段落，按 10 个 gzip 分片保存；提供来源、字段说明、读取方法和 SHA-256 校验清单。
- [Rust 示例](docs/library-examples.md)、[元数据过滤与排序](FACETED_SEARCH.md)、[N-gram 配置](NGRAM_SEARCH.md)。本地执行 `cargo doc --workspace --no-deps` 生成与当前源码一致的 Rust 文档。
- [HTTP 请求示例](anyseek_server/test_api.rest)。运行 `target/debug/anyseek_server generate_openapi=true`，在可执行文件目录生成 `openapi.json` 和 `openapi.yml`；CI 也会提供生成文件。完整路由见功能清单。
- 运行 `cargo test --workspace --all-targets`；HTTP 冒烟使用 `python3 scripts/record_service_smoke.py --server target/debug/anyseek_server`。浏览器回归需安装 Python Playwright/Chromium，并加 `--browser`。
- 核心默认 features：`zh`（中文分词）、`vb`（结果诊断信息）；内存集合使用 AHash，持久词项键使用固定 XXH64（格式 8）。HTTP 对象与 OpenAPI 定义均在服务工程，核心通过 `log` 发出日志，由调用方配置输出。具体见 [Cargo.toml](anyseek/Cargo.toml)。
- [验证记录](VALIDATION.md) 保存实际测试和历史失败；[CHANGELOG](CHANGELOG.md) 保留上游历史，不作为当前功能列表。

本项目使用 [Apache-2.0 许可证](LICENSE)。
