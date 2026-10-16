# Seekpower

Seekpower 是单机全文、向量与混合检索引擎。当前组成是 `seekpower` Rust 核心库、`seekpower_server` HTTP 服务和内嵌 Web UI。模型推理、文档解析与切块由调用方完成。

[完整功能清单](FEATURES.md) · [服务使用](seekpower_server/README.md) · [架构](ARCHITECTURE.md) · [迁移说明](MIGRATION.md) · [状态](STATUS.md) · [后续计划](SEEKPOWER_PLAN.md)

## 构建与启动

从本仓库构建核心库和 HTTP 服务。

```sh
cargo build --workspace
export MASTER_KEY_SECRET='replace-with-your-private-secret'
cargo run -p seekpower_server -- local_ip=127.0.0.1 local_port=8080
```

打开 `http://127.0.0.1:8080/`，将启动日志中的 `Master API key` 填入连接表单，即可创建索引、写入和检索。主密钥由 `MASTER_KEY_SECRET` 派生，重启复用其索引；不自动生成普通 key。需要独立数据作用域时，可用主密钥创建/撤销额外 key。完整流程见 [服务使用](seekpower_server/README.md)。

## 当前能力

| 领域 | 保留内容 |
| --- | --- |
| 全文 | BM25F/邻近度评分、布尔/短语、多字段、中文分词、停用词、N-gram |
| 向量 | 外部 JSON/base64 向量、多向量字段、F32/I8、三种距离、量化、全簇扫描与 IVF ANN |
| 混合 | 全文/向量独立召回后 RRF 融合 |
| 元数据与结果 | 全文/向量/混合元数据过滤、多字段排序、计数、分页、字段投影、双向遍历 |
| 数据与服务 | JSON 单条/批量写入、读取、更新、删除；索引生命周期；API key 鉴权与数据作用域 |
| Web UI | 密钥连接、索引创建/选择、字段 schema 配置、JSON 粘贴/文件写入与提交、三种检索模式、外部向量输入、分页、原文/链接预览、日期输入过滤与排序 |

完整接口、配置、外围功能及限制见 [FEATURES.md](FEATURES.md)。特别注意：HTTP 向量检索默认搜索全部簇，核心显式 ANN 仍受簇探测范围影响；UI 尚无完整管理页面；内部 DocID 不是稳定业务 ID；实时可见不等于崩溃可恢复。分布式与可靠性补强属于后续阶段。

## 外部向量示例

创建索引时使用以下 JSON（鉴权与路径见服务文档）：

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

向 `/api/v1/index/{id}/doc` POST `{"body":"example text","vector":[1,0,0]}`，再 PATCH `/api/v1/index/{id}` 提交。向 `/api/v1/index/{id}/query` POST：

```json
{"query":"example","query_vector":[1,0,0]}
```

仅文本执行全文检索，仅 `query_vector` 执行向量检索，两者都有执行混合检索。空对象 `{}` 浏览文档；查询默认包含未提交写入，持久化仍需提交。写入必须包含所有已配置的向量字段。二进制向量字段仍以 base64 字符串放在 JSON 中，与已删除的 v2 二进制查询协议无关。

## 性能基线

Linux 下使用独立临时目录启动 release 服务，通过本机 HTTP 长连接测量；只需为压测脚本安装 NumPy：

```sh
cargo build --release -p seekpower_server
python3 -m pip install numpy
python3 scripts/performance_baseline.py --output /tmp/seekpower-baseline.json
```

默认固定种子生成 20,000 篇合成文档、128 维向量和 64 个查询，测全文/向量/混合检索，并发 1、4，每组预热后运行 5 秒、重复 3 次。使用默认计数和正文返回，Top-10；向量采用 HTTP 默认的全簇检索。每个连接由独立进程驱动，压测端与服务共享机器资源。

JSON 记录每轮 QPS、HTTP P50/P95/P99、服务端/压测端 CPU、服务端采样 RSS、索引磁盘占用，以及单次批量写入和提交耗时。汇总值为各轮指标的中位数；向量 Recall@10 对照独立 float64 全量余弦计算，全文核对匹配计数。请求失败会中止，不产生成功结果文件。

[首版实测数据](benchmarks/http-baseline-2026-10-15.json) 包含源码与二进制哈希、数据哈希、配置和环境。结果用于同环境版本对比：属于合成数据的热缓存、闭环负载，不是容量上限；未衡量真实语料的全文/混合相关性、冷缓存、过滤和混合读写。提交耗时表示 API 完成时间，不验证崩溃恢复或持久化保证。`--help` 可查看数据量、维度、时长和并发等覆盖参数；输出文件不会被覆盖。

真实语料使用完整 [BEIR SciFact](https://github.com/beir-cellar/beir)：5,183 篇文档、300 条测试查询。固定 `intfloat/multilingual-e5-small` 模型版本，CPU ONNX FP32 推理、384 维、最长 512 token、带掩码均值池化和 L2 归一化；查询/文档分别加 `query: `/`passage: ` 前缀。向量预先生成，耗时单独记录：

```sh
python3 -m pip install numpy==2.2.6 onnxruntime==1.23.2 tokenizers==0.22.2
python3 scripts/prepare_retrieval.py --cache /tmp/seekpower-model --output /tmp/seekpower-scifact
python3 scripts/performance_baseline.py --dataset /tmp/seekpower-scifact --output /tmp/seekpower-scifact-result.json
```

正文合并标题和文本，全文/混合请求显式使用 `operator=or`；其他 HTTP 配置不变。性能测 Top-10，相关性另取 Top-100，按人工标注计算 nDCG@10、Recall@100、MRR@10；最近邻 Recall@10 单独列出。[SciFact 实测数据](benchmarks/scifact-baseline-2026-10-15.json) 保留模型和输入哈希。SciFact 结果不代表百万级索引表现。模型、数据和向量保留在指定缓存/输出目录，不提交仓库。

MIRACL 中文使用固定版本的官方完整语料（4,934,368 段）、393 条开发集查询和 3,928 条正负相关性标注，自动选择 `UnicodeAlphanumericZH` 分词。完整语料准备、嵌入和压测命令：

```sh
python3 scripts/prepare_retrieval.py --dataset miracl-zh --cache /tmp/miracl-cache --output /tmp/miracl-full
python3 scripts/performance_baseline.py --dataset /tmp/miracl-full --commit-every 20 --output /tmp/miracl-full-result.json
```

准备程序按块生成向量并记录进度；中断后使用相同命令续跑，校验输入和模型一致性。完成的输出不会覆盖。压测使用磁盘请求缓存、内存映射向量和分块精确余弦参考，不会构造全量查询×文档矩阵；`--commit-every 20` 每 20 个批次提交一次以限制未提交数据，耗时计入写入阶段。服务端自身的资源需求仍随数据量增长。

资源有限时，显式加 `--documents 10000` 生成子集：保留全部开发集正负标注文档，并从全语料按固定 SHA-256 顺序补足背景文档。报告标记 `full_corpus=false`，这种筛选降低检索难度，不能与官方全量成绩比较。仅准备全文数据可用 `--text-only`，压测对应使用 `--modes lexical`。`--max-rss-mib` 可设置写入时的服务端内存保护阈值，超限失败不会输出成功报告。

人工标注指标包含 nDCG@10、Recall@10、Recall@100、MRR@10，与向量最近邻 Recall 分开记录。CI 只运行指标、分块参考检索和流式加载的轻量测试，不下载语料或执行性能门槛判断。

## 开发与文档

- [Rust 示例](docs/library-examples.md)、[元数据过滤与排序](FACETED_SEARCH.md)、[N-gram 配置](NGRAM_SEARCH.md)。本地执行 `cargo doc --workspace --no-deps` 生成与当前源码一致的 Rust 文档。
- [HTTP 请求示例](seekpower_server/test_api.rest)。运行 `target/debug/seekpower_server generate_openapi=true`，在可执行文件目录生成 `openapi.json` 和 `openapi.yml`；CI 也会提供生成文件。完整路由见功能清单。
- 运行 `cargo test --workspace --all-targets`；HTTP 冒烟使用 `python3 scripts/http_smoke.py --server target/debug/seekpower_server`。浏览器回归需安装 Python Playwright/Chromium，并设置 `SEEKPOWER_BROWSER_TEST=1`。
- 核心默认 features：`zh`（中文分词）、`vb`（结果诊断信息）；哈希实现固定使用 AHash。HTTP 对象与 OpenAPI 定义均在服务工程，核心通过 `log` 发出日志，由调用方配置输出。具体见 [Cargo.toml](seekpower/Cargo.toml)。
- [验证记录](VALIDATION.md) 保存实际测试和历史失败；[CHANGELOG](CHANGELOG.md) 保留上游历史，不作为当前功能列表。

本项目使用 [Apache-2.0 许可证](LICENSE)。
