# SeekPower

基于 SeekStorm 的单机全文、向量与混合检索引擎。当前组成是 `seekstorm` Rust 核心库、`seekstorm_server` HTTP 服务和内嵌 Web UI。模型推理、文档解析与切块由调用方完成。

[完整功能清单](FEATURES.md) · [服务使用](seekstorm_server/README.md) · [架构](ARCHITECTURE.md) · [迁移说明](MIGRATION.md) · [状态](STATUS.md) · [后续计划](SEEKPOWER_PLAN.md)

## 构建与启动

从本仓库构建；上游发布包和镜像不包含 SeekPower 的修改。两个 crate 暂时保留原名，不发布到上游包名。

```sh
cargo build --workspace
export MASTER_KEY_SECRET='replace-with-your-private-secret'
cargo run -p seekstorm_server -- local_ip=127.0.0.1 local_port=8080
```

打开 `http://127.0.0.1:8080/`。空数据目录首次启动会自动生成普通 API key，在日志中显示 `Initial API key`；将它填入页面的连接表单，即可在界面创建和选择索引。重启不会重复生成 key。文档可通过界面的 **Write data** 或 JSON API 写入；由 `MASTER_KEY_SECRET` 派生的主密钥用于额外 key 的创建/撤销。完整流程见 [服务使用](seekstorm_server/README.md)。

## 当前能力

| 领域 | 保留内容 |
| --- | --- |
| 全文 | BM25F/邻近度评分、布尔/短语、多字段、中文分词、词干化、停用词、同义词、N-gram |
| 向量 | 外部 JSON/base64 向量、多向量字段、F32/I8、三种距离、量化、全簇扫描与 IVF ANN |
| 混合 | 全文/向量独立召回后 RRF 融合 |
| 元数据与结果 | 全文/向量/混合元数据过滤、多字段排序、计数、分页、字段投影、双向遍历 |
| 数据与服务 | JSON 单条/批量写入、读取、更新、删除；索引生命周期；API key 鉴权与数据作用域 |
| Web UI | 密钥连接、索引创建/选择、字段 schema 配置、JSON 粘贴/文件写入与提交、三种检索模式、外部向量输入、分页、原文/链接预览、日期输入过滤与排序 |

完整接口、配置、外围功能及限制见 [FEATURES.md](FEATURES.md)。特别注意：ANN 过滤仍受簇探测范围影响；UI 尚无完整管理页面；内部 DocID 不是稳定业务 ID；实时可见不等于崩溃可恢复。分布式与可靠性补强属于后续阶段。

## 外部向量示例

创建索引时使用以下 JSON（鉴权与路径见服务文档）：

```json
{
  "index_name": "external_demo",
  "schema": [
    {"field":"body","field_type":"Text","store":true,"index_lexical":true},
    {"field":"vector","field_type":"Json","store":false,"index_lexical":false,"index_vector":true}
  ],
  "inference":{"External":{"dimensions":3,"precision":"F32","quantization":"None","similarity":"Cosine"}},
  "clustering":"None"
}
```

向 `/api/v1/index/{id}/doc` POST `{"body":"example text","vector":[1,0,0]}`，再 PATCH `/api/v1/index/{id}` 提交。向 `/api/v1/index/{id}/query` POST：

```json
{"query":"example","query_vector":[1,0,0],"search_mode":{"Hybrid":{"similarity_threshold":null,"ann_mode":"All"}}}
```

Vector/Hybrid 必须提供 `query_vector`；Lexical 仅需文本。写入必须包含所有已配置的向量字段。二进制向量字段仍以 base64 字符串放在 JSON 中，与已删除的 v2 二进制查询协议无关。

## 开发与文档

- [Rust 示例](docs/library-examples.md)、[元数据过滤与排序](FACETED_SEARCH.md)、[N-gram 配置](NGRAM_SEARCH.md)。本地执行 `cargo doc --workspace --no-deps` 生成与当前源码一致的 Rust 文档。
- [HTTP 请求示例](seekstorm_server/src/test_api.rest)、[OpenAPI JSON](seekstorm_server/openapi/openapi.json) / [YAML](seekstorm_server/openapi/openapi.yml)。路由覆盖差异见功能清单。
- 运行 `cargo test --workspace --all-targets`；HTTP 冒烟使用 `python3 scripts/http_smoke.py --server target/debug/seekstorm_server`。浏览器回归需安装 Python Playwright/Chromium，并设置 `SEEKPOWER_BROWSER_TEST=1`。
- 核心默认 features：`zh`（中文分词）、`vb`（结果诊断信息）；`gxhash` 可选。HTTP 对象与 OpenAPI 定义均在服务工程，核心通过 `log` 发出日志，由调用方配置输出。具体见 [Cargo.toml](seekstorm/Cargo.toml)。
- [验证记录](VALIDATION.md) 保存实际测试和历史失败；[CHANGELOG](CHANGELOG.md) 保留上游历史，不作为当前功能列表。

来源与许可：[UPSTREAM.md](UPSTREAM.md)、[Apache-2.0 LICENSE](LICENSE)。保留上游及第三方署名和许可。
