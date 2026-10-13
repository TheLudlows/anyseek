# 裁剪迁移说明

适用于 S1 与 PR #10 的 B1/B2。当前功能见 [FEATURES.md](FEATURES.md)，合入状态见 [STATUS.md](STATUS.md)。

| 已移除能力 | 调用方调整 |
| --- | --- |
| MCP 服务、独立 SDK 工程 | 使用 HTTP JSON 或从本仓库嵌入 Rust 核心；上游包不包含本分支修改 |
| 内置模型、Embedding 与自动切块 | 应用解析/切块/生成向量；索引使用 `Inference::External`，Vector/Hybrid 显式传 `query_vector` |
| PDF/CSV/目录、本地 JSON/SIFT 导入及 SIFT 评测 | 外部准备记录，通过 JSON 文档 API 单条/批量写入 |
| PDF 附件托管 | 源文件由外部服务托管，记录存 URL；历史附件不自动删除，旧文件路由返回 404 |
| 纠错、补全、自动改写 | 删除 `query_rewriting/spelling_correction/query_completion/dictionary_source/completion_source`；旧选项返回 400 |
| 地理字段/过滤/排序 | 删除 Point、`distance_fields`、`result_sort[].base`（包括 None）；旧请求返回 400；经纬度可作为普通数据保存 |
| 高亮与摘要 | 删除 `highlights`；文档读取也删除 `query_terms`；返回存储原文，搜索响应的 `query_terms` 保留 |
| 配额与内置限流 | 创建 key 使用 `{}` 或 `{"demo":true}`；删除 quota 参数，创建 key URL 参数返回 400；限流由外层承担 |
| v2 二进制查询（B1） | 改为 v1 POST JSON；`/api/v2/index/{id}/query` 返回 404 |
| GET/HTTP QUERY 搜索与遍历（B2） | query/iterator 使用 POST JSON；旧方法返回 405 + `Allow: POST, OPTIONS`；POST URL 非空参数返回 400 |

普通资源 GET、JSON/base64 外部向量、API key 数据隔离和 Web UI 保留。Binary/base64 是 JSON 中的字段编码，不是被删除的整包二进制查询协议。

## 现有数据

- 启用了已删除内置模型、Point 或纠错/补全配置的旧索引，需要用相应旧版导出，在应用侧转换后重建；没有自动模型迁移。旧纠错/补全配置为 null 的索引仍可读取。
- 普通字段及既有外部向量数据布局保留。旧 `apikey.json` 的 quota 被忽略，key ID/hash 和路径不变，启动不重写旧 key 文件。
- 非法向量配置、维度、精度、非有限值、过大范数、零余弦范数及畸形 base64 返回错误。所有向量字段写入时必须存在。
- 批次输入在入队前校验；非法更新在删除旧记录前拒绝。这不等于崩溃安全事务。
- Vector/Hybrid 非空 `facet_filter` 返回 400，不能把字段选择 `field_filter` 当作元数据过滤替代。

## Rust 与服务接入

- 写入/更新检查 `Result<(), String>`；检索检查 `ResultObject.error`。
- `index_document(document)` 不再接收附件参数；`get_file` 删除。
- `get_document(doc_id, include_uncommitted, &fields)` 不再接收高亮、query_terms 或地理距离参数；`Search::search` 不再接收最终的 rewriting 参数。
- 核心统计使用 `info::collect_index_info`；终端表格与日期格式化位于服务。OpenAPI derive 通过可选 `api-schema` 启用。
- 服务启动仅接受文档列出的 `name=value` 参数；未知、重复、非法值失败。文档读取的字段选择通过 JSON body `fields`，iterator 拒绝未知选项。


## 统计聚合裁剪

- 删除 `query_facets` 参数，旧请求即使传空数组也返回 400；删除响应 `facets` 和索引信息 `facets_minmax`。
- Rust 删除 `QueryFacet/RangeType/Ranges/ResultFacet/MinMaxField/MinMaxFieldJson` 以及索引聚合方法；`Search::search` 删除 `query_facets` 参数。
- 保留 `facet:true` 字段、`facet_filter`、`result_sort`、按文档读取元数据值、文档命中计数及分页。Vector/Hybrid 的过滤限制不变。
- UI 删除直方图和滑块，改为 UTC 起止日期输入，保留日期过滤/排序；不再自动查询最早/最晚日期。
- 旧字典的统计值读取时丢弃；保存时仅写兼容占位（计数 0、极值 None）。字典顺序和 facet.bin 的值/ID 编码不变。旧版本若重新打开此数据，不能把占位值当成准确统计；如需旧聚合结果，应重新构建。
- 内部 ANN/量化所需的 min/max 数值范围不属于字段统计，不删除。
