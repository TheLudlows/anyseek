# 裁剪迁移说明

记录已删除接口及调用方调整；不提供旧索引读取、转换或升级流程。当前功能见 [FEATURES.md](FEATURES.md)，合入状态见 [STATUS.md](STATUS.md)。

| 已移除能力 | 调用方调整 |
| --- | --- |
| MCP 服务、独立 SDK 工程 | 使用 HTTP JSON 或从本仓库嵌入 Rust 核心；上游包不包含本分支修改 |
| 内置模型、Embedding 与自动切块 | 应用解析/切块/生成向量；索引使用 `Inference::External`，Vector/Hybrid 显式传 `query_vector` |
| PDF/CSV/目录、本地 JSON/SIFT 导入及 SIFT 评测 | 外部准备记录，通过 JSON 文档 API 单条/批量写入 |
| PDF 附件托管 | 源文件由外部服务托管，记录存 URL；历史附件不自动删除，旧文件路由按未知路径返回 501 |
| 纠错、补全、自动改写 | 删除 `query_rewriting/spelling_correction/query_completion/dictionary_source/completion_source`；旧选项返回 400 |
| 地理字段/过滤/排序 | 删除 Point、`distance_fields`、`result_sort[].base`（包括 None）；旧请求返回 400；经纬度可作为普通数据保存 |
| 高亮与摘要 | 删除 `highlights`；文档读取也删除 `query_terms`；返回存储原文，搜索响应的 `query_terms` 保留 |
| 配额与内置限流 | 创建 key 使用 `{}`；删除 quota 参数，创建 key URL 参数返回 400；限流由外层承担 |
| v2 二进制查询（B1） | 改为 v1 POST JSON；旧路径按未知路径返回 501，OPTIONS 仅返回通用预检 204 |
| GET/HTTP QUERY 搜索与遍历（B2） | query/iterator 使用 POST JSON；旧方法返回 405 + `Allow: POST, OPTIONS`；POST URL 非空参数返回 400 |

普通资源 GET、JSON/base64 外部向量、API key 数据隔离和 Web UI 保留。Binary/base64 是 JSON 中的字段编码，不是被删除的整包二进制查询协议。

## 现有数据

- 当前索引格式为 **7.0**，只读取该版本；旧索引不支持、不转换。使用新目录从原始记录建立新索引。
- `facet.json` 直接保存字段名及有序字符串/集合字典，不再读写计数、min/max 或兼容占位。
- 索引元数据按当前结构严格解析；未知字段（包括值为 null）直接报错。
- API key 身份文件不属于索引格式；key ID/hash 和路径保持现有行为。
- 非法向量配置、维度、精度、非有限值、过大范数、零余弦范数及畸形 base64 返回错误。所有向量字段写入时必须存在。
- 批次输入在入队前校验；非法更新在删除旧记录前拒绝。这不等于崩溃安全事务。
- `facet_filter` 现已支持 Lexical/Vector/Hybrid；`field_filter` 仍只选择检索字段。未知过滤字段、类型不匹配及反向/非有限范围现在明确报错，原来被静默忽略的条件须修正。

## Rust 与服务接入

- 写入/更新检查 `Result<(), String>`；检索检查 `ResultObject.error`。
- `index_document(document)` 不再接收附件参数；`get_file` 删除。
- `get_document(doc_id, include_uncommitted, &fields)` 不再接收高亮、query_terms 或地理距离参数；`Search::search` 不再接收最终的 rewriting 参数。
- HTTP 请求/响应对象移至 `seekstorm_server::api_types`，从核心删除 `api-schema` feature；嵌入核心继续使用 `Search`、`create_index` 等操作及其核心参数类型。
- 服务启动仅接受文档列出的 `name=value` 参数；未知、重复、非法值失败。文档读取的字段选择通过 JSON body `fields`，iterator 拒绝未知选项。


## 内核信息与日志

- `collect_index_info(&index).await` 返回 `IndexInfo`，不再接收展示标签/耗时字符串；旧 `Info { entry, value }` 与 `entries` 删除。读取 `indexed_documents`、`index_size_bytes` 等数值字段，在调用方格式化。
- 统计采集不触发 commit/fsync。持久化成功与错误语义仍以提交接口实际实现为准。
- 核心只使用 `log` 门面，调用方安装 logger 后才输出；保留 `mute` 对进度日志的控制。服务默认 info，可用 `RUST_LOG=seekstorm=warn` 或 `RUST_LOG=off` 控制日志；启动与索引概况改用普通日志；主密钥和首次普通 key 保留直接输出。

## 统计聚合裁剪

- 删除 `query_facets` 参数，旧请求即使传空数组也返回 400；删除响应 `facets` 和索引信息 `facets_minmax`。
- Rust 删除 `QueryFacet/RangeType/Ranges/ResultFacet/MinMaxField/MinMaxFieldJson` 以及索引聚合方法；`Search::search` 删除 `query_facets` 参数。
- 保留 `facet:true` 字段、`facet_filter`、`result_sort`、按文档读取元数据值、文档命中计数及分页。Vector/Hybrid 过滤已由后续共享谓词批次实现。
- UI 删除直方图和滑块，改为 UTC 起止日期输入，保留日期过滤/排序；不再自动查询最早/最晚日期。
- 字典仅保存过滤/排序需要的值；旧聚合字典格式不再读取。
- 内部 ANN/量化所需的 min/max 数值范围不属于字段统计，不删除。

## C1～C5 服务与代码收敛

- 创建 API key 只接受空 JSON 对象 `{}`，始终随机生成；`demo` 字段/URL 参数返回 400。已有 key 身份文件与数据作用域不变。
- 删除 stdin 的 create/delete/info/search/openapi/help/quit；通过 REST、Web UI 和启动参数 `generate_openapi=true` 操作。Ctrl-C/SIGINT、SIGTERM 正常结束连接后提交并关闭索引。
- 删除 HTTP PUT `/api/v1/index/{id}` 关闭入口和 `/version`，使用 PATCH 提交、GET `/api/v1/live` 存活检查。这两个旧入口按未知路由返回 501。
- Web UI 不再从 URL `api_key` 登录；在连接表单输入普通 key，当前标签页的 sessionStorage 保留。`index_id` URL 参数继续支持。
- 删除未使用的 tokenizer、根索引高频词字符串集合、恒关闭的实验分支、彩色表格依赖及无引用的根目录历史图片。正常全文/向量/混合检索、核心 close 与词法 Count 优化保留。

## N-gram 修复与 D1～D3

- `ngram_indexing` 是独立位掩码：0/1/2/4/8/16/32/64；默认 FF | FFF = 9。FFR 和 FRF 修复为检查自己的开关。旧有受影响索引须从原始数据重建，不自动修补 postings。
- 文档压缩仅保留 `None`、`Snappy`（默认）；删除 `Lz4`、`Zstd` 和编解码依赖。创建请求及索引元数据中的旧枚举值直接拒绝；相关旧索引需要重建，不提供转换。
- 核心 `AccessType` 仅保留 `Mmap`，删除 `Ram` 全量加载实现及 RAM 专用索引副本。Rust 调用方指定 `AccessType::Mmap`；旧 `Ram` 元数据直接拒绝。保留实时写入缓冲，Mmap 不表示零内存使用。
- 格式版本仍为 7.0，保留的 Mmap + None/Snappy 文件布局不变；不扩大任何旧格式读取范围。
- 删除未使用的 `half` 直接依赖。Web UI、向量精度/量化、BM25F Proximity、N-gram、普通过滤/排序继续保留。

## 同义词与词干器删除（D5）

创建索引 JSON 删除 `synonyms`、`stemmer` 字段（包括空数组、null 和 `None`）；传入旧字段返回 400。GET/POST/PUT `/api/v1/index/{id}/synonyms` 已删除。Rust `create_index` 不再接收同义词参数，`IndexMetaObject` 不再含 `stemmer`，相关类型和管理方法已删除。索引信息不再返回同义词计数。

含旧 `stemmer` 元数据的索引不能打开，应从原始文档重建；不提供兼容读取或转换。新索引不写 `synonyms.json`。查询与写入保留分词和大小写等规范化，但不再做同义词扩展或词干化。停用词、高频词和词级 N-gram 保留。

## 固定哈希实现与旧路由清理（E1/E2）

删除核心 gxhash feature、依赖及服务侧显式启用配置，统一使用 AHash，保留原 AHash seeds 与词项掩码。此前实际使用 gxhash 的索引必须从源文档重建，不提供哈希格式检测或转换。

删除旧 v2 查询和文件路由的专用迁移报错，统一走未知路由处理；通用 OPTIONS 预检不表示该路径可执行。现行 v1 query/iterator 的 POST JSON、405/Allow 和 URL 参数校验保留。
