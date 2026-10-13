# 当前功能清单

核查日期：2026-10-13（Asia/Shanghai）。基于主干 `af56be6`（PR #10 已合入），本分支继续裁剪分面统计。主干与本分支的合入状态见 [STATUS.md](STATUS.md)。本文按本分支实现列举功能，不将路线图视为已实现能力。

## 1. 检索与数据能力

| 类别 | 当前保留功能 | 条件与边界 |
| --- | --- | --- |
| 全文检索 | AND/OR/NOT、短语、查询中的 `+`/`-`/双引号、多字段检索、字段权重 | 默认 `Intersection`；显式选择 `Lexical` |
| 相关性 | `Bm25f`、`Bm25fProximity` | 尚未收缩为单一评分方式 |
| 分词 | `AsciiAlphabetic`、`UnicodeAlphanumeric`、`UnicodeAlphanumericFolded`、`Whitespace`、`WhitespaceLowercase`、`UnicodeAlphanumericZH` | 中文分词依赖默认启用的 `zh`；Folded 支持字符规范化 |
| 文本配置 | 词干化、停用词、高频词、N-gram 索引 | 38 个非 None 词干器枚举；停用词/高频词支持英德法西预设及自定义 |
| 同义词 | 单向/多向同义词；创建索引时设置；运行时读取、追加、替换 | 仅单词级同义词，索引时展开；变更不会自动重建历史文档 |
| 向量输入 | 外部向量、多个向量字段、JSON 数组或 Binary/base64 | `Inference::External`；所有向量字段共享索引级配置；应用负责模型和切块 |
| 向量校验 | 维度、精度、有限数值/范围、非零余弦范数、base64 格式 | 非法请求显式报错；批次入队前校验输入不等于事务 |
| 向量检索 | 全簇扫描、Leveled IVF ANN、K-Medoid 聚类 | `AnnMode::All/Nprobe/Similaritythreshold/NprobeSimilaritythreshold`；全簇扫描仍使用配置的存储精度/量化，不等于原始 F32 精确评分 |
| 向量配置 | F32/I8；Cosine/Dot/Euclidean；不量化、ScalarQuantizationI8、TurboQuantI8 | I8 输入只能配 `quantization: None`；聚类 `None/Auto/Fixed`；可设相似度阈值 |
| 混合检索 | 全文与向量分别召回，按文档 ID 合并并执行 RRF | 当前融合参数在代码内固定；无自动模式选择、模型重排或跨节点协调 |
| 字段选择 | `field_filter` 选择检索字段；`fields` 投影存储字段 | 字段选择不等于元数据值过滤 |
| 元数据过滤 | 字符串/字符串集合、整数、浮点、时间戳范围过滤 | 依赖 `facet:true`；仅 Lexical 支持；Vector/Hybrid 非空 `facet_filter` 返回 400 |
| 排序 | 相关性、facet 字段升降序、多字段组合、`_score`/`_id` | Vector/Hybrid 也会对已召回候选排序，但向量召回先按相似度截断，不保证全库字段排序 Top-K |
| 结果与分页 | `Topk/Count/TopkCount`、`offset/length`、显式空查询浏览 | 完整计数/空查询能力主要在词法路径；ANN 和 Hybrid 的 `count_total` 不应作为全库精确去重命中数 |
| 实时可见性 | 搜索 `realtime:true` 可包含尚未提交的数据 | 可搜索不等于已持久化；异常退出可能丢失未提交数据 |
| 索引生命周期 | 创建、打开、查看、提交、关闭、删除；核心库清空索引 | HTTP 服务启动加载磁盘索引；Rust 库支持 Ram/Mmap；HTTP 创建索引使用 Mmap |
| 文档写入 | JSON 单条/批量写入、按内部 DocID 单条/批量替换更新 | 当前更新不是业务 ID upsert，也不是字段局部合并；内部 DocID 不提供稳定业务 ID/版本契约 |
| 文档读取 | 按 DocID 读存储字段、字段投影、保留原文 | 不生成摘要或高亮；非存储字段不能作为原文返回 |
| 文档删除 | 单个/多个 DocID 删除、按查询选取文档删除 | 按查询删除仍保留；没有跨记录事务承诺 |
| 遍历 | 双向 iterator、起点、skip/take、字段投影、可选文档内容 | 正 take 向前、负 take 向后；`include_deleted` 可返回删除 ID，不代表恢复原文 |
| 本地存储 | 倒排、向量、facet、文档存储、删除标记；增量提交及重新打开 | RAM 缓冲、本机 shard/level、mmap；没有集群复制或已验收的 WAL/检查点恢复协议 |
| 压缩与执行 | None/Lz4/Snappy/Zstd 文档压缩；posting 编码、SIMD、Block-max WAND、MaxScore | 默认文档压缩 Snappy；不是已完成当前版本性能认证 |

字段类型：`U8/U16/U32/U64`、`I8/I16/I32/I64`、`Timestamp`、`F32/F64`、`Bool`、`String16/String32`、`StringSet16/StringSet32`、`Text`、`Json`、`Binary`。字段类型存在不代表每种类型都支持元数据过滤、全文或向量索引。向量使用 `Json` 或 `Binary` 加 `index_vector:true`，没有独立的 `FieldType::Vector`。

## 2. HTTP 接口全集

表中 `{id}` 为索引 ID，`{doc}` 为内部文档 ID。数据接口使用 `apikey` 请求头；主密钥仅用于创建/撤销 key。接口以 [实际路由](seekstorm_server/src/http_server.rs) 为准，OpenAPI 尚未覆盖标记为“未登记”的操作。

| 方法 | 路径 | 功能 | OpenAPI |
| --- | --- | --- | --- |
| GET | `/api/v1/live` | 存活检查 | 已登记 |
| GET | `/version` | 版本字符串，无鉴权 | 未登记 |
| POST | `/api/v1/apikey` | 创建随机 key；`{"demo":true}` 创建固定 demo key，需主密钥 | 已登记 |
| DELETE | `/api/v1/apikey` | 撤销指定 key 及其索引；主密钥 + `apikey_base64` JSON | 已登记 |
| GET | `/api/v1/apikey` | 查看当前 key 下的索引信息，不是全站密钥列表 | 已登记 |
| POST | `/api/v1/index` | 创建索引/schema | 已登记 |
| GET | `/api/v1/index/{id}` | 索引信息、schema、文档计数 | 已登记 |
| PATCH | `/api/v1/index/{id}` | 提交索引 | 已登记 |
| PUT | `/api/v1/index/{id}` | 关闭索引（遗留维护入口） | 未登记 |
| DELETE | `/api/v1/index/{id}` | 删除索引 | 已登记 |
| POST | `/api/v1/index/{id}/doc` | 单条/批量写入 | 已登记 |
| PATCH | `/api/v1/index/{id}/doc` | 单条/批量完整替换更新 | 已登记 |
| DELETE | `/api/v1/index/{id}/doc` | 按 ID、ID 数组或查询删除 | 已登记 |
| GET | `/api/v1/index/{id}/doc/{doc}` | 读取文档；可用 JSON body 指定 `fields` | 已登记 |
| DELETE | `/api/v1/index/{id}/doc/{doc}` | 删除单条文档 | 已登记 |
| POST | `/api/v1/index/{id}/query` | Lexical/Vector/Hybrid JSON 查询 | 已登记 |
| POST | `/api/v1/index/{id}/iterator` | 双向文档遍历 | 已登记 |
| GET | `/api/v1/index/{id}/synonyms` | 读取同义词 | 未登记 |
| POST | `/api/v1/index/{id}/synonyms` | 追加同义词 | 未登记 |
| PUT | `/api/v1/index/{id}/synonyms` | 替换同义词，空数组可清空 | 未登记 |
| GET | `/` 及固定静态资源路径 | 内嵌 Web UI、CSS/JS、图标 | 不作为数据 API 登记 |
| OPTIONS | 服务路由 | CORS 预检；query/iterator 声明 `POST, OPTIONS` | 不作为数据 API 登记 |

以上为 19 个 `/api/v1` 业务/存活操作，加 `/version`、UI/静态资源和预检。B1/B2 后 query/iterator 的 GET 与 HTTP QUERY 返回 405；POST URL 非空参数返回 400；旧 `/api/v2/index/{id}/query` 返回 404。旧 `/file` 路由仅返回 404 迁移提示。

## 3. Web UI

| 当前功能 | 实现范围 |
| --- | --- |
| 三种检索模式 | 默认 Lexical；Vector/Hybrid 手动输入 JSON 向量；不会自动生成向量 |
| 结果浏览 | 文本输入即时搜索、分页、键盘/触摸翻页、结果数量与耗时 |
| 文档展示 | 存储的标题/正文、原文预览、外部 URL/iframe、图片 URL 或内嵌 data:image |
| 日期交互 | 通过日期起止输入进行时间范围过滤，保留最新/最旧排序；依赖对应 schema，主要用于词法检索 |
| 索引信息 | 名称、版本、文档数、字段映射；URL 参数 `api_key/index_id` 选择 key 和索引 |
| 错误展示 | 非法向量和服务端错误显示在输入区域；Vector/Hybrid 使用日期过滤会收到过滤未支持错误 |

UI 默认使用 demo key 和索引 0；初始化 key/索引及写入使用 API。当前没有创建/删除索引、编辑 schema、文档 CRUD、密钥管理或集群管理页面。它是保留的搜索调试界面，不是完整管理控制台。

## 4. 服务、构建与维护

- API key 鉴权、按 key 的索引/数据目录隔离、随机 key 与固定 demo key；无内置配额或限流。
- 启动参数：`index_path`、`local_ip`、`local_port`、`force_shard_number`、`generate_openapi`；未知、重复、非法参数启动失败。
- 终端命令：`create`、`delete`、`info`、`search`、`openapi`、`help`、`quit`。`search -v [1,0,0]` 是 demo key/索引 0 的向量诊断入口。
- 生成 OpenAPI JSON/YAML；服务端终端状态表；核心库 `info::collect_index_info` 统计采集。
- Rust 核心库可嵌入应用；HTTP 服务内嵌静态 UI；保留 Dockerfile 从源码构建。两个 crate 均 `publish=false`。
- 核心默认 features 为 `zh`、`vb`。`vb` 提供来源、两路分数及 shard/level/cluster 等诊断字段；这些字段不表示保留自动切块。`api-schema` 可选、服务开启；`gxhash` 可选。
- CI 保留独立核心构建/依赖边界、workspace 构建与测试、HTTP/Chromium、静态资源及生成 OpenAPI 一致性检查。

## 5. 已移除与尚未实现

已移除：分面计数、直方图、范围聚合、字段 min/max；MCP 服务、独立客户端/SDK 工程、内置模型/下载/Embedding、自动切块、PDF/CSV/目录和本地 JSON/SIFT 导入及 SIFT 评测、PDF 附件服务、拼写纠错/自动补全/自动改写、地理检索/距离排序、高亮/摘要、内置配额/限流，以及 B1/B2 的旧查询协议。迁移方式见 [MIGRATION.md](MIGRATION.md)。

尚未实现或验收：向量/混合元数据过滤、稳定业务 ID/版本/upsert 契约、可恢复 WAL/检查点/一致快照、共同可见视图、完整资源约束和生产可靠性验收、分布式路由/复制/弹性、对象存储。

多字段排序、命中计数、分页、多向量、同义词、词干器、量化选项、N-gram、控制台、demo key 和 `vb` **仍保留**；本次聚合裁剪不删除这些能力。

核查入口：[数据类型](seekstorm/src/index.rs)、[检索执行](seekstorm/src/search.rs)、[向量配置](seekstorm/src/vector.rs)、[ANN 模式](seekstorm/src/vector_similarity.rs)、[服务路由](seekstorm_server/src/http_server.rs)、[终端命令](seekstorm_server/src/server.rs)、[UI](seekstorm_server/web/js/master.js)、[构建特性](seekstorm/Cargo.toml)。
