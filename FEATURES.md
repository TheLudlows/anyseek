# 当前功能清单

本地 P1 实施进展见 [#57 实施与验证记录](docs/implementation-57.md)。可靠 Record 模块已在本地接入增量索引、活动统计及在线 WAL 检查点测试，默认 HTTP/UI 已接入独立持久授权目录与 Record 路径，共享 key、撤销输出屏障和浏览器原请求重试已有本地端到端测试；现已补充 iterator、条件删除计划/批量执行、collection 逻辑删除及本地 collection 备份恢复，补充根目录中断恢复、备份退役管理及创建请求重试/拒绝释放；41f5f601 已通过 Linux/Windows 原生 CI，9ebd0bd8 也已双平台通过；现已实现完整 service/catalog 备份、当前 master 专属的新目录恢复及旧授权隔离，最终实现 b5cc072 已通过 Linux/Windows 原生 CI；系统崩溃/断电验收仍待隔离环境；下表不能据此扩展为可靠服务承诺。

核查日期：2026-10-15（Asia/Shanghai）。基于主干 `4e171b5`，本分支收敛 HTTP 参数；Rust 核心配置与索引格式保持不变。

## 1. 检索与数据能力

| 类别 | 当前保留功能 | 条件与边界 |
| --- | --- | --- |
| 全文检索 | AND/OR/NOT、短语、查询中的 `+`/`-`/双引号、多字段检索、字段权重 | HTTP 默认 AND；只有文本时自动使用全文检索 |
| 相关性 | `Bm25f`、`Bm25fProximity` | 核心保留两种；HTTP 新建索引固定 Bm25fProximity |
| 分词 | `AsciiAlphabetic`、`UnicodeAlphanumeric`、`UnicodeAlphanumericFolded`、`Whitespace`、`WhitespaceLowercase`、`UnicodeAlphanumericZH` | 中文分词依赖默认启用的 `zh`；Folded 支持字符规范化 |
| 文本配置 | 停用词、高频词、词级二元/三元 N-gram 索引 | N-gram 默认位掩码 9，不提供字符前缀/子串匹配；停用词保留配置；HTTP 高频词固定 English，核心仍支持自定义 |
| 向量输入 | 外部向量、多个向量字段、JSON 数组或 Binary/base64 | HTTP 使用 `vector:{"dimensions":N}`，核心仍为 `Inference::External`；所有向量字段共享索引级配置；应用负责模型和切块 |
| 向量校验 | 维度、精度、有限数值/范围、非零余弦范数、base64 格式 | 非法请求显式报错；批次入队前校验输入不等于事务 |
| 向量检索 | 全簇扫描、Leveled IVF ANN、K-Medoid 聚类 | `AnnMode::All/Nprobe/Similaritythreshold/NprobeSimilaritythreshold`；全簇扫描仍使用配置的存储精度/量化，不等于原始 F32 精确评分 |
| 向量配置 | F32/I8；Cosine/Dot/Euclidean；不量化、ScalarQuantizationI8、TurboQuantI8 | I8 输入只能配 `quantization: None`；聚类 `None/Auto/Fixed`；可设相似度阈值 |
| 混合检索 | 全文与向量分别召回，按文档 ID 合并并执行 RRF | 已有 weighted RRF、独立候选预算和权重；HTTP 按文本/向量输入推导模式；无模型重排或跨节点协调 |
| 字段选择 | `search_fields` 选择检索字段；`fields` 投影存储字段 | 字段选择不等于元数据值过滤 |
| 元数据过滤 | 字符串/字符串集合、整数、浮点、时间戳范围过滤 | 创建请求使用 `filterable:true`（核心/索引响应仍为 `facet:true`）；三种模式使用共享谓词，向量在评分/Top-K 前过滤；ANN 仍受簇探测范围限制 |
| 排序 | 相关性、facet 字段升降序、多字段组合、`_score`/`_id` | Vector/Hybrid 也会对已召回候选排序，但向量召回先按相似度截断，不保证全库字段排序 Top-K |
| 结果与分页 | `Topk/Count/TopkCount`、`offset/length`、显式空查询浏览 | 完整计数/空查询能力主要在词法路径；ANN 和 Hybrid 的 `count_total` 不应作为全库精确去重命中数 |
| 实时可见性 | HTTP 默认包含未提交数据；Rust 核心显式选择 realtime | 可搜索不等于已持久化；异常退出可能丢失未提交数据 |
| 索引生命周期 | 创建、打开、查看、提交、关闭、删除；核心库清空索引 | HTTP 服务启动加载磁盘索引；Rust 库与 HTTP 统一使用 Mmap；实时写入缓冲仍在内存 |
| 文档写入 | JSON 单条/批量写入、按内部 DocID 单条/批量替换更新 | 当前更新不是业务 ID upsert，也不是字段局部合并；内部 DocID 不提供稳定业务 ID/版本契约 |
| 文档读取 | 按 DocID 读存储字段、字段投影、保留原文 | 不生成摘要或高亮；非存储字段不能作为原文返回 |
| 文档删除 | 单个/多个 DocID 删除、按查询选取文档删除 | 按查询删除仍保留；没有跨记录事务承诺 |
| 遍历 | 双向 iterator、起点、skip/take、字段投影、可选文档内容 | 正 take 向前、负 take 向后；HTTP 默认 10 个未删除文档；核心仍可显式返回删除 ID，不代表恢复原文 |
| 本地存储 | 倒排、向量、facet、文档存储、删除标记；增量提交及重新打开 | RAM 缓冲、本机 shard/level、mmap；没有集群复制或已验收的 WAL/检查点恢复协议 |
| 压缩与执行 | None/Snappy 文档压缩；posting 编码、SIMD、Block-max WAND、MaxScore | 默认文档压缩 Snappy；不是已完成当前版本性能认证 |

字段类型：`U8/U16/U32/U64`、`I8/I16/I32/I64`、`Timestamp`、`F32/F64`、`Bool`、`String16/String32`、`StringSet16/StringSet32`、`Text`、`Json`、`Binary`。字段类型存在不代表每种类型都支持元数据过滤、全文或向量索引。向量使用 `Json` 或 `Binary` 加 `index_vector:true`，没有独立的 `FieldType::Vector`。

HTTP 新建索引采用 Snappy、English 高频词、N-gram=9、Auto 聚类；查询搜索全部向量簇。核心显式配置仍保留，已有索引继续使用其持久化配置。HTTP 默认包含未提交写入；空查询浏览文档；`count:false` 不报告总数，返回 `count_total:null`。旧调优参数直接拒绝。

## 2. HTTP 接口全集

表中 `{id}` 为索引 ID，`{doc}` 为内部文档 ID。数据接口使用 `apikey` 请求头；主密钥可直接操作自己作用域的索引，并用于创建/撤销额外 key。接口以 [实际路由](seekpower_server/src/http_server.rs) 为准，OpenAPI 覆盖下表 15 个业务/存活操作。

| 方法 | 路径 | 功能 | OpenAPI |
| --- | --- | --- | --- |
| GET | `/api/v1/live` | 存活检查 | 已登记 |
| POST | `/api/v1/apikey` | 使用 `{}` 创建随机 key，需主密钥 | 已登记 |
| DELETE | `/api/v1/apikey` | 撤销指定 key 及其索引；主密钥 + `apikey_base64` JSON | 已登记 |
| GET | `/api/v1/apikey` | 查看当前 key 下的索引信息，不是全站密钥列表 | 已登记 |
| POST | `/api/v1/index` | 创建索引/schema | 已登记 |
| GET | `/api/v1/index/{id}` | 索引信息、schema、文档计数 | 已登记 |
| PATCH | `/api/v1/index/{id}` | 提交索引 | 已登记 |
| DELETE | `/api/v1/index/{id}` | 删除索引 | 已登记 |
| POST | `/api/v1/index/{id}/doc` | 单条/批量写入 | 已登记 |
| PATCH | `/api/v1/index/{id}/doc` | 单条/批量完整替换更新 | 已登记 |
| DELETE | `/api/v1/index/{id}/doc` | JSON：`ids`、非空 `query` 或 `all:true` | 已登记 |
| GET | `/api/v1/index/{id}/doc/{doc}` | 读取文档；可用 URL `?fields=body,title` 投影 | 已登记 |
| DELETE | `/api/v1/index/{id}/doc/{doc}` | 删除单条文档 | 已登记 |
| POST | `/api/v1/index/{id}/query` | Lexical/Vector/Hybrid JSON 查询；默认 ANN，`nprobe` 覆盖预算，`all:true` 全簇 | 已登记 |
| POST | `/api/v1/index/{id}/iterator` | 双向文档遍历 | 已登记 |
| GET | `/` 及固定静态资源路径 | 内嵌 Web UI、CSS/JS、图标 | 不作为数据 API 登记 |
| OPTIONS | 服务路由 | CORS 预检；query/iterator 声明 `POST, OPTIONS` | 不作为数据 API 登记 |

以上为 15 个 `/api/v1` 业务/存活操作，加 UI/静态资源和预检。B1/B2 后 query/iterator 的 GET 与 HTTP QUERY 返回 405；POST URL 非空参数返回 400；旧 `/api/v2/index/{id}/query` 与 `/file` 路由无专用处理，和其他未知路径一样返回 404；OPTIONS 使用通用 CORS 预检返回 204。

## 3. Web UI

| 当前功能 | 实现范围 |
| --- | --- |
| 三种检索模式 | 默认 Lexical；Vector/Hybrid 手动输入 JSON 向量；不会自动生成向量 |
| 结果浏览 | 文本输入即时搜索、分页、键盘/触摸翻页、结果数量与耗时 |
| 文档展示 | 存储的标题/正文、原文预览、外部 URL/iframe、图片 URL 或内嵌 data:image |
| 日期交互 | 通过日期起止输入进行时间范围过滤，保留最新/最旧排序；依赖对应 schema，支持 Lexical/Vector/Hybrid |
| 数据写入 | 粘贴 JSON 对象/数组或浏览器读取 JSON 文件；复用 POST 写入与 PATCH 提交；显示数量，失败时可只重试提交 |
| 连接与索引 | Master API key 或已有普通 key 连接；列出/切换当前 key 的索引；Lexical/Vector/Hybrid 创建预设、向量维度、创建时 schema JSON 编辑；名称/版本/文档数 |
| 错误展示 | 非法向量、过滤字段/类型/范围和服务端错误显示在输入区域 |

首次使用在表单输入启动日志中的 Master API key。凭证与选中索引使用标签页 sessionStorage，支持忘记凭证；URL 仅支持选择 `index_id`，不读取 `api_key` 作为凭证。创建调用已有 REST API。当前没有删除索引、修改已有索引 schema、文档更新/删除、服务端密钥管理或集群管理页面。

## 4. 服务、构建与维护

- 倒排写入缓冲区按需增长，提交/清空后释放；位置压缩缓存按实际使用的词项分段分配，不再为每个分片固定预分配 400 MB。
- 启动直接使用由 MASTER_KEY_SECRET 派生的主密钥及其持久化索引作用域；不自动生成普通 key 或初始化标记。
- API key 鉴权、按 key 的索引/数据目录隔离、随机 key；无内置配额或限流。
- 启动参数：`index_path`、`local_ip`、`local_port`、`force_shard_number`、`default_nprobe`、`generate_openapi`；未知、重复、非法参数启动失败。
- Ctrl-C/SIGINT 或 SIGTERM 停止接收请求、结束已有连接，再提交并关闭全部索引；无 stdin 交互命令。
- 启动参数 `generate_openapi=true` 生成 OpenAPI JSON/YAML；启动/索引概况使用普通日志；核心库 `info::collect_index_info` 返回结构化统计，采集不提交或同步文件。核心日志由调用方配置；服务支持 `RUST_LOG`。
- Rust 核心库可嵌入应用；HTTP 服务内嵌静态 UI；保留 Dockerfile 从源码构建。两个 crate 均 `publish=false`。
- 核心默认 features 为 `zh`、`vb`。`vb` 提供来源、两路分数及 shard/level/cluster 等诊断字段；这些字段不表示保留自动切块。内存集合使用 AHash；持久词项键使用固定 XXH64（格式 8），见 [哈希契约](docs/persistent-term-hash.md)。HTTP DTO 与 OpenAPI 全部位于服务，核心不提供 `api-schema` feature。
- CI 保留独立核心构建/依赖边界、workspace 构建与测试、HTTP/Chromium、静态资源及 OpenAPI 契约检查与产物生成。

## 5. 已移除与尚未实现

已移除：同义词及其 REST 管理接口、词干器及 Snowball 依赖；RAM 全量加载模式、LZ4/Zstd 文档压缩及未引用的 half 依赖；固定 demo key、终端交互、彩色状态表、HTTP 关闭索引入口、`/version` 和 URL key 登录；未使用 tokenizer/状态字段/恒关闭分支及历史图片；分面计数、直方图、范围聚合、字段 min/max；MCP 服务、独立客户端/SDK 工程、内置模型/下载/Embedding、自动切块、PDF/CSV/目录和本地 JSON/SIFT 导入及 SIFT 评测、PDF 附件服务、拼写纠错/自动补全/自动改写、地理检索/距离排序、高亮/摘要、内置配额/限流，以及 B1/B2 的旧查询协议。迁移方式见 [MIGRATION.md](MIGRATION.md)。

尚未实现或验收：稳定业务 ID/版本/upsert 契约、可恢复 WAL/检查点/一致快照、共同可见视图、完整资源约束和生产可靠性验收、分布式路由/复制/弹性、对象存储。

多字段排序、命中计数、分页、多向量、量化选项、N-gram 和 `vb` **仍保留**。

核查入口：[数据类型](seekpower/src/index.rs)、[检索执行](seekpower/src/search.rs)、[向量配置](seekpower/src/vector.rs)、[ANN 模式](seekpower/src/vector_similarity.rs)、[服务路由](seekpower_server/src/http_server.rs)、[启动与退出](seekpower_server/src/server.rs)、[UI](seekpower_server/web/js/master.js)、[构建特性](seekpower/Cargo.toml)。
