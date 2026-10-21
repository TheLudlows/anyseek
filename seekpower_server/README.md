# 当前 P1-A 开发分支接口

默认服务现已切换至 Record/catalog；本文件后续旧接口说明保留作历史参考，
不得作为当前默认服务契约。实际路由以 `generate_openapi=true` 输出及
[阶段记录](../docs/implementation-57.md) 为准。P1-A 尚未完成。

首次启动 `index_path` 必须不存在（父目录存在）；重启必须指向完整 catalog。
不导入旧索引或空目录。`MASTER_KEY_SECRET` 必须在重启时保持一致，
启动输出的 master API key 是可信管理入口，普通共享 key 不能注册身份或管理授权。
所有请求仍使用 `apikey` header；不增加账户或读写角色平台。

创建 collection 使用 `{operation_id,collection_id,config,embedding_space_id?}`，
其中 `config` 是原创建 schema/config，向量 collection 需要 embedding space。
写入 `/api/v1/index/{id}/doc` 使用
`{operation_id,record_id,routing_key,expected,document}`；
`expected` 为 `"Absent"` 或 `{"Match":N}`，`document:null` 是条件删除。
成功写入已 durable 且 visible，无需随后 PATCH 才可见；PATCH collection
现在执行在线检查点。读结果/查询命中使用 Record envelope 和稳定字符串 ID。
操作 ID 为 `{epoch:UTC日编号,nonce:唯一字符串}`；`sys:` nonce 前缀保留。
丢失响应/`outcome_unknown` 必须复用原 ID、原请求；到期后不能假设未执行。

master 可 POST `/api/v1/apikey` 注册 `{operation_id,key_id,apikey_base64}`，
DELETE 同一路径撤销 `{operation_id,key_id}`；grant 接口与请求见生成参考。
撤销 key 不删除 collection。操作状态与数据读取同样检查现有授权。
POST `iterator` 接收 `{cursor?,length?,fields?}`，返回同一 visible_seq 的原始记录页；
写入后旧 cursor 返回409。POST `delete-plan` 接收 `{select:"all"}`、
`{select:"ids",record_ids:[...]}` 或 `{select:"query",query:{query:"text"}}`。
保存返回 targets、为每个目标分配并保存 operation_id，再 POST `batch` 的
`{writes:[...]}` 执行；每批最多100条，非原子，返回逐项结果。不要在重试时重新生成删除计划。
Master DELETE collection 接收 `{operation_id}`，永久保留ID墓碑并撤销所有grant；
文件回收后置，撤销key仍不删除collection。
POST collection `backup` 接收 `{operation_id}`，返回独立本地快照 backup_id；
相同ID重试返回原快照。Master POST `/api/v1/restore` 接收
`{operation_id,collection_id,backup_id}`，恢复到新collection，不恢复旧grant。
保留完整原始数据/配置/版本/操作收据，备份最多32份；尚无备份导出/退役工具，
不等于完整服务catalog备份或异地容灾。
Web UI 已接入创建、原请求重试、搜索和向量配置；批量 JSON 输入逐条提交。

---

# Seekpower HTTP 服务

本服务托管 JSON API 与内嵌搜索 UI。完整路由及功能边界见 [FEATURES.md](../FEATURES.md)；请求字段见生成的 OpenAPI 文件（`generate_openapi=true`）和 [HTTP 示例](test_api.rest)。

## 启动

```sh
cargo build --workspace
export MASTER_KEY_SECRET='replace-with-your-private-secret'
target/debug/seekpower_server local_ip=127.0.0.1 local_port=8080
```

| 参数 | 默认值/用途 |
| --- | --- |
| `index_path` | `seekstorm_index`（沿用已有数据目录）；相对路径按可执行文件目录解析 |
| `local_ip` | `0.0.0.0` |
| `local_port` | `80` |
| `force_shard_number` | 未设置时按物理 CPU 核数；显式设置须为正整数，是本机并行分片数 |
| `default_nprobe` | `200`（初始校准值，非通用最优）；必须为正整数，请求 `nprobe` 可覆盖 |
| `generate_openapi` | `false`；设为 `true` 时生成 JSON/YAML 到可执行文件目录后退出 |

仅接受 `name=value`，未知、重复、非法参数返回退出码 2。`MASTER_KEY_SECRET` 用于派生主密钥；须配置私有值，源码默认值不适合部署使用。可使用仓库 [Dockerfile](../Dockerfile) 构建此分支。

## 首次启动与界面创建索引

服务启动时输出 `Master API key: ...`。它由 `MASTER_KEY_SECRET` 派生，可直接连接界面及调用索引、写入、检索接口。磁盘身份文件只保存哈希。

1. 打开 `http://127.0.0.1:8080/`，输入这个 Master API key 并点击 **Connect**。
2. 填写索引名称，选择 **Lexical / Vector / Hybrid**。向量模式填写外部模型维度；默认 F32、Cosine，向量字段名为 `vector`。
3. 可展开高级选项编辑字段 schema，然后点击 **Create & open index**。
4. 点击右上角 **Write data**，粘贴单条 JSON/对象数组或选择 JSON 文件，再点击 **Write & commit**。也可使用下面的 JSON API；右上角 **Indexes / API key** 可重新连接、创建或切换索引。

重启复用已持久化的 key/索引，不自动生成普通 key，也不需要初始化标记。需要独立数据作用域时，用主密钥调用创建 key API；新 key 不会接管其他 key 的索引。

## API 创建索引并检索

服务默认输出 info 级别日志，可通过 `RUST_LOG=seekpower=warn` 限制核心日志，或 `RUST_LOG=off` 关闭日志。启动与索引概况使用普通日志；主密钥仍会输出。

以下为 POSIX shell 示例，直接将启动日志显示的主密钥填入 `API_KEY`。

```sh
BASE='http://127.0.0.1:8080'
API_KEY='paste-master-key'
curl -X POST "$BASE/api/v1/index" -H "apikey: $API_KEY" -H 'Content-Type: application/json' -d '{"index_name":"example","schema":[{"field":"body","field_type":"Text"}]}'
```

将创建索引响应的 ID 填入 `INDEX_ID`，然后执行：

```sh
INDEX_ID=0
curl -X POST "$BASE/api/v1/index/$INDEX_ID/doc" -H "apikey: $API_KEY" -H 'Content-Type: application/json' -d '[{"body":"hello world"},{"body":"hello search"}]'
curl -X PATCH "$BASE/api/v1/index/$INDEX_ID" -H "apikey: $API_KEY"
curl -X POST "$BASE/api/v1/index/$INDEX_ID/query" -H "apikey: $API_KEY" -H 'Content-Type: application/json' -d '{"query":"hello"}'
curl -X POST "$BASE/api/v1/index/$INDEX_ID/iterator" -H "apikey: $API_KEY" -H 'Content-Type: application/json' -d '{"take":10,"include_document":true,"fields":["body"]}'
```

搜索与遍历仅接受 POST JSON；普通索引/文档读取仍为 GET。按 DocID 读取时，字段投影使用 URL `?fields=body,title`。更新使用 `{"id":0,"document":{"body":"new text"}}`，表示完整替换并分配新 ID；数组可批量替换。删除请求使用 `{"ids":[0,1]}`、`{"query":"text"}`（全部匹配）或 `{"all":true}`。成功响应直接返回 JSON 值，错误统一为 `{"code":"bad_request","message":"..."}` 并使用相应 HTTP 状态码。向量配置与示例见 [根 README](../README.md)。

撤销 key 需要主密钥，body 为 `{"apikey_base64":"要删除的key"}`；同时删除其索引。创建 key 只接受 `{}` 并生成随机密钥；`demo` 参数返回 400。没有内置配额/限流。

## Web UI

访问 `http://127.0.0.1:8080/`，通过连接表单输入 Master API key（已有普通 API key 也可连接）。密钥和选中的索引仅保存在当前标签页的 sessionStorage；点击 **Forget key** 清除本标签页凭证，但不会撤销服务端 key。URL 可指定 `index_id`，不接受 `api_key` 登录。UI 默认 Lexical，Vector/Hybrid 需要手动粘贴 JSON 查询向量，文本变化后由调用方更新向量。

保留结果分页、原文/外部链接预览、图片展示、UTC 日期起止输入、三种检索模式日期过滤和排序。日期交互依赖配置好的时间字段；Vector/Hybrid 需要有效查询向量；非法过滤条件明确报错。ANN 过滤受簇探测范围限制。索引创建/选择及 JSON 文档写入有界面入口。写入会调用文档 POST，再调用索引 PATCH 提交，展示数量并可返回搜索；若 POST 已接受但 PATCH 失败，提供只重试提交的按钮，避免重复写入。索引删除、文档更新/删除及 key 创建/撤销仍通过 API 完成。JSON 文件仅在浏览器读取后发送普通 JSON 请求，不是服务端文件解析/目录导入。

搜索逻辑在 [web/js/master.js](web/js/master.js)，连接与索引创建在 [web/js/setup.js](web/js/setup.js)，JSON 写入在 [web/js/documents.js](web/js/documents.js)。静态资源嵌入可执行文件，修改后需重新构建。

## 维护与退出

使用 Ctrl-C（SIGINT）或 SIGTERM 退出：先停止接收请求、结束已有连接，再提交并关闭全部索引。服务不读取 stdin 命令。导出接口文档使用启动参数 `generate_openapi=true`。索引信息通过 Web UI 或 GET `/api/v1/index/{id}` 查看；提交使用 PATCH 同一路径。HTTP 关闭索引入口和 `/version` 已删除，存活检查使用 GET `/api/v1/live`。

正常提交/关闭及本地索引文件仍保留；当前没有在线一致快照或崩溃恢复可靠性承诺。文件解析、SIFT 导入/评测等入口已删除，迁移细节见 [MIGRATION.md](../MIGRATION.md)。

统计聚合已移除：`query_facets`（包括空数组）返回 400，查询不再返回 `facets`，索引信息不再返回 `facets_minmax`。UI 不再依赖自动计算的数据日期范围，起止日期由用户输入，结束日期包含当天。

倒排写入缓冲区从空开始，按每条 posting 所需空间增长，提交/清空后释放；位置压缩缓存只为使用到的词项分段按需分配。已删除每个分片固定预分配 400 MB 的策略。实际内存仍随分片、字段、未提交记录与向量数量增长；`force_shard_number=1` 可限制新建索引的本机分片数，不会改变磁盘已有索引的分片布局。

普通创建请求只需名称和 schema；向量字段另传 `vector:{"dimensions":N}`。Text 字段默认存储并建立全文索引；向量字段默认不存储原始值、不建立全文索引；其他字段默认存储。过滤/排序字段显式设置 `filterable:true`。

查询模式由文本/向量输入推导；空请求 `{}` 浏览文档，查询包含未提交写入。默认返回 10 条并统计总数；`count:false` 返回 `count_total:null`，全文/浏览查询可用 `length:0` 只统计，向量/混合查询仍要求正的结果窗口。`operator:"or"` 改为任一词匹配，默认 `and`。向量/混合总数仍受候选检索语义限制，不承诺全库精确计数。遍历默认返回 10 个未删除文档，支持字段投影。

## 向量搜索预算（默认行为变更）

Vector/Hybrid 请求不再默认全簇扫描，而使用服务的 `default_nprobe`。纯词法行为不变。要保持旧版的全簇行为，请显式传 `all:true`；核心库的 AnnMode 默认值没有变化。

```json
{"query_vector":[1,0,0]}
{"query":"example","query_vector":[1,0,0],"nprobe":100}
{"query_vector":[1,0,0],"all":true}
```

`nprobe` 必须是正整数；省略时继承服务预算。`all:false` 同样使用请求或服务预算。`all:true` 与 `nprobe` 同传返回400；没有 query_vector 的关键词/浏览请求携带任一字段也返回400。显式null、错误类型、0、负数、小数、整数溢出均拒绝。

预算按每分片、每level计算，超过该level簇数时由核心取min；不是全局候选文档数、超时限制或召回保证。未提交向量另行扫描，预算不限制这部分工作。相似度阈值可同时使用，仍是文档评分/距离过滤，不是簇预算。全簇扫描使用已配置的存储精度/量化，不等于原始F32精确评分。不要依赖ANN的count_total作为全库精确命中数。

参数应在代表性train集合选择并冻结，再用dev验证。默认值只是初始折中，不是通用最优；数据、分片数、簇大小及过滤条件改变后应重新校准。

### 初始默认值的校准依据

初始 `default_nprobe=200` 来自 SeekPower 自身的 train-only 校准：固定100k MIRACL中文缩减语料、BGE-small-zh-v1.5 512维F32、无量化、单分片，128条固定train查询各重复两次；候选15/50/100/200/300/400。预先规定在平均精确邻居Recall@100至少0.98的候选中选择客户端p95最低者，未使用dev选参。

200的train邻居召回为0.983203、nDCG@10为0.818170、p50/p95为9.02/18.17ms；All对照为1.0、0.818951、17.10/34.06ms。该小样本顺序试验没有置信区间，不是性能承诺，尤其不能保证其他数据或dev的召回达到0.98。完整候选、查询ID和原始结果哈希见 [校准记录](../docs/ann-default-calibration.json)。启动时用 `default_nprobe=...` 调整，请求可再覆盖。
