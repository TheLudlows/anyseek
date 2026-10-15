# SeekPower HTTP 服务

本服务托管 JSON API 与内嵌搜索 UI。完整路由及功能边界见 [FEATURES.md](../FEATURES.md)；请求字段见 [OpenAPI](openapi/openapi.yml) 和 [HTTP 示例](src/test_api.rest)。

## 启动

```sh
cargo build --workspace
export MASTER_KEY_SECRET='replace-with-your-private-secret'
target/debug/seekstorm_server local_ip=127.0.0.1 local_port=8080
```

| 参数 | 默认值/用途 |
| --- | --- |
| `index_path` | `seekstorm_index`；相对路径按可执行文件目录解析 |
| `local_ip` | `0.0.0.0` |
| `local_port` | `80` |
| `force_shard_number` | 未设置时按物理 CPU 核数；显式设置须为正整数，是本机并行分片数 |
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

服务默认输出 info 级别日志，可通过 `RUST_LOG=seekstorm=warn` 限制核心日志，或 `RUST_LOG=off` 关闭日志。启动与索引概况使用普通日志；主密钥仍会输出。

以下为 POSIX shell 示例，直接将启动日志显示的主密钥填入 `API_KEY`。

```sh
BASE='http://127.0.0.1:8080'
API_KEY='paste-master-key'
curl -X POST "$BASE/api/v1/index" -H "apikey: $API_KEY" -H 'Content-Type: application/json' -d '{"index_name":"example","schema":[{"field":"body","field_type":"Text","store":true,"index_lexical":true}]}'
```

将创建索引响应的 ID 填入 `INDEX_ID`，然后执行：

```sh
INDEX_ID=0
curl -X POST "$BASE/api/v1/index/$INDEX_ID/doc" -H "apikey: $API_KEY" -H 'Content-Type: application/json' -d '[{"body":"hello world"},{"body":"hello search"}]'
curl -X PATCH "$BASE/api/v1/index/$INDEX_ID" -H "apikey: $API_KEY"
curl -X POST "$BASE/api/v1/index/$INDEX_ID/query" -H "apikey: $API_KEY" -H 'Content-Type: application/json' -d '{"query":"hello","search_mode":"Lexical"}'
curl -X POST "$BASE/api/v1/index/$INDEX_ID/iterator" -H "apikey: $API_KEY" -H 'Content-Type: application/json' -d '{"take":10,"include_document":true,"fields":["body"]}'
```

搜索与遍历仅接受 POST JSON；普通索引/文档读取仍为 GET。按 DocID 读取时，字段投影使用 JSON body `{"fields":["body"]}`。更新为完整记录替换。向量配置与示例见 [根 README](../README.md)。

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
