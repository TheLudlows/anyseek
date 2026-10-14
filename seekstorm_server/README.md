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

全新、空的数据目录首次启动时，服务自动生成一个随机普通 API key，日志中显示 `Initial API key: ...`。保存这个 key；明文只在初始化时显示一次，磁盘身份文件仅保存哈希。

1. 打开 `http://127.0.0.1:8080/`，输入这个普通 key 并点击 **Connect**。
2. 填写索引名称，选择 **Lexical / Vector / Hybrid**。向量模式填写外部模型维度；默认 F32、Cosine，向量字段名为 `vector`。
3. 可展开高级选项编辑字段 schema，然后点击 **Create & open index**。
4. 通过下面的 JSON API 写入文档；右上角 **Indexes / API key** 可重新连接、创建或切换索引。

重启复用已持久化的 key/索引。已有 key 的安装不会额外创建新 key。数据根目录的 `bootstrap.complete` 记录初始化已完成，应与整个数据目录一同保存；撤销全部 key 后重启也不会重新生成凭证。丢失首次输出的 key 时，用主密钥调用创建 key API 获取新 key。新 key 是独立的数据作用域，不会自动接管旧 key 下的索引。

## API 创建 key、索引并检索

服务默认输出 info 级别日志，可通过 `RUST_LOG=seekstorm=warn` 限制核心日志，或 `RUST_LOG=off` 关闭日志。终端交互/状态表由服务单独展示。

以下为 POSIX shell 示例。已有首次初始化的 key 时可直接填入 `API_KEY`，跳过创建 key 步骤。需要额外 key 时，将 `MASTER_KEY` 替换为启动日志显示的主密钥，再保存创建响应。

```sh
BASE='http://127.0.0.1:8080'
MASTER_KEY='paste-master-key'
curl -X POST "$BASE/api/v1/apikey" -H "apikey: $MASTER_KEY" -H 'Content-Type: application/json' -d '{}'

API_KEY='paste-created-key'
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

撤销 key 需要主密钥，body 为 `{"apikey_base64":"要删除的key"}`；同时删除其索引。创建固定 demo key 使用 `{"demo":true}`。没有内置配额/限流。

## Web UI

访问 `http://127.0.0.1:8080/`，通过连接表单输入普通 API key。密钥和选中的索引仅保存在当前标签页的 sessionStorage；点击 **Forget key** 清除本标签页凭证，但不会撤销服务端 key。仍接受 `/?api_key=KEY&index_id=ID`（key 须 URL 编码，例如 `+` 为 `%2B`）；读取后从地址栏移除 key，推荐直接使用连接表单。UI 默认 Lexical，Vector/Hybrid 需要手动粘贴 JSON 查询向量，文本变化后由调用方更新向量。

保留结果分页、原文/外部链接预览、图片展示、UTC 日期起止输入、三种检索模式日期过滤和排序。日期交互依赖配置好的时间字段；Vector/Hybrid 需要有效查询向量；非法过滤条件明确报错。ANN 过滤受簇探测范围限制。索引创建/选择有界面入口；文档写入、索引删除和 key 创建/撤销仍通过 API 完成。

搜索逻辑在 [web/js/master.js](web/js/master.js)，连接与索引创建在 [web/js/setup.js](web/js/setup.js)。静态资源嵌入可执行文件，修改后需重新构建。

## 终端命令与维护

| 命令 | 作用 |
| --- | --- |
| `create` | 创建固定 demo key；索引通过界面或 HTTP 创建 |
| `delete` | 删除 demo key 及其所有索引 |
| `info` | 显示当前索引信息 |
| `search -v [1,0,0]` | 对 demo key 下索引 0 运行向量诊断，维度须匹配 |
| `openapi` | 导出 OpenAPI |
| `help` | 命令帮助 |
| `quit` | 退出服务 |

正常提交/关闭及本地索引文件仍保留；当前没有在线一致快照或崩溃恢复可靠性承诺。文件解析、SIFT 导入/评测等入口已删除，迁移细节见 [MIGRATION.md](../MIGRATION.md)。

统计聚合已移除：`query_facets`（包括空数组）返回 400，查询不再返回 `facets`，索引信息不再返回 `facets_minmax`。UI 不再依赖自动计算的数据日期范围，起止日期由用户输入，结束日期包含当天。
