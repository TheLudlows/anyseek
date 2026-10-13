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

## 创建 key、索引并检索

以下为 POSIX shell 示例。将 `MASTER_KEY` 替换为启动日志显示的主密钥，再把创建响应的 key 填入 `API_KEY`。

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

访问 `http://127.0.0.1:8080/`。默认使用 demo key 与索引 0，或用 `/?api_key=KEY&index_id=ID` 选择；URL 中的 key 应编码（例如 `+` 为 `%2B`）。UI 默认 Lexical，Vector/Hybrid 需要手动粘贴 JSON 查询向量，文本变化后由调用方更新向量。

保留结果分页、原文/外部链接预览、图片展示、日期直方图、词法日期过滤和排序。日期交互依赖配置好的时间字段；Vector/Hybrid 的元数据过滤会明确报错。创建索引、文档写入和 key 管理通过 API 完成。

显示字段与默认 key/index 在 [web/js/master.js](web/js/master.js) 配置。静态资源嵌入可执行文件，修改后需重新构建。

## 终端命令与维护

| 命令 | 作用 |
| --- | --- |
| `create` | 创建固定 demo key；索引仍通过 HTTP 创建 |
| `delete` | 删除 demo key 及其所有索引 |
| `info` | 显示当前索引信息 |
| `search -v [1,0,0]` | 对 demo key 下索引 0 运行向量诊断，维度须匹配 |
| `openapi` | 导出 OpenAPI |
| `help` | 命令帮助 |
| `quit` | 退出服务 |

正常提交/关闭及本地索引文件仍保留；当前没有在线一致快照或崩溃恢复可靠性承诺。文件解析、SIFT 导入/评测等入口已删除，迁移细节见 [MIGRATION.md](../MIGRATION.md)。
