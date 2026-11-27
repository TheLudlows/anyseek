# anyseek Record HTTP 服务

**简体中文** · [English](README.en.md)

通过 HTTP API 按租户命名空间管理集合和记录，提供全文、向量与混合检索。同步 `/batch` 成功项持久且可见；`/ingest` 返回持久业务回执，索引异步推进，详见[写入与重试](#稳定身份写入和重试)。Web 控制台为独立进程。

[Record 契约](../../../docs/record-model.md)说明职责与一致性边界；[路线与验收证据](../../../docs/roadmap.md)记录剩余工作与验证范围。

## 构建与启动

从仓库根目录执行。Linux / macOS：

```sh
cargo build
export MASTER_KEY_SECRET='replace-with-your-private-secret'
target/debug/anyseek_server local_ip=127.0.0.1 local_port=8080
```

Windows PowerShell：

```powershell
cargo build
$env:MASTER_KEY_SECRET = 'replace-with-your-private-secret'
.\target\debug\anyseek_server.exe local_ip=127.0.0.1 local_port=8080
```

`MASTER_KEY_SECRET` 必须显式设置且不能全为空白；缺失或无效时在创建数据目录、监听端口前报错退出。新部署请使用随机生成并妥善保存的私密值（例如 `python3 -c "import secrets; print(secrets.token_hex(32))"`），不要使用示例占位符。普通启动日志不输出 secret 或 Master Key。需要初始化时，在同一环境下单独运行：

```sh
target/debug/anyseek_server print_master_key=true
```

Windows PowerShell 使用 `.\target\debug\anyseek_server.exe print_master_key=true`。此命令只向标准输出打印 `Base64(SHA-256(secret 的 UTF-8 字节))` 并退出，不打开数据目录、不启动服务；输出本身就是管理凭据，不要记录到共享日志。它不生成或轮换密钥。

服务凭据通过 `apikey` 请求头用于 Origence／网关到 AnySeek 的全部受保护接口；数据接口另需 `X-AnySeek-Tenant`。`/api/v1/live` 保持公开；如果请求携带 `apikey`，则严格验证该服务凭据，支持宿主在无租户/集合时检查连接。用户认证与权限由上游完成，网关须校验用户对所选租户及服务管理操作的权限。AnySeek 不注册租户，不签发、保存或撤销租户 Key。使用 GET `/api/v1/index` 列出集合。租户命名空间须匹配 `^[a-z0-9][a-z0-9_-]{0,62}$`（支持数字开头的 UUID），集合名须匹配 `^[a-z][a-z0-9_-]{0,62}$`，不转换大小写。Origence 直接传业务 tenant UUID，并以 `ws_<workspace UUID 无连字符>` 作为集合名。默认数据目录位于可执行文件旁的 `anyseek_index`。首次 `index_path` 必须不存在且父目录存在；重启须指向可恢复的 Catalog 根。支持的格式见[存储协议](../../../docs/storage.md)。

独立 HTTP 服务启动时核对 Catalog 并完成未决的创建和恢复。已存在的活动集合在首次获准访问时打开，同一租户、集合及代次的并发请求共享一次 Recovery 打开。某个请求取消后，其余请求继续等待。GET `/api/v1/index` 返回实时统计，也会打开该租户仍处于冷态的集合。冷集合上的打开错误由相应请求返回，后续请求可以重试。

`ANYSEEK_RESIDENT_MEMORY_BYTES` 设定独立 HTTP 服务的驻留淘汰阈值。默认使用 `ANYSEEK_BUILD_SHARED_MEMORY_BYTES` 的 75%，配置值须大于零且不得超过该共享上限。预算按进程已预留的实际字节计算。压力下服务关闭最久未访问的空闲代次，并在关闭及释放内存后重新计算预算。活动请求、已接收写入、后台工作、备份和仍在传输的输出持有代次。预算等待每个准入阶段最多 2 秒，超时返回 `busy`。淘汰仅关闭内存中的代次，Catalog 和持久化数据保留，同代次可通过下一次获准访问重新打开。

| 参数 | 默认值与约束 |
| --- | --- |
| `index_path` | `anyseek_index`；相对路径以可执行文件目录为基准 |
| `local_ip` | `0.0.0.0` |
| `local_port` | `8080` |
| `default_nprobe` | `200`；正整数，请求可覆盖 |
| `print_master_key` | `false`；`true` 显式输出 Master Key 后退出，需要 secret；不能与 `generate_openapi=true` 同时使用 |
| `generate_openapi` | `false`；`true` 输出实际路由的 JSON/YAML 后退出 |

只接受 `name=value`；未知、重复、非法参数返回非零状态。服务使用 `log`，通过 `RUST_LOG` 配置日志。Ctrl-C/SIGINT/SIGTERM 停止接收请求并等待连接结束；成功写入已持久，不依靠退出时提交派生索引才成立。服务凭据只从进程配置读取，不写入 Catalog；轮换配置时须同步更新网关的服务间凭据。secret 不裁剪空白，派生算法不变。

## 启动 Web UI

API 与 UI 分别启动。控制台通过已认证网关选择租户命名空间，配置见[控制台指南](../../../console/README.md#本地启动-ui)；浏览器不保存或提交 master key。API 根路径 `/` 不提供页面。

## S3 后端（显式启用）

服务构建始终包含 S3 支持；运行时默认使用 `local` 后端。启用新部署示例：

```sh
export ANYSEEK_STORAGE_BACKEND=s3
export ANYSEEK_S3_ENDPOINT='https://your-s3-endpoint'
export ANYSEEK_S3_BUCKET='your-bucket'
export ANYSEEK_S3_REGION='your-region'
export ANYSEEK_S3_ACCESS_KEY_ID='your-access-key'
export ANYSEEK_S3_SECRET_ACCESS_KEY='your-secret-key'
```

只有明确配置的开发 HTTP endpoint 才设置 `ANYSEEK_S3_ALLOW_HTTP=1`。可选 `ANYSEEK_S3_MAX_OBJECT_BYTES` 默认 64 MiB，`ANYSEEK_S3_MAX_TAIL_BLOCKS` 默认 1,000,000；单次 tail 字节预算同时受 collection 日志预算和 1 GiB 上限约束。凭证只在运行时使用。

Catalog 使用根目录下的 `metadata.sqlite` 保存集合、备份归属和管理回执；本地和 S3 模式都保留该库及 SQLite WAL、owner 锁。Catalog 7 不包含租户注册和 Key 表，也不保存服务凭据。S3 模式另在同库保存 Record 协调状态；不同独立服务使用不同 bucket，完整服务恢复启动前须配置目标独立 bucket。对象位于 `anyseek/{tenant_id}/{collection_id}/`，不创建自动 namespace 或 Bucket 归属标记。SQLite 元数据不是可删除缓存，备份使用服务 API，不在运行中复制数据库文件。collection 内的本地 S3 缓存可从权威检查点及 tail 重建；对象缓存及锁的保护规则见[存储设计](../../../docs/storage.md)。

PATCH collection 发布完整远端 checkpoint 后再回收已覆盖且无 pin 的对象。collection backup 的相同请求在中断后继续原 cut；恢复到新 collection，复制到目标集合新代次，不包含服务凭据。完整服务备份沿用本地独立归档格式，包含 catalog 和完整数据；在 S3 模式恢复启动时会发布到目标独立 bucket。

该后端已合入；真实目标服务的条件写/读后可见性认证与其余维护路径审计仍待完成，见[存储协议](../../../docs/storage.md)。SQLite 首版是单主机协调，不提供跨主机 HA。

## 字段类型与数值

字段类型：String、StringSet、Int、Float、Text、Timestamp、Json、Binary。类型存在不代表支持全部索引方式。向量使用 Json/Binary 加 index_vector；已配置向量字段可缺席，缺向量记录不参与该字段召回；后补向量须使用精确版本条件。过滤字段需显式 filterable:true。不提供分面统计聚合、直方图或字段 extrema。

Int 是有符号 64 位整数（-9223372036854775808 至 9223372036854775807），Float 是有限的 64 位浮点数。Int 要求整数且必须在范围内。Timestamp 使用有符号 64 位 Unix 秒。Json 字段支持布尔值。向量精度可配置为 F32 或 I8。

String 使用完整字符串进行精确过滤；StringSet 是字符串集合，按任一成员精确匹配，去重并以 32 位字典 ID 存储。Text 保留分词全文检索。Int/Float/Timestamp 范围过滤为左闭右开。null 和缺失字段保留原始 payload 表示，不匹配元数据过滤，升序和降序排序均置于有值字段之后。

StringSet 排序以排序去重后的首个成员为键：升序空集合在非空集合之前，降序在非空集合之后；null/缺失字段始终排在有值字段（包括空集合）之后。相同键继续采用后续排序键或稳定记录身份排序。

Web 文档编辑器保留输入 JSON 的原始数值文本，新增、条件更新信封及暂存重发均不会经过 JavaScript Number 重新编码；嵌套 Json、整数和小数保持原文。旧版客户端已经暂存并可能发送的请求只保留其已有内容与操作身份，无法恢复过去丢失的精度。

Web 检查面板和搜索结果的 Document JSON 展开区保留服务器响应原文；搜索字段展示也保留原始数值，复制到编辑器可无损写回。业务逻辑仍使用普通 JavaScript 对象，原文仅用于响应展示和复制。

## 稳定身份、写入和重试

collection ID、record ID 是字符串业务身份，内部 DocID 不作为 HTTP 身份。所有数据请求使用 `apikey` 请求头。

- 创建：`{operation_id,collection_id,config,embedding_space_id?}`。`config` 包含名称/schema/可选向量配置；集合信息返回只读 `vector_dimensions`（无向量时为 null），供客户端校验。向量由调用方生成；`embedding_space_id` 仅为可选说明，不参与模型调用或向量来源校验。
- 写入：`{operation_id,record_id,expected,document}`。`document` 必须显式提供；对象完整替换，`null` 删除。
- `expected` 为 `"Absent"`、`{"Match":N}` 或 `"Any"`。删除后重建继续推进版本，避免旧条件命中新内容。
- 操作 ID 是 `{epoch,nonce}`，epoch 是自 Unix epoch 起的 UTC 日数。当前日和前一日可重试；正常当日签发至少有 24 小时窗口。控制操作保留 `sys:` nonce 前缀。
- 同步 `/batch` 成功表示 durable + visible，不需随后 PATCH。`/ingest` 的 committed 项只保证持久业务结果，索引异步推进。PATCH collection 是在线完整 checkpoint，不是确认写入的第二步；操作历史由保留期限及安全 GC 条件决定，不能通过手工 checkpoint 清空窗口内回执。

发送前保存完整请求。重试必须复用原 ID、原条件和原内容；相同请求返回原回执，更换内容会冲突。不能因查询不到回执或操作过期就重新生成 ID 补写。

| 结果 | 含义与处理 |
| --- | --- |
| `rejected` / `conflict` | 本次请求被拒绝或条件/身份冲突；曾经未知的请求不能仅凭后续拒绝认定此前未执行 |
| `busy`，HTTP 429 | admission 拒绝，可能尚未查询旧操作回执；保留原请求后重试 |
| `outcome_unknown` | 无法确认结果；原请求重试或恢复后查账 |
| `durable_pending` | 已持久，尚未完成派生应用；需要恢复 |
| `recovery_required` / `storage_error` | 停止依赖当前实例继续写入，按恢复流程处理 |
| `operation_expired`，HTTP 410 | 超出支持窗口；不证明此前没有执行 |

UI 在发送前将原请求和未确认状态保存到 sessionStorage；丢响应、刷新、后续拒绝不会解锁未知写入。只有未经历未知结果的明确拒绝才能释放被拒绝项及未发送余项。客户端备份这些待处理请求的方式由调用方负责。

集合活动记录数统一读取 `statistics.active_records`；索引消费进度通过 `/ingest` 查询。

## 同步写入

同步写入只提供 `POST /api/v1/index/{id}/batch`，单条也使用 `{"writes":[RecordWrite]}`；不再提供 `/doc` 的 POST/PATCH/DELETE 写入别名。读取 `/doc/{record_id}` 与状态查询保留。支持新增、expected 条件更新、显式 `document:null` 删除和 `vector_patch` 补写。

每批 1..100 项，按序独立执行，非事务；已成功项不会回滚。外层非 2xx 是请求级失败；HTTP 200 仍须逐项检查 `results`：只有 `code=applied` 且带 `receipt` 才确认持久且可见。`conflict`/`rejected` 后继续；`outcome_unknown`、`durable_pending`、恢复要求或存储故障停止后续，尾部返回 `not_attempted`。结果未知不等于失败，重试必须保留原 Operation ID 和完整 RecordWrite。浏览器已有待确认请求只添加 batch 外层，不改变原始数字或身份。

## 整组原子替换

`POST /api/v1/index/{id}/group/replace` 接收同一个 collection 内一组 Record 的完整新版：

```json
{
  "operation_id": {"epoch": 123, "nonce": "replace-guide-v1"},
  "group_id": "guide",
  "expected_group_version": 0,
  "records": [
    {"record_id": "guide-1", "document": {"body": "第一段"}},
    {"record_id": "guide-2", "document": {"body": "第二段"}}
  ]
}
```

`operation_id` 使用与普通 Record 写入相同的当前 epoch 生成规则；上面的 epoch 只是占位值。首次创建组使用 `expected_group_version: 0`；后续替换提交当前组版本。`records` 是完整成员列表，不是增量；空列表清空组但保留单调递增的组版本。每个成员必须带非 null 的 `document` 对象，不接受成员级 operation ID 或 vector patch；上层负责分块与向量化。

成功返回 `atomic: true`、原 operation ID 和 `receipt`（`seq`、`group_id`、`group_version`、`record_count`）。超时或结果未知时，重用同一 operation ID 与完整原请求取得原回执，不能生成新 ID 猜测重试结果。`GET /api/v1/index/{id}/group?group_id=guide` 返回当前组版本；从未存在的组返回 404。group ID 是不透明业务值，请按 URL 查询参数编码。当前版本查询不是某次操作的结果查询。

两个接口都使用 `apikey` 服务凭据和 `X-AnySeek-Tenant` 命名空间，请求作用于该租户的当前集合代次。普通 `/batch` 与 `/ingest` 仍是独立 Record 写入，`atomic: false`，不能用它们绕过已有组成员的归属约束。

## 异步索引写入

`POST /api/v1/index/{id}/ingest` 接收 `{"records":[RecordWrite,...]}`，每次 1..64 项；每项携带自己的稳定 Record ID、条件版本和 Operation ID。文档显式为对象或 null，也可提交 vector_patch。HTTP body 仍为 2 MiB；partition 队列按在途项目总数 64 和序列化请求总量 4 MiB 有界准入，超额拒绝。

返回 `atomic:false`、`searchability:"asynchronous"` 和与输入位置对应的 `results`。HTTP 200 只表示本次取得确定的逐项业务结果，不表示每项均成功：

| results[].code | 含义与处理 |
| --- | --- |
| committed | 附带持久 Record receipt、operation_id 和 expires_at；不保证全文/向量已消费到 receipt.seq |
| rejected | 本项明确业务拒绝，error 中保留具体原因；后续项可以继续执行 |
| outcome_unknown | 本次发布结果不明，保存原请求，恢复后查账或原身份重试 |
| not_attempted | 本次未尝试权威发布；不否定同身份早先尝试可能已提交 |

中断可返回 429/503，并保留已经确定的逐项回执。先保存并检查响应 body，不以 HTTP 状态整体重建请求或重新分配 Operation ID。HTTP 取消不取消已接管的持久任务。

复用下节预先保存的 `requests/write.json`，生成一次批请求文件：

```sh
python3 - <<'PYCODE'
import json
from pathlib import Path
write = json.loads(Path('requests/write.json').read_text())
with Path('requests/ingest.json').open('x') as output:
    json.dump({'records': [write]}, output)
PYCODE
curl -X POST "$BASE/api/v1/index/demo/ingest" -H "apikey: $API_KEY" -H "X-AnySeek-Tenant: $TENANT" -H 'Content-Type: application/json' --data-binary @requests/ingest.json
curl "$BASE/api/v1/index/demo/ingest" -H "apikey: $API_KEY" -H "X-AnySeek-Tenant: $TENANT"
```

示例复用同一原始操作，已通过同步接口写入过的请求返回原回执，不创建第二条记录；未知结果直接重发保存的文件。

`GET /api/v1/index/{id}/ingest` 返回 committed_seq、lexical_seq、vector_seq、index_error 和 recovery_required。需要搜索包含某个提交时，检查所需消费者水位至少达到该 receipt.seq，并处理 index_error/recovery_required；各索引水位可以不同，不要求全局统一。恢复必需时 committed_seq 为 null。

同步写入和异步导入统一通过 `GET /api/v1/index/{id}/operation/{epoch}/{nonce}` 查询 Record 业务回执。缺席或过期不能证明先前未提交。实现对应 `commit_writes` 和 `committed_progress`，见[服务内部模块](../../../docs/development.md#服务内部模块)。

## 可直接执行的 HTTP 示例

由受信任的服务调用方提供 `API_KEY` 服务凭据，并设置 `TENANT` 命名空间；浏览器经网关调用时不持有服务凭据。在新目录准备请求文件一次；超时后重发文件，不要重新运行身份生成步骤。

```sh
BASE='http://127.0.0.1:8080'
TENANT='my_workspace'
API_KEY='paste-service-key'
mkdir requests
python3 - <<'PY'
import json, time, uuid
from pathlib import Path
op = lambda: {'epoch':int(time.time()//86400), 'nonce':uuid.uuid4().hex}
requests = {
    'create': {'operation_id':op(), 'collection_id':'demo', 'config':{
        'index_name':'Demo', 'schema':[{'field':'body','field_type':'Text'}]}},
    'write': {'writes':[{'operation_id':op(), 'record_id':'welcome',
              'expected':'Absent', 'document':{'body':'hello search'}}]}
}
for name, request in requests.items():
    with (Path('requests')/(name+'.json')).open('x') as output:
        json.dump(request, output)
PY
curl -X POST "$BASE/api/v1/index" -H "apikey: $API_KEY" -H "X-AnySeek-Tenant: $TENANT" -H 'Content-Type: application/json' --data-binary @requests/create.json
curl -X POST "$BASE/api/v1/index/demo/batch" -H "apikey: $API_KEY" -H "X-AnySeek-Tenant: $TENANT" -H 'Content-Type: application/json' --data-binary @requests/write.json
curl -X POST "$BASE/api/v1/index/demo/query" -H "apikey: $API_KEY" -H "X-AnySeek-Tenant: $TENANT" -H 'Content-Type: application/json' -d '{"query":"hello"}'
curl "$BASE/api/v1/index/demo/doc/welcome" -H "apikey: $API_KEY" -H "X-AnySeek-Tenant: $TENANT"
```

写入返回 `receipt`、`operation_id`、`expires_at`。查询返回 `hits[].record`（包含 record_id、version、document）、score、visible_seq、statistics 和 count_total；按 ID 读取返回 Record envelope。更多条件更新、删除、授权、向量、备份示例见 [test_api.rest](test_api.rest)。

## 配置与查询诊断

Collection info 返回 `analysis`：版本化 format、完整配置 fingerprint、tokenizer 及实现/
字典身份、stop_words 模式及摘要。fingerprint 是原生包校验使用的 index/schema/tokenizer
身份 tuple 的 SHA-256；自定义词表内容不返回。展示它以便调用方核对并固定配置，
不代表已验证检索质量。现有 version、limits、embedding_space_id 同时保留。

所有 HTTP 响应携带服务端生成的 `x-anyseek-request-id`，不复用客户端传入值；
服务错误另有安全的 `x-anyseek-error-code`。查询成功增加格式 v2 的 `diagnostics`，包含同一个 ID、
引擎版本、browse/lexical/vector/hybrid 模式、elapsed_us、分析摘要、embedding_space_sha256
和 index_progress。空间摘要是当前集合 embedding_space_id UTF-8 字节的 SHA256，未配置为 null；
调用方可逐次核对，原始空间标识不进入诊断。分析摘要只描述 index/schema/tokenizer，
不能替代空间身份核对，也不证明向量实际由该模型生成。
进度复用 committed/lexical/vector 序号及恢复状态，index_failed 只表示有错误，不复制
原始错误诊断。它在查询后另行采样，sampling 明确为 after_query_not_atomic_with_result；
不能作为结果的原子快照、跨系统事务或已交付证明。既有 score/channels 保持原语义。

服务端耗时覆盖查询准备、执行及进度采样，排除外层准入和完整响应交付；调用方往返
耗时另测。调试日志 response_prepared 可用该 ID 关联状态/耗时，body_delivery 始终
标为 unconfirmed。诊断不附带查询、向量、过滤值、正文或凭据。错误/取消路径可能没有
查询诊断正文，调用方应保留缺口，不能补造进度。

## 查询与遍历

文本选择 Lexical，单向量选择 Vector，文本加向量选择 Hybrid；`rrf` 提供加权融合和独立候选预算。空对象 `{}` 浏览活动记录，包括纯向量 collection；`length:0` 可仅统计词法/浏览结果。`count:false` 返回 `count_total:null`。默认返回 10 条，支持 offset、stored 字段投影、过滤和排序。查询字段投影只输出 schema 中 `store:true` 的字段；按 ID 读取和 iterator 可读取完整权威原始数据，并可显式投影。

Vector/Hybrid 默认每 level 探测最多 `default_nprobe` 个簇，`nprobe` 可覆盖，`all:true` 扫描全部簇。`all:true` 与 `nprobe` 不可同时指定；这些参数必须伴随 query_vector。ANN/Hybrid 的 count_total 不是全库精确并集，候选窗口内按字段重排也不保证全库字段 Top-K。全簇扫描仍使用配置的存储精度/量化。默认值不是性能或精确召回承诺。

POST `iterator` 接收 `{cursor?,length?,fields?}`，按稳定 ID 正向分页；写入改变 visible_seq 后继续旧 cursor 返回 409。旧 skip/take 双向 HTTP 协议不再支持，低层 Rust iterator 另有自己的 API。

## 租户命名空间、删除与批量

租户仅作为命名空间，由可信上游通过 `X-AnySeek-Tenant` 传入；不需要预注册。所有非公开接口均校验同一服务凭据，数据请求还检查租户头存在、格式有效且不重复。同一命名空间的集合访问范围由上游权限策略决定；AnySeek 不提供 User、Project、Collection Grant、角色或共享授权。

已接收工作保留准入时的租户上下文和资源所有权，不重新检查上游用户权限；数据失败 fence 仍在响应和 body frame 交付时检查。

可信服务调用方在指定租户头后可 DELETE `/api/v1/index/{id}`（`{operation_id}`）删除该命名空间的集合。删除后可同名重建；新集合获得内部新代次，客户端不用传代次。旧后台工作、回执、备份身份和 GC 不会作用于新代次。删除和重建前，客户端须停止旧请求及其重试；仅携带名称的旧请求无法与新集合请求区分。物理回收仍延后执行。

`delete-plan` 接收 `{select:"all"}`、`{select:"ids",record_ids:[...]}` 或 `{select:"query",query:{query:"text"}}`。保存返回的 targets，为每项分配并保存 operation_id，再用 `/batch` 的 `{writes:[...]}` 提交。每批 1..100 个独立操作，返回逐项结果，**不是原子事务**。未知/待恢复错误停止未尝试尾部；重试不能重新生成删除选择条件。

## 备份与恢复


Collection backup 使用 POST `/api/v1/index/{id}/backup` 和 `{operation_id}`；原 ID 重试保留同一快照。携带相同租户头 POST `/api/v1/restore`，body 为 `{operation_id,collection_id,backup_id}`，恢复到新身份，归属于该租户命名空间；其他 Tenant 的备份 ID 不可访问。可信服务可 GET `/api/v1/backups`，或 DELETE `/api/v1/backups/{id}`（`{operation_id}`）持久退役备份；未完成 restore 会 pin 来源。`cleanup_pending:true` 表示逻辑退役已完成，物理清理还需重试。

master POST `/api/v1/service-backups`（`{operation_id}`）冻结 catalog、所有保留 collection 数据和未退役 collection backups。manifest 校验权威对象内容，外层发布有完成见证，不递归包含旧 service archives。

可信服务 POST `/api/v1/service-restores`（`{operation_id,backup_id}`）返回**新的、未启动的 target_root**，不覆盖运行根。使用目标进程配置的 `MASTER_KEY_SECRET` 单独启动；命名空间和备份归属保留，历史管理请求显式冲突；Record 数据版本和回执保留。来源备份及恢复 ID 必须使用返回的小写 hex。

## 当前资源和验收边界

集合使用统一的资源预算。集合统计返回已保留的身份和回执数，`limits` 返回日志字节和查询候选限制。

| 边界 | 当前行为 |
| --- | --- |
| Record 身份含墓碑、保留操作 | 由内存、日志和检查点字节等资源预算约束。回执默认最早在两天后进入回收资格，另须超出当前/前一 UTC 日重试窗口并满足认证历史切点及保护条件；维护可延迟，不保证两天内完成清理 |
| 查询候选 | 10,000 |
| 操作 / HTTP body / 响应 | 1 MiB / 2 MiB / 16 MiB |
| Record WAL / 完整 checkpoint | 256 MiB 代预算。WAL 和检查点的单个帧最多 16 MiB，完整检查点可分帧保存 |
| Catalog | 16 MiB 元数据、10,000 操作，仅安全回收过期控制回执 |
| admission | 普通 32/每集合 16、恢复 2/1、后台 2/1、可信控制 8/2、公共/存活 8/8；partition 内部 16；30 秒请求期限 |
| 调度 | 不同 collection 可并行；共同快照保留全局屏障，已接管工作保留命名空间，资源不足立即 Busy |
| collection backups | 32 个，可持久退役 |
| service archives / 新恢复根 | 每源根最多 4 个 archive/准备项，托管父目录最多 4 个生成根，备份权威 payload 预检 1 GiB |

资源通道在读取大 body 前按路由和解码后的已登记 collection 身份选择；未知身份共享服务额度。所有通道先校验服务凭据，数据路由另检查租户头；通道分类不代替上游权限。管理回执查询有独立控制通道，不等待长备份/恢复工作流锁；删除保留生命周期与恢复工作流的原有顺序。恢复/创建、PATCH checkpoint/collection backup/service backup 分别归恢复和后台通道。

| 类别 | 全服务 / 每集合预留字节 | 全服务 / 每集合 I/O 并发 |
| --- | --- | --- |
| FOREGROUND | 2048 / 512 MiB | 8 / 2 |
| RECOVERY | 512 / 256 MiB | 2 / 1 |
| BACKGROUND | 4096 / 2048 MiB | 2 / 1 |
| CONTROL | 144 / 36 MiB | 2 / 1 |
| PUBLIC | 144 / 144 MiB | 1 / 1 |

部署可设置 `ANYSEEK_{类别}_SLOTS`、`_PER_COLLECTION`、`_BYTES`、`_PER_COLLECTION_BYTES`、`_IO`、`_PER_COLLECTION_IO`，类别取上表大写名称，字节值为十进制字节数。例如 `ANYSEEK_FOREGROUND_PER_COLLECTION=2`。配置必须为正整数、局部不大于全局、每集合字节至少 18 MiB；单类 slots/I/O 不超过 64，前三类 slots 总和不超过 48、CONTROL slots 不超过 8，以保留 catalog 控制余量。非法配置阻止启动。

30 秒截止时间覆盖认证/身份查找及 handler；准入超时且未启动 handler 时返回 Busy，已接管工作超时仍 outcome_unknown。按 ID 的 GET 和查询在原请求期限内等待发布，直到交付响应。发布失败时以 `recovery_required` 拒绝数据交付。每个 HTTP 请求保守预留 18 MiB body/输出工作区；构建快照/tail、对象下载和 PUT 副本另计。S3 缓冲及累积解码 tail 额度跟随实际所有者（包括迭代器和已取出的条目），owned task 和已交付传输帧保留原请求准入，取消等待不提前退款。JSON 在增长前检查响应上限；I/O 无额外等待队列。已知未发布的资源拒绝可返回 busy，发布结果未知时继续 outcome_unknown/恢复协议，重试原操作身份。此账目不覆盖所有运行态对象、分配器、mmap/OS 缓存或 SDK 内存，不承诺全进程 RSS、系统 I/O 带宽或完整磁盘硬上限。每 authority 对象缓存为 256 MiB / 4096 块；Catalog 始终使用 `metadata.sqlite`；S3 各 collection 共用同库的协调表及其旁的对象缓存。物化索引、备份及多 authority 库场景的总占用仍需整体评估。

Service archive/root 清理是明确的离线操作，不提供删除运行根的接口。系统崩溃、文件系统崩溃、断电矩阵尚未验收；原生进程 kill/exit、故障注入与 fsync 调用不能代替。历史回执安全 GC 和已删除集合代次后台回收已有实现；持续运行容量与组合隔离验收、跨主机协调和分布式仍留在后续阶段；可选向量已实现。

## 开发验证

构建、单测、真实 HTTP/故障进程、S3 验收与 CI 完整矩阵统一见[开发指南](../../../docs/development.md)；浏览器测试按[控制台指南](../../../console/README.md#验证)运行。正常构建默认关闭故障注入。

小型调用方示例使用 `scripts/anyseek_client.py`：设置 `ANYSEEK_API_KEY` 后运行 `python3 scripts/index_write_query_smoke.py --port 8080 --requests requests.json`；重复执行同一命令重试保存的请求，不生成新身份。

`generate_openapi=true` 在可执行文件目录输出 `openapi.json` 和 `openapi.yml`。生成结果包含当前路由与请求 schema，公共请求设置在 `api_types.rs`，默认服务实现为 `record_service.rs`。

## 普通来源元数据

`document_id`、`chunk_index`、章节、`source_id`、`source_version` 等信息可作为普通 `document` 字段保存；按 schema 配置过滤或检索。引擎不解析其业务含义，不自动维护依赖、stale 或级联隐藏。

顶层 `system` 写入、`/evidence`、`/entities`、`/neighbors` 和查询的 `relation_fusion` 已撤下。历史字节/页码摘录、来源版本和派生重算由应用负责。全文/向量的通用 `rrf` 混合排序继续保留。

## 可选向量与后补

正文可先写入，不需零向量占位。沿用写入端点提交 `operation_id,record_id,expected:{"Match":当前正文版本},vector_patch:{"vector":[2,0]}`，省略 document；仅填充尚未存在的 schema 向量字段。正文版本保持不变，提交序号推进；重复请求必须保留 operation_id。已删除记录不可补写，正文版本改变后旧请求冲突。

状态接口返回 `pending_vector_fields`。真实 Web UI 的 envelope 模式支持该请求；可选向量契约见[Record 语义](../../../docs/record-model.md)。


## 时间条件与 TTL

系统时间元数据使用 UTC Unix **毫秒**，与用户 schema 中既有 Timestamp 秒字段分开。完整 Record 写入可携带：

```json
{"temporal":{"event_time_ms":1735689600000,"valid_from_ms":1735689600000,"valid_to_ms":1767225600000,"expire_at_ms":1893456000000}}
```

业务有效期是半开区间 [valid_from_ms,valid_to_ms)，不隐式产生 TTL。到达 expire_at_ms 后，按 ID、分页及检索排除正文；读请求不持久推进过期或历史裁剪。后台有界维护另行写入过期墓碑并回收索引，没有固定完成时限。已到期身份允许完整正文重用，版本递增；Absent 匹配无当前有效正文，Match 仍比较技术版本，旧向量补写不能匹配新正文。省略 temporal 时仅继承尚有效正文的元数据，重用已到期身份默认清除旧 TTL。

本进程观察时间不回退。重启仅保留已持久化的写入/维护水位；尚未回收的到期正文可能因墙钟回退跨过 deadline 而重新可见。大幅时间偏差返回 clock_uncertain。正文还可存在于 WAL 和备份中，TTL 不承诺隐私删除。

检索在捕获时刻过滤 TTL，并在评分后按最新时间重验候选，期间到期可能短页；count_total 是捕获索引的观测数，不是最终时刻精确总数。后台回收前的过期索引仍可能影响 BM25 语料统计。分页跨越最早 TTL 会使 continuation 冲突，需要重启遍历。

`/doc/{record_id}/status` 保留当前技术版本、visible/deleted/expired 状态和 pending_vector_fields。完整边界见[Record 契约](../../../docs/record-model.md)，验证状态见 [路线与验收证据](../../../docs/roadmap.md)。

### 显式事件时间窗口

服务管理员用 `POST /api/v1/event-time-checkpoints` 为本地集合构建投影。使用服务密钥，省略 `X-AnySeek-Tenant`，在 JSON 正文指定 `tenant_id` 和 `collection_id`。网关须限制此操作为管理员。构建受现有后台和分区准入约束，成功返回 `indexed_seq`。本地 SDK 的 `build_event_time_projection(tenant, collection)` 由嵌入宿主授权。此构建启用当前认证基座，增量 checkpoint 可沿用该基座，普通完整 checkpoint（包括常规 PATCH）会选用一个不含投影的新基座。重开后可通过同一管理入口再次构建。远端和归档集合不支持此选项。

构建接口不接受 `operation_id`。等待超时发生在后台 job 准入前时返回 429 `busy`，本次请求不会启动构建。已接收后等待超时返回 503 `outcome_unknown`，原构建仍可能继续。SDK 用相同的 `LocalError::code()` 区分。遇到这两种结果，先用事件时间查询检查当前投影；查询成功可能读到旧投影，无法确认本次刷新已经完成。确认仍需要构建时，再显式调用管理入口。这是一次新构建尝试。

租户通过 `POST /api/v1/index/{id}/event-time/query` 读取窗口，或使用 SDK `query_event_time`。示例见 [HTTP 请求](test_api.rest)。`temporal.event` 必须提供非空半开区间的两个 UTC 毫秒端点，可同时使用 `temporal.ingested`、`temporal.valid_at_ms` 和普通 `filter`。结果返回 `visible_seq`、`evaluation_time_ms` 和 `hits`，按事件时间降序、同时间按记录 ID 升序。`length` 为 1 至 128，默认 10。`fields` 只选择存储字段。携带总数、分页、文本、向量、混合或自定义排序参数返回 400。

查询固定同一个已发布规范切点并应用 TTL。投影缺失返回 400 `event_time_unavailable`。尾段超过 128 个变更身份或 2 MiB、基座之后超过 4096 个序号，或候选扫描或正文读取额度用尽时，返回 400 `event_time_budget`，不交付部分窗口。投影缺失或尾段超额时，可用同一管理入口刷新到新认证基座。单次窗口扫描超额时，缩小查询范围。基础扫描最多访问 4096 个候选，且受集合候选上限约束。读取的基座正文累计最多 16 MiB。选根或 TTL 在交付前变化可返回 429 `busy`，使用原请求期限。已认证的投影文件缺损时返回 503 `storage_error`，该集合打开失败。SDK 的 `LocalError::code()` 提供相同错误码。普通 `/query` 继续接受既有时间谓词。

### 时间、全文与精确向量混排

本地无归档集合在已有认证时间投影时，可在 `POST /api/v1/index/{id}/query` 显式传 `recall.time`。半开 UTC 毫秒窗口只限制时间通道。`filter` 和 `temporal` 限制每个通道，`fields` 只选择允许返回的存储字段。传 `query_vector` 和 `all:true` 可使用原有时间＋精确向量模式，SDK 对应 `query_time_vector`。传文本 `query` 可走时间＋全文，SDK 对应 `query_time_text`。同时传文本、`query_vector` 和 `all:true` 则启用三路。示例见 [HTTP 请求](test_api.rest)。

全文输入上限为 1024 字节，必须得到恰好一个分析后的正向词，包含同词跨字段并集。标点或纯排除等零正向词输入，以及需要多词布尔、排除或短语位置的计划都返回 400。有效词无匹配时，全文通道为空。普通全文接口仍支持这些语法。混排不接受 ANN、关系、自定义排序或总数。`visible_seq` 是经过核对的共同切点，全文要求全文和元数据双水位。混排的 `count_total` 为 null。

各路先保留至多 `offset + length` 条，合计受集合候选上限约束。分页在融合后执行，两路的 `offset + length` 最大 64，三路最大 42。结果 `channels` 报告零基 rank、全文或向量原始分数以及 RRF 贡献。同分按记录 ID 升序。默认等权，带全文的混排可传 `rrf.constant`、`time_weight`、`keyword_weight`、`vector_weight`。权重为有限非负数，至少一个参与权重为正。按参与通道计算的最大融合分必须为有限 f32。零权重通道仍在同一切点验证并计费，其候选不贡献分数或排名证据，不计入交付的过期下界。交付下界包含所有正权重通道候选，也包含当前页以外的候选。

各路共享 65,536 个逻辑工作单位、2,048 次范围读和 16 MiB 范围读字节。范围读计入真实请求的页、缓冲块和规范记录正文。精确向量访问所选段中的向量，全文按段预扣 postings 和评分游标。全文和精确向量都计入 facet 字典扩展和匹配工作，时间侧 StringSet 预扣比较工作。筛掉的工作也计费。RRF 对最多 128 个通道候选的合并和排序计费，合并至多 8,640 个单位，最多 128 个合并条目进入排序，排序至多 8,128 次比较。请求超限返回 400，切点未齐、交付前过期或超时返回 429 `busy`。混排整体成功或失败。所有交付沿用点读观察到的进程内时间高水位，墙钟回拨不会恢复已观察到期的候选。已发响应头时，过期或超时会终止数据帧。只读超时可直接重试查询，无需操作 ID。

### 回执保留边界

Rust RecordConfig.history.retention_ms 默认及最低为 172800000（2×24 小时），Rust 配置可显式保存更长期限；已有显式七日值不被改写。HTTP 创建集合使用引擎默认值，本轮没有新增 HTTP 保留期参数。Rust 配置反序列化省略 retention_ms 时采用当前默认值。API 的幂等/查账支持窗口仍是当前及前一 UTC 日。回收同时要求认证 history floor 覆盖并且 operation epoch 已超窗口；不按墙钟到期直接删除 WAL。

近期幂等仍保留完整语义请求用于冲突判断；已认证 checkpoint 覆盖的旧操作可由现有 GC 摘要替代。SQLite 内部 publication receipts 还用于远端 WAL 计费、索引切点、恢复和快照，不是重复正文，本轮保留；不能仅按两日窗口删这些行。WAL/检查点/备份的安全回收边界独立于 API 查账窗口。
