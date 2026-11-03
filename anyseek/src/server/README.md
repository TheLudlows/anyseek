# anyseek Record HTTP 服务

`POST /api/v1/index/{id}/ingest` 在业务判定和持久提交后返回非原子的逐项结果。HTTP 200 不代表全部成功，只有 `code=committed` 且带 Record `receipt` 的项成功；明确拒绝后继续处理后续项。429／503 保留已有回执，并区分 `outcome_unknown` 与 `not_attempted`。索引异步，可检查 `committed_seq`、`lexical_seq`、`vector_seq` 和 `index_error`；`/operation/{epoch}/{nonce}` 查询 Record 回执。现有 `/batch` 同步语义保留，独立 Intake 日志已移除。详见下方[异步索引写入](#异步索引写入)。

默认服务通过 `record_service.rs` 使用 SQLite Catalog 和 Record partition，提供全文、向量和混合检索 API；Web 控制台位于独立的 `console/`，不嵌入服务。提供稳定记录、可选向量、时间过滤、全文/向量混合及逐命中解释；[职责收敛状态](../../../docs/engine-scope.md)单独记录尚未完成的内核整理。A/P3 本地 Segment、compaction 与安全回收已合入；系统崩溃/断电验收仍未完成。[当前状态](../../../STATUS.md) 与 [Record 契约](../../../docs/record-model.md) 区分实现和验收。

## 构建与启动

```sh
cargo build
export MASTER_KEY_SECRET='replace-with-your-private-secret'
target/debug/anyseek_server local_ip=127.0.0.1 local_port=8080
```

启动日志中的 `Master API key` 只用于管理 Tenant 和普通 API Key，以及现有服务备份管理；不能用于集合数据接口。先 POST `/api/v1/tenants` 提交 `{operation_id,tenant_id}`，再由 master POST `/api/v1/apikey` 提交 `{operation_id,tenant_id,key_id,apikey_base64}`，签发普通 Key。按[控制台指南](../../../console/README.md)启动独立前端后使用该 Tenant Key 连接。API 默认端口为 8080，根路径不提供页面。Tenant ID 与 Collection ID 均须匹配 `^[a-z][a-z0-9_-]{0,62}$`，不转换大小写，Tenant 不支持改名或删除。首次 `index_path` 必须不存在且父目录存在；重启必须指向可恢复的 catalog 根，并保持 `MASTER_KEY_SECRET` 一致。旧按 key 组织的目录、已有空目录和不完整权威状态不会被当成空数据库导入。没有旧格式升级或迁移接口。

| 参数 | 默认值与约束 |
| --- | --- |
| `index_path` | `anyseek_index`；相对路径以可执行文件目录为基准 |
| `local_ip` | `0.0.0.0` |
| `local_port` | `8080` |
| `default_nprobe` | `200`；正整数，请求可覆盖 |
| `generate_openapi` | `false`；`true` 输出实际路由的 JSON/YAML 后退出 |

只接受 `name=value`；未知、重复、非法参数返回非零状态。服务使用 `log`，通过 `RUST_LOG` 配置日志。Ctrl-C/SIGINT/SIGTERM 停止接收请求并等待连接结束；成功写入已持久，不依靠退出时提交派生索引才成立。默认 secret 不适合部署，必须配置自己的值。

## 启动 Web UI

保持 API 服务运行，在另一个终端从仓库根启动独立控制台，需要 Node.js 24 和 npm：

```sh
cd console
npm ci
npm run dev
```

打开 `http://127.0.0.1:5173`，用上面签发的 Tenant API Key 连接。UI 默认代理到 `http://127.0.0.1:8080`；API 地址配置、远程访问及生产部署统一见 [UI 启动与部署](../../../console/README.md)。

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

Catalog 与 S3 协调元数据共用服务根目录下的 `metadata.sqlite`；保留该数据库、SQLite WAL、owner 锁文件。不同独立服务必须使用不同 bucket；完整服务恢复启动前应配置目标服务的独立 bucket。对象位于 `anyseek/{tenant_id}/{collection_id}/`，不创建自动 namespace 或 Bucket 归属标记。这些是持久协调状态，不能作为缓存删除或在运行中复制 SQLite 文件作备份。collection 内的本地日志/payload/索引可从权威检查点及 tail 重建。C2 另在每个权威库旁的 `*.object-cache` 保存校验对象块（256 MiB / 4096 块，损坏或丢失回源）；旁边的 `*.object-cache.lock` 仅用于本机缓存操作协调，运行中清空缓存目录时保留该锁文件。缓存不包含凭据，也不决定当前 head 或权限；备份应使用服务 API，以同时固定元数据和对象闭包。

PATCH collection 发布完整远端 checkpoint 后再回收已覆盖且无 pin 的对象。collection backup 的相同请求在中断后继续原 cut；恢复到新 collection，复制到目标集合新代次，不包含服务凭据，受保护证据保持隔离。完整服务备份沿用本地独立归档格式，包含 catalog 和完整数据；在 S3 模式恢复启动时会发布到目标独立 bucket。

该后端已合入；真实目标服务的条件写/读后可见性认证与其余维护路径审计仍待完成，见[存储协议](../../../docs/storage.md)。SQLite 首版是单主机协调，不提供跨主机 HA。

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
curl -X POST "$BASE/api/v1/index/demo/ingest" -H "apikey: $API_KEY" -H 'Content-Type: application/json' --data-binary @requests/ingest.json
curl "$BASE/api/v1/index/demo/ingest" -H "apikey: $API_KEY"
```

示例复用同一原始操作，已通过同步接口写入过的请求返回原回执，不创建第二条记录；未知结果直接重发保存的文件。

`GET /api/v1/index/{id}/ingest` 返回 committed_seq、lexical_seq、vector_seq、index_error 和 recovery_required。需要搜索包含某个提交时，检查所需消费者水位至少达到该 receipt.seq，并处理 index_error/recovery_required；各索引水位可以不同，不要求全局统一。恢复必需时 committed_seq 为 null。

同步写入和异步导入统一通过 `GET /api/v1/index/{id}/operation/{epoch}/{nonce}` 查询 Record 业务回执。缺席或过期不能证明先前未提交。实现对应 `commit_writes` 和 `committed_progress`，见[服务内部模块](../../../docs/development.md#服务内部模块)。

## 可直接执行的 HTTP 示例

将日志中的 key 填入 `API_KEY`。在新目录准备请求文件一次；超时后重发文件，不要重新运行身份生成步骤。

```sh
BASE='http://127.0.0.1:8080'
API_KEY='paste-tenant-key'
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
curl -X POST "$BASE/api/v1/index" -H "apikey: $API_KEY" -H 'Content-Type: application/json' --data-binary @requests/create.json
curl -X POST "$BASE/api/v1/index/demo/batch" -H "apikey: $API_KEY" -H 'Content-Type: application/json' --data-binary @requests/write.json
curl -X POST "$BASE/api/v1/index/demo/query" -H "apikey: $API_KEY" -H 'Content-Type: application/json' -d '{"query":"hello"}'
curl "$BASE/api/v1/index/demo/doc/welcome" -H "apikey: $API_KEY"
```

写入返回 `receipt`、`operation_id`、`expires_at`。查询返回 `hits[].record`（包含 record_id、version、document）、score、visible_seq、statistics 和 count_total；按 ID 读取返回 Record envelope。更多条件更新、删除、授权、向量、备份示例见 [test_api.rest](test_api.rest)。

## 查询与遍历

文本选择 Lexical，单向量选择 Vector，文本加向量选择 Hybrid；`rrf` 提供加权融合和独立候选预算。空对象 `{}` 浏览活动记录，包括纯向量 collection；`length:0` 可仅统计词法/浏览结果。`count:false` 返回 `count_total:null`。默认返回 10 条，支持 offset、stored 字段投影、过滤和排序。查询字段投影只输出 schema 中 `store:true` 的字段；按 ID 读取和 iterator 可读取完整权威原始数据，并可显式投影。

Vector/Hybrid 默认每 level 探测最多 `default_nprobe` 个簇，`nprobe` 可覆盖，`all:true` 扫描全部簇。`all:true` 与 `nprobe` 不可同时指定；这些参数必须伴随 query_vector。ANN/Hybrid 的 count_total 不是全库精确并集，候选窗口内按字段重排也不保证全库字段 Top-K。全簇扫描仍使用配置的存储精度/量化。默认值不是性能或精确召回承诺。

POST `iterator` 接收 `{cursor?,length?,fields?}`，按稳定 ID 正向分页；写入改变 visible_seq 后继续旧 cursor 返回 409。旧 skip/take 双向 HTTP 协议不再支持，低层 Rust iterator 另有自己的 API。

## Tenant 认证、删除与批量

Tenant 由 master 创建和查询（POST/GET `/api/v1/tenants`），普通 Key 只能由 master 签发或撤销。普通 Key 认证成功后由服务端确定 Tenant，不接受客户端声明的 Tenant 作为数据访问身份。同 Tenant 的有效 Key 对其所有集合拥有相同访问范围，不提供 User、Project、Collection Grant、角色或共享授权。

DELETE `/api/v1/apikey` 使用 `{operation_id,key_id}` 撤销凭据；不会删除 Tenant 或集合，也不影响本租户其他有效 Key。撤销后新请求认证失败，已经通过认证开始执行的操作和响应允许完成；不再进行凭据撤销的 body frame 复核。来源删除等数据有效性保护仍在输出时检查。

普通 Tenant Key 可 DELETE `/api/v1/index/{id}`（`{operation_id}`）删除自己的集合。删除后可同名重建；新集合获得内部新代次，客户端不用传代次。旧后台工作、回执、备份身份和 GC 不会作用于新代次。删除和重建前，客户端须停止旧请求及其重试；仅携带名称的旧请求无法与新集合请求区分。物理回收仍延后执行。

`delete-plan` 接收 `{select:"all"}`、`{select:"ids",record_ids:[...]}` 或 `{select:"query",query:{query:"text"}}`。保存返回的 targets，为每项分配并保存 operation_id，再用 `/batch` 的 `{writes:[...]}` 提交。每批 1..100 个独立操作，返回逐项结果，**不是原子事务**。未知/待恢复错误停止未尝试尾部；重试不能重新生成删除选择条件。

## 备份与恢复


Collection backup 使用 POST `/api/v1/index/{id}/backup` 和 `{operation_id}`；原 ID 重试保留同一快照。本 Tenant 的普通 Key POST `/api/v1/restore`，body 为 `{operation_id,collection_id,backup_id}`，恢复到新身份，归属于当前认证 Tenant；其他 Tenant 的备份 ID 不可访问。master 可 GET `/api/v1/backups`，或 DELETE `/api/v1/backups/{id}`（`{operation_id}`）持久退役备份；未完成 restore 会 pin 来源。`cleanup_pending:true` 表示逻辑退役已完成，物理清理还需重试。

master POST `/api/v1/service-backups`（`{operation_id}`）冻结 catalog、所有保留 collection 数据和未退役 collection backups。manifest 校验权威对象内容，外层发布有完成见证，不递归包含旧 service archives。

master POST `/api/v1/service-restores`（`{operation_id,backup_id}`）返回**新的、未启动的 target_root**，不覆盖运行根。用当前 MASTER_KEY_SECRET 单独启动新根；历史普通 Key 全部禁用；使用目标 master 为已有 Tenant 重新签发 Key 后才能访问数据。历史控制请求显式冲突；Record 数据版本和回执保留。不需要重建 Collection 授权关系。来源备份及恢复 ID 必须使用返回的小写 hex。

## 当前资源和验收边界

| 边界 | 当前行为 |
| --- | --- |
| Record 身份含墓碑、保留操作、候选 | 默认身份/操作各 10,000，可显式选有界 100,000 档；候选仍 10,000。操作历史至少保留七天，满足历史水位、epoch 和保护条件后才安全回收 |
| 操作 / HTTP body / 响应 | 1 MiB / 2 MiB / 16 MiB |
| Record WAL / 完整 checkpoint | 256 MiB 代预算，完整 checkpoint 另受 16 MiB 单帧限制 |
| Catalog | 16 MiB 元数据、10,000 操作，仅安全回收过期控制回执 |
| admission | 普通 32/每集合 16、恢复 2/1、后台 2/1、可信控制 8/2、公共/存活 8/8；partition 内部 16；30 秒请求期限 |
| 调度 | 不同 collection 可并行；共同快照保留全局屏障，Key 撤销不排空数据工作，资源不足立即 Busy |
| collection backups | 32 个，可持久退役 |
| service archives / 新恢复根 | 每源根最多 4 个 archive/准备项，托管父目录最多 4 个生成根，备份权威 payload 预检 1 GiB |

资源通道在读取大 body 前按路由和解码后的已登记 collection 身份选择；未知身份共享服务额度。平台管理通道只在 master 认证后开放，不被数据工作借用；Key 管理不等待备份/恢复工作流锁，仍参与共同快照排序，但不排空已认证数据工作；删除保留与多阶段恢复工作流的原有顺序。恢复/创建、PATCH checkpoint/collection backup/service backup 分别归恢复和后台通道。

| 类别 | 全服务 / 每集合预留字节 | 全服务 / 每集合 I/O 并发 |
| --- | --- | --- |
| FOREGROUND | 2048 / 512 MiB | 8 / 2 |
| RECOVERY | 512 / 256 MiB | 2 / 1 |
| BACKGROUND | 4096 / 2048 MiB | 2 / 1 |
| CONTROL | 144 / 36 MiB | 2 / 1 |
| PUBLIC | 144 / 144 MiB | 1 / 1 |

部署可设置 `ANYSEEK_{类别}_SLOTS`、`_PER_COLLECTION`、`_BYTES`、`_PER_COLLECTION_BYTES`、`_IO`、`_PER_COLLECTION_IO`，类别取上表大写名称，字节值为十进制字节数。例如 `ANYSEEK_FOREGROUND_PER_COLLECTION=2`。配置必须为正整数、局部不大于全局、每集合字节至少 18 MiB；单类 slots/I/O 不超过 64，前三类 slots 总和不超过 48、CONTROL slots 不超过 8，以保留 catalog 控制余量。非法配置阻止启动。

30 秒截止时间覆盖认证/身份查找及 handler；准入超时且未启动 handler 时返回 Busy，已接管工作超时仍 outcome_unknown。每个 HTTP 请求保守预留 18 MiB body/输出工作区；构建快照/tail、对象下载和 PUT 副本另计。S3 缓冲及累积解码 tail 额度跟随实际所有者（包括迭代器和已取出的条目），owned task 和已交付传输帧保留原请求准入，取消等待不提前退款。JSON 在增长前检查响应上限；I/O 无额外等待队列。已知未发布的资源拒绝可返回 busy，发布结果未知时继续 outcome_unknown/恢复协议，重试原操作身份。此账目不覆盖所有运行态对象、分配器、mmap/OS 缓存或 SDK 内存，不承诺全进程 RSS、系统 I/O 带宽或完整磁盘硬上限。每 authority 对象缓存为 256 MiB / 4096 块；默认服务各 collection 共用 `metadata.sqlite` 及其旁的对象缓存。物化索引、备份及多 authority 库场景的总占用仍需整体评估。

Service archive/root 清理是明确的离线操作，不提供删除运行根的接口。系统崩溃、文件系统崩溃、断电矩阵尚未验收；原生进程 kill/exit、故障注入与 fsync 调用不能代替。历史回执安全 GC 和已删除集合代次后台回收已有实现；持续运行容量与组合隔离验收、跨主机协调和分布式仍留在后续阶段；可选向量已实现。

## 开发验证

```sh
cargo test --workspace --all-targets
python3 scripts/check_console_boundary.py
python3 scripts/record_service_smoke.py --server target/debug/anyseek_server
# 独立前端浏览器检查：见 console/README.md
```

浏览器测试在 `console/` 使用 Node.js Playwright 和 Chromium，按[控制台指南](../../../console/README.md#验证)运行。故障矩阵需先构建 `cargo build -p anyseek --features fault-injection`，再运行同一 smoke 脚本的 `--faults`。正常构建默认关闭故障注入。

小型调用方示例使用 `scripts/anyseek_client.py`：设置 `ANYSEEK_API_KEY` 后运行 `python3 scripts/index_write_query_smoke.py --port 8080 --requests requests.json`；重复执行同一命令重试保存的请求，不生成新身份。旧 `http_smoke.py` 已由完整 Record smoke 取代。

`generate_openapi=true` 在可执行文件目录输出 `openapi.json` 和 `openapi.yml`。路由清单见 [FEATURES](../../../FEATURES.md)，公共请求设置在 `api_types.rs`，默认服务实现为 `record_service.rs`。

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

`/doc/{record_id}/status` 保留当前技术版本、visible/deleted/expired 状态和 pending_vector_fields。完整边界与当前验证状态见[职责收敛说明](../../../docs/engine-scope.md)。
