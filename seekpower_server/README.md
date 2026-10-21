# Seekpower Record HTTP 服务

默认服务通过 `record_service.rs` 使用独立 catalog 和 Record partition，保留全文、向量、混合检索及 Web UI。这是 #57 的有界 P1-A 与分批 P2 实现；已接入来源/证据、来源修订、generation、可选向量及本地 TTL/时间批次。完整 P2、系统崩溃/断电验收和 P3 持续回收尚未完成。[实施与验证记录](../docs/implementation-57.md) 区分各阶段证据。

## 构建与启动

```sh
cargo build --workspace
export MASTER_KEY_SECRET='replace-with-your-private-secret'
target/debug/seekpower_server local_ip=127.0.0.1 local_port=8080
```

打开 `http://127.0.0.1:8080/`，使用启动日志中的 `Master API key` 连接。首次 `index_path` 必须不存在且父目录存在；重启必须指向可恢复的 catalog 根，并保持 `MASTER_KEY_SECRET` 一致。旧按 key 组织的目录、已有空目录和不完整权威状态不会被当成空数据库导入。没有旧格式升级或迁移接口。

| 参数 | 默认值与约束 |
| --- | --- |
| `index_path` | `seekstorm_index`；相对路径以可执行文件目录为基准 |
| `local_ip` | `0.0.0.0` |
| `local_port` | `8080` |
| `force_shard_number` | 默认可靠服务固定为 1；显式值也必须为 1 |
| `default_nprobe` | `200`；正整数，请求可覆盖 |
| `generate_openapi` | `false`；`true` 输出实际路由的 JSON/YAML 后退出 |

只接受 `name=value`；未知、重复、非法参数返回非零状态。核心使用 `log`，服务支持 `RUST_LOG`。Ctrl-C/SIGINT/SIGTERM 停止接收请求并等待连接结束；成功写入已持久，不依靠退出时提交派生索引才成立。默认 secret 不适合部署，必须配置自己的值。

## 稳定身份、写入和重试

collection ID、record ID 是字符串业务身份，内部 DocID 不作为 HTTP 身份。所有数据请求使用 `apikey` 请求头。

- 创建：`{operation_id,collection_id,config,embedding_space_id?}`。`config` 包含名称/schema/可选向量配置；向量 collection 要求外部 embedding space 身份。
- 写入：`{operation_id,record_id,routing_key,expected,document}`。`document` 必须显式提供；对象完整替换，`null` 删除。已有身份不能更换 routing key。
- `expected` 为 `"Absent"`、`{"Match":N}` 或 `"Any"`。删除后重建继续推进版本，避免旧条件命中新内容。
- 操作 ID 是 `{epoch,nonce}`，epoch 是自 Unix epoch 起的 UTC 日数。当前日和前一日可重试；正常当日签发至少有 24 小时窗口。控制操作保留 `sys:` nonce 前缀。
- 成功写入表示 durable + visible，不需随后 PATCH。PATCH collection 是在线完整 checkpoint，不是确认写入的第二步，也不会回收 Record 操作账本。

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

## 可直接执行的 HTTP 示例

将日志中的 key 填入 `API_KEY`。在新目录准备请求文件一次；超时后重发文件，不要重新运行身份生成步骤。

```sh
BASE='http://127.0.0.1:8080'
API_KEY='paste-master-key'
mkdir requests
python3 - <<'PY'
import json, time, uuid
from pathlib import Path
op = lambda: {'epoch':int(time.time()//86400), 'nonce':uuid.uuid4().hex}
requests = {
    'create': {'operation_id':op(), 'collection_id':'demo', 'config':{
        'index_name':'Demo', 'schema':[{'field':'body','field_type':'Text'}]}},
    'write': {'operation_id':op(), 'record_id':'welcome', 'routing_key':'welcome',
              'expected':'Absent', 'document':{'body':'hello search'}}
}
for name, request in requests.items():
    with (Path('requests')/(name+'.json')).open('x') as output:
        json.dump(request, output)
PY
curl -X POST "$BASE/api/v1/index" -H "apikey: $API_KEY" -H 'Content-Type: application/json' --data-binary @requests/create.json
curl -X POST "$BASE/api/v1/index/demo/doc" -H "apikey: $API_KEY" -H 'Content-Type: application/json' --data-binary @requests/write.json
curl -X POST "$BASE/api/v1/index/demo/query" -H "apikey: $API_KEY" -H 'Content-Type: application/json' -d '{"query":"hello"}'
curl "$BASE/api/v1/index/demo/doc/welcome" -H "apikey: $API_KEY"
```

写入返回 `receipt`、`operation_id`、`expires_at`。查询返回 `hits[].record`（包含 record_id、routing_key、version、document）、score、visible_seq、statistics 和 count_total；按 ID 读取返回 Record envelope。更多条件更新、删除、授权、向量、备份示例见 [test_api.rest](test_api.rest)。

## 查询与遍历

文本选择 Lexical，单向量选择 Vector，文本加向量选择 Hybrid；`rrf` 提供加权融合和独立候选预算。空对象 `{}` 浏览活动记录，包括纯向量 collection；`length:0` 可仅统计词法/浏览结果。`count:false` 返回 `count_total:null`。默认返回 10 条，支持 offset、stored 字段投影、过滤和排序。查询字段投影只输出 schema 中 `store:true` 的字段；按 ID 读取和 iterator 可读取完整权威原始数据，并可显式投影。

Vector/Hybrid 默认每 level 探测最多 `default_nprobe` 个簇，`nprobe` 可覆盖，`all:true` 扫描全部簇。`all:true` 与 `nprobe` 不可同时指定；这些参数必须伴随 query_vector。ANN/Hybrid 的 count_total 不是全库精确并集，候选窗口内按字段重排也不保证全库字段 Top-K。全簇扫描仍使用配置的存储精度/量化。默认值的历史校准见 [记录](../docs/ann-default-calibration.json)，不是本可靠服务的性能承诺。

POST `iterator` 接收 `{cursor?,length?,fields?}`，按稳定 ID 正向分页；写入改变 visible_seq 后继续旧 cursor 返回 409。旧 skip/take 双向 HTTP 协议不再支持，低层 Rust iterator 另有自己的 API。

## 共享授权、删除与批量

master 注册调用方生成的 32 字节 key：POST `/api/v1/apikey`，body 为 `{operation_id,key_id,apikey_base64}`。master 在 `/api/v1/index/{id}/grants` POST `{operation_id,key_id,allow}` 管理授权。普通 key 可创建新 collection，访问已获授权的 collection，但不能管理其他身份或扩大授权。首版不区分读写角色。

DELETE `/api/v1/apikey` 使用 `{operation_id,key_id}` 撤销身份；不会删除 collection，也不影响其他已授权 key。删除 collection 是独立的 master 操作，需 `{operation_id}`；保留身份墓碑，物理回收后置。撤销与写入发布有确定顺序，响应状态/头和 body 交付均复核 grant revision；已交给传输层的字节不能收回。

`delete-plan` 接收 `{select:"all"}`、`{select:"ids",record_ids:[...]}` 或 `{select:"query",query:{query:"text"}}`。保存返回的 targets，为每项分配并保存 operation_id，再用 `/batch` 的 `{writes:[...]}` 提交。每批 1..100 个独立操作，返回逐项结果，**不是原子事务**。未知/待恢复错误停止未尝试尾部；重试不能重新生成删除选择条件。

## 备份与恢复

Collection backup 使用 POST `/api/v1/index/{id}/backup` 和 `{operation_id}`；原 ID 重试保留同一快照。master POST `/api/v1/restore`，body 为 `{operation_id,collection_id,backup_id}`，恢复到新身份，不恢复旧 grants。master 可 GET `/api/v1/backups`，或 DELETE `/api/v1/backups/{id}`（`{operation_id}`）持久退役备份；未完成 restore 会 pin 来源。`cleanup_pending:true` 表示逻辑退役已完成，物理清理还需重试。

master POST `/api/v1/service-backups`（`{operation_id}`）冻结 catalog、所有保留 collection 数据和未退役 collection backups。manifest 校验权威对象内容，外层发布有完成见证，不递归包含旧 service archives。

master POST `/api/v1/service-restores`（`{operation_id,backup_id}`）返回**新的、未启动的 target_root**，不覆盖运行根。用当前 MASTER_KEY_SECRET 单独启动新根；历史 key 禁用、历史 grants 清空，当前 master 获得初始访问。历史控制请求显式冲突；Record 数据版本和回执保留。共享访问需注册新 key 并重新授权。来源备份及恢复 ID 必须使用返回的小写 hex。

## 当前资源和验收边界

| 边界 | 当前行为 |
| --- | --- |
| Record 身份含墓碑、保留操作、候选 | 各 10,000；Record 操作是累计保留上限，checkpoint 不释放额度 |
| 操作 / HTTP body / 响应 | 1 MiB / 2 MiB / 16 MiB |
| Record WAL / 完整 checkpoint | 256 MiB 代预算，完整 checkpoint 另受 16 MiB 单帧限制 |
| catalog | 16 MiB 代、10,000 操作，仅安全回收过期控制回执 |
| admission | 64 请求、16 控制任务、8 partition 操作；30 秒请求期限 |
| 调度 | catalog 屏障覆盖完整授权操作，当前不同 collection 也会相互等待 |
| collection backups | 32 个，可持久退役 |
| service archives / 新恢复根 | 每源根最多 4 个 archive/准备项，托管父目录最多 4 个生成根，备份权威 payload 预检 1 GiB |

Service archive/root 清理是明确的离线操作，不提供删除运行根的接口。系统崩溃、文件系统崩溃、断电矩阵尚未验收；原生进程 kill/exit、故障注入与 fsync 调用不能代替。长期物理回收、Record 账本 GC、来源/evidence、可选向量和分布式能力留在后续阶段。

## 开发验证

```sh
cargo test --workspace --all-targets
python3 scripts/check_core_boundary.py
python3 scripts/record_service_smoke.py --server target/debug/seekpower_server --browser
```

浏览器测试需 Python Playwright 和 Chromium。故障矩阵需先构建 `cargo build -p seekpower_server --features fault-injection`，再运行同一 smoke 脚本的 `--faults`。正常构建默认关闭故障注入。

小型调用方示例使用 `scripts/seekpower_client.py`：设置 `SEEKPOWER_API_KEY` 后运行 `python3 scripts/index_write_query_smoke.py --port 8080 --requests requests.json`；重复执行同一命令重试保存的请求，不生成新身份。旧 `http_smoke.py` 已由完整 Record smoke 取代。

`generate_openapi=true` 在可执行文件目录输出 `openapi.json` 和 `openapi.yml`。路由清单见 [FEATURES](../FEATURES.md)，公共请求设置在 `api_types.rs`，默认服务实现为 `record_service.rs`。

## P2 本地来源/证据切片

来源删除检查点 aa168ca 已通过 Linux/Windows 原生 CI；新增接管恢复的最新验证见实施记录，系统断电尚未验收。完整设计与限制见 [实施记录](../docs/implementation-57-p2.md#12-来源证据删除切片本地实现等待用户检视)。

沿用 `POST /api/v1/index/{collection_id}/doc`，在完整 Record 请求上增加可选 `system`：

```json
{
  "operation_id": {"epoch": 20747, "nonce": "save-source"},
  "record_id": "source-1", "routing_key": "document-1", "expected": "Absent",
  "document": {"body": "Original source text"},
  "system": {"kind": "source"}
}
```

示例 epoch 仅作结构说明，实际按当前 UTC 日生成并随原请求保存。派生记录用自己的稳定 ID 和 `system:{"kind":"derived","evidence_refs":[{"record_id":"source-1","routing_key":"document-1","version":1}]}`。来源与派生均要求满足 collection 既有 schema/向量约束；向量字段可以缺失，缺失内容不参加向量召回。

删除仍提交 `operation_id,record_id,routing_key,expected:{"Match":1},document:null`，省略 system。确认成功后直接和间接派生内容立即从查询、按 ID 读取和统计隐藏；权限撤销与数据输出各自复核。引用只允许当前 collection，未知 collection 字段明确拒绝。Source 更新要求 `expected:{"Match":当前版本}`，可省略 system（继承来源类型）；保留旧修订，新来源可见，依赖旧版的直接/间接派生立即 stale 并默认隐藏。派生重算必须创建新 ID、引用最新版本。derived 更新、删除后重建暂不支持，不能把 business document 中的同名字段当作已实现语义。

Web UI 的显式 Record envelope 选项支持上述内容及删除，自动保存 operation_id 供重试。**独立备份中的 typed 内容恢复后保持 `evidence_quarantined:true`，当前没有解除隔离入口**；原文被保存但不可搜索，避免旧备份复活后来的删除。live WAL/在线 checkpoint 恢复不新增隔离。当前仍存活的原服务可以通过下面的受控接管创建已核验的新目标；这不会清除任意独立恢复目录的隔离。

`GET /api/v1/index/{collection_id}/doc/{record_id}/status` 返回 `record_id/version/visibility/visible_seq`，只返回状态，不提供隐藏正文；同样执行授权与数据输出屏障。状态包括 visible、stale、deleted、unavailable、quarantined。

Record 权威格式使用 V6 entry 标签，拒绝 V5/V4/V3/V2/P1 目录；不提供升级迁移或旧格式兼容。


### 同服务受控接管

本分支使用 `POST /api/v1/takeovers`，请求为 `operation_id`、`source_collection_id`、新的 `collection_id` 和 `backup_id`。只接受当前服务中仍有完整权威历史的源及同谱系备份。此操作会永久关闭原 collection，目标追平最终封存水位，保留全部已确认写入，**不是回退到备份时点**；旧 grants 不复制。返回 `status:"active"`、`recovery:"final_sealed_waterline"` 和最终 certificate。

Web UI 的 “Take over a collection” 输入同样字段，先保存请求，响应丢失/重载后原样重试。相同目标不得换源或备份；中断后服务按持久意图续办，目标认证前不开放。完整服务备份拒绝未完成接管。独立恢复目录（含空或纯普通记录备份）不能冒充当前删除权威；没有强制解除隔离、跨服务自动灾备或双活入口。详见 [接管边界与验证](../docs/implementation-57-p2.md#13-同服务受控接管恢复进行中)。


### 可选向量与后补

正文可先写入，不需零向量占位。沿用写入端点提交 `operation_id,record_id,routing_key,expected:{"Match":当前正文版本},vector_patch:{"vector":[2,0]}`，省略 document/system；仅填充尚未存在的 schema 向量字段。正文版本保持不变，提交序号推进；重复请求必须保留 operation_id。已删除、stale 或隔离 typed 记录不可补写，来源正文版本改变后旧请求冲突。

状态接口返回 `pending_vector_fields`。真实 Web UI 的 envelope 模式支持该请求；可选向量契约见[第 15 节实施记录](../docs/implementation-57-p2.md)。


### 文档 generation

使用同一写入端点，record_id 代表独立文档 control ID，省略 document/system/vector_patch，传入 generation：

- Stage：`{action:"stage",generation_id,source:{record_id,routing_key,version},members:[成员ID],chunks:[{record_id,document,evidence_refs?}]}`。清单必须完整，1..100 成员，同 Source routing group；首次 expected 为 Absent，后续 Match 当前 control version。单次 envelope 仍受 1 MiB 上限。
- Publish/Abort：`{action:"publish"或"abort",generation_id}`，必须 Match control version。成功 staging 回执 `staged_only:true`，表示仅持久、不可搜索；首次 stage version 1，publish 后 version 2。替换前重新验证来源及最终依赖可见性。
- `GET /api/v1/index/{id}/doc/{record_id}/generation` 只返回版本、active/staged 身份和成员数。已发布成员保持精确证据并支持向量后补；换代使旧代及其派生隐藏。Abort 释放未发布成员 ID，generation ID 不可复用。

Web UI envelope 模式可运行上述流程并保存原操作重试。完整示例、预留预算、恢复与验证边界见[第 16 节实施记录](../docs/implementation-57-p2.md)。

## P2 时间条件与不可逆 TTL

本地 TTL 批次使用 UTC Unix **毫秒**，与用户 schema 中既有 `Timestamp` 秒字段分开。完整 Record 写入和 generation 的每个 chunk 可携带：

```json
{"temporal":{"event_time_ms":1735689600000,"valid_from_ms":1735689600000,"valid_to_ms":1767225600000,"expire_at_ms":1893456000000}}
```

`temporal` 是系统元数据，不放入自定义 `document`。业务有效期是半开区间 `[valid_from_ms,valid_to_ms)`，缺失边界无界；它不会自动删除数据。`expire_at_ms` 必须严格晚于可信评估时间。完整替换省略 temporal 时继承旧值；到期前可通过合法的完整条件替换修改/清除 TTL，来源仍要求精确 Match，derived/chunk 仍不允许原地正文重算。向量后补不能改变时间元数据或 `ingested_at_ms`。

到期会持久化新版本墓碑，隐藏直接及间接依赖，更新所有活动检索通道/统计。即便使用 `Any`/`Absent`、延长 TTL 或清空 temporal，也不能复活到期身份；用新身份创建新内容。原 operation_id 重试仍返回原回执，回执不是当前仍可见的保证。`/doc/{record_id}/status` 的 `visibility=expired` 不返回历史正文。源 TTL 可隐藏整文派生；chunk TTL 作用于该身份及其下游，不隐式给同 generation 其他成员添加 TTL。staging 期间到期的 chunk 不可 publish，需要 abort 后重新准备。

查询可附加以下条件，全部在全文/向量/hybrid 候选选择前 AND；不做 TopK 后过滤：

```json
{"query":"example","temporal":{"event":{"from_ms":1735689600000,"to_ms":1767225600000},"ingested":{"from_ms":1735689600000},"valid_at_ms":1750000000000}}
```

所有窗口均为 `[from_ms,to_ms)`；缺失 event 的记录不匹配 event 条件。`valid_at_ms` 只决定业务有效期过滤，不改变 TTL 的当前可信时间，不支持查询已过期历史。空/反向范围拒绝，时间值限制公元 1–9999 年（TTL 非负）。不提供隐式按当前业务有效期隐藏；未传条件时仅按权限、来源、generation 和 TTL 决定可见性。

查询、iterator page、status 和 statistics 返回同 guard 取得的 `evaluation_time_ms`。单次查询不混合时间视图；iterator 若过期提交改变 seq 会拒绝旧 cursor。查询 `count_total` 维持模式原有精确/近似边界，`statistics.active_records` 是该视图的全局活动集合，不是时间过滤后的命中数。内部 `__seekpower_time_` 字段仅存在可重建检索缓存中，该前缀不能用于 schema；原始 document 不被内部字段改写。

到期采用请求驱动的 owned 提交，取消请求不会中断已接管提交。持久化结果不确定返回 `outcome_unknown`，已持久未应用返回 `durable_pending`，之后数据读写失败关闭直到重开恢复；保留这两类错误回执不绕过任何授权输出检查。恢复后重试查询或原写操作，不为重试生成新 operation_id。旧固定时间读视图可完成；来源显式删除和撤权仍即时阻断未交付输出。

时钟依赖可信系统 UTC。进程内 wall/monotonic 差异或对持久水位的回拨超过 5 分钟返回 `clock_uncertain`；小幅回拨钳制为进程内已观察时间或恢复后的已持久水位，不能复活墓碑。停机期间错误向前跳时无法由同一机器自身鉴别。旧备份中的 TTL/来源证据保持隔离，不推进归档权威状态；同服务受控接管复制最终完整状态和过期历史，再为新视图处理到期，不能强制解除历史不明的隔离。

当前 journal 为 V8，拒绝旧格式；无迁移。Web UI 的 Record envelope 编辑支持 temporal，独立时间筛选区显示实际评估时间。TTL 本批的平台和故障验证状态见 [实施记录第 17 节](../docs/implementation-57-p2.md)，不把 PR #63 的 CI 当成本批验收。


## P2 change feed 与快照续传

本地 feed 批次提供 `POST /api/v1/index/{id}/changes`，请求 `{"after":null,"length":100}`。事件只包含已提交身份/版本/操作 ID 和失效原因，不提供历史正文。每 partition 有序、至少一次；使用 `(scope,seq,event_index)` 幂等消费，再保存响应 next。`rescan_required` 表示需重建有效视图，不能只删除事件中一个 ID。

游标过期返回 410 `cursor_expired`，不自动跳到尾部。重建流程：GET 同路径前缀的 `/bootstrap` 取得 request_id_prefix，追加 16 个随机 hex；POST `/bootstrap`，传 `request_id,after:null,length`，使用返回 next 继续同切点分页。完整快照应用成功后保存 continuation，再 DELETE `/bootstrap/{request_id}` 释放 pin，随后从 continuation 消费 changes。重启/释放/到期/隐藏后旧 bootstrap 请求返回 410 `bootstrap_expired`，必须丢弃不完整页重新开始。每 partition 一份五分钟有界 pin，所有接口要求当前 collection 授权。

创建 config 可携带 `change_feed:{retention_ms:604800000,max_events:100000,max_bytes:67108864}`。保留期至少七天；事件与紧急失效预留达到配额时新写入返回 429 `feed_full`，不会提前丢事件。既有 Record/journal 配额也继续有效。逻辑 feed 裁剪不删除原幂等账本，不等于 P3 物理历史回收。

独立恢复或受控接管会换 scope，旧 feed 游标不能直接复用；bootstrap 后重新接续。Web UI 的记录编辑页提供快照/feed 人工检查面板。详细协议、资源边界、审查及分平台证据见[实施记录第 18 节](../docs/implementation-57-p2.md)。本批尚未发布分支或获得 Windows 原生验证，不能把已有 PR 的 CI 当作本批通过。

## P2 实体与显式一跳关系（本地批次）

通过现有 RecordWrite 创建 `system:{"kind":"entity"}`，或 `system:{"kind":"relation","relation":{"from":{"record_id":"a","routing_key":"r","version":1},"to":{"record_id":"b","routing_key":"r","version":1},"relation_type":"knows","weight":0.5}}`。正文仍在 document；weight 可选，仅为调用方声明，不自动改变检索分数。evidence_refs 单独声明证据，端点不自动视为内容证据。

端点必须是同 collection/partition 当前可见的精确非关系版本。关系与双端的来源、generation、TTL 共同控制可见性；端点替换、删除和 ABA 不会把旧边重新绑定。实体/关系正文可以 Match 精确更新，但 system 声明不可变，重算或重绑定使用新 Record ID。

- `POST /api/v1/index/{id}/entities`：`{"cursor":null,"length":100}`，稳定 ID 分页；视图变化拒绝旧 cursor。
- `POST /api/v1/index/{id}/neighbors`：`{"seeds":[{"record_id":"a","routing_key":"r","version":1}],"direction":"both","relation_types":[],"per_seed":128,"max_candidates":1000,"allow_partial":false}`。

一跳最多 32 个不同 seeds，类型最多 16 个，每 seed 最多 128 条，候选上限不超过 collection 配置，结果内容预算 4 MiB。排序为 seed 输入顺序再 neighbor ID/edge ID；自环去重。默认超预算拒绝，显式 allow_partial 才返回 complete=false、reason 与 count.relation=window；count 是返回的有效边路径数量，不是唯一邻居数。返回共同 visible_seq/evaluation_time_ms/statistics，数据和授权输出屏障与查询相同。Web UI 记录页可提交实体/关系 JSON，并检查实体目录与一跳结果。

本批 journal 为 V9，拒绝旧 V8 等格式；无迁移。未实现关系与全文/向量的单次融合查询、模型实体消歧或完整图语言。测试状态见[实施记录第 19 节](../docs/implementation-57-p2.md#19-实体与显式一跳关系)。

## P2 精确来源摘录（本地收尾批次）

`POST /api/v1/index/{id}/evidence` 接受 `{record_id,routing_key,version,locator}`。locator 可为 `{"kind":"utf8","field":"body","start":0,"end":5}`（UTF-8 字节半开区间，必须字符边界），或 `{"kind":"page","field":"pages","page":1}`（源字段为字符串数组、一基页码）。仅定位已持久内联 Source，不下载 URL 或解析 PDF；关系 endpoints/seeds 不接受 locator。派生 evidence_refs 和 generation source 可携带该定位。

响应返回选中文本及完整字段/页的 SHA-256、固定 text/plain UTF-8 表示、精确 reference、共同水位与时间。Source 修订后可显式读取保留旧修订，普通 stale 派生仍隐藏；当前 source 已删除/过期/隔离时拒绝摘录，授权及输出屏障继续适用。定位写入和恢复均校验范围，journal V10 拒绝旧格式，无升级迁移。UI 记录页可输入精确 reference 查看摘录。分平台结果见实施记录第 20 节。

## P2 staging 租约与私有清理（本地批次）

Stage 获得 30 分钟租约，期限为持久可信 UTC 毫秒时间 + 1800000。使用 `generation:{"action":"renew","generation_id":"g"}` 及精确 `expected:{"Match":当前控制版本}` 续租；回执 staged_only=true，不发布成员。generation 状态返回 lease_expires_at_ms、lease_fence、lease_requires_renewal、private_cleanup_pending。Publish/Abort/Renew 共用 control version fencing；到期不可续租，新建需新 generation ID。同 owner 重启保留剩余期限，接管后的 scope 变更要求显式续租。

Abort/到期持久退休后，只释放未发布私有 Stage 正文，保留规范化请求摘要和原回执；原 operation_id 重试返回原成功结果但不复活 staging。后续请求安全 checkpoint/reclaim，删除旧 WAL 失败保持 pending、未知发布 fence；不是普通历史 GC，也不删除已有备份。UI 记录页可检查租约，通过原编辑器提交 renew 并保留精确重试请求。完整请求 Stage 前不提供分片上传租约。Journal V11 拒绝旧格式；验证见实施记录第 21 节。

### Explicit same-view relation fusion

`POST /api/v1/index/{id}/query` optionally accepts `relation_fusion`:

```json
{"query":"alpha","length":10,"relation_fusion":{"neighbors":{"seeds":[{"record_id":"entity-a","routing_key":"route","version":1}],"direction":"both","per_seed":10,"max_candidates":100,"allow_partial":false},"rrf":{"constant":0.6,"keyword_weight":1,"vector_weight":1,"keyword_candidates":100,"vector_candidates":100},"relation_weight":1}}
```

Add `query_vector` for the vector channel, with existing `all`/`nprobe`/threshold options. No embedding is computed. At least one enabled text/vector channel is required. `relation_weight` must be finite and positive. Existing `RrfOptions` validation applies; an enabled text/vector channel requires its explicit positive candidate budget. The sum of enabled text/vector budgets and `neighbors.max_candidates` must not exceed the collection candidate limit. Top-level `rrf`, field sorting and count-only/zero-length requests are rejected in this mode. Ordinary queries are unchanged.

All channels share one locked partition view, evaluation time, active corpus and metadata/time filters. Filters apply to neighboring result records before per-seed limiting; seeds and edges must also be current and visible. A hidden/filtered adjacency still consumes the scan budget. Relation ranks follow caller seed order then neighbor/edge stable IDs; deduplicate neighbors before zero-based RRF contribution `weight/(constant+rank)`. Edge metadata `weight` is not a ranking input. Final ties use record ID. This is configurable rank fusion, not a relevance-quality claim.

`fusion.count_relation="candidate_window"` and `fusion.candidates` describe the unique candidate union before pagination. Neither `count_total` nor `relation_complete` implies exact corpus recall. `relation_complete` only covers adjacency traversal; without `allow_partial`, exhaustion rejects the entire request, otherwise `relation_reason` identifies the limit. ANN stays approximate. Output is limited to 4 MiB and uses the same field projection, authorization and deletion barriers as ordinary queries. No persistent format changes are introduced by fusion.

### Bounded multipart generations (V12)

A generation may use the existing complete `stage`, or `begin` → `stage_batch` (one or more) → `seal` → `publish`. Every command is an ordinary Record envelope with stable `operation_id`, document `record_id`/`routing_key`, and exact `expected` control version (`Absent` only for the initial document). Commands increment that control version. Preserve the original request for retry; an old successful retry does not append members or extend the lease again.

```json
{"record_id":"doc","routing_key":"r","expected":"Absent","operation_id":{"epoch":20747,"nonce":"begin-example"},"generation":{"action":"begin","generation_id":"g","source":{"record_id":"source","routing_key":"r","version":1},"members":["a","b"]}}
```

Use a current operation epoch, not the example value. Next send `generation:{"action":"stage_batch","generation_id":"g","chunks":[{"record_id":"a","document":{"body":"first chunk"}}]}` with `expected:{"Match":1}`, then the batch for `b` with Match 2. Members are reserved at Begin; unknown/duplicate members, changed source, stale control version and expired/unrenewed owner scope are rejected. Body replacement within an upload is not supported: abort it and start a new generation identity.

Read `GET .../doc/doc/generation`. It reports received/expected members, `sealed` and `manifest_sha256`, without private bodies. Seal with `generation:{"action":"seal","generation_id":"g","manifest_sha256":"<verified current status digest>"}` and Match 3. Incomplete membership or a different digest fails without sealing. Publish with Match 4. Seal and Publish contain no chunk payload. UI uses these same envelopes and displays staging success as durable, not searchable.

Digest encoding is SHA-256 over the engine's recursively key-sorted compact UTF-8 JSON object `{source,members,chunks}`, where `chunks` is keyed by member ID and contains the typed chunks (including default temporal/evidence fields). `members` retains declared order; batch arrival order does not affect the digest or publication order. This is the V12 engine encoding, not a claim of RFC 8785/JCS compatibility. A client can inspect the server's digest then seal that exact version; it must not reuse a digest from another control version.

Single commands remain limited to 1 MiB, received chunk JSON to 4 MiB per generation, and declared members to 100. A partition's identity, operation and complete checkpoint limits can reject earlier. This is bounded multipart ingestion, not unlimited streaming or a raw-file Blob service. All active staging and retained history remain within the existing bounded checkpoint model. Begin, each successful new batch, Seal and Renew grant a 30-minute lease; exact retries do not renew it. A new restored owner must explicitly Renew before further batches, Seal or Publish. Abort/expiry discard every unpublished batch body while preserving request digests and receipts; safe checkpoint/WAL retirement finishes physical cleanup. Published content and its history are not removed by private cleanup. V11 and other older formats are rejected; no migration is provided.
