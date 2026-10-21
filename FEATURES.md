# 当前功能与边界

默认 HTTP/Web UI 使用 Record/catalog 路径；低层 Rust 索引库保留独立 API。当前包括 #57 的有界 P1-A 与本地 P2 来源/证据、来源修订、可选向量及 generation 切片，不代表全部 P1/P2 完成。[服务使用](seekpower_server/README.md) · [架构](ARCHITECTURE.md) · [验证记录](docs/implementation-57.md)

## 检索和数据

| 领域 | 当前能力与边界 |
| --- | --- |
| 全文 | BM25F/邻近度、布尔/短语、多字段、中文分词、停用词和 N-gram |
| 向量 | 外部 JSON/base64、多向量字段、F32/I8、三种距离、量化、IVF ANN；模型推理由调用方提供 |
| 混合 | 两路召回后 RRF，支持权重和独立候选预算；候选窗口计数不当作全库精确并集 |
| 过滤和排序 | 共享元数据谓词、字段排序、分页和投影；ANN/Hybrid 候选内重排不保证全库字段 Top-K |
| 身份和写入 | 稳定 Record ID、不可变 routing key、版本条件、墓碑、防 ABA、原 operation_id 重试 |
| 原始数据 | 权威状态保留完整 Document 和原始向量；schema.store 控制查询投影，不控制原始数据持久保存 |
| 浏览 | 空查询浏览活动记录，包括纯向量 collection；iterator 按稳定 ID 分页，visible_seq 改变使 cursor 失效 |
| 持久化 | WAL、冗余见证、完整 checkpoint、进程恢复；成功写入 durable + visible；系统断电矩阵未验收 |
| 授权 | key、collection、grant 独立；多个 key 共享数据，撤销不删除 collection；输出复核 revision |
| 备份 | collection 独立快照、备份退役、完整 service/catalog 备份、新未启动根恢复 |

当前底层保留本机 shard/level、mmap、None/Snappy 文档压缩、posting 编码、SIMD、Block-max WAND/MaxScore。低层直接使用 IndexArc 的应用仍需处理 commit/close 的 Result；这些可变索引文件不是原子多文件事务，可靠服务从 Record 权威状态重建它们。

字段类型：U8/U16/U32/U64、I8/I16/I32/I64、Timestamp、F32/F64、Bool、String16/String32、StringSet16/StringSet32、Text、Json、Binary。类型存在不代表支持全部索引方式。向量使用 Json/Binary 加 index_vector；当前每次替换必须提供所有已配置向量字段。过滤字段需显式 filterable:true。不提供分面统计聚合、直方图或字段 extrema。

## 默认 HTTP 路由

`{id}` 是稳定 collection ID，`{record_id}` 是稳定 Record ID，均为字符串。除 live 外使用 apikey header。默认路由以 [record_service.rs](seekpower_server/src/record_service.rs) 为准；准确路由清单可生成 OpenAPI 查验。

| 方法 | 路径 | 当前契约 |
| --- | --- | --- |
| GET | `/api/v1/live` | 存活/版本 |
| GET | `/api/v1/apikey` | 列出当前身份已获授权的 collections |
| POST | `/api/v1/apikey` | master 注册 `{operation_id,key_id,apikey_base64}` |
| DELETE | `/api/v1/apikey` | master 撤销 `{operation_id,key_id}`，保留共享数据 |
| POST | `/api/v1/index` | `{operation_id,collection_id,config,embedding_space_id?}` |
| GET | `/api/v1/index/{id}` | 配置和活动统计 |
| PATCH | `/api/v1/index/{id}` | 在线完整 checkpoint，返回 visible_seq |
| DELETE | `/api/v1/index/{id}` | master 持久删除身份，body `{operation_id}` |
| POST | `/api/v1/index/{id}/grants` | master 管理 `{operation_id,key_id,allow}` |
| POST/PATCH/DELETE | `/api/v1/index/{id}/doc` | 条件 RecordWrite；对象完整替换、显式 document:null 删除、vector_patch 后补或 generation 命令 |
| GET | `/api/v1/index/{id}/doc/{record_id}/status` | 元数据可见原因及 pending_vector_fields，不返回隐藏正文 |
| GET | `/api/v1/index/{id}/doc/{record_id}/generation` | 文档控制版本、active/staged 身份、租约期限/fence 和清理状态 |
| GET | `/api/v1/index/{id}/doc/{record_id}` | 原始 Record；可用 `?fields=body,title` 投影 |
| POST | `/api/v1/index/{id}/query` | Lexical/Vector/Hybrid/空查询；返回 hits[].record |
| POST | `/api/v1/index/{id}/iterator` | `{cursor?,length?,fields?}`；不支持旧 skip/take 协议 |
| POST | `/api/v1/index/{id}/evidence` | 受保护精确来源版本的 UTF-8 区间/内联页摘录及摘要 |
| POST | `/api/v1/index/{id}/entities` | 稳定 ID 实体目录，同视图分页 |
| POST | `/api/v1/index/{id}/neighbors` | 精确版本 seeds、有预算一跳、exact/window 计数 |
| POST | `/api/v1/index/{id}/changes` | 至少七天内容无关通知；过期游标明确要求重建 |
| GET / POST | `/api/v1/index/{id}/bootstrap` | 获取运行期身份 / 固定切点分页及 feed continuation |
| DELETE | `/api/v1/index/{id}/bootstrap/{request_id}` | 释放有界快照 pin，旧请求身份失效 |
| POST | `/api/v1/index/{id}/delete-plan` | all/ids/lexical query 选择；返回精确版本目标 |
| POST | `/api/v1/index/{id}/batch` | `{writes:[...]}`，1..100 独立操作，非原子 |
| GET | `/api/v1/index/{id}/operation/{epoch}/{nonce}` | 已授权数据回执查询，缺席不证明失败 |
| GET | `/api/v1/catalog/operation/{epoch}/{nonce}` | master 控制操作查询 |
| POST | `/api/v1/index/{id}/backup` | 独立 collection 快照；同操作 ID 保留原快照 |
| POST | `/api/v1/restore` | master 恢复到新 collection，不恢复历史 grants |
| GET | `/api/v1/backups` | master 查看 collection 备份及退役状态 |
| DELETE | `/api/v1/backups/{id}` | master 退役备份；未完成 restore pin 来源 |
| POST | `/api/v1/service-backups` | master 完整 catalog/数据/collection backups 快照 |
| POST | `/api/v1/service-restores` | master 恢复到新的未启动根；旧 key 禁用 |

固定静态资源另由同一服务提供。没有旧按 key 数据路由、按数字 DocID 删除路由或旧 CORS 预检层的兼容承诺。query 参数放在 POST JSON body，未知路径返回 404，不支持的方法由路由器返回 405。

## Web UI 和工具

UI 增加实体目录/一跳关系检查及有界 bootstrap/feed 检查与响应丢失重试；保留连接、创建/选择 collection、JSON 粘贴/文件输入、Lexical/Vector/Hybrid 搜索、外部向量、分页、链接/原文/图片预览、日期过滤和排序。创建/写入发送前保存原请求；未知结果在刷新或后续拒绝后仍保留身份。普通写入确认后直接可见；generation staging 回执明确仅持久，必须显式 Publish 才开放成员。界面支持 staging 租约检查和 renew 条件写入；未发布私有正文在 abort/到期后安全清理并保留摘要回执。界面不再做 POST 后 PATCH 提交。master 可在 UI 发起完整服务备份与新根恢复；没有通用 key/grant 或逐条更新/删除管理页面。

完整冒烟为 `scripts/record_service_smoke.py`，`--browser` 启用 Chromium，显式 fault-injection 构建可运行 `--faults`。`scripts/seekpower_client.py` 和 `index_write_query_smoke.py` 使用新 Record 请求，后者发送前保存请求文件。OpenAPI 通过 `generate_openapi=true` 生成；HTTP 类型和 schema 留在服务 crate。

## 资源与未完成项

Record 身份（含墓碑）、累计保留操作、候选各最多 10,000；单操作 1 MiB，完整 checkpoint 单帧 16 MiB，Record WAL 代 256 MiB；catalog 16 MiB、10,000 操作。Record checkpoint 不做幂等账本 GC。请求 body 2 MiB、响应 16 MiB；请求/control/partition admission 分别为 64/16/8。catalog 全局屏障覆盖完整授权操作，不同 collection 也会互相等待；请求期限不等于取消已接管的持久任务。

collection backups 最多 32 个；service archives 每源根 4 个，生成恢复根每托管父目录 4 个；完整备份权威 payload 预检 1 GiB。达到边界显式拒绝，不静默淘汰。当前不承诺持续无限运行或长期稳定磁盘占用。

尚未完成：系统/文件系统崩溃与断电验收、Record 操作账本持续 GC、物理版本压缩、关系与全文/向量单请求融合、管理型 Blob、分布式与对象存储。模型推理、解析切块及 reflect 由上层提供。低层库不是旧 HTTP/磁盘格式兼容入口。

早期移除功能及历史协议见 [MIGRATION](MIGRATION.md)；历史记录不能替代上述当前契约。
