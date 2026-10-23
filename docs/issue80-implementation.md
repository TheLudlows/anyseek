# Issue #80 实施记录

本分支是进行中的实现，**不是完成声明**。生产 Record 查询尚未切换到 Tantivy，旧全文实现尚未删除。测试模块不是生产双写、引擎切换开关或多引擎框架。

## 可复核基线

- 仓库：TheLudlows/anyseek；issue #80 全文已读，评论为空。
- 独立目录：`/workspace/anyseek-issue80-independent`；分支 `issue-80/tantivy-new-format`。
- master：`3893cd54f0d8d47b6b9e35fa3cce34e579117764`。
- PR #79 设计文档：`0ac8d32b46256fb7c9334d526973ff6bb84944ee`，未合入；没有把该 PR 的其他文档修改混入本分支。
- 仓库中没有 AGENTS.md 或 .agents/skills；工作区级 .agents 为空。
- 执行环境：Linux x86_64，rustc 1.99.1。未访问或修改旧任务的仓库内容。

## 已实现的语义验证组件

`record/lexical` 由 `cfg(test)` 隔离。Tantivy 精确固定 `=0.27.0`，关闭默认 feature，只启用 mmap 和 lz4-compression；Cargo.lock 固定完整依赖解析。原生内部格式与 anyseek 外层包格式分别标识，禁止把旧 format 8 当作 Tantivy 编码读取。

私有候选包含 `package.json` 和 `lexical/` 原生文件。元数据保存候选 cut、字段顺序和 `(record_id, version, revision)`；原生 fast field 只保存包内身份序号及精确字段长度。正文只作为瞬时 pre-tokenized 输入，不配置任何 stored 正文字段或向量字段。完整文件清单及 SHA-256 由外层 authority 持有；打开时重验整个清单、schema、数量和每个 DocAddress 的唯一映射。找出一个较新的 meta.json 不能选定它。

候选 writer 使用一个索引线程、一个 merge 线程并设置 NoMergePolicy，完成 commit 后显式 drain。reader 使用 Manual，查询对象持有固定 Searcher。原生 Directory 的写入和 atomic_write 在落盘前计入同一保守字节/文件预算，不因删除或失败退款；Directory/Writer 克隆持有实际内存及服务任务/缓冲额度，I/O 使用捕获的 Scope，不依赖原生线程继承 Tokio task-local。

活动视图按精确修订映射筛选，拒绝同一稳定 ID 的多个活动修订和不存在的修订。活动 N、df、字段 token 总量不包含隐藏历史；请求元数据过滤不改变语料统计。候选执行在 Top-K 前过滤，平分按稳定身份；显式遍历完整匹配集合，不使用尚未证明对自定义统计成立的 block-max pruning。结果存储有 K 上限；它不是全进程 RSS 上限。

分词入口已抽出不依赖 Shard 的 AnalysisContext，现有生产入口继续调用同一实现。中文词典、OpenCC 1.1.9、字符折叠、停用词与查询操作符不自动换成 Tantivy 默认分析器。

## 评分决策及适配范围

尚未批准改变公开排名。字段加权 Tantivy BM25 并不等价于现有 BM25F：字段 DF 与跨字段 DF 不同，平均长度分母不同，原有 Shared/PerField 和长度量化也不同；默认 Bm25fProximity 的 N-gram 评分不能直接取消。

当前提供两个明确命名的测试探针：

- `bm25_probe`：验证原生统计扩展、过滤、短语和字段加权的行为，不宣称旧排名等价。
- `bm25f_unigram_probe`：在原生 postings 上保持跨字段活动 DF、原长度量化、Shared/PerField、boost 和稳定累加顺序；与同一 Record cut 下的实际旧引擎分数及向量视图比对。**尚未覆盖全部 N-gram/邻近度和查询语法**。

需要的产品决策已提出：保留现有 BM25F/邻近度并完成专用适配，还是明确修改 API 采用新的字段加权 BM25。未答复时不将第二种探针接入生产。

## 后续实现边界

1. 完成所有评分/分词/查询契约原型，确定最终外层新格式标识和配置。
2. 将 RetrievalView 的全文、向量、身份/元数据职责拆开；普通写入在业务 authority 选择后、成功响应前建立同一 cut 的全文/向量/关系视图。Tantivy commit 不授予 durable 或 visible。
3. 冻结包与同引擎 tail 共用活动统计；向量后补记录独立索引修订；generation、来源隐藏/stale、TTL 在截断前统一生效；各通道候选仅做一次全局 RRF。
4. 让 anchor/SQLite manifest 认证完整原生闭包，复用对象分块及本地完整物化；接入恢复、备份 pin、构建 pin、安全 GC 和本地发布证书。不存在迁移、旧格式 reader 或双写目标。
5. 将候选预算和 owned task 生命周期接入正式发布错误边界，保留 Busy / outcome_unknown / durable_pending；禁止后台 reload 越过认证边界。
6. 删除旧全文倒排和执行路径，更新 API/架构/功能文档，再执行 Linux/Windows 验收。

## 验收记录

| #80 项目 | 当前证据 | 尚未完成 |
| --- | --- | --- |
| 版本/schema/分词/评分 | 精确依赖、pre-tokenized schema、原分析器复用和两个评分探针 | 最终评分/API 与完整查询编译 |
| 布局/身份/manifest | 原生候选完整清单、版本/修订映射、手动 Searcher | 正式 anchor/descriptor/tail 集成 |
| 即时可见/过滤/活动统计 | 原生 commit 后立即打开、隐藏高分版本前置过滤、与干净语料统计比较 | 正式写入响应的统一可见边界 |
| 全文/向量同 cut | 测试在 Record 锁内捕获实际向量查询和候选输入 | 所有正式通道及一次全局 RRF 集成 |
| 恢复/备份/GC | 文件复制重开、hash/额外文件/未选 commit 拒绝 | 同一新格式业务恢复、S3 冷恢复、备份和安全 GC |
| 资源/任务 | native 写入前预算、跨线程额度所有权测试 | 生产错误分类和全路径预算验证 |
| 清理旧实现 | 尚未执行 | 生产替换后删除旧全文代码/依赖 |
| 语义矩阵 | 多字段、预分词中文、实际中文/OpenCC/折叠、短语、缺词、稳定平分、计数/预算 | 完整 generation/source/stale/TTL/向量后补/混合验收 |
| 故障/撤权 | 旧生产回归持续检查 | 新引擎发布前后退出、响应丢失、撤权及未交付输出 |
| 跨平台/成本 | 本地 Linux 编译及单测 | Windows 原生、新引擎全矩阵及最终成本报告 |

真实 S3、系统/文件系统崩溃和硬断电没有运行；复制索引包、fixture 与进程退出不能替代这些验收。没有删除部署数据，没有 push/创建 PR/合入/关闭 issue。

执行命令：

```sh
cargo check -p anyseek
cargo test -p anyseek --lib record::lexical
cargo test -p anyseek --lib
cargo test -p anyseek --no-default-features --lib
python3 scripts/check_docs_brand.py
git diff --check
```

2026-10-23 本检查点结果：默认核心单测 **190 passed / 0 failed / 10 ignored**（90.52 秒）；无默认 feature 核心单测 **189 passed / 0 failed / 10 ignored**（85.99 秒）。其中原生 Tantivy 验证为 12 项通过。ignored 包含由父测试单独调用的进程 worker；没有把 ignored 全部称为直接运行。文档检查通过（20 份 Markdown、115 个执行表面），diff 空白检查通过。server/all-targets、浏览器、Windows 和真实 S3 未在本检查点运行。

初次新增测试发现过中文分词数量假设、测试 schema 缺少 store 和 postings 反向 seek 问题；均已修正并完成上述最终回归。日志及本地未发布的 draft PR 文本保存于 `target/issue80-checkpoint/`（构建证据，不纳入产品文件）。这些计数只证明本检查点，不能冒充未来生产替换版本的验收。

## 当前成本与限制

本检查点 Cargo.lock 共 331 个 package 条目（含工作区包）；并非全部都会进入默认核心产物。原型每 writer 设置 20,000,000 字节 arena，一个 indexing worker、一个 merge worker，另有 Tantivy 自身的协调线程。输入分析缓冲按显式保守估计另计，预算不是 RSS 限制。候选执行目前遍历全部匹配，活动 df 查询也扫描原生 postings；这是有界正确性路径，尚无延迟/吞吐或维护成本改善结论。外层包不含规范 Payload，单独复制它不构成可恢复的数据库备份。
