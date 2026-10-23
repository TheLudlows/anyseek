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

已明确选择保留实际支持的 BM25F / Bm25fProximity 契约，不改用字段加权 Tantivy BM25。字段 DF、平均长度分母、Shared/PerField、旧长度量化及默认 N-gram 评分都继续保留。`bm25_probe` 仅用于原生能力验证，不是生产排名选择。

第二阶段的 `Analyzer` 复用原分析器及相同 stop/frequent 字典，生成含重叠 unigram/N-gram 位置的原生 postings，精确 unigram 长度另存 fast field。显式编译 Term/Boolean/PhraseQuery，不调用 Tantivy QueryParser，避免引入新语法或恢复已删除的改写、拼写纠正、补全、同义词、词干等能力。空串浏览、空白/标点无匹配、纯否定无匹配、短语位置和单一不同词的重复短语行为按现有实际路径保留。

评分在原生 postings 上采用跨字段活动 DF、原长度量化、Shared/PerField、boost 和稳定累加顺序。Bm25f 的 N-gram 由整体 gram 匹配门控后按组成词评分，Bm25fProximity 按整体 gram 评分。没有引入物理统计的 block-max pruning。

`native_query_contract_parity_and_field_diagnostics` 覆盖 2 种长度归一化、2 种相似度、10 个 N-gram mask（0/1/2/4/8/16/32/64/9/127）、提交前后、3 种查询模式、26 个查询和 3 个字段范围，共 18,720 次比较。其中 **6,240 个不限定字段的比较全部要求身份/总数/分数一致**；其余为字段范围诊断，不宣称完整兼容验收。

### 需要统一的真实旧契约矛盾

诊断发现 80 个分歧，全部是已提交路径的限定字段 OR 查询 `alpha beta`，横跨上述 40 个配置。记录 f 为 body=`alpha`, title=`beta`，查询限定 body 时，实时路径不返回 f，提交路径返回 f；限定 title 也存在同样矛盾。Shared/Bm25f/mask=0 时，body 路径提交后 f 分数为 3.5540719，title 为 0.888517975。因此不能同时保留两条旧路径，又保证同一 cut 的统一语义。

已提出采用已提交路径作为兼容基准、修复实时漏召回的推荐选择，尚待答复。当前测试模块暂按实时限定字段规则编译，生产没有接入该规则；诊断 JSON 保存于 `target/issue80-phase2/field-contract-differences.json`。确定选择后，还需把评分组门控限制到选定字段（组成词 TF/DF 仍按既有评分范围），并将这些诊断改成严格一致断言。

另一个独立的短语游标错误已修复：旧执行器跳过未选择字段时，已消费的首位置未计入 cursor，导致后续字段短语漏召回。`field_restricted_phrase_keeps_first_position_after_skipping_earlier_field` 对提交前后分别验证两条应命中的记录；此修复不修改评分公式。该修复同时使查询矩阵中的短语分歧消失。

CI 依赖清理检查仅允许 Tantivy 依赖边下出现 crossbeam-channel/lz4_flex，仍禁止 anyseek 直接依赖及绕过 Tantivy 的传递路径；其余已裁剪能力依赖仍禁止。

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
| 版本/schema/分词/评分 | 精确依赖、pre-tokenized schema、原分析器复用、BM25F/邻近度/N-gram 对照 | 限定字段 OR 统一决策、生产查询编译接入 |
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

2026-10-23 第一阶段检查点结果：默认核心单测 **190 passed / 0 failed / 10 ignored**（90.52 秒）；无默认 feature 核心单测 **189 passed / 0 failed / 10 ignored**（85.99 秒）。其中原生 Tantivy 验证为 12 项通过。ignored 包含由父测试单独调用的进程 worker；没有把 ignored 全部称为直接运行。文档检查通过（20 份 Markdown、115 个执行表面），diff 空白检查通过。server/all-targets、浏览器、Windows 和真实 S3 未在本检查点运行。

初次新增测试发现过中文分词数量假设、测试 schema 缺少 store 和 postings 反向 seek 问题；均已修正并完成上述最终回归。日志及本地未发布的 draft PR 文本保存于 `target/issue80-checkpoint/`（构建证据，不纳入产品文件）。这些计数只证明本检查点，不能冒充未来生产替换版本的验收。

## 第二阶段安全检查点（2026-10-23）

新增完整现有词项/短语编译和 N-gram 评分适配测试、共享分析字典、两条旧执行路径的字段短语游标修复及 CI 依赖边审计。生产全文构建/查询仍使用旧引擎；本阶段没有完成生产替换、统一发布恢复、备份/GC 或旧实现清理。

已实际完成的验证：

- `cargo build --workspace --features server,fault-injection` 通过。
- 当前最终 all-targets 运行已完成核心 **230 passed / 0 failed / 12 ignored**（含 14 个 lexical 测试）及 server **15 passed / 0 failed**；随后集成目标也全部结束，完整日志的 34 个 test-result 汇总共 331 passed / 0 failed / 13 ignored（核心和 server 已含在内）。
- `python3 scripts/record_service_smoke.py --server target/debug/anyseek_server --faults` 退出 0；Record/locator/relations/temporal/multipart/staging leases/feed 及对应进程故障矩阵通过。
- 文档链接/品牌检查及 diff 空白检查通过；Tantivy 专属传递依赖审计通过。
- 初次 all-targets 因上述短语漏召回失败；已修复，最终核心和专项回归均通过，未隐瞒早期失败。

应父任务要求保存安全检查点时，无默认 feature 与 S3 fixture 运行继续保留，不能提前算通过；all-targets 已在保存过程中结束：

- `cargo test --workspace --features server,fault-injection --all-targets`，原 PID 28223（已结束），日志 `/tmp/anyseek80-phase2-all-targets-final.log`。
- `cargo test -p anyseek --no-default-features --lib`，PID 37660，工具 session 32994，日志 `/tmp/anyseek80-phase2-no-default-final.log`。
- `python3 scripts/s3_acceptance.py --backend fixture --suite all --server target/debug/anyseek_server --faults --report target/issue80-phase2/s3-fixture.json`，PID 26879，工具 session 93332，日志 `/tmp/anyseek80-phase2-s3.log`。

这些 PID 只是本环境检查点记录，续接必须先核验进程/完整日志，再决定是否重跑。HTTP 完成日志及查询差异 JSON 保存在 `target/issue80-phase2/`；运行日志仍以以上 `/tmp` 文件为最新来源。fixture 中名为 real_s3_compatible_contract 的测试实际使用本地 fixture，不代表真实 S3。Windows、真实 S3、系统断电均未运行。

## 当前成本与限制

本检查点 Cargo.lock 共 331 个 package 条目（含工作区包）；并非全部都会进入默认核心产物。原型每 writer 设置 20,000,000 字节 arena，一个 indexing worker、一个 merge worker，另有 Tantivy 自身的协调线程。输入分析缓冲按显式保守估计另计，预算不是 RSS 限制。候选执行目前遍历全部匹配，活动 df 查询也扫描原生 postings；这是有界正确性路径，尚无延迟/吞吐或维护成本改善结论。外层包不含规范 Payload，单独复制它不构成可恢复的数据库备份。

## 第三阶段：最小契约证据与跨包组合

### 限定字段 OR 最小复现

只写入一条记录 f：`body="alpha"`、`title="beta"`。使用 `QueryType::Union`、查询 `alpha beta`，检索字段限定为 `body`（Record Rust 参数 `fields`，HTTP 的对应检索参数为 `search_fields`，不同于输出投影 `fields`）。

| 状态 | 返回 ID | 总数 |
| --- | --- | --- |
| 写入已成功、旧引擎尚未内部 commit | 空 | 0 |
| 同一记录和查询、仅执行旧引擎内部 commit 后 | f | 1 |

新增 `minimal_field_or_documents_existing_commit_contradiction` 已实测以上行为。原 80 项数字来自配置矩阵，不是 80 种不同规则。文档 [查询示例](library-examples.md) 明确称 Union 为 OR；[字段说明](../FACETED_SEARCH.md) 把 search_fields 定义为选择检索字段。按这些定义，body 中命中 alpha 即满足 OR，title 中 beta 不应导致排除。因此推荐以提交后规则为准，修复实时错误。用户可见影响是成功写入后立即能查到 f；总数增加、分页和排名可能变化；匹配评分组只取选中字段中存在的词，公式/活动 DF/长度归一化保持不变。不是新增 OR 语法或新的字段加权 BM25，但因为纠正已有可见行为，仍保留待决，不静默接入。

### 邻近度支持证据

- [HTTP 创建集合配置](../anyseek/server/record_service.rs) 的 `record_config` 实际固定 `LexicalSimilarity::Bm25fProximity`、`FrequentwordType::English`、`ngram_indexing: 9`，服务默认请求会进入该功能。
- [N-gram 文档](../NGRAM_SEARCH.md) 明确写“当前保留此功能”，说明 Rust 的独立 mask、HTTP 固定 FF | FFF、BM25F/Proximity 实时/提交/重启回归；不只是旧枚举残留。
- [核心定义](../anyseek/src/index.rs) 保留 Bm25f/Bm25fProximity，现有矩阵在两种相似度和十个 mask 上执行真实查询。短语相邻 N-gram 评分与已删除的拼写纠正/自动补全无关；不恢复后者。
- [字段长度说明](field-length-normalization.md) 仍记录 #42 历史实时 N-gram 问题；当前有限矩阵通过不代表该历史问题所有输入均已解决。

### 新增组合与资源准备

`CorpusView` 在一个外层选择的 cut 下组合冻结包和原生尾部包，拒绝未来 cut、字段顺序不同、缺失修订、跨包重复活动修订及同稳定 ID 的多个活动修订。统一活动 N、跨字段 DF 和解码长度均值，历史版本不计入；请求过滤在 Top-K 前执行，不改变统计。各包以同一语料评分后合并稳定 Top-K，尚未在此层做 RRF。测试用旧包中的 b/v1 和尾部中的 b/v2，验证与完全清洁重建的身份/计数/分数一致，并覆盖过滤与 K=0/1/10。

原生打开现在在完整清单校验、元数据解析和 mmap 前预留内存及服务 buffer 额度；Snapshot 持有这些额度至固定 Searcher 释放，失败自动释放部分申请。资源测试覆盖已占满额度时拒绝、Snapshot 存活期间不可提前复用、Drop 后恢复以及拒绝后可重试。

这些是正式接入需要的组件，**仍在测试隔离模块内**。本阶段尚未把 Record authority/anchor 改成 Tantivy 新格式，不能称为生产替换或新格式恢复/备份/GC 已完成。跨包统计目前重复扫描 postings，未声称性能优化；最终分析配置认证和统一生产调用点仍待接入。

第二阶段后台无默认 feature 验证已结束：191 passed / 0 failed / 10 ignored（102.53 秒）。S3 fixture 原 PID 26879 继续运行，未重启；当前日志中的 real HTTP / real_s3_compatible_contract 名称不意味着使用真实 S3 服务。

第三阶段验证：默认 feature 原生模块 17 passed / 0 failed（9.19 秒）；无默认 feature 同一模块 17 passed / 0 failed（8.48 秒）。日志在 `target/issue80-phase3/`。生产代码本阶段未变，未把前一提交的 all-targets/HTTP 结果冒充新生产引擎验收。S3 fixture 已继续完成 base、locator、graph、temporal 冷缓存阶段，当前 upload 阶段尚在运行，尚无最终报告。
