# Issue #80 实施记录

本分支是进行中的实现，**不是完成声明**。Record 生产全文已使用 Tantivy；旧公开全文入口和执行/格式模块已删除，向量/metadata已独立。外部集成测试迁移后的全目标编译已恢复通过。阶段12新二进制的完整HTTP（默认15秒）和S3 storage fixture均通过；阶段13预算/生命周期补强后的最终矩阵正在重跑，Windows仍未运行。历史阶段按时间保留，不代表最新验收状态。

## 可复核基线

- 仓库：TheLudlows/anyseek；issue #80 全文已读，评论为空。
- 独立目录：`/workspace/anyseek-issue80-independent`；分支 `issue-80/tantivy-new-format`。
- master：`3893cd54f0d8d47b6b9e35fa3cce34e579117764`。
- PR #79 设计文档：`0ac8d32b46256fb7c9334d526973ff6bb84944ee`，未合入；没有把该 PR 的其他文档修改混入本分支。
- 仓库中没有 AGENTS.md 或 .agents/skills；工作区级 .agents 为空。
- 执行环境：Linux x86_64，rustc 1.99.1。未访问或修改旧任务的仓库内容。

## 已实现的语义验证组件

第一至三阶段 `record/lexical` 由 `cfg(test)` 隔离；第四阶段已移除此隔离，历史阶段证据仍按对应提交理解。Tantivy 精确固定 `=0.27.0`，关闭默认 feature，只启用 mmap 和 lz4-compression；Cargo.lock 固定完整依赖解析。原生内部格式与 anyseek 外层包格式分别标识，禁止把旧 format 8 当作 Tantivy 编码读取。

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

## 第四阶段：生产包边界与确认后的 OR 修复

用户已确认统一限定字段 OR：选定字段存在任意查询词即满足查询，不再等待语义决策。生产实时执行器现在仅排除不在选定字段匹配的评分组，而非排除整个记录；Intersection/短语仍要求相应条件。原生编译器使用同一选定字段谓词和评分组门控，TF/活动 DF/长度公式保持原有范围。矩阵的字段差异断言改为必须为空，共 18,720 次比较均要求身份、计数和分数一致。

原最小复现已转为修复回归，并加入 f=(alpha,beta)、g=(beta,alpha)、h=(gamma,alpha) 三记录，限定 body，验证两个命中、两页和越界页、Count，并逐项比较提交前后分数。g 的 beta 活动 DF 更低，应在 f 之前；初版分页测试误以为两者平分，已修正测试预期，没有为通过测试改评分。

### 实际生产变更

- 生产 `anchor::Frozen` 改用共享 `segment::Package`。原本内嵌于旧 index loader 的路径校验、封存、同步、发布目录及认证完整文件闭包，已抽到与引擎无关的不可变包边界。已有 local/SQLite/S3 anchor 仍使用相同 id/files 序列化和外层业务证书，目录发布不会赋予业务可见性。没有添加双写或第二套 head。
- `lexical::package::NativePackage` 在正常生产编译中提供构建/加载：按现有字段类型提取正文，复用分析器，记录分析配置指纹，调用原生 writer 后通过上述相同 Package 边界封存。加载时验证所选完整闭包、配置指纹、cut 和原生结构，返回持有额度的固定 Snapshot。
- 原生同步 writer 在 owned blocking worker 内运行；请求取消后 worker 继续持有 Scope、输入额度、磁盘预算至原生线程 drain。私有目录由 worker 清理；已经封存但尚未被 authority 选定的对象保留为可回收候选，不擅自删除部署数据。

**尚未接上的部分**：Record anchor 当前仍调用旧 Frozen 构建器；NativePackage 还没有替代该调用点，SQLite/S3 的新原生包发布与恢复也尚未运行。这里的生产包组件接入不等于生产全文替换完成。下一阶段需要把向量/metadata 从旧全文 Shard 中拆出，再一次性替换 frozen/tail 的构建和查询调用，不能靠长期双写过渡。backup/GC 的路径清单已共用，但新格式的业务端到端验收仍待完成。

原始 S3 fixture PID26879 已结束，9 阶段全部 exit0，`all_requested_checks_passed=true`、`real_endpoint_exercised=false`。报告 `target/issue80-phase2/s3-fixture.json`；固定服务二进制 SHA256 为 `7f90125d036ef19b916f4331a5954221773912e3dac47d00e81eab04843df1d4`。这仍是第二阶段二进制，不能算第四阶段原生包或当前生产变更的验证。base 阶段用时702.535秒，期间日志持续进展；总共约1176秒，没有卡住或重启。

第四阶段验证过程中，首轮 all-targets 除旧分页预期外还出现 11 个子进程启动 `NotFound`：在该轮仍运行时重编译了相同测试可执行文件，旧进程通过 current_exe 启动 worker 的路径已被替换。此轮结果为222 passed/12 failed/12 ignored，不算通过。已启动最终全目标运行，并停止同时重编译相同 feature 的测试目标；必须以最终日志为准。专项默认18 passed/0 failed（7.12秒）、无默认18 passed/0 failed（6.54秒），生产 anchor 专项7 passed/0 failed。

正式切换前仍需把 native blocking worker 纳入已有 `Inner.background` 构建 pin/partition owner 生命周期，把 Snapshot 路径纳入 view 的 GC 保留集；当前独立组件只保证自身资源与私有目录所有权，尚不代表完整业务锁和 GC pin 已接上。还需覆盖无词法字段 schema 和单索引字段 boost 的旧规则，不能把当前限定矩阵当完整配置验收。

第四阶段最终全目标运行已完成核心234 passed/0 failed/12 ignored（144.45秒）、server15 passed/0 failed（1.97秒）；保存检查点时集成测试仍在运行，日志 `/tmp/anyseek80-phase4-all-targets-final.log`、session68318。若后续报告补充完整总数，应以该进程退出和日志汇总为准。

## 第五阶段：生产回收与原生 owner/pin 接通

第四阶段最终 all-targets 已退出0：34个结果汇总335 passed / 0 failed / 13 ignored。此前有旧分页预期和重编译干扰的失败轮次保持记录，不用最终结果覆盖失败原因。

本阶段的 `package_pins` 已被生产 `reclaim_anchor_objects` 调用。注册 pin 与完整删除操作由同一锁协调；不是先检查 pin 再无锁删除。按规范化父目录与直接子路径匹配，重复路径去重，引用计数溢出/锁中毒时拒绝回收。该注册表只保护本机文件生命周期，不能代替 SQLite 对象 manifest、备份 pin 或业务 authority。

`NativePackage::prepare` 改为接收并持有 partition owner（`Arc<Mutex<Inner>>`），从 owner 获取根目录。后台 blocking worker 保留 owner、Scope 和资源额度至完成；调用方取消也不提前释放 journal 的 OS owner lock。构建前同时 pin 私有目录与最终不可变包名；成功返回的候选保留 pin，反序列化 descriptor 不自动取得 pin。加载时先 pin、再校验和打开，pin 随固定 Snapshot 释放。

新增真实生产 GC 回归验证：未被 authority 选择的原生候选在构建句柄存在时不被回收；丢弃候选而保留 Snapshot 时仍不回收；最后一个 reader 释放后可以回收，随后重开失败。取消回归暂停真实 owned worker，取消请求并释放外层 RecordPartition 后尝试重开，确认 owner lock 仍阻止接管；释放 worker 后可恢复并清理未选包。3个包/生命周期专项全部通过（0.08秒）。

仍未完成：Record authority 的正式构建/加载调用点未改选 NativePackage，生产检索仍是旧全文引擎。`Frozen::load` 返回 IndexArc，`RetrievalView` 的词法/向量/元数据仍共用旧 Shard；必须拆开这组接口后整体切换，不能插入双写或长期多引擎选择。新包在 SQLite/S3 的端到端发布/冷恢复、备份/GC 验收及旧实现清理仍待完成。

第五阶段完整核心回归（server,fault-injection）236 passed / 0 failed / 12 ignored，143.24秒。随后把分析器/中文词典的创建移至资源预留之后，追加相关专项回归；此微调不改变发布/查询语义。日志位于 `/tmp/anyseek80-phase5-*.log`，保存副本位于 `target/issue80-phase5/`。

预算顺序修正后的包专项最终结果：server/fault-injection 3 passed/0 failed，无默认feature 3 passed/0 failed，均0.08秒。该阶段测试均已退出，无待收取进程。

## 第六阶段：Record 的真实生产纵向切换

`build_at_selected` 现在把 Record 的旧 retrieval schema 中 `index_lexical` 全部关闭，旧 IndexArc 只构建向量与 metadata；同一批规范 Payload 输入经 Runtime 生成原生 postings，不进行全文双写。普通 publication 的 append 同时传入稳定 ID、业务 version 和本次 sequence 作为索引 revision。向量后补保留业务 version，但形成不同索引 revision。批量构建后一次 native commit；live tail 每次写入 native commit/reload 后捕获固定 Searcher。只有外层 Record durable + visible 协议完成，才开放查询，native commit 不是业务提交。

`Frozen::load` 在认证完整 segment 文件闭包之后打开 `native/`，检查原生格式、完整原配置、身份序号和字段长度映射。tail rotation/多冻结包合成同时搬移原生组件与 vector/metadata 分片。Record checkpoint 格式改为 `RecordCheckpointV13`，anchor manifest 改为2；不读取旧 Record 格式、不迁移、不删除部署数据。原生文件在同一个已认证 segment 闭包中，现有远端文件清单能够包含它们；尚未用新服务二进制完成远端恢复验收。

`RetrievalView::search` 的全文入口改为 native runtime query。在同一业务 cut/evaluation_time 下按 metadata 的精确活动 ordinal 选择原生身份；来源隐藏、stale/TTL、generation 等仍由同一受保护活动视图控制。全局活动 N/DF/解码长度供 BM25F/Proximity 评分，metadata 过滤发生在 Top-K 前。向量通道继续用旧向量执行器，全局一次 RRF；关系融合继续调用同一 Inner 视图。排序复用原 metadata 排序比较器。无词法字段的 schema 现在允许 identity-only native schema；单个实际 indexed field 的 boost 按旧规则取1。

### 新的直接证据

`record_native_write_query_checkpoint_reopen_has_no_legacy_lexical_postings` 通过真实 Record 写入 alpha，立即查询命中；断言旧 metadata Index 的 body.index_lexical=false，且直接调用旧全文执行器返回0条，而 Record 查询返回1条；checkpoint 后、重开后仍返回1条，重开再写入第二条立即返回2条。这证明生产纵向链路实际使用 Tantivy，而不是只编译了组件或仍由旧 postings 提供结果。

扩大后的默认 Record 回归120 passed / 0 failed / 5 ignored（119.29秒）。初轮118/2失败是测试夹具仍只拼接/注入旧 Shard：跨包统计测试手动把旧统计 corpus 挂到两个不同 partition，64K 边界测试只注入旧物理 rows。现已将跨包对照改为组合实际 native + vector 组件，64K 夹具同时通过实际 native writer 注入对应的退休物理历史；未忽略测试或取消断言。所有其他源隐藏、generation、TTL、向量后补、混合/关系融合和恢复默认测试继续执行。

常驻 native writer 使用计费 Directory，限制单 indexing worker、单 merge worker及20MB arena。保留组件的 I/O/字节额度归 Background lane、保留相同subject，不占用已经结束的请求 slot、不长期占用Control预算。Runtime和固定Snapshot pin所属cache/segment路径，释放writer和Searcher后才释放pin；Inner字段释放顺序保持索引先于journal owner。末尾另加请求slot释放专项。

### 尚未完成及需核验的限制

- 未构建/运行切换后的服务HTTP、SQLite/S3冷恢复、backup/GC故障矩阵；旧S3九阶段只证明旧检查点。Windows、真实S3、系统断电仍未运行。
- 旧低层 IndexArc 全文API/代码仍存在；Record没有旧全文回退，但最终清理尚未执行。
- 当前live tail每次写入commit/reload，快照身份/长度映射重新读取；尚无维护/延迟成本改善结论。长期资源预算、中文分析器构造成本及高历史量情形还需完整验证。
- 混合结果计数当前按全局两通道总数max，需专门核验与旧多分片sum(max)布局相关行为的差别；不能把已通过的有限RRF测试泛化为全部API边界已验收。
- 当前生产Runtime与前期NativePackage构建辅助均属同一Tantivy编码；后续应合并重复辅助路径、收敛接口，不保留多引擎框架。

第六阶段最后的纵向/请求slot专项2 passed / 0 failed（0.64秒），覆盖末尾Background计费调整。当前无后台测试进程。

## 第七阶段：切换后的服务验收与 I/O 竞争修复

生产检索已在第六阶段切换，本阶段首先使用新服务二进制跑 HTTP 和 SQLite/S3 fixture。S3 storage 套件完整退出0，262.215秒，`all_requested_checks_passed=true`、`real_endpoint_exercised=false`，二进制 SHA256 `8add9f8620f8e0df3f1849017bd6369d2ec96b3047fd2ad4464e4cd409bd3db2`，报告 `target/issue80-phase7/s3-storage.json`。覆盖 checkpoint 与未索引 tail 的冷缓存词法/向量/混合恢复、SQLite 发布故障、接管各边界、远端 feed floor、联合备份中断恢复/命名空间隔离/回收以及完整服务归档恢复。它是原生切换后的证据，但早于本阶段后续修复，不能冒充最终二进制全矩阵验收。

HTTP 暴露两处真实问题：纯文本集合的 vector/metadata 组件没有 indexed field，旧初始化的 `len()-1` 下溢；现改为饱和减法，新增纯文本 Record 写入/查询/checkpoint/重开测试。随后 HTTP 通过向量后补、来源修订、generation 基本测试，但 generation 故障重启时返回 DurablePending(15)。增加不包含文档正文的组件错误日志后，定位为 native reload 打开 term 文件时 I/O admission Busy：原生 merge/index/reload 内部线程竞争同一主体仅1个Background I/O额度。

为避免一次已接纳的 native commit 因自身并发而拒绝，本阶段为原生短文件操作增加共享串行 gate，在 gate 内获取服务 I/O 额度，释放额度后才解锁；不扩大服务额度，不在操作间保留额度。全进程串行化是明确维护成本，仍需观察对服务吞吐的影响，本任务不展开性能专项。其他非原生工作耗尽额度时仍可拒绝，不能声称消除了全部资源拒绝。新增8线程各100次读取的单主体额度回归。首次 gate 构建期间修改过 guard/lease 释放顺序，其 HTTP 仍失败；以最终重跑结果为准。

同时修正 WeightedHybrid 的零向量权重：仍执行向量计数和验证，只禁止其 RRF 贡献，保留旧实际契约。新增两条文档（一条alpha、一条beta）在零向量权重时返回1个关键词hit但总数2的写后/checkpoint/重开回归。跨包 `max(global counts)` 与旧 `sum(per-shard max)` 的布局相关计数差异尚未解决，不能将此回归泛化为完整混合计数验收。

Runtime 身份 manifest 写入改走同一个计费 Directory 的原子写入与目录同步，计入文件/累计写入预算，不再直接绕过预算用 std::File。固定 Searcher 的内存估计纳入 ID 字符串长度，读取长度行拒绝超过原生 MAX_TOKENS 的值。尚需完整核对 Analyzer、身份长期持有/序列化临时副本、merge/download/output 等额度与取消生命周期；并非本阶段已全部完成。

旧公开低层 create_index/open_index/IndexArc、Commit/Search trait、iterator 的去留已提出具体 API 选择，待答复：推荐 RecordPartition 为唯一公开存储检索入口，保留查询/评分/schema/向量配置类型，删除旧低层全文执行器；这将要求直接使用旧 Rust 接口的调用方改用 Record API，默认 HTTP 不变。未收到答复前未静默删除公开接口。Windows CI、真实S3和系统断电均未运行；没有推送、发布PR、合入或关闭issue。

本阶段最终 native 专项25 passed / 0 failed（20.03秒），已覆盖manifest计费改动。最终 gate 的 HTTP 重跑通过此前失败的generation四类故障、source及revision故障，随后在接管恢复的本地15秒就绪期限超时，未出现新的明确组件错误。没有据此宣称HTTP通过：脚本新增可选 `--startup-timeout`（默认仍15秒），当前使用60秒诊断重跑，超过5秒时打印实际READY耗时；完整all-targets同时运行。检查点若在运行中，须收取日志而不是重复启动。

## 第八阶段：计数契约、原生预算与最终代码回归队列

已收取第七阶段两个进程。HTTP基础故障套件（`--section base --faults --startup-timeout 60`）退出0，包括接管、完整服务快照恢复和客户端重试。该轮没有输出任何超过5秒的READY记录，因此60秒诊断轮并未实际消耗放宽后的就绪窗口；但这不能证明此前15秒超时的唯一原因。该轮曾与编译/测试并发，后续最终二进制必须在没有并发编译/测试的条件下按默认15秒重跑，不能修改默认期限掩盖问题。

第七阶段all-targets核心结果240 passed / 1 failed / 12 ignored，失败在 `sqlite_committed_remote_tail_recovers_before_local_journal_append` 的feed trim远端checkpoint：该夹具把完整checkpoint文件和WAL块共享上限设为32，新增Tantivy文件超出预算。该测试并非容量拒绝测试，现将其明确有限上限设为128；生产配置没有放宽。新增完整文件闭包的chunk数预计算，在任何上传之前拒绝不足额度，错误包含required/limit，并新增空文件、整块和尾部不足一块的边界测试。修正后的远端恢复专项1 passed / 0 failed（3.47秒）。

混合计数契约已核对 `docs/weighted-rrf.md`，明确保留各组件 `max(lexical_count, vector_count)` 后求和，而非全局max或精确并集。向量执行器通过内部函数返回每组件计数，不改变公开配置/返回类型；原生评分同一遍候选过滤记录各组件词法命中数。最小回归：冻结包包含仅正文alpha的记录a，实时尾部包含正文beta和向量的记录b，查询alpha并给向量，结果两条、总数2，重开仍为2。这个总数仍是既有布局相关启发式，不承诺精确并集；没有借本次替换更改评分公式。零权重仍执行/计数对应通道。vb内部诊断保留向量来源字段与词法原始分数，分页后裁掉未返回候选的解释。

生产Runtime现在在分词词典构造前预留额度，持有并复用Analyzer，避免每个写入/查询重新构造中文词典。长期身份表按增长持有全局和服务字节额度，释放时归还；新增额度增长失败不改变原额度、成功增长随owner释放的回归。manifest读取/反序列化前按文件大小预留，flush身份克隆/序列化预留独立临时空间，查询活动身份映射和term/field scorer缓冲也在构造前预留。

Tantivy自主merge改为关闭：其内部合并分配不能由当前owned build额度可靠约束。Record compaction仍通过既有owned、受预算的重建任务产生新原生包并统一认证发布，未引入新业务head。代价是tail在compaction前持有更多原生小段；固定Searcher映射仍在每次可见写入commit后重新捕获，不承诺写延迟/吞吐改善。原生I/O继续经串行gate和服务额度执行，不能将逻辑额度描述成进程RSS硬限制。download、输出与所有取消边界的完整审计仍待收尾。

向量/metadata组件没有词法字段时，现不构造旧词法词典/中文词典、不运行旧高频词warmup。原始Record配置仍交给原生Analyzer；新增English频繁词+mask9的写入/checkpoint/重开回归，断言伴随组件的frequent词典为空且原生查询正常。这是删除无效旧初始化工作的直接证据，尚不能单独断言它解释了之前那一次15秒超时。

隔离Candidate/NativePackage及原生BM25探针现在只在测试编译；生产使用Runtime与正式BM25F/Proximity，不构建这些未被调用的辅助路径。已批准删除旧公开低层Rust API，以RecordPartition统一入口；新版library-examples改为Record写入/重试/查询/checkpoint/重开，facet文档同步RecordError。旧低层执行器、公开API及旧集成测试的实际删除/迁移尚未完成，不能把cfg(test)探针隔离或文档替换当作整体清理完成。

本阶段中间all-targets完整退出0：34组汇总345 passed / 0 failed / 13 ignored；它早于末尾关闭自主merge、跳过metadata词典/warmup及查询缓冲预留。末尾原生专项27 passed / 0 failed（22.21秒），包含这些改动。为避免冒用较早结果，已启动串行最终验证队列 `target/issue80-phase8/validate.sh`：all-targets → doctests → no-default core → build服务 → 默认15秒完整HTTP → 新二进制S3 storage fixture。每阶段退出码单独写入 `target/issue80-phase8/*.exit`，`current-stage`记录当前阶段，服务SHA单独保存；任一失败即停，必须诊断后续跑。运行期间禁止重编译正在测试的同一可执行文件。

仍未运行Windows、真实S3、系统断电。旧API清理已授权，无需再次询问。尚未达到发布候选状态，没有推送/发布/合入/关闭issue。

## 第九阶段检查点：实际删除旧执行器，独立向量/元数据格式（尚未完成测试迁移）

第八阶段串行队列已结束：all-targets 34组汇总346 passed / 0 failed / 13 ignored；doctests 16 passed；no-default核心205 passed / 0 failed / 10 ignored。服务构建成功，SHA256 `74ff11f5182caebb5e33c5b86cf4a9effd9241ddfbd6a99c6e90413d50d15d5f`。完整HTTP按默认15秒运行，经过基础恢复/接管/备份等阶段后，在record_lease_smoke.py第69行过期清理状态断言失败，队列停止，S3 storage未执行。该轮失败不是15秒启动超时；不能据此声称之前的超时根因已解决。相同二进制单独执行lease故障部分通过，完整套件状态交互仍待诊断。断言现在包含fault/status以便下一轮定位。

本阶段已从源码删除add_result、compress_postinglist、doc_store、field_length、index_posting、info、intersection、intersection_simd、iterator、realtime_search、single、union共12个旧模块；index.rs保留配置与分析类型，旧公开create/open/IndexArc及Search/Commit入口改为内部组件接口；search.rs只执行内部向量检索，明确拒绝词法请求。旧倒排构建、压缩、实时/冻结全文执行路径及旧docstore不再编译。

新增独立component.rs，持有向量与metadata，写component.json、vectors.bin、metadata.bin及schema.json，不生成index.bin/docstore.bin/shards目录，不再借旧倒排level结构保存向量层级。Record checkpoint标签更新V14，认证manifest版本3；面向新库，不添加旧格式读取。修正向量末尾不完整层重建时重复累计cluster_count。活动成员集合与原生全文统计分离；保留原生BM25F/Proximity和向量量化实现。

核心测试已迁移旧Corpus/heap/字典断言到实际Record或独立membership契约；中文分词位置断言保留，中文短语检索改经Record写入/compaction/重开。核心库check成功，lib tests --no-run成功；生产运行时专项6 passed / 0 failed（1.64秒），包含实际写入→即时查询→checkpoint→重开、纯文本、新组件无旧文件、混合计数和零权重向量。该结果对应本阶段源码，不是旧服务二进制结果。

此提交是明确的中间检查点，不是可发布候选：外部集成测试仍引用已删除的公开低层API，尚待迁移，未运行新代码all-targets；旧微测试残留文件和无用依赖/文档还需清理。component描述符读取目前在读取后检查大小，必须改为读取前的大小/预算接纳；格式和取消/输出/download预算审计尚待完成。完整HTTP租约清理状态交互、原15秒超时证据及最终新二进制SQLite/S3/备份GC矩阵仍未收尾。Windows、真实S3、系统断电仍未运行。检查点时没有在运行的测试/构建进程；未推送、发布、合入或关闭issue。

## 第十阶段检查点：外部测试迁移恢复编译，清理旧依赖，运行回归中

已把外部集成测试的旧公开创建/写入/检索/删除接口迁移到RecordPartition。测试辅助common/records.rs仅调用公共Record写入、版本条件删除/更新与统计，不实现旧引擎兼容层。实际全目标 `cargo check -p anyseek --all-targets --features server,fault-injection --keep-going` 退出0，修复了上阶段整体无法编译的状态。随后删除残留旧模块微测试、清理依赖后启动全目标运行，已完成编译并进入测试。

迁移保留N-gram字面公式、BM25F/Proximity、NOT、规范化/原文、稳定排序、RRF、向量精度/量化/阈值、元数据过滤、删除计数及重开回归。Record查询结果使用稳定ID和规范Payload；错误用Result；已成功写入的可见性不取决于旧include_uncommitted参数。原Record的active::scoring_df在活动集合存在时已覆盖物理压缩DF，因此100/104/105条评分边界现在断言精确活动DF，而非旧低层96/104量化值，生产公式未改变。canonical Payload即使store:false或字段不在派生schema也保留原文；HTTP投影应由HTTP验收覆盖。

旧65,536倒排块、Bitmap内部编码和70k物理ordinal夹具不再假装验证已删除编码；迁移保留独立成员集合/排序/计数/公式，缩小为Record限额内数据。真实向量物理块边界仍由核心record_incremental_crosses_physical_block_with_retired_history覆盖。旧clear复用ordinal的测试尚需按稳定ID不复用审查运行结果；不能把机械迁移编译通过视作行为验收。完整有效覆盖的逐项复核仍在进行。

component.json在读取前通过metadata限制64MiB，并在读取/解析前预留保守8倍解析空间的全局与服务额度；有界读取检测文件长度变化。长期组件内存、I/O及取消/output/download完整审计尚未结束。物理删除了已取消编译的scoring_buffer_tests、scoring_order_tests、field_length_tests、warmup_cache_tests、commit_io_tests；去掉snap、smallvec、async-recursion直接依赖。更新公共模块说明、FEATURES、字段长度与持久化格式文档；历史实现记录按阶段保留。

实测：第一组迁移测试共11 passed / 0 failed（N-gram DF2、flags3、Proximity1、NOT1、规范化4），日志 `/tmp/anyseek80-phase10-text.log`；随后又有清理改动，因此仍在最终全量重跑。当前全量日志 `/tmp/anyseek80-phase10-all-tests.log`：核心229 passed / 0 failed / 12 ignored（105.15秒），服务15 passed / 0 failed（2.14秒）。检查点时外部bitmap_delete_consistency仍运行，不能宣称全部外部回归通过。命令 `CARGO_INCREMENTAL=0 cargo test -p anyseek --all-targets --features server,fault-injection --no-fail-fast`；session96805，cargo PID203821，当前测试PID210346。下一阶段先收取结果，不重复启动或覆盖正在执行的测试二进制。

构建曾因32GiB盘耗尽失败。只清理本独立工作区target/debug/incremental（约11GiB）和本次已生成替代版本的56个旧可执行缓存（约15.7GiB），保留日志/检查点及当前测试二进制。清理清单 `/tmp/anyseek80-phase10-cache-cleanup.txt`；没有触碰旧任务工作区或部署数据。

HTTP尚未用本轮二进制复现。阅读lease清理路径确认publish_anchor_state在healthy时失败可留下pending并被吞掉，需要下一轮记录具体错误、复现全套状态交互；这只是诊断线索，未确认根因，未放宽断言。先前15秒恢复超时也未闭环。下一步收取全量运行并修复真实失败，检查剩余测试旧状态预期，再完成生命周期审计、doctests/no-default及新服务二进制默认15秒完整HTTP和S3矩阵。Windows、真实S3、系统断电未运行；不发布、推送、合入或关闭issue。

## 第十一阶段检查点：HTTP额度根因与空尾部writer修复

先收取并保留第十阶段全量运行。核心229/0/12 ignored、服务15/0保持有效；密集删除两项通过（922.09秒）、配置统计1/0、删除计数2/0、外部向量3/0、规范Payload读取2/0、混合stored/nonstored邻接读取1/0。旧clear测试1失败已证明是迁移预期错误：Record不会把新插入的稳定ID复用为旧ordinal。新版直接验证Match(1)对已删除版本冲突，新ID记录仍存在；Absent按原mutation契约表示逻辑不存在，包括墓碑，允许操作但保留递增版本（返回3），不会重置身份。随后精确删除新ID，验证其余记录及4个保留身份。新版4项删除专项通过，不修改生产身份/版本语义。

另有真实失败不能忽略：frequent_topk_consistency在第2299次写入返回DurablePending(2299)，耗时559.34秒。该失败发生于写入，尚未到Top-K断言；根因未查清，可能涉及原生文件/累计写入预算，但目前只是待证实线索，不能缩小夹具或修改期望掩盖。旧全量进程继续执行lexical_correctness，尚无总结果。

HTTP完整故障套件用新组件二进制两次复现lease after_append_unknown：恢复version=2、staged_members=0正确，但private_cleanup_pending=true。新增healthy publication失败的warn日志和测试失败时服务日志尾部输出。第一次日志被执行环境继承的RUST_LOG error过滤；显式RUST_LOG=anyseek=warn,anyseek_server=warn重跑后，得到Storage("resource budget exhausted")，不是业务事件丢失或启动超时。

增加无正文/无subject身份内容的Governor拒绝额度日志，进一步复现得到精确原因：Background已用786,913,140字节，再请求20,000,000字节，超过默认805,306,368字节全局上限。所有测试保持默认15秒启动期限和原断言，没有扩大生产额度。诊断日志 `/tmp/anyseek80-phase11-http-budget.log`；此前诊断二进制SHA记录在 `/tmp/anyseek80-phase11-budget-server.sha256`。

最小核心复现：同一默认Governor创建39个空collection，保留owner，仅向其中一个写入一条记录，再checkpoint；修复前0 passed / 1 failed，checkpoint返回相同Storage资源拒绝（0.53秒）。旧Runtime在空尾部也提前启动并保留20MB writer额度。修复为首次实际append之前接纳并创建writer，空尾部flush仍发布空manifest/固定Searcher，immutable组件仍禁止append。writer额度保存在Directory共享owner中，原生线程持有的clone在取消/错误/Runtime释放时继续保有额度，Runtime析构仍join原生worker；不是去掉或扩大额度。最小复现现在通过，原生专项29/0（22.98秒）。它解释并修复已确认的空尾部资源浪费；完整HTTP仍在修复后二进制重跑，不能仅据此宣称HTTP全部通过。

资源审计另外修正冻结包重开绕过Directory的问题：Runtime不再直接Index::open_in_dir，认证选择后的Directory检查文件/总字节边界，保留JSON元数据解析空间额度，并计费I/O；拒绝写入、删除和writer锁，允许读者meta锁。新增不可变读包/超限边界测试。component描述符读取增加I/O接纳；超64MiB稀疏描述符读取前拒绝测试1/0，未修改文件或生成组件。保留既有固定Searcher/pin、活动统计及输出屏障。

本阶段源代码无默认feature核心完整192 passed / 0 failed / 10 ignored（79.67秒），doctests 9 passed（1.96秒），新版身份删除4/0；日志 `/tmp/anyseek80-phase11-nodefault-all.log`、`/tmp/anyseek80-phase11-doctests.log`、`/tmp/anyseek80-phase11-stable-delete.log`。这些对应本阶段lazy writer与只读Directory改动。默认全量仍是第十阶段二进制，不能混用为本阶段全量证据。

检查点时两个运行队列须继续收取而非重启：原session96805/cargo203821（当前lexical_correctness PID321399），日志 `/tmp/anyseek80-phase10-all-tests.log`；修复后完整HTTP session48349，日志 `/tmp/anyseek80-phase11-http-fixed.log`，SHA `/tmp/anyseek80-phase11-fixed-server.sha256`。先前单次15秒就绪超时仍缺直接根因证据；本阶段复现的失败是资源清理拒绝，不将二者混称。S3最终新二进制矩阵、默认最终回归、完整预算/生命周期审计仍待完成；Windows、真实S3、系统断电未运行。未推送、发布、合入或关闭issue。


## Phase 12 — native tail maintenance and idle component reservations

Baseline: `e8f5d9d85a46bd8e991d7d394f76b1996ecff395`, independent
`issue-80/tantivy-new-format` branch. No remote publication.

The obsolete phase-10 all-targets run (cargo 203821, lexical child 321399)
was identified by executable/parent before stopping only those processes;
`/tmp/anyseek80-phase12-stopped-old-run.txt` records the action. Its results are
historical, not final-code acceptance. Phase-11 HTTP session 48349 completed
with `crash_after_visible` private cleanup pending and a Record build resource
budget rejection; lazy native writers alone did not resolve the full suite.

A direct native reproduction with a 1 MiB journal-derived budget fails after
349 visible writes with `native writer byte budget exceeded` in a segment term
file. This confirms cumulative metadata rewrite traffic can exhaust the native
budget despite a small live directory. The historical 2299 failure had no logger,
so its exact first error cannot be recovered from that old log. New diagnostics
record requested/cumulative bytes, cap and file count without document contents.
The original 3000-record fixture is preserved at its original size in source.

Production now requests an ordinary certified anchor rotation before WAL append
when the mutable native tail has published 128 updates. Bulk rebuilds commit once;
Tantivy commits still do not select business state. Existing Record publication,
pins, recovery and GC select the replacement. Generation publication remains one
business operation (maximum 100 members); its admission fanout needs a final
boundary audit, including a generation beginning just below the rotation boundary.
No byte/file/worker budget was raised. A retained fixed Searcher continues to own
its original cut. Successfully joined writers release their 20 MB reservation even
if reader Directory clones remain; a failed join conservatively retains accounting.

A 65-idle-collection regression reproduced shared build-budget exhaustion. Each
component retained a legacy 64 MiB posting-builder scratch allowance although that
builder and allocation have been removed. Removed that obsolete reservation;
component schema/document admission and independently owned native writer,
analyzer and reader reservations remain. The same 65-collection write/checkpoint/
query test now passes. Global 4 GiB and per-reservation 256 MiB limits are unchanged.

Evidence so far (all logs under `/tmp/anyseek80-phase12-*`):
- `native-budget.log`: direct finite-traffic reproduction, 1 pass, 16.23 s.
- `idle-before.log`: failure before obsolete reservation removal;
  `idle-after.log`: 1 pass, 0.69 s afterwards.
- `rotation-retry.log`: 1 pass, 5.86 s. Certifies rotation before write 129,
  injects DurablePending, fences output, reopens, retries original operation ID
  twice with identical seq 129/version 1; 129 matches and original 128-identity
  Searcher retained. This is an injected recovery proof, not recovery of the
  historical unlogged 2299 failure directory.
- `frequent3000.log`: original 3000 writes and Top-K/count assertions pass,
  138.58 s. This binary includes rotation and reservation fix but predates the
  subsequent joined-writer release edit; final all-targets must rerun it.
- New full HTTP (default readiness deadline 15 s) and S3 storage fixture/fault
  suites use server SHA256 `ae607146fa981b5a5ca24a1d4f98633e38cd8c08d1d239eb8def8ef32a2f666f`.
  Their final results and current no-default lib result are pending below.

Remaining: collect these suites; final default all-targets/doctests/no-default
matrix; boundary/resource lifecycle audit (including generation fanout); documentation
and draft PR review. Windows, real S3, and system power loss have not been run.
Process exit fault fixtures do not substitute for those platforms/failures.

Phase-12 completed results: no-default library 194 passed / 0 failed / 10 ignored
(105.09 s); new-binary S3 storage fixture/fault suite all requested checks passed
(152.019 s), report `/tmp/anyseek80-phase12-s3-report.json`. Includes checkpoint
and tail cold recovery, SQLite publication failures, original receipt replay,
takeover boundaries, feed floors, joint backups and service archive restore.
`real_endpoint_exercised=false`; this is not real-S3 or system power-loss evidence.

No-default doctests: 9 passed, 0 failed (1.88 s). Full HTTP session 83044
(Python PID 362221) is still running; no final success is claimed for that suite.


## Phase 13 — whole-generation admission and worker ownership

Phase-12 full HTTP session 83044 completed successfully (exit 0), including lease
cleanup and all process fault boundaries, using the recorded `ae607146...` binary
and the unchanged default 15-second startup deadline. The earlier one-off startup
failure has not been proven to have the same cause as the budget rejection; this
is successful end-to-end evidence, not a retrospective root-cause claim.

Native admission now counts every member of a pending generation Publish before
WAL append. `current_updates + incoming_updates > 128` requests one certified
rotation, so a generation cannot overshoot the mutable-tail commit bound. Stage,
renew, abort and deletion do not invent native inserts. The regression starts with
127 native updates and publishes 100 members, exercises normal and injected partial
publication, verifies pre-WAL rotation, fenced output after failure, full 100-member
recovery, and original operation ID replay at seq 129/version 2. Passed in 25.66 s.
Its fixture uses 64 KiB maximum request bodies and 16 MiB journal capacity to admit
a valid 100-member stage under existing checkpoint-reserve validation; production
limits and validation are unchanged. Initial smaller fixture configurations were
correctly rejected by request/checkpoint budgets rather than silently bypassed.

Native Directory clones now own the package pin as well as byte leases. This
closes the failed-join boundary: a worker that outlives Runtime cannot lose its GC
protection. Successful join frees writer memory without waiting for a fixed
Searcher to disappear. New tests check the live-writer reservation versus the
joined-writer release while a Searcher remains, and a detached Directory clone's
pin/reservation lifetime. Existing owned-builder cancellation and selected-reader
GC tests remain. Native merges remain disabled; bounded Record reconstruction is
the only merge/publication mechanism.

Audit evidence: `storage/s3_compatible.rs::get_verified` admits I/O and twice the
reference length before cache/network reads, checks committed lengths and hashes,
and returns a lease-owning Buffer; `object_log` reserves decoded/flattening storage
before parsing. HTTP `admission::encode` bounds serialization; response/frame
owners retain Scope, and authorization plus data output permits are checked on
handoff. `record/indexer` owns the partition, semaphore, snapshot/tail buffers and
candidate through completion even if the caller drops its job. These are logical
explicit-buffer/operation budgets, not allocator or process RSS limits.

CI no longer invokes deleted `commit_io_tests` (which silently matched zero tests).
Linux and Windows persistence jobs now invoke native lexical, independent component,
public Record integration, no-default core and doctest suites. Job deadlines were
extended for the added suites; no HTTP deadline or behavioral assertion was relaxed.
The 3000-record regression now uses an isolated temporary directory and drops the
partition before cleanup, preserving all 3000 writes and expected counts while
avoiding a Unix-only shared path. Windows execution remains pending publication/CI.

Final-code service binary copied to `/tmp/anyseek80-phase13-server`, SHA256
`c8e32b37efc453e38df28477c57a1ecc919d87ca041059086cfd3576b11edc66`.
Full HTTP, S3 storage fixture/faults and all-targets have been started against this
source; no final result is claimed until their logs complete. Isolated standalone
consumer dependency/build checks pass with default and no-default features.


Phase-13 completed results (production sources unchanged since the fixed binary):
- Default core: 237 passed / 0 failed / 12 ignored, 166.83 s; server: 15 passed,
  2.39 s. The encompassing all-targets run continues with integration fixtures.
- No-default core: 197 passed / 0 failed / 10 ignored, 132.87 s.
- Default and no-default doctests: 9 passed each (2.43 s / 2.34 s).
- S3 storage fixture/fault matrix: all requested checks passed, 189.818 s,
  `server_sha256=c8e32b37...edc66`; object contract fixture separately passed.
- Additional HTTP Indexer and authorization sections both exit 0, verifying owned
  builds, concurrent acknowledged writes, source deletion, scoped revocation and
  restart. Full HTTP and seven-group S3 cold-cache semantic results still pending.
- Isolated core dependency/build and Markdown/branding checks passed. Baseline-wide
  `git diff --check` now passes after removing only added trailing whitespace in
  migrated tests; assertions, inputs and production code were not changed by that
  cleanup. The executing tests were compiled before this whitespace-only cleanup.

The default core's twelve ignored entries are eleven explicit child-process helpers
(invoked by their parent crash/cancellation tests) plus the endpoint contract test;
the latter was explicitly run against the fixture, not a real S3 endpoint. Logs:
`/tmp/anyseek80-phase13-ignored-core.log` and `s3-contract-report.json`.

Phase-13 full HTTP session 82612 subsequently completed with exit 0, unchanged
15-second default startup limit, and all listed process fault/lease/feed scenarios.
Generated OpenAPI parses successfully and contains 28 paths. The remaining active
acceptance runs are external all-targets integration tests and the seven-group
S3 cold-cache semantic fixture; no Windows/real-endpoint/power-loss claim is made.

The final writer/pin/generation-admission implementation has now passed the original
3000-record frequent-term integration fixture in all-targets (141.44 s), including
unchanged membership/count/Top-K assertions. Bitmap deletion/count (2 tests,
281.68 s), stable deletion identity/version (4), external vectors (3), core info,
count/delete and stored/nonstored payload regressions also passed. All-targets is
currently in `lexical_correctness`; the complete suite result remains pending.
S3 cold-cache semantic base and locator groups have passed; remaining groups run
under the same fixed binary. The only edits after these binaries were built are
Markdown/Rustdoc clarification and added-test trailing-whitespace cleanup; no
runtime behavior, fixture data or assertions were changed by those edits.

Retained `DocumentCompression::{None,Snappy}` values are serialized configuration
labels, not encoding selectors for the new Payload/native packages. Public docs
now say this explicitly rather than advertising a removed Snappy docstore. No
additional compatibility engine or old-format reader was introduced.
