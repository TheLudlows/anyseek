# 资源预算与并发准入：实现检查点

基线是已合入的 `6d28798a300bb947b2eb1d3e48e928d193103e17`。实验报告单独位于草稿PR #82；本实现不改匹配语义、记录身份/操作账本上限，不覆盖原始benchmark证据。

## 已定位的拒绝

- 中文配置：在第385次文档写入前，Governor后台collection字节已用357,579,584，申请67,174,400，超过402,653,184上限。申请值对应中文Analyzer的64MiB+64KiB保留预算；轮换同时保留旧视图及私有候选。旧配置重复相同operation_id拒绝5次，used/请求值始终一致，可见记录始终384，没有观察到该拒绝路径逐次泄漏。此结论不等于证明所有路径无泄漏。
- 默认分词：第2177次写入触发全量构建，单份申请390,894,208超过268,435,456。与全部2176份文档的保守document_estimate合计一致；明确是单份限制，而非共享计数器耗尽。此估算是既定保守逻辑上界，不是RSS；本次不借调参删除估算保护。
- 4查询+1写入：旧集合任务上限4，63次HTTP429 Busy；另2次Broken pipe发生于拒绝后复用连接。读取body前拒绝现在对HTTP/1明确关闭连接，HTTP/2不发送连接级header；下述强制超限HTTP实验验证了该路径；不声称所有输出撤回时的连接错误均已消除。
- 原来继承的RUST_LOG只允许error，所以旧release日志无warn；本轮诊断显式启用warn。保留原空日志，不把新增诊断冒充原日志。

## 有限调整

|预算|旧值|新默认|
|---|---:|---:|
|Foreground全局/集合slots|32 / 4|32 / 16|
|Partition任务上限|8|16|
|Foreground全局/集合bytes|1024 / 256MiB|2048 / 512MiB|
|Background全局/集合bytes|768 / 384MiB|4096 / 2048MiB|
|单份构建预留|256MiB|1GiB|
|共享构建预留|4GiB|4GiB（不变）|

中文旧/新视图各可含多个分析器，2GiB集合后台额度留出轮换峰值余量；默认语料5000份的完整估算约806MiB，1GiB单份允许该规模但不承诺10000份。16个集合任务容纳原8查询以及额外写入，仍受全局32和partition16限制。任务、buffer、文件/I/O和控制容量各自约束，不引入无限队列，不提前退款取消任务。

新增 `ANYSEEK_BUILD_MEMORY_BYTES` / `ANYSEEK_BUILD_SHARED_MEMORY_BYTES`，进程固定，校验64MiB <= 单份 <= 共享 <= 8GiB。已有 `ANYSEEK_{FOREGROUND,RECOVERY,BACKGROUND,CONTROL,PUBLIC}_{SLOTS,PER_COLLECTION,BYTES,PER_COLLECTION_BYTES,IO,PER_COLLECTION_IO}` 继续有效：均为正数，集合额度不超过通道全局额度；集合bytes至少18MiB，通道bytes至多8GiB，slots和I/O至多64，前台+恢复+后台slots总和至多48、控制slots至多8。Partition内部16任务上限仍是固定安全界限。默认预算依据本机16GiB容器做有界验证，逻辑预算不是进程RSS硬上限；部署必须考虑运行时、State和其他集合的额外开销，小内存机器应显式降低配置。

错误详情保留分类、used/requested/limit，HTTP Busy和提交语义不变；控制通道和授权输出屏障未放宽。测试按新上限真正占满额度再检查拒绝/保留/撤权，未删除断言。

## 总额与部署风险

**默认账目额度算术和约10.78 GiB，不是RSS上限，不能保证小内存机器安全。**
Governor各通道全局bytes为2048+512+4096+144+144=6944 MiB（6.78 GiB），加独立共享构建4096 MiB，共11040 MiB。账目可能重叠，且并未覆盖全部State、运行时、分配器、mmap和OS缓存；因此不能用相加结果预测实际RSS或保证不会OOM。每通道合法配置上限也不等于全机安全容量。

实验环境的cgroup内存上限17179869184字节（16 GiB）、CPU配额4、无swap；性能测试中服务和客户端各固定2个CPU，串行运行各产品。仅验证所列单集合负载，未验证各通道/集合同时满额。全量编译时容器接近内存上限，主要是文件缓存；累计oom/oom_kill为0。这是整个环境的历史计数，不是单次benchmark RSS或小内存安全证据。小内存部署须显式降低额度并验证负载。

## 验证状态

- cargo check通过；资源单测5项、构建预算6项、服务单测16项通过。独立release进程的8项非法配置（解析、上下界、层级和控制预留）均在开始服务前拒绝，详见 `scripts/verify_resource_configuration.py`；显式有效低额度覆盖见强制Busy实验。
- 最终默认核心239通过/12忽略；无默认核心199通过/10忽略；默认和无默认doctest各9通过。默认忽略项为11个由父测试调用的子进程worker和1个需要专用真实S3环境的测试；无默认忽略项为10个worker，没有新增跳过。
- 普通release完整HTTP/restart验收通过；启动仍采用原15秒边界。取消waiter、实际writer/directory所有者与保留准入额度测试通过。
- 初轮核心3个失败来自旧预算夹具；调整为实际占满新额度，并保留拒绝、WAL前原子性、归还和I/O断言，定向及上述核心重跑均通过。
- S3首次误用普通release，测试时钟未编译导致8天推进失效，过期cursor期望410但得到200。原日志保留；按CI构建独立fault-injection服务后，S3全矩阵9组全部通过（对象契约、base/locator/graph/temporal/upload/lease/feed冷缓存语义、checkpoint/备份/takeover），退出码0，原410断言通过；未改断言或生产时钟。首次启动all套件另遇PATH缺少cargo，补齐环境后运行，原失败日志同样保留。最终报告 `s3-all-fault-env.json`，二进制SHA256 `5dd8a8d90f43cddb515039d4dcf6824633c51b5d4388f967584df1fab849ac22`。
- 最终Linux `cargo test --locked -p anyseek --all-targets --features server,fault-injection -- --test-threads=1`：343通过、0失败、13忽略，34个测试程序，退出码0。测试源码为 `f71594f938d9e6d61b820af09f681700f12dcdb1`，生产代码对应 `62135f2`；随后只改本文和README。13忽略为11个由已通过父测试调用的子进程worker、1个专用真实S3测试、1个要求相反AES构建的手工测试。未缩小语料或调整断言/超时。
- 报告PR #82 HEAD `230d63f3bab42c26b3452c1e0d86090f6fee5650` 的[CI 40106616646](https://github.com/TheLudlows/anyseek/actions/runs/40106616646)已9/9成功，仍为未合入草稿；它仅覆盖原生产代码和报告，不能冒充本预算实现的Windows验收。
- 本预算分支Windows CI、真实S3、硬断电和手工跨AES仍未测。

本分支未发布。#80的关闭不替代本轮资源验证。日志与结构化结果位于本工作区 `artifacts/benchmark/validation-final/`。

## 同口径实验结果（本地 release）

二进制SHA256 `b93f113f0a1e689fee21b528430062120c859cbe7085a8058c061e18e8b31921`；生产源码对应62135f2，编译后仅修改cfg(test)旧额度夹具及本文。4CPU quota/16GiB容器，每次单服务CPU0–1、客户端2–3；相同固定语料、40问句、两遍预热、并发1/4/8各三轮5秒。RUST_LOG=error与原实验一致，未并行编译。不是严格冷热缓存比较，也不据全空结果给引擎质量排名。

|配置|已接受 / 目标|构建至可搜索秒|流程RSS峰值MiB|并发8错误率|
|---|---:|---:|---:|---:|
|旧default 1000|1000 / 1000|14.47|113.9|88.28%|
|新default 1000|1000 / 1000|15.35|118.5|0%|
|旧中文1000|384 / 1000|未完成|234.5（部分装载）|未测|
|新中文1000|1000 / 1000|20.43|661.4|0%|
|旧default 5000|2176 / 5000|未完成|166.2（部分装载）|未测|
|新default 5000|5000 / 5000|122.57|310.2|0%|
|新中文5000|5000 / 5000|160.32|1021.4|0%|

新四组均完成checkpoint、重开、数量及代表原文核验。不能将旧部分装载峰值与新全量峰值当作相同规模的内存回归；中文多个字典的保留成本依然存在，本次没有实现共享字典优化。

|default 1000|旧QPS中位数→新|旧P95→新P95 ms（成功请求）|
|---|---|---|
|并发1|630.7 → 609.8|2.14 → 2.10|
|并发4|743.5 → 769.7|6.89 → 6.56|
|并发8|661.8 → 764.1|9.70 → 12.28|

并发8现在接受全部请求，成功延迟增加；旧结果丢弃了大量请求，不能只比较成功延迟断言性能倒退或优化。新default/中文5000并发8分别约107.5/119.2成功QPS，P95为84.12/79.10ms，都是相同自然问句的全空结果负载，不能解释为一般中文搜索性能。

### 写入共存与强制拒绝

原定4查询客户端+每秒2次更新共100次，均用独立1000条根；两轮均无自动写重试，以保留原口径：

- 旧：35次确认成功、63次429、2次Broken pipe；新：100次确认成功，独立重开后100次更新均存在，无未确认但持久化更新。双方读写都持续取得进展，不等于任意过载下无拒绝。
- 旧查询：2001次recovery_required、1604次Busy、290次传输错误，总错误率10.16%；新：8177次recovery_required、1次连接提前关闭，总错误率21.43%。成功QPS 689.1→599.6，P95 7.35→8.49ms。新配置实际完成的写入更多，写入窗口输出屏障的拒绝也更多。
- 精确响应：8177次HTTP503均为 `{"code":"recovery_required","message":"RecoveryRequired"}`，无403；其请求时间窗全部与成功写入HTTP时间窗重叠，没有窗外错误。普通MIRACL文档更新没有generation/system/关系引用，不满足`hides_evidence`，不会递增隐藏证据revision；所有写入仍会在append到publish期间临时设置`output.fenced`。因此证据支持临时数据fence解释，不能统称授权撤销或版本变化。
- `query`捕获`output_permit`、`protected_data`和`delivery_gate`都可能返回同一个503；原日志没有逐请求服务端callsite，不能进一步给8177次分摊精确触发点。`AuthorizedBody::poll_frame`在更晚的交付检查失败时返回body错误，可能中断连接。实际唯一传输错误为`Remote end closed connection without response`，0字节，7.25ms，与第90次成功写入重叠；这是候选解释而非已证实来源，不能宣称所有连接错误消失。
- 未修改输出屏障。时间重叠由同一客户端`perf_counter`起止记录计算，并不等同服务端链路跟踪。原始分类及复核结果见 `validation-final/output-memory-diagnosis.json` 与 `validation-final/output-write-overlap.json`。
- 显式设 `ANYSEEK_FOREGROUND_PER_COLLECTION=1`，独立20条根、8客户端3秒强制超限：3429成功、8566次429、0传输错误，拒绝后下一次写入applied。证明“拒绝未读取body后明确close”路径的连接行为；不替代所有输出撤回场景。

原始新证据：`artifacts/benchmark/comparison.json`、`raw/adjusted-*/`、`raw/adjusted-update-verification/verification.json`、`raw/adjusted-forced-busy-http/verification.json`。
复算脚本 `scripts/summarize_resource_benchmark.py --baseline <旧artifacts/benchmark> --adjusted <新artifacts/benchmark> --output <输出JSON>`；不会覆盖原测量。
