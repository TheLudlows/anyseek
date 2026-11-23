# 最终构建的异步写入对照

**简体中文** · [English](ASYNC-PROTOCOL.md)

此工具对比一个固定 AnySeek release 与 Meilisearch。项目路线中的 AnySeek 优化前后实验另行执行。待所选实现检查通过后运行 `async_compare.py`，记录实际 release 二进制哈希与源码修订。原始数据、数据库、日志和下载二进制应写入仓库外的新目录，保留已有运行结果。

两个服务均使用经验证的 `prepare.py` 10,001 文档 MIRACL 样本的前 2,305 条记录和全部 393 个冻结查询。这是富集相关记录的小语料，不代表全语料质量或容量。两个服务串行运行在 CPUs 0–1，客户端运行在 CPUs 2–3，使用相同的现有 cgroup CPU 与内存限制。测量期间不能同时编译或运行其他基准。保留至少 4 GiB 可用磁盘。共 145 批（144 批各 16 条，一批 1 条），跨越十六个 128 次更新的可变 tail 边界，意在触发自动派生缓存合并。tail/checkpoint 策略可能变化，不能仅凭文档数认定覆盖。必须观察到合并捕获、构建、安装及清理事件。缺失事件属于验收缺口，不能计为零成本。工作负载仍是小规模有限样本。

每批提交 16 条相同文档。AnySeek 仅在 HTTP 200 的每项都带 committed Record 回执时记为业务完成。分别测确认（含 Busy 等待）及随后索引水位完成。批次切点是返回回执的最大序号，全文和向量水位都须达到。显式 429 重试保留所有原始身份，包含已提交的项目。未知结果退出本轮，不能偷偷生成新身份。索引时长来自轮询，不是精确内部时间戳。核查原操作可查到各回执，再用代表性标题检索并验证可见文档数。Meilisearch 分别测 HTTP 202 受理和任务成功，并检查可见数及代表性查询。202 不作为业务完成或可检索。轮询粒度计入完成延迟，AnySeek 为 5 ms，Meilisearch 为 10 ms。工具没有对应的独立 Meilisearch 水位，所以其业务和索引完成观察使用同一成功任务值。这源于 API 差异，不能证明 AnySeek 两阶段等价。水位或任务观察不能替代最终代表性查询。摘要字段为 `batch_business_completion_observed` 和 `batch_index_watermarks_observed`。兼容字段 `batch_published_completion` 保留后者值，不单独测得可检索时刻。

记录批次受理和完成 p50、p95、p99，总加载墙钟时间、批次累计完成时间、观测峰值 RSS、查询延迟与空结果数、可用的 checkpoint 时间、重开数量以及数据库磁盘大小。`reopen_health_ms`（亦保留为 `reopen_ms`）截止健康端点。`reopen_verified_count_ms` 单独计启动加首次计数查询及预期数量核验，含延迟分区恢复。计数后重放一个此前成功的代表性查询，并断言原文档 ID 仍返回。摘要只留验证布尔值及 `reopen_verified_search_ms`（启动至计数和代表性查询核验完毕），查询及命中 ID 保留于私有原始事件。这只验证重启后一个词法查询，不验证完整查询正确性或向量恢复。健康与计数不能证明搜索。串行批次写入速率不代表饱和吞吐。单批完成区间不计逐操作回执核对，总加载时间计入。峰值 RSS 每 100 ms 采样，可能漏掉短峰值。

查询字段为 title/text。AnySeek 显式使用 JiebaMixedV1 和 AND，Meilisearch 使用 all，并关闭容错与前缀。分词边界和评分不同。查询延迟不能作为质量排名，尤其大量查询无结果时。脚本在本地保留全部原始结果 ID。此工具的 Meilisearch 无等价逐索引 checkpoint，任务成功和进程重开分别报告。

固定二进制为 Meilisearch Community v1.56.0 的[官方发布](https://github.com/meilisearch/meilisearch/releases/tag/v1.56.0)，资产 `meilisearch-linux-amd64`，357,300,520 字节，SHA256 为 `5a32e1d7a6a69e0ee5634a7456c421f59273d73f078b6f0d8f01384eafe79729`。下载、样本、日志和新库预算最多 2 GiB，独立于已保存的编译与测试二进制。前 2,305 条编码后为 1,277,744 JSON 字节，不含请求信封，最大一条 2,986 字节。341 MiB Meili 二进制与 4.3 MiB 冻结样本使 2 GiB 规划中还剩超过 1.5 GiB。工具在批次间检查运行目录已分配大小，超过 1.5 GiB 停止并保留数据。这是保护条件，不能硬限制单批短暂峰值。此前保留的测试程序和日志不计增量预算，不能删除。

补充 Debug 维护插桩须在同一最终源码上单独运行。`ANYSEEK_TEST_PHASE_TIMINGS` 记录具名区间，只有明确持锁的区间可称为持锁观测，不能累加嵌套区间。checkpoint HTTP 时长不等于 Record mutex 持有时长。模拟 S3、真实云 S3 和物理断电证据彼此独立。

```bash
python3 benchmarks/capacity/async_compare.py \
  --anyseek /path/to/final/anyseek_server \
  --meili /path/to/meilisearch-v1.56.0 \
  --data /path/to/data/corpus-10001.json \
  --out /path/outside/repository/new-run \
  --revision FINAL_REVISION \
  --expected-anyseek-sha256 FINAL_BUILD_SHA256 \
  --take 2305
```

仅显式 HTTP 429 `code=busy` 以完全相同的请求与操作 ID 重试，退避 50 ms，期限 180 s。保留每次尝试。传输失败、未知结果及其他拒绝码停止。受理墙钟延迟与批次完成延迟均含 Busy 尝试和退避，成功 HTTP 请求耗时单列。checkpoint 墙钟时长也包含全部 Busy 等待，并记录尝试及拒绝数。测量全过程保持后台自动合并。

工具在启动前核验最终二进制负责方提供的 AnySeek 二进制 SHA256，并拒绝将原始输出写入仓库。完成运行不保证覆盖维护。检查 AnySeek 汇总中的 `required_maintenance_observed`、`missing_required_maintenance_phases` 和各项 `maintenance_coverage`。默认本地负载需要观察到以下 info 日志区间。

- Record 持锁阶段为 `cache_merge_capture_locked`、`cache_merge_install_locked` 和 `cache_merge_cleanup_locked`。
- Record 锁外阶段为 `cache_merge_build_unlocked`。

`maintenance_phases_by_lock_scope` 分开报告持锁、锁外和未分类区间，后者不对锁范围作判断。缺失的必测覆盖用 `null` 明示，不带延迟统计，不计为零时长。区间可能重叠，错误或取消也会打结束日志。观察一个区间不能证明合并成功。显式 journal Compact 可能出现 `compact_tail_handoff_locked` 和 `compact_native_membership_capture_locked`，在 `optional_explicit_compact_coverage` 中报告，缺失为 `null`。本工具的 HTTP PATCH 调用 `JobMode::Index`，这些有条件 Compact 阶段并非默认必测项。声称相关持锁时间前须另做显式 Compact 实验。Debug 测试阶段计时只作补充回归证据，不能当作 release 性能值。

工具显式为服务设置 `RUST_LOG=error,anyseek::record::maintenance=info`，因环境继承的仅错误过滤会屏蔽维护证据。这只调整日志。失败时保留部分受理和完成延迟、受理记录数、已核对回执和批次数、最后观察到的提交和索引水位。不得把部分结果写成已完成的配对比较。
