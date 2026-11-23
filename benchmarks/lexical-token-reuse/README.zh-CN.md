# 全文词项复用的本地证据

**简体中文** · [English](README.md)

生产源码基线为 `ed3052f2afdbcf18392a76a4bd3816e4e96275a1`。候选为本报告中的词项复用实现。[results.csv](results.csv) 每行均记录二进制 SHA256。

确定性结果是每个可见行的字段仅分析一次。基线一次性回归在交接前观察 9 次，交接后 18 次，候选一直为 9 次。每个 24 条记录的负载中，基线观察 48 次，候选 24 次。查询编译不增加该实例计数器。

## 测量与限制

这是内部 `RecordPartition` 从写入至原生交接的微基准，含 WAL 与索引工作。它**既不是独立分析器计时，也不是端到端 HTTP 吞吐**。本实验不测生产容量。

- Linux x86-64/glibc、Rust 1.99.1、cgroup 配额 4 CPU 和 16 GiB。进程绑定 CPUs 0–1，Tokio/Rayon 各 2 线程。共享宿主，本地文件系统。见 [environment.json](environment.json)。
- 优化的**测试**二进制使用 opt-level 3、关闭 LTO、16 codegen units、默认 features 加 fault-injection，以及测试 unwind 行为。与生产 release/LTO/panic-abort 不同。测量期间不并行运行 Cargo/rustc/AnySeek，工具持有 `/tmp/anyseek-performance.lock`。测得 cgroup 节流差值为零。
- 每个新 partition 先写一条预热记录，再做 24 次不同单条插入。两版本的固定正文（`ALPHA beta gamma delta 臺灣搜索 中文分詞 ` 重复 128 次）、二维辅助向量、profile、归一化和持久性参数一致。Ngram mask 127 显式用 alpha/beta/gamma 作为高频词，回归核验实际三个位置的 gram。
- 单调用方，每四次写入及结束时等待原生交接。`records/s` 包含等待且排除随后验证查询。单条请求时 requests/s 等于 records/s。ACK 为各 `write()` 的耗时。CPU ticks 包含写入、交接**及随后一次验证查询**，分辨率 10 ms。查询后采 RSS，进程 HWM 和逻辑预留也保存在 CSV。
- 每轮核验交接后全部 25 条可查及字段调用次数。任一写错误判本轮失败，不剔除记录或错误。
- 前五对交替基线与候选顺序。中文/unigram 出现与其他组不同趋势，另用相同二进制和负载交替顺序跑 20 对复核。两批分别保存，不合并。共 200 次成功场景运行、4,800 次计时插入及预热，早期基线试跑不计入。
- 单轮计时窗口很短。P95/P99 为各轮分位数的中位数，不是聚合尾部。每次只有 24 个样本，P99 等于该轮最大值。不声称统计显著或普遍提速。

## 首批五组对照

| Profile / mask | 基线 records/s | 候选 records/s | 变化 | 候选更快对数 |
|---|---:|---:|---:|---:|
| TantivySimpleV1 / 0 | 180.96 | 198.25 | +9.56% | 5/5 |
| TantivySimpleV1 / 127 | 145.98 | 160.92 | +10.23% | 4/5 |
| JiebaMixedV1 / 0 | 188.49 | 170.48 | -9.55% | 2/5 |
| JiebaMixedV1 / 127 | 168.62 | 189.48 | +12.37% | 4/5 |

## 独立 20 对复核

表中取中位数，B/C 表示基线/候选。CPU 秒数涵盖上述稍宽区间。

| Profile / mask | records/s B/C | 变化 | 候选更快对数 | CPU s B/C | ACK P95 ms B/C | ACK P99 ms B/C | RSS MiB B/C |
|---|---:|---:|---:|---:|---:|---:|---:|
| TantivySimpleV1 / 0 | 189.44/195.46 | +3.18% | 12/20 | 0.090/0.080 | 1.395/1.370 | 1.854/1.859 | 43.05/42.31 |
| TantivySimpleV1 / 127 | 175.12/183.78 | +4.95% | 14/20 | 0.105/0.100 | 1.512/1.667 | 1.828/2.035 | 45.37/46.10 |
| JiebaMixedV1 / 0 | 185.07/202.42 | +9.38% | 15/20 | 0.100/0.080 | 1.622/1.560 | 1.890/1.931 | 82.72/81.05 |
| JiebaMixedV1 / 127 | 170.11/186.01 | +9.34% | 14/20 | 0.110/0.095 | 1.727/1.729 | 1.873/2.004 | 93.76/81.58 |

确认实验中的吞吐中位数提高，但 20 对中只有 12–15 对提升。首次中文/unigram 吞吐下降 9.55%，确认实验的方向相反。ACK 尾延迟并非全部改善，standard/Ngram P95 为 1.512→1.667 ms，P99 为 1.828→2.035 ms。补丁把仅词法的估算移入可见行构建，报告未单独测得这项工作对延迟的贡献。原生字节排空阈值保持原值。

排空后，各 profile 的逻辑预留在两版本间一致，按表顺序为 35,050,830 / 35,050,894 / 118,936,814 / 118,936,878 字节。RSS 是分配器和进程表现，与逻辑准入不同。交接后采样不能界定旧快照仍持完整 Token 时的物理峰值。不声称省内存或长期稳定表现。

## 正确性与生命周期验证

改生产代码前，`python3 scripts/ci_write_path_cases.py` 54/54，`cargo test --locked -p anyseek --test lexical_correctness` 6/6。

最终生产源码验证如下。

- `cargo test --locked -p anyseek --lib --features fault-injection record::lexical::runtime::live::reuse_tests -- --test-threads=1 --nocapture`。2 项通过，手工基准 ignored。
- `python3 scripts/ci_write_path_cases.py`。56/56 精确隔离用例，覆盖 WAL 前取消和部分范围退款、原生任务取消、旧快照固定时的满内存推进、物化失败封锁与重放、关闭及重开、远端 checkpoint 取消。清单只新增两个词法用例。
- `python3 scripts/ci_core_cases.py`。203/203 精确用例通过。
- `cargo test --locked -p anyseek --lib record::lexical::tests:: -- --test-threads=1`。19/19，覆盖 Ngram、字段长度、查询和评分契约、固定读者、标点及重开。
- `cargo test --locked -p anyseek --test lexical_correctness`: 6/6.
- `cargo check --locked -p anyseek --no-default-features` 通过。
- 源码边界、已退役依赖和词法分区检查通过。`python3 scripts/test_s3_acceptance.py` 通过**运行器单测**，未跑真实 S3 验收。

新用例比较交接或重开前后的完整 Token（offset_from、offset_to、position、position_length 与归一化文本）、字段长度、JSON 字符串展开与缺失字段，以及精确分数和顺序。以大型非词法或向量输入，对照旧纯词法投影核对原生估算。原已分析行随固定快照保留，原生克隆在现有 WAL 前工作区检查后执行，并持有其额度直至原生队列排空。没有工作区的已分析输入直接拒绝，不另申请额度。

批量入口、消费者水位契约、WAL 前预算算法、存储格式、同步与异步 API 语义及任务所有权未改动。本轮未执行 Windows、真实 S3、完整 HTTP 验收、生产 release panic 探针、严格 Clippy/完整验收、硬断电或长期容量测试。原编译警告不视作无警告 lint 结果。

## 复现

在兼容 Linux 主机使用新临时目录，保留历史结果。取得含本报告的候选 checkout 并保存 SHA。将本目录的 `measure.py` 和 `baseline-instrumentation.patch` 复制到临时目录。补丁只在固定基线增加测试插桩和工具，归一化空白上下文但不改变应用后的源码，两个哈希均在 environment.json。

```sh
# Run from the candidate repository; choose a new scratch directory.
SCRATCH=/tmp/anyseek-token-reuse-reproduction
mkdir -p "$SCRATCH"
CANDIDATE=$(git rev-parse HEAD)
cp benchmarks/lexical-token-reuse/{measure.py,baseline-instrumentation.patch} "$SCRATCH/"
git worktree add --detach "$SCRATCH/baseline" ed3052f2afdbcf18392a76a4bd3816e4e96275a1
git worktree add --detach "$SCRATCH/candidate" "$CANDIDATE"
git -C "$SCRATCH/baseline" apply "$SCRATCH/baseline-instrumentation.patch"
export CARGO_TARGET_DIR="$SCRATCH/target" CARGO_BUILD_JOBS=2
export CARGO_PROFILE_RELEASE_LTO=false CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
for label in baseline candidate; do
  (cd "$SCRATCH/$label" && cargo test --locked -p anyseek --lib     --features fault-injection --release --no-run --message-format=json) > "$SCRATCH/$label-build.jsonl"
  python3 - "$SCRATCH/$label-build.jsonl" "$SCRATCH/$label-bench" <<'COPY'
import json, shutil, sys
items = [json.loads(line) for line in open(sys.argv[1])]
paths = [x['executable'] for x in items if x.get('reason') == 'compiler-artifact'
         and x.get('executable') and x.get('target', {}).get('name') == 'anyseek'
         and x.get('profile', {}).get('test')]
assert len(paths) == 1
shutil.copy2(paths[0], sys.argv[2])
COPY
done
python3 "$SCRATCH/measure.py" pilot
python3 "$SCRATCH/measure.py" paired
python3 "$SCRATCH/measure.py" confirm
```

CPU affinity 0–1 和 cgroup v2 `/proc` 是明确要求，调整会改变环境。各 JSON 保留原始 ACK 样本、状态、二进制摘要及 cgroup 计数。`results.csv` 保留初始与确认的所有样本和原始 ACK 数组，不替换失败尝试。
