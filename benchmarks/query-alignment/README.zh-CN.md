# 查询身份对齐的有界验证

**简体中文** · [English](README.md)

基线 `fbdb904e83461cc5fccde0bb7a2953340959adec`。添加复现文件前的生产与测试 tree 为 `45a5c1bf9f7ed74e10b15642e6555d10a31c2842`。

改动删除重复的原生身份选择及有所有权的身份副本，保留活动向量与原生记录交集（异步消费者可能落后）、全库 BM25 统计、过滤、排序、Count、Hybrid/RRF、准入与固定快照和 pin 生命周期。不改变持久存储或新增全局缓存。

## 未改动生产与测试源码的检查
core 207/207、隔离写入 62/62、查询 11/11。词法单测 59 项通过，忽略一项无关的 I/O 诊断。小规模词法正确性 6/6、八个附加集成目标 15/15。默认和无默认 feature、源码边界及空白检查均通过。独立源码审查未发现阻塞。复用相同源码的结果，新增基准脚本和数据不代表再次跑 Rust 回归。原有编译警告保留。未执行严格 Clippy、全规模、真实 S3、本地原生 Windows 和断电验证。Windows 以此 PR head 的 CI 为准。#209 历史 Windows 恢复失败仍未解释，未纳入其临时诊断。

## 核心 API 基线
每个数据规模执行三组 ABBA，1000 条 TTL 记录和 8000 条无 TTL 记录，两词元正文和 3D 精确 ANN。每进程每用例运行 10 次预热和 60 个样本。精确 ID、分数位、通道排名及计数一致。各轮中位数的中位数，全文为 2.652 → 1.749 ms 和 23.154 → 14.770 ms，Hybrid 为 2.909 → 1.998 ms 和 23.399 → 15.448 ms。原始样本保存在 core-api-samples.json.gz。短正文计时无法单独证明 HTTP 或生产容量。

## Rust 分配器实际申请
用独立的 GlobalAlloc 包装 System，仅作诊断。所有线程统计 alloc/alloc_zeroed/realloc 的调用和请求字节。Realloc 计新大小全量，不计净增。计数器各自原子读取，可能有查询边界与后台噪声。不计 C 分配、mmap、释放、驻留或峰值。准入估算不变，不把它作为分配节省。插桩二进制不能作干净延迟证据。共用核心夹具与三组 ABBA，每版本、数据规模和用例360样本，一致性通过。

下表为每次测量查询的中位数。

|N|用例|基线调用|候选调用|基线请求字节|候选请求字节|
|---:|---|---:|---:|---:|---:|
|1000|词汇|7020|2425|819225|448065|
|1000|计数|6464|1869|670043|298883|
|1000|混合|7129|2534|789869|418709|
|8000|词汇|53872|17142|6270406|3307702|
|8000|计数|50644|13914|5206310|2243606|
|8000|混合|53336|16606|5720885|2758181|

## 含持续异步更新的 HTTP 查询
N=2048，seed20261109 生成的768维 F32 向量。正文为公共词、标签、唯一标记和另外24个生成 Token，facet 为整数。ExactANNAll，查询 common、Top10，投影 body/serial/group。一个集合，四个串行查询 Worker 各120次、间隔30ms，另一个写入方执行72批、每批4条条件更新、间隔50ms。这是有固定工作量和节奏的闭环，未保证开放式到达率。各轮计时前预计算相同更新负载，每轮使用同一不可变种子副本。每种 Lexical/Hybrid/Facet 排序模式三组 ABBA，共36个进程、17,280次查询计时、10,368条更新。每轮十次查询预热。初始和最终的稳定身份、版本、正文、分数、通道及更新前后词项一致性检查通过。签名不含运行时钟。异步消费期间计数可能暂降，排空前后核对精确 N。保留全部尝试和失败，只有显式 Busy 用原 ID 重试，传输未知不重试。

Linux，Rust1.99.1，release opt3、target-cpu=native、LTO=false 和默认 features。服务绑定 CPU 0、1，客户端绑定 2、3。Tokio 和 Rayon 各 2 个 worker，cgroup 配额 4 CPU，内存上限 16 GiB。无并行编译、测试或基准。有 cgroup 节流（全文基线累计 0.302 秒，Hybrid 候选 0.183 秒），记录在 SUMMARY.json。此共享本地环境无法估算生产容量。

下表单位毫秒，是六个进程各自百分位的中位数。SUMMARY.json 还记录总体分位数、调度至完成、ACK 和公开游标边界、CPU/RSS、计数与限流。请求包含 Busy 重试时间（本轮未发生），调度完成耗时含累计节奏落后。每个版本和模式共2880次查询、432个写请求及1728条记录。

|模式|版本|P50|P95|P99|各轮最大值|
|---|---|---:|---:|---:|---:|
|lexical|基线|29.67|67.06|146.38|473.80|
|lexical|候选|19.17|50.91|166.72|429.49|
|hybrid|基线|30.35|59.63|128.44|259.25|
|hybrid|候选|19.99|41.85|96.64|725.80|
|facet|基线|27.26|51.71|111.44|245.05|
|facet|候选|15.12|29.63|77.64|300.66|

**保留不利样本。** 全文各轮分位数的 P99 从 146.38 增至 166.72 ms。Hybrid 最大值从 259.25 增至 725.80 ms，Facet 最大值从 245.05 增至 300.66 ms。全文写入 ACK 汇总 P99 从 153.93 增至 195.65 ms，Facet ACK P95 从 46.93 增至 56.63 ms。Facet 从调度到完成的汇总 P99 从 655.72 增至 862.39 ms。本次改动仍有尾延迟和读写干扰。此数据集上的查询 P50 和 CPU 有改善，不代表普遍的延迟保证。

两个版本的 36 轮 Busy 和错误均为 0。ACK 后的公开游标追平取自稀疏轮询，因此只能给出上界，不能反映内部原生物化时间。最终排空在读者结束后开始并单独报告（<=2.41ms），不衡量 ACK 到可查询的延迟。排空后查询核验可见性。采样的峰值 RSS 约为 358 至 395 MiB，包含服务和缓存，无法单独归因于查询。

## 复现
用可丢弃的 checkout 和现有工具链。不得指向用户服务或数据目录。使用同一 release flags 串行构建基线与候选。把二进制保存为 `$ANYSEEK_QUERY_BENCH_ROOT/bin/base-server` 和 `candidate-server`。分配探针需把 allocation_probe.rs 复制到各临时 checkout 的 anyseek/examples/query_allocation_probe.rs 再构建，只做诊断，不进入生产代码。

```sh
export ANYSEEK_QUERY_BENCH_ROOT=/absolute/new/disposable-directory
mkdir -p "$ANYSEEK_QUERY_BENCH_ROOT/bin"
export CARGO_PROFILE_RELEASE_LTO=false CARGO_INCREMENTAL=0 CARGO_BUILD_JOBS=2
# In each temporary checkout, first copy allocation_probe.rs into anyseek/examples/.
cargo build --locked -p anyseek --release --bin anyseek_server --example query_allocation_probe
# In the baseline checkout (use candidate-server/candidate-alloc for the candidate):
cp target/release/anyseek_server "$ANYSEEK_QUERY_BENCH_ROOT/bin/base-server"
cp target/release/examples/query_allocation_probe "$ANYSEEK_QUERY_BENCH_ROOT/bin/base-alloc"
# Adapt target paths if CARGO_TARGET_DIR is set; save both versions before proceeding.
# Ensure no compilation/tests run concurrently.
python3 benchmarks/query-alignment/http_compare.py 3
python3 benchmarks/query-alignment/allocation_compare.py --before /absolute/base-alloc --after /absolute/candidate-alloc --output /absolute/new/alloc --cpus 0,1 --workloads 1000:true,8000:false
```

按主机调整 CPU ID，保持两版本一致。该 HTTP 工具在这里要求 CPUs0–3，隔离数据目录，一致性通过后才删除成功数据，保留失败数据和日志。不复用输出目录。种子元数据核对复制文件哈希。正式矩阵前另跑冷热正确性试验，每 Worker 12 次查询及8批写入。公开脚本使用环境指定输出根，这个配置是在测量后新增的路径选择，不改变负载或语义。

http-samples.json.gz 保存全部计时样本、尝试状态、进度和资源，不保存体积较大的响应正文。allocation-samples.json.gz 保留全部诊断样本与签名。原始完整响应日志保存在交付的证据归档。SUMMARY.json 记录可执行文件 SHA256。本地回归命令以 docs/development.md 和任务证据为准。

## 未覆盖范围
未验证近似 ANN 召回、真实 S3、硬断电、长期稳定性或任意高并发容量。仅测一种高维及数据规模。全局分配器请求字节下降不等于驻留内存下降。Hybrid/Facet 组件 guard 的所有权、慢查询与消费干扰另行诊断和设计，本次无相应生产重构。

## 共享运行器维护
分配兼容入口现使用 `../shared_query_compare.py`，默认三组 ABBA 和资源辅助文件。HTTP 生命周期、传输与显式 Busy 重试通过 `../shared_record_http.py` 与写入成本实验共用。负载、线程本地状态和采样仍由各实验管理。整理不会替换原始样本或改变其来源。运行器回归见 `../README.md`。
