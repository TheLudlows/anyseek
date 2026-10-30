# Catalog / Generation 更新成本复核（#107）

基线：`9c9e5d2dc8125bea355f4f43694639fdabba9b46`。日期：2026-10-30。
本次加入两个手工 profile 和原始样本，生产路径保持原实现。

## 测量方法

- Rust 1.99.1，Release opt-level=3，关闭 LTO，target-cpu=native，无默认特性；8 核 CPU 配额、8 GiB 内存上限。
- 三次独立测试进程。每个场景预热 5 次，再采样 25 次；表中 P50 是三次运行的范围，P95 是各次 P95 的中位数，单位 ms。
- Catalog 使用真实 SQLite WAL/FULL，计时分为 State clone、apply、全量序列化/反序列化校验及 persist。构造 16/128/1024/4096 个集合及对应创建/激活回执，再执行 30 次独立 RevokeKey 更新。最后一组保留 8224 项操作，未超过当前 10000 项控制回执上限。
- Generation 使用实际 apply 构造状态和真实 `generation::prepare(Renew)`。正文/操作仍使用当前共享值和预计算长度；仅复制容器，不深拷贝全部正文。每个 staged generation 有一个私有成员，保留 16 MiB 状态/检查点预算，并分别增加记录数和同时 staging 的文档数。
- Catalog 内部段不包含 HTTP、认证、操作查重、排队和调度。Generation 是内存准备成本，排除 WAL/S3、索引消费和响应。临时文件系统、缓存、未绑核和短样本会影响结果；不能据此推导生产 P99、QPS 或磁盘持久性保证。

原始数据和环境字段见 [results.json](results.json)，包括状态大小变化和每次样本。Catalog 新回执在采样期间增长，不把各次状态描述成完全等长。

## Catalog

| 集合数 | 更新内部段 P50 | P95 | 全量校验 P50 | persist P50 |
| --- | ---: | ---: | ---: | ---: |
| 16 | 0.599–0.681 | 0.848 | 0.143–0.163 | 0.434–0.504 |
| 128 | 1.689–1.916 | 2.668 | 0.896–1.002 | 0.694–0.829 |
| 1024 | 10.800–12.173 | 15.605 | 7.243–8.367 | 2.737–3.182 |
| 4096 | 43.622–49.578 | 55.614 | 29.935–34.703 | 10.959–11.697 |

4096 集合的 State 约 7.1 MB。clone P50 为 1.920–2.062 ms；主要成本来自完整校验，其次是 persist 的全表比较/序列化与 SQLite 事务。代码中的规模放大确实存在，不能称为已经实现了局部更新。

## Generation Renew

| 记录数 | 同时 staged 文档 | prepare P50 | P95 | 全量 generation 校验 P50 |
| --- | ---: | ---: | ---: | ---: |
| 101 | 1 | 0.020–0.021 | 0.022 | 0.003–0.004 |
| 1001 | 1 | 0.099–0.113 | 0.154 | 0.012–0.017 |
| 5001 | 1 | 0.571–0.661 | 0.848 | 0.130–0.150 |
| 10001 | 1 | 1.315–1.751 | 1.974 | 0.293–0.380 |
| 1001 | 16 | 0.226–0.308 | 0.365 | 0.096–0.124 |
| 1001 | 64 | 0.560–0.617 | 0.791 | 0.349–0.356 |
| 1001 | 256 | 2.078–2.286 | 2.838 | 1.383–1.501 |
| 10001 | 128 | 14.325–15.126 | 17.732 | 9.786–10.413 |

同时 staging 的数量是重要维度。每个 live staged manifest 都会重建其操作历史，只测试一个 generation 会低估这一部分成本。10001 条记录、128 个 staged 文档时，状态约 7 MB；全量 generation 校验约 10 ms，准备总计约 15 ms。

## 本轮决定

保留当前生产实现。上述是有界控制操作和 Renew 准备路径，尚未获得真实管理/上传并发、操作频率或尾延迟目标，不能据此认定它们已经构成用户业务瓶颈。此结论不代表成本不存在，也不代表较大规模无需优化。

若实际负载接近上表规模且控制/上传延迟不可接受，下一步分别限定为 Catalog 的受影响行更新，以及 Generation 对受影响 manifest/控制历史的局部校验；必须证明全局不变量仍成立，并在加载/恢复边界保留完整校验。无需为此引入通用 SQL、缓存或任务调度框架。

## 复现

```bash
CARGO_PROFILE_RELEASE_LTO=false CARGO_INCREMENTAL=0 cargo test --release \
  -p anyseek --no-default-features --lib cost_profile --no-run
CARGO_PROFILE_RELEASE_LTO=false CARGO_INCREMENTAL=0 \
ANYSEEK_COST_OUTPUT=/tmp/anyseek-control-costs cargo test --release \
  -p anyseek --no-default-features --lib cost_profile -- --ignored --test-threads=1
```

两个 profile 默认 ignored，不加入普通 CI，也不设置易波动的耗时断言。
