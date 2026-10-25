# 分词器质量诊断（本地，2026-10-25）

生产实现 `253df6e6e8e10f6294af67a2158d55d12bfe2877`，基线主干
`6d28798a300bb947b2eb1d3e48e928d193103e17`；没有混入预算 PR #83。
这是一轮小语料质量诊断，不是吞吐排名，也不代表完整 MIRACL 或大规模性能。

## 数据与配置

复用 PR #82 的固定 1,000 条真实 MIRACL 中文文档、40 个 dev 问题及 qrels。
包含这些问题的全部 397 篇已判定文档，其中 91 篇正例；正例覆盖 100%。
其余为原协议按种子哈希抽取的干扰文档。语料包含 264 篇有 Latin 字符的文档，
但没有新增中英混合人工标注，不能据此声称双语检索已全面评估。
固定关键词在查看本轮结果前从问题文本编写，保留“新加披”等原拼写。
qrels 是原问题的标注，对缩写后的关键词仅是代理标注。

- 语料 SHA256：`dcdeaa9f3136686a3e717d2324ea129036a1c240f29ed283e902ede4dca3201a`
- 问题 SHA256：`d0f5c9d6fa8fc4d16bdf1563af457dfb59f60e58c365b20c07209838a18d881a`
- 关键词 SHA256：`d46949cabe5e48ece3a8364cca48f42e953c554af530aaa7dc95f90bd4535e3b`

anyseek：包版本 3.4.1，Tantivy 0.27.0、tantivy-jieba 0.21.0、jieba-rs 0.11.0，
明确选用 JiebaMixedV1；准确模式、HMM 关闭、Unicode 小写、无长度删除过滤器。
落盘配置确认 Bm25fProximity、English frequent words、word-N-gram mask 9、
Shared 字段长度、无停用词、OpenCC 规则 None；title/text boost 均为 1。
没有改变 AND/OR、评分公式、预算、记录/账本/快照硬上限。

Meilisearch Community v1.55.0（MIT），固定原验证二进制；typoTolerance 关闭，
prefixSearch disabled，搜索 title/text，分别执行 matchingStrategy all/last。
双方 top 10、offset 0、无业务过滤、无向量。anyseek 使用 count=true；
这轮不报告 QPS 或延迟排名。**AND/all 不保证同样的词边界，OR/last 更不是等价操作。**

同一 Linux 宿主机，AMD EPYC 9V74，cgroup 4 CPU/16 GiB；服务固定 CPU 0/1，
客户端 2/3，服务依次运行，评测期间无编译。并未清理 OS page cache，重启不等于冷缓存。

## 当前分词器与 Meilisearch

| 引擎 | 输入 / 匹配 | 空结果 / 40 | Recall@10 | binary nDCG@10 | MRR@10 |
|---|---|---:|---:|---:|---:|
| anyseek Jieba | 问句 / AND | 40 | 0.0000 | 0.0000 | 0.0000 |
| anyseek Jieba | 问句 / OR | 0 | 0.7754 | 0.5027 | 0.4527 |
| anyseek Jieba | 关键词 / AND | 13 | 0.2473 | 0.2587 | 0.3458 |
| anyseek Jieba | 关键词 / OR | 0 | 0.7942 | 0.5329 | 0.4867 |
| Meilisearch | 问句 / all | 40 | 0.0000 | 0.0000 | 0.0000 |
| Meilisearch | 问句 / last | 1 | 0.6925 | 0.4279 | 0.3955 |
| Meilisearch | 关键词 / all | 8 | 0.4264 | 0.3814 | 0.4325 |
| Meilisearch | 关键词 / last | 0 | 0.8425 | 0.5976 | 0.5835 |

全部 320 个请求 HTTP 成功。两方都成功写入/重开 1,000 条，未触发预算拒绝或降规模；
分别通过按 ID 读取逐条核对所有原始 title/text/docid。anyseek checkpoint 的
visible_seq/indexed_seq 都为 1,000；读取统计的 active_records 为 1,000。

指标均按 40 个问题宏平均：Recall 为 top10 正例数除以该问题 qrels 正例数；
DCG 使用二元相关性与 `1/log2(rank+1)`，除以理想 top10 DCG；MRR 为首个正例的
倒数排名，无正例命中记零。未判定文档按不相关处理。正例富集抽样限制了外推。

### 为什么 AND 问句仍为空

| 问句 | 实际 query_terms | 一篇已知正例 | OR 中该正例排名 |
|---|---|---|---:|
| 有哪些国家环太平洋？ | 有 / 哪些 / 国家 / 环 / 太平洋 | 173274#1《環太平洋地區》 | 9 |
| 酒精有哪些种类？ | 酒精 / 有 / 哪些 / 种类 | 7829344#0《酒精飲料列表》 | 1 |
| 台湾有哪些中央研究院的学者？ | 台湾 / 有 / 哪些 / 中央研究院 / 的 / 学者 | 5092815#7《搶救學人》 | 7 |

这三篇正例原文均没有“哪些”，AND 不允许忽略该词。分词已经生效，OR 能返回已知
正例；不能把 AND 空结果解释为不能检索中文，也不能宣称换分词器已解决问句召回。
保留 AND/OR 策略；是否采用问句改写、停用词或不同默认策略属于另一个产品决定。
繁简规则 None 也意味着本实验没有消除正例中的繁简差异；OpenCC 能力另有功能测试。

## 对旧分析器的同口径复核

已核验旧主干 release SHA256
`f113cde6bd36384434977c222db75fbd27b3f5cf363266a976ce03221b36792e`，
在全新数据根复核相同 1,000 条和相同 40 个问题/关键词。除分析器以外保持同样
正式 HTTP 查询参数；没有并行编译或其他测试服务。旧默认 UnicodeAlphanumeric 的
问句 AND、关键词 AND 都是 40/40 空、Recall@10=0；问句 OR 为 38/40 空、
Recall@10=0.0250，关键词 OR 为 8/40 空、Recall@10=0.2029。
新版对应 Recall 为 0、0.2473、0.7754、0.7942。新切词改善了这些固定输入的召回，
**没有改善保留 AND 策略的原始问句结果**。

还进行了独立的中文算法配对：旧 UnicodeAlphanumericZH（WordSegmentationTM）
与新 JiebaMixedV1，均为 256 条，保留全部 91 篇正例，再以
`anyseek-tokenizer-20261025-v1` 种子 SHA256 排序加入 165 篇干扰文档。
两边均写入/重开 256 条、逐条核对原文，320 个查询无 HTTP 错误，预算不变。
这是额外的小样本对照，不是把 1,000 条失败改成成功；1,000 条新引擎实验已成功。
**下表只能在这组 256 条内部比较，不能与上面的 1,000 条指标混用。**

| 输入 / 操作符 | 旧中文 Recall / nDCG / MRR | 新中文 Recall / nDCG / MRR |
|---|---|---|
| 问句 AND | 0 / 0 / 0 | 0 / 0 / 0 |
| 问句 OR | 0.9242 / 0.7630 / 0.7444 | 0.9242 / 0.7661 / 0.7558 |
| 关键词 AND | 0.2473 / 0.2702 / 0.3625 | 0.2473 / 0.2702 / 0.3625 |
| 关键词 OR | 0.9500 / 0.8033 / 0.7992 | 0.9417 / 0.7949 / 0.7879 |

保留一个退步反例：qid `7335325#0`，关键词“西班牙 足球 欧洲杯 冠军”，两边
query_terms 相同。旧 top10 含正例 `4811584#17`（第 10 名）；新 top10 不含该文档，
该问题 Recall 从 1 变成 2/3，导致宏平均 Recall 下降 0.00833。文档侧分析会改变
长度/词频等评分输入；未逐词归因，不宣称所有排序变化都有质量收益。评分公式源码
没有修改。成熟组件的主要收益是维护边界简化及默认中文可用，而非保证优于旧中文
算法。本轮没有为提高分数添加停用词、改 AND/OR、改 qrels 或调评分参数。

旧默认原始数据：`previous-unicode-1000/`；中文配对：`paired-chinese-256/`。
配对语料哈希与 seed 在该目录的 `baseline-manifest.json`。复核脚本为
`compare_previous.py`；指定 `--new-binary` 执行中文 256 配对，不指定则复核旧默认
1,000 条。两种运行均要求全新 `--out` 路径，保留旧证据。

## 资源观察与限制

本次一次性载入至可搜索约 anyseek 15.77 秒、Meilisearch 1.13 秒；100ms 采样最大
RSS 分别约 164.6 MiB、155.9 MiB。只是此次顺序运行的观测，不是统计显著的性能对比，
也不是内存上限；每条同步 durable/visible 与 Meili 异步 task 的持久性承诺并不等价。
双方进程重开验证都通过，不代表系统断电验证。原始记录另保留各路径逻辑/allocated
字节，可能重复计入硬链接，不能直接用作磁盘优劣结论。

## 复现与证据

先用 PR #82 的 `benchmarks/miracl-meili/fetch_inputs.py`、`prepare_data.py` 准备固定
输入并核对 manifest；使用本分支 release/server 构建和原验证 Meili 二进制：

```sh
cargo build --locked -p anyseek --release --features server --bin anyseek_server
python3 benchmarks/tokenizer/diagnose.py --data /path/to/original/data \
  --anyseek target/release/anyseek_server --meili /path/to/meilisearch-v1.55.0 \
  --out artifacts/benchmark/a-new-unique-run
```

原始响应、资源样本、输入哈希、落盘分析器配置及服务日志位于本地
`artifacts/benchmark/tokenizer-253df6e/`；不覆盖 PR #82 的旧实验。
服务 release SHA256：`0badaf4a1d270042330539943d71e3d89b69de7609304b21cc630dda57ce1893`。
Meili SHA256：`7e33f5889235f14290074a19a6d3574bd9b10fb2ee8731eabb7dafd0714b0513`。

## 最终本地验收与交付边界

全目标 345 通过、0 失败、13 ignored（35 个测试程序，退出 0）；无默认 feature
核心 200 通过、0 失败、10 个 worker ignored；默认/无默认 doctest 各 9 通过。
普通 HTTP/重开验收通过；S3 对象 fixture 契约、7/7 冷缓存语义组及存储/联合备份/
retention/GC 故障矩阵均退出 0。原 15 秒普通启动超时未放宽；S3 冷缓存测试沿用
原测试自己的时间界限。11 个 ignored worker 由通过的父测试执行；真实 S3 和
手工跨 AES 仍未执行。Windows、系统硬断电也未执行，不能用 fixture/进程退出替代。

本地源码和报告尚未推送；本轮不包含预算 PR #83。机器可读汇总见
[RESULTS.json](RESULTS.json)，设计/默认/清理边界见
[实施说明](../../docs/tokenizer-implementation.md)。建议先审查并获得独立 PR/双平台 CI
授权；问句检索策略与更大、更均衡的质量评测另行决策，不在本轮偷偷实施。
