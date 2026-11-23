# 单集合容量边界探针

**简体中文** · [English](README.md)

本分阶段工具测量达到有限容量时的拒绝情况。它不用于 100k 基准或 Meilisearch 排名。历史基线、修复、所有权审计以及容量计划记录保留于 Git 历史。

输入为仓库固定的 `miracl-corpus-v1.0-zh/manifest.json` 和全部十分片，以及官方 MIRACL 中文 dev 查询和相关性标注，取自[版本 5be20db](https://huggingface.co/datasets/miracl/miracl/tree/5be20db9509754dadad47689368639fcec739c00)。原查询 SHA256 为 `5b284a9aabf08bb2d1c88ed7ea276025c9d23846457c5350dc8d391b5e0d0a13`，qrels SHA256 为 `5546474d3dc8139014e6571e9f4041848bb0a314ca6d2d4bac708174d71c59eb`。冻结的 `queries-393.json` SHA256 为 `ebb564fd7b99fb02cdad3d9e9809bd62f71ed4237edcb10cc473c979b42a6447`。其中包含原始的 393 道 dev 题和 3,928 条标注。先前用过的 40 题明确列为非盲测，另保留 353 题。不改写关键词。

样本先纳入全部 3,786 个有标注文档 ID（966 个不同的正例），再根据文档 ID 哈希确定性地选入干扰项，总计 10,001 篇。这是富集相关文档的样本，无随机或全语料召回估计。全部 994 对查询与正例文档均纳入分母，并记录部分加载覆盖率。HTTP 工具保留原文，使用实际默认分词器配置、原始问句 OR、title/text 词法字段、top10 和计数。不声称与 Meili all/last 语义等价或测到全量召回。

```bash
python3 benchmarks/capacity/prepare.py \
  --corpus miracl-corpus-v1.0-zh \
  --out artifacts/benchmark/capacity/data
cargo build --locked -p anyseek --release  --bin anyseek_server
python3 benchmarks/capacity/baseline.py \
  --revision "$(git rev-parse HEAD)" \
  --binary target/release/anyseek_server \
  --data artifacts/benchmark/capacity/data/corpus-10001.json \
  --out artifacts/benchmark/capacity/new-run
```

每次运行使用新的输出目录，保留旧数据库和结果。工具要求 Linux CPUs 0–3，服务使用 0–1，客户端使用 2–3。避免同时构建和测量，不覆盖 ANYSEEK 预算，也不清除系统缓存。首个批次失败后停止，执行查询探测、checkpoint、重启，并核对初始成功与失败的操作 ID。批次 HTTP 200 不表示每项写入成功。启动探测诊断上限为 60s，socket 请求超时 35s。这些值不会改变生产超时，也不能证明服务达到 15s 恢复目标。

内置凭据只有明确的 localhost 合成测试值。原始服务日志、数据库、二进制和语料正文不提交，日志可能含这类本地测试凭据。不读取、修改或移除部署数据。
