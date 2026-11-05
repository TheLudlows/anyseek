# 检索配置与排序

HTTP 查询用法见[服务指南](../anyseek/src/server/README.md#查询与遍历)。本文集中说明文本分析、评分、候选排序与融合契约。

## 文本归一化

创建集合时可在 `config` 中选择 `text_normalization: "OpenCcT2s1_1_9"`；省略为 `"None"`，缺少该字段的元数据也按 None 解释。不支持查询级覆盖、在线修改或旧索引迁移。例如配置片段：

```json
{
  "index_name": "normalized-zh",
  "tokenizer": "JiebaMixedV1",
  "text_normalization": "OpenCcT2s1_1_9",
  "schema": [{"field":"body","field_type":"Text","index_lexical":true,"store":true}]
}
```

规则保存在索引元数据中；文档与查询使用同一 tokenizer 入口，在分词和 N-gram 前转换。只产生一条词项流，不保存第二份简体正文，不修改原始文档。自定义停用词/高频词的运行时字典使用相同转换，用户提供的元数据保持原样；重开恢复同一规则。客户端发送原文，不需额外做地区词汇转换。元数据响应展示所选规则；内部 Rust 配置使用 `IndexMetaObject::text_normalization`。

Rust 适配器只实现锁定的 OpenCC 1.1.9 t2s：最长词组优先，再取首个字符映射；不是完整 OpenCC API。官方 TSPhrases、TSCharacters 及 Apache-2.0 许可固定到提交 `556ed22496d650bd0b13b6c163be9814637970ae`，来源和 SHA-256 见[provenance.json](../anyseek/data/opencc-1.1.9/provenance.json)。进程级不可变 LazyLock 字典初始化一次，运行时不下载数据、启动转换进程或安装原生库。

这是繁转简，不做台港/大陆地区词汇改写：`軟體` 转为 `软体`，不会变成 `软件`。ASCII 字节（含查询引号、加减操作符）不被映射消费或引入。嵌入 NUL 及后续文本保留，与 OpenCC 1.1.9 C-string 路径在 NUL 截断的行为有明确差异。原始 UTF-8 正文始终保留。查询 NOT/前导 CJK 操作符有独立解析与回归，不属于归一化；本配置不增加高亮、前缀或 edge-ngram。

## N-gram 索引

N-gram 是相邻两个或三个词项组成的索引项，用于词法短语检索；不是字符 N-gram、拼写纠错或自动补全。

内部使用 `frequent_words` 和 `ngram_indexing`，HTTP 不接受这两个参数。高频词集合可用 English、German、French、Spanish、Custom 或 None；F 表示高频词，R 表示其余词。HTTP 新建集合固定为 English 与 FF | FFF（数值 9）；有效组合需要相应高频词集合。

| 枚举 | 数值 | 组合 |
| --- | --- | --- |
| SingleTerm | 0 | 不额外索引 N-gram |
| NgramFF | 1 | 高频 + 高频 |
| NgramFR | 2 | 高频 + 其余 |
| NgramRF | 4 | 其余 + 高频 |
| NgramFFF | 8 | 三个高频词 |
| NgramRFF | 16 | 其余 + 高频 + 高频 |
| NgramFFR | 32 | 高频 + 高频 + 其余 |
| NgramFRF | 64 | 高频 + 其余 + 高频 |

多个选项按位或组合，仍保留普通单词索引。FFR 和 FRF 分别检查自身开关，不依赖 RFF；5 表示 FF | RF，默认 9 为 FF | FFF。增加组合会增加空间与构建工作量，需要用实际数据测量。

关闭 N-gram 仍支持普通短语搜索；开启也不会使 `rust` 被 `r`、`ru`、`us` 或 `r*` 匹配。独立位、默认及混合组合的既有回归覆盖提交前后、重开后的短语成员/计数与 BM25F/Proximity。历史 FFR/FRF 漏 postings 的修复不会补齐旧索引，受影响数据需从原始记录重建；默认 9 不启用这两种组合。

定义见 [NgramSet 与 FrequentwordType](../anyseek/src/index.rs)、[CreateIndexRequest](../anyseek/src/server/api_types.rs)，执行见 [tokenizer.rs](../anyseek/src/tokenizer.rs) 和 [search.rs](../anyseek/src/search.rs)。

## 字段长度归一化

`field_length_normalization` 接受 `"Shared"` 或 `"PerField"`，默认 Shared，创建后不可修改；内部 Rust 字段为 `IndexMetaObject::field_length_normalization`。

生产 Tantivy 评分保留 BM25F/Bm25fProximity、字段 boost、k1/b 与解码长度量化。Shared 使用共享平均长度，PerField 将各字段分母改为该字段非空解码长度的平均值；空总体使用 1。词项 DF 按同一读取 cut 的活动 Record 群体跨字段、tail 与冻结组件统计，退休记录不参与。

TTL 在 Top-K 前排除过期候选；后台回收退休其索引行之前，这些行仍可能影响语料统计。元数据过滤只限制候选，不重定义统计总体。同步/异步可见性以[Record 契约](record-model.md)为准，checkpoint 和原生 commit 不建立第二个业务可见性开关。

独立公式 oracle 位于 `anyseek/tests/common/ngram_oracle.rs` 与 `anyseek/tests/lexical_correctness.rs`，覆盖 boost、不等 DF、Shared/PerField 与持久化状态；不依赖已删除的旧 posting 编码。

## 稳定排序

固定逻辑数据、评分配置和候选集合时，相关性按数值分数降序，再按稳定 `record_id` 字符串升序；候选准入和最终排序都使用该规则。物理组件 ordinal 是地址，切段/合并不应以插入顺序重定义平分顺序；普通名为 `docid` 的字段不能替代 Record 身份。

显式排序保留声明的字段顺序和方向；`_score` 同分时继续后续字段，全部显式字段相同时回退到分数降序和稳定 ID 升序。`_id` 按稳定身份排序。空查询默认按 ID 升序；所有分数为零时显式 `_score` 也如此。元数据 null、缺失及 StringSet 排序见[字段类型与数值](../anyseek/src/server/README.md#字段类型与数值)。

有符号零比较相等；无穷保留数值顺序。内部比较器将 NaN 排在数值之后、NaN 之间视为同分；外部非有限向量仍被拒绝。

纯全文收集器使用窗口大小为 `offset + length` 的有界堆，Hybrid 词法通道使用其候选预算。候选选择与输出使用相同的请求排序；纯全文 Count 跳过评分与候选收集。实现位于 `record/lexical/runtime_query.rs`、`min_heap.rs` 和向量候选路径；既有 stable_ranking、weighted_rrf、collector 与 comparator 回归覆盖平分、显式排序、分页、数值边界及组件布局。

ANN 和有界融合可能遗漏候选，稳定平分排序不保证精确召回。Vector/Hybrid 的候选内字段排序不等同全库字段 Top-K。并发写入或 TTL 可在最终重验时缩短页，确定排序不提供跨请求持久分页快照。

## Weighted RRF

Hybrid 在每个通道跨组件形成全局排名后做一次加权 RRF。省略 `rrf`（内部 `RrfOptions::default()`）时，零基 rank 常数为 0.6，两路权重均为 1，每路默认候选窗口为 `offset + length`；这些默认值未重新调优，不是质量或延迟保证。

HTTP 仅允许在非空文本查询同时携带 query_vector 时设置 `rrf`：

```json
{
  "query": "example",
  "query_vector": [1, 0, 0],
  "length": 10,
  "rrf": {
    "constant": 5,
    "keyword_weight": 0.5,
    "vector_weight": 1,
    "keyword_candidates": 100,
    "vector_candidates": 200
  }
}
```

向量维度必须匹配集合；示例用于说明格式，不是经过测量的推荐参数。内部 Rust 使用统一的 `SearchMode::Hybrid { similarity_threshold, ann_mode, rrf }` 校验默认与自定义设置。

每条结果的融合分是各参与通道 `weight / (constant + rank)` 之和，rank **从 0 开始**。每路按原始分数降序、稳定 Record ID 升序排名，缺席通道贡献为零，最终同分也按稳定 ID 升序。此处 constant=5 等价于一基 rank 的 constant=4；若比较常见的一基 constant=60，应在此使用 61。UI 将名次从 1 展示，不改变计算。

| 参数 | 约束 |
| --- | --- |
| constant | 有限正数 |
| keyword_weight / vector_weight | 有限非负，至少一路为正；可能使最大融合 f32 溢出的组合被拒绝 |
| keyword_candidates / vector_candidates | 显式值为正整数；省略或 null 使用各路全局 `offset + length` |
| 其他数值字段及 rrf 对象 | 不接受 null；未知 RRF 字段被拒绝 |

权重为零的通道不贡献命中或分数，但检索仍运行两路，不意味着节省查询工作或放宽向量校验。诊断分数与来源标签描述实际贡献通道。

每路预算限制的是**融合前全局保留的候选数**；组件局部前缀可参与全局合并，组件数不乘大最终预算。预算不限制扫描文档数、耗时或向量探测数，与输出窗口和 `nprobe`/`all` 独立。预算小于输出窗口可能返回不足；更多向量候选不能代替足够簇探测。更多组件和更大预算仍会增加中间 Top-K、合并内存及排序工作。

跨页固定显式预算可固定候选窗口大小；省略预算时，窗口随 `offset + length` 改变。Vector/Hybrid HTTP 要求正输出长度。Hybrid 的 count_total 沿用各组件两路计数最大值之和，不是精确并集或融合候选数；RecordSearch 没有通用完整性布尔标记。显式字段排序保留词法候选选择语义，融合仍按选中候选的相关性排名。

ANN 与有界窗口均不保证全库融合召回；关系遍历及关系融合由应用负责。调参需固定语料、向量、查询/qrel 划分及原始排名，并在独立留出数据上同时测量资源。已看过的 dev 查询不能证明盲测质量。性能与回归执行结果应引用对应报告的精确版本，见[证据索引](roadmap.md#验收证据)。
