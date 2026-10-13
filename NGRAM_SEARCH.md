# N-gram 索引

N-gram 是相邻的两个或三个词项组成的索引项，用于词法短语检索；这里不是字符 N-gram，也不是已删除的拼写纠错/自动补全。

创建索引时配置 `frequent_words` 和 `ngram_indexing`。高频词集合可用 English、German、French、Spanish、Custom 或 None；F 表示高频词，R 表示其余词。当前 HTTP 默认高频词配置为 English，N-gram 位掩码默认 FF | FFF（数值 9）；有效组合需要相应的高频词集合。

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

多个选项按位或组合。N-gram 仍保留单词索引，查询执行根据已配置的组合利用相邻词项。更多组合增加索引空间与构建工作量，应使用实际数据测量，不能直接沿用上游旧版本 benchmark 数字。

定义见 [NgramSet、FrequentwordType 与 CreateIndexRequest](seekstorm/src/index.rs)，执行见 [tokenizer.rs](seekstorm/src/tokenizer.rs) 和 [search.rs](seekstorm/src/search.rs)。当前保留此功能；是否进一步裁剪需另行确认。




