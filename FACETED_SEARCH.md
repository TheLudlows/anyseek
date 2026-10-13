# 元数据过滤与排序

分面计数、直方图、范围聚合和字段 min/max 已移除。`facet:true` 仍是过滤/排序字段的 schema 标记，不表示提供统计聚合。

| 参数 | 作用 |
| --- | --- |
| `facet_filter` | 按字符串、字符串集合、数字或时间范围限制三种检索模式的结果 |
| `result_sort` | 按字段升降序，可组合多字段以及 `_score`、`_id` |
| `field_filter` | 选择检索哪些字段，不是按元数据值过滤 |
| `fields` | 选择返回哪些已存储字段 |

创建索引时，将 `String16/String32/StringSet16/StringSet32`、数值或 `Timestamp` 字段设为 `facet:true`。例如：

```json
{"index_name":"documents","schema":[{"field":"body","field_type":"Text","store":true,"index_lexical":true},{"field":"category","field_type":"String16","store":true,"index_lexical":false,"facet":true},{"field":"date","field_type":"Timestamp","store":true,"index_lexical":false,"facet":true}]}
```

通过 POST JSON 搜索：

```json
{"query":"database","facet_filter":[{"String16":{"field":"category","filter":["guide"]}},{"Timestamp":{"field":"date","filter":{"start":1704067200,"end":1735689600}}}],"result_sort":[{"field":"date","order":"Descending"}],"fields":["body","date"]}
```

范围过滤为左闭右开；start=end 合法但没有匹配结果，start>end 或非有限浮点边界报错。各谓词之间为 AND（包括同字段重复条件），单个字符串/集合谓词中的值为 OR。空字符串值列表或不存在的字典值没有匹配结果。未知字段、未启用 facet:true 或类型不匹配返回 400，Rust 调用检查 ResultObject.error。

Lexical、Vector、Hybrid 复用共享过滤逻辑。向量在评分及 Top-K 入堆前过滤，Hybrid 的两路候选都过滤后再融合。realtime 包含的未提交记录使用当前字典；已删除候选不入堆。字符串集合按实际成员匹配，不按字典组合键匹配。

ANN 仍只搜索 ann_mode 配置选中的簇；过滤不自动扩大 Nprobe，可能返回不足 K。需要全簇过滤基线时使用 ann_mode:All；它仍按配置的存储精度/量化评分。Vector/Hybrid count_total 仍不是全库精确去重计数。过滤不建立共同可见视图或持久化保证，这些属于后续阶段。

旧 `query_facets` 请求（包括空数组）返回 400；查询响应不再有 `facets`，索引信息不再有 `facets_minmax`。文档命中总数 `count_total`、分页和字段排序保留。

原有 `facet.bin` 的值和 ID 编码保留；`facet.json` 的旧计数与极值读取时被丢弃。为保持磁盘结构兼容，重写时旧计数槽为 0、min/max 槽为 None；它们仅是格式占位，不参与计算或 API 输出。向量量化的内部数值范围与本次删除的字段统计无关。

接口与限制见 [FEATURES.md](FEATURES.md)、[服务文档](seekstorm_server/README.md)。
