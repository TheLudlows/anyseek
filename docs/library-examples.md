# Rust 核心库示例

[文档索引](README.md) · [Record 模型](record-model.md) · [开发与验证](development.md)

使用本仓库的 path/git 依赖。存储、写入、读取和检索统一使用 `RecordPartition`。
旧低层 `create_index/open_index/IndexArc`、`Commit/Search` trait 和物理 ordinal iterator
不再是应用接口；新库不提供旧格式读取或升级迁移。schema、评分、分词和向量配置类型继续使用。

下面是新目录上的纯文本库。Payload 保留规范原文，Tantivy 只持有派生 postings；
`store:false` 不会删除规范原文。成功写入已满足 durable + visible，无需另行 commit 才能搜索。

```rust,no_run
use anyseek::{
    index::{AccessType, Clustering, DocumentCompression, FieldLengthNormalization,
        FrequentwordType, IndexMetaObject, LexicalSimilarity, StopwordType,
        TextNormalization, TokenizerType},
    record::{ExpectedVersion, OperationId, RecordConfig, RecordPartition, RecordWrite},
    search::SearchMode,
    vector::Inference,
};
use serde_json::json;
use std::{error::Error, path::Path};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let path = Path::new("records-new"); // 必须是新目录；不要覆盖已有部署数据。
    let config = RecordConfig {
        change_feed: Default::default(),
        index: IndexMetaObject {
            id: 0, name: "documents".into(),
            lexical_similarity: LexicalSimilarity::Bm25fProximity,
            tokenizer: TokenizerType::UnicodeAlphanumeric,
            text_normalization: TextNormalization::None,
            field_length_normalization: FieldLengthNormalization::Shared,
            stop_words: StopwordType::None,
            frequent_words: FrequentwordType::None,
            ngram_indexing: 0,
            document_compression: DocumentCompression::Snappy,
            access_type: AccessType::Mmap,
            clustering: Clustering::None,
            inference: Inference::None,
        },
        schema: serde_json::from_value(json!([
            {"field":"body", "field_type":"Text", "store":false, "index_lexical":true}
        ]))?,
        embedding_space_id: None,
        max_records: 1000,
        max_record_bytes: 64 * 1024,
        max_operations: 10000,
        max_journal_bytes: 16 * 1024 * 1024,
        max_candidates: 1000,
    };
    let journal_limit = config.max_journal_bytes;
    let partition = RecordPartition::create(path, config).await?;
    let write = RecordWrite {
        operation_id: OperationId::new("write-guide-1".into()),
        record_id: "guide".into(), routing_key: "manual".into(),
        expected: ExpectedVersion::Absent,
        document: Some(serde_json::from_value(json!({"body":"durable search"}))?),
        vector_patch: None, generation: None, temporal: None, system: None,
    };
    let receipt = partition.write(write.clone()).await?;
    // 在重试保留期内，重试必须保留完全相同的命令及 operation_id。
    assert_eq!(partition.write(write).await?, receipt);
    let results = partition.search("durable".into(), None, SearchMode::Lexical, vec![], 10).await?;
    assert_eq!(results.hits[0].record.record_id, "guide");
    assert!(partition.get("guide").await?.is_some());

    // 这是受认证的 checkpoint 发布与 WAL 回收，不是另一个可见性开关。
    partition.compact_journal().await?;
    drop(partition);
    let reopened = RecordPartition::open(path, journal_limit).await?;
    assert_eq!(reopened.get("guide").await?.unwrap().version, receipt.version);
    Ok(())
}
```

更新使用稳定 record ID、相同 routing key、新 operation ID 和 `ExpectedVersion::Match(version)`；
删除使用相同条件和 `document:None`。删除保留版本身份与墓碑，不复用物理 ordinal。
`OutcomeUnknown`/`DurablePending` 不能通过生成新 operation ID 绕过；先恢复，再重试原命令。

`RecordPartition::query(RecordQuery)` 提供布尔/短语查询、字段限制、过滤、排序及分页。
`fields` 是检索字段限制；metadata `filters` 在候选截断前执行。空查询用于浏览。
筛选/排序定义见 [元数据文档](../FACETED_SEARCH.md)，查询错误通过 `Result<_, RecordError>` 返回。

外部向量使用 `Inference::External`，声明维数、精度、量化与距离，并提供稳定的
`embedding_space_id`；schema 向量字段设 `index_vector:true`。文档可先只有正文，
稍后对精确正文版本使用 `vector_patch` 补充向量。库不下载模型或生成 embedding。
`SearchMode::Vector`、`Hybrid` 和 `WeightedHybrid` 的查询向量必须符合该空间配置。

混合检索按全局通道排名进行一次 RRF；权重为零只排除排名贡献，不省略该通道验证或计数。
混合总数保留各组件通道计数取最大后求和的启发式，不是精确并集；ANN 计数也不保证精确。
固定候选预算的分页规则见 [RRF 文档](weighted-rrf.md)。

稳定视图 iterator、批量条件操作、备份与恢复均使用 Record API。不要通过内部向量/metadata
组件的文件或物理 ordinal 实现应用读写，也不要以 Tantivy 内部 commit 判断业务提交是否成功。
