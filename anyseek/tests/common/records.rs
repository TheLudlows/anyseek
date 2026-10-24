//! Test fixtures use only public Record operations; successful writes are visible.
#![allow(dead_code)]
use anyseek::{index::{Document, IndexMetaObject, SchemaField}, record::{RecordConfig, RecordPartition, RecordWrite, OperationId, ExpectedVersion}};
pub const JOURNAL_LIMIT: u64 = 64 * 1024 * 1024;
pub async fn create(path: &std::path::Path, index: IndexMetaObject, schema: Vec<SchemaField>) -> RecordPartition {
    RecordPartition::create(path, RecordConfig {
        change_feed: Default::default(), embedding_space_id: if matches!(index.inference, anyseek::vector::Inference::External { .. }) {Some("test-space".into())} else {None}, index, schema,
        max_records: 10000, max_record_bytes: 64*1024, max_operations: 20000,
        max_journal_bytes: JOURNAL_LIMIT, max_candidates: 10000,
    }).await.unwrap()
}
pub async fn append(partition: &RecordPartition, docs: Vec<Document>) {
    let start = partition.statistics().await.unwrap().retained_identities;
    for (ordinal, document) in docs.into_iter().enumerate() {
        let id = format!("{:020}", start + ordinal);
        partition.write(RecordWrite {
            operation_id: OperationId::new(format!("insert-{id}")), record_id: id,
            routing_key: "fixture".into(), expected: ExpectedVersion::Absent,
            document: Some(document), vector_patch: None, generation: None, temporal: None, system: None,
        }).await.unwrap();
    }
}
pub async fn insert(partition: &RecordPartition, docs: Vec<Document>) -> Result<(), anyseek::record::RecordError> {
    let start = partition.statistics().await?.retained_identities;
    for (ordinal, document) in docs.into_iter().enumerate() {
        let id = format!("{:020}", start + ordinal);
        partition.write(RecordWrite {
            operation_id: OperationId::new(format!("insert-{id}")), record_id: id,
            routing_key: "fixture".into(), expected: ExpectedVersion::Absent,
            document: Some(document), vector_patch: None, generation: None, temporal: None, system: None,
        }).await?;
    }
    Ok(())
}
pub async fn delete(partition: &RecordPartition, ids: Vec<usize>) {
    for ordinal in ids {
        let id = format!("{ordinal:020}");
        if let Some(record) = partition.get(&id).await.unwrap() {
            partition.write(RecordWrite {
                operation_id: OperationId::new(format!("delete-{id}-{}", record.version)),
                record_id: id, routing_key: record.routing_key,
                expected: ExpectedVersion::Match(record.version), document: None,
                vector_patch: None, generation: None, temporal: None, system: None,
            }).await.unwrap();
        }
    }
}
pub async fn update(partition: &RecordPartition, ordinal: usize, document: Document) {
    let id=format!("{ordinal:020}");let old=partition.get(&id).await.unwrap().unwrap();
    partition.write(RecordWrite {operation_id:OperationId::new(format!("update-{id}-{}",old.version)),record_id:id,
        routing_key:old.routing_key,expected:ExpectedVersion::Match(old.version),document:Some(document),
        vector_patch:None,generation:None,temporal:None,system:None}).await.unwrap();
}
pub async fn clear(partition: &RecordPartition) {
    let total=partition.statistics().await.unwrap().retained_identities;
    delete(partition,(0..total).collect()).await;
}
