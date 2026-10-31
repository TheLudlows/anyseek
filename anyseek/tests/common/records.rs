//! Test fixtures use only public Record operations; successful writes are visible.
#![allow(dead_code)]
use anyseek::{index::{Document, IndexMetaObject, SchemaField}, record::{RecordConfig, RecordPartition, RecordWrite, OperationId, ExpectedVersion}};
/// Retry only unaccepted Busy with the identical operation identity. Other
/// errors (including OutcomeUnknown/DurablePending) are never replayed here.
pub async fn write(partition:&RecordPartition,request:RecordWrite)->Result<anyseek::record::WriteReceipt,anyseek::record::RecordError> {
    let started=std::time::Instant::now();
    loop {match partition.write(request.clone()).await {
        Err(anyseek::record::RecordError::Busy) if started.elapsed()<std::time::Duration::from_secs(20)=>
            tokio::time::sleep(std::time::Duration::from_millis(5)).await,
        result=>return result,
    }}
}
pub const JOURNAL_LIMIT: u64 = 64 * 1024 * 1024;
pub async fn create(path: &std::path::Path, index: IndexMetaObject, schema: Vec<SchemaField>) -> RecordPartition {
    RecordPartition::create(path, RecordConfig {
        history: Default::default(), embedding_space_id: if matches!(index.inference, anyseek::vector::Inference::External { .. }) {Some("test-space".into())} else {None}, index, schema,
        max_records: 10000, max_record_bytes: 64*1024, max_operations: 20000,
        max_journal_bytes: JOURNAL_LIMIT, max_candidates: 10000,
    }).await.unwrap()
}
pub async fn append(partition: &RecordPartition, docs: Vec<Document>) {
    insert(partition, docs).await.unwrap();
}

pub async fn insert(partition: &RecordPartition, docs: Vec<Document>) -> Result<(), anyseek::record::RecordError> {
    let start = partition.statistics().await?.retained_identities;
    for (ordinal, document) in docs.into_iter().enumerate() {
        let id = format!("{:020}", start + ordinal);
        write(partition,RecordWrite {
            operation_id: OperationId::new(format!("insert-{id}")), record_id: id,
             expected: ExpectedVersion::Absent,
            document: Some(document), vector_patch: None, temporal: None, system: None,
        }).await?;
    }
    Ok(())
}
pub async fn delete(partition: &RecordPartition, ids: Vec<usize>) {
    for ordinal in ids {
        let id = format!("{ordinal:020}");
        if let Some(record) = partition.get(&id).await.unwrap() {
            write(partition,RecordWrite {
                operation_id: OperationId::new(format!("delete-{id}-{}", record.version)),
                record_id: id,
                expected: ExpectedVersion::Match(record.version), document: None,
                vector_patch: None, temporal: None, system: None,
            }).await.unwrap();
        }
    }
}
pub async fn update(partition: &RecordPartition, ordinal: usize, document: Document) {
    let id=format!("{ordinal:020}");let old=partition.get(&id).await.unwrap().unwrap();
    write(partition,RecordWrite {operation_id:OperationId::new(format!("update-{id}-{}",old.version)),record_id:id,
        expected:ExpectedVersion::Match(old.version),document:Some(document),
        vector_patch:None,temporal:None,system:None}).await.unwrap();
}
pub async fn clear(partition: &RecordPartition) {
    let total=partition.statistics().await.unwrap().retained_identities;
    delete(partition,(0..total).collect()).await;
}
