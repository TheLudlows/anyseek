//! Release-only subprocess probe; not a test-harness binary.
use anyseek::record::*;
use anyseek::index::*;
use anyseek::vector::Inference;
use std::path::Path;

    fn config() -> RecordConfig {
        RecordConfig {history:HistoryRetention::default(),index:IndexMetaObject{name:"records".into(),lexical_similarity:LexicalSimilarity::Bm25f,
            tokenizer:TokenizerType::TantivySimpleV1,text_normalization:TextNormalization::None,
            field_length_normalization:FieldLengthNormalization::Shared,stop_words:StopwordType::None,
            frequent_words:FrequentwordType::None,ngram_indexing:0,clustering:Clustering::None,
            inference:serde_json::from_value::<Inference>(serde_json::json!({"External":{"dimensions":2,"precision":"F32","quantization":"None","similarity":"Cosine"}})).unwrap()},
            schema:serde_json::from_value(serde_json::json!([
                {"field":"body","field_type":"Text","store":false,"index_lexical":true},
                {"field":"vector","field_type":"Json","store":false,"index_lexical":false,"index_vector":true}
            ])).unwrap(),embedding_space_id:Some("test-model-f32-cosine-v1".into()),max_records:32,max_record_bytes:4096,
            max_operations:100,max_journal_bytes:1024*1024,max_candidates:100}
    }
    fn write(nonce: &str, id: &str, body: &str, expected: ExpectedVersion) -> RecordWrite {
        RecordWrite {
            vector_patch: None,
            temporal: None,
            operation_id: OperationId::new(nonce.into()),
            record_id: id.into(),
            expected,
            document: Some(
                serde_json::from_value(
                    serde_json::json!({"body":body,"vector":[2.0,0.0],"not_indexed":"preserved"}),
                )
                .unwrap(),
            ),
        }
    }

#[tokio::main]
async fn main() {
    assert!(cfg!(panic = "abort"), "probe must use the actual abort release profile");
    let args: Vec<String> = std::env::args().collect();
    let root = Path::new(&args[2]);
    match args[1].as_str() {
        "seed" => {
            let p = RecordPartition::create(root, config()).await.unwrap();
            let receipt = p.write(write("seed", "seed", "alpha", ExpectedVersion::Absent)).await.unwrap();
            assert_eq!(receipt.seq, 1);
        }
        "vector" => {
            let p = RecordPartition::open(root, config().max_journal_bytes).await.unwrap();
            let pause = root.join(".test-vector-consumer-pause");
            std::fs::write(&pause, b"pause").unwrap();
            std::fs::write(root.join(".test-vector-consumer-panic-after-append"), b"panic").unwrap();
            let batch = p.commit_writes(vec![write("tail", "tail", "alpha", ExpectedVersion::Absent)]).await.unwrap();
            assert!(matches!(&batch.results[..], [CommittedItem::Committed(r)] if r.seq == 2));
            std::fs::write(root.join("acknowledged"), b"2").unwrap();
            std::fs::remove_file(pause).unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            std::process::exit(70); // panic hook failed to fire
        }
        "queue" | "unknown" => {
            let p = RecordPartition::open(root, config().max_journal_bytes).await.unwrap();
            let marker = if args[1] == "unknown" { ".test-committed-panic-after-publication" } else { ".test-committed-panic" };
            std::fs::write(root.join(marker), b"panic").unwrap();
            let _ = p.commit_writes(vec![write("tail", "tail", "alpha", ExpectedVersion::Absent),
                write("remaining", "remaining", "alpha", ExpectedVersion::Absent)]).await;
            std::process::exit(70);
        }
        "verify-vector" | "verify-queue" | "verify-unknown" => {
            for marker in [".test-vector-consumer-panic-after-append", ".test-committed-panic", ".test-committed-panic-after-publication"] {
                let _ = std::fs::remove_file(root.join(marker));
            }
            let p = RecordPartition::open(root, config().max_journal_bytes).await.unwrap();
            assert!(p.get("seed").await.unwrap().is_some());
            let tail = p.operation(&OperationId::new("tail".into())).await.unwrap();
            if args[1] == "verify-vector" {
                assert_eq!(tail.unwrap().seq, 2);
                assert_eq!(std::fs::read(root.join("acknowledged")).unwrap(), b"2");
            } else if args[1] == "verify-unknown" {
                assert_eq!(tail.unwrap().seq, 2);
                assert_eq!(p.operation(&OperationId::new("remaining".into())).await.unwrap().unwrap().seq, 3);
            } else {
                assert!(tail.is_none());
                assert!(p.get("remaining").await.unwrap().is_none());
            }
            let receipt = p.write(write("tail", "tail", "alpha", ExpectedVersion::Absent)).await.unwrap();
            assert_eq!(receipt.seq, 2); // same operation ID: recover or execute once
            assert!(p.get("tail").await.unwrap().is_some());
        }
        _ => panic!("unknown probe mode"),
    }
}
