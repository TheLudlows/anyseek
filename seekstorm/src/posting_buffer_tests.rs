//! Growth must preserve linked postings and partial-block restoration.
use crate::{
    commit::Commit,
    index::{
        AccessType, Close, Clustering, DocumentCompression, FrequentwordType, IndexDocument,
        IndexMetaObject, LexicalSimilarity, SchemaField, StemmerType, StopwordType, TokenizerType,
        create_index, open_index,
    },
    search::{QueryType, ResultType, Search, SearchMode},
    vector::Inference,
};
use serde_json::json;
use std::{fs, time::Duration};

#[tokio::test]
async fn lazy_buffers_grow_commit_restore_and_clear() {
    for (name, access_type) in [("ram", AccessType::Ram), ("mmap", AccessType::Mmap)] {
        let directory =
            std::env::temp_dir().join(format!("seekpower-postings-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&directory);
        let meta = IndexMetaObject {
            id: 0,
            name: "buffers".into(),
            lexical_similarity: LexicalSimilarity::Bm25fProximity,
            tokenizer: TokenizerType::UnicodeAlphanumeric,
            stemmer: StemmerType::None,
            stop_words: StopwordType::None,
            frequent_words: FrequentwordType::English,
            ngram_indexing: 9,
            document_compression: DocumentCompression::Snappy,
            access_type,
            clustering: Clustering::None,
            inference: Inference::None,
        };
        let schema: Vec<SchemaField> = serde_json::from_value(json!([
            {"field":"title","field_type":"Text","store":true,"index_lexical":true},
            {"field":"body","field_type":"Text","store":true,"index_lexical":true}
        ]))
        .unwrap();
        let index = create_index(&directory, meta, &schema, &vec![], 11, true, Some(3))
            .await
            .unwrap();
        for shard in &index.read().await.shard_vec {
            let shard = shard.read().await;
            assert_eq!(shard.postings_buffer.capacity(), 0);
            assert!(
                shard
                    .segments_level0
                    .iter()
                    .all(|s| s.positions_compressed.capacity() == 0)
            );
        }
        // Multiple fields, embedded and nonembedded positions, frequent-word n-grams.
        let body = "the alpha and beta in the gamma delta epsilon zeta eta theta ".repeat(30);
        for i in 0..2400 {
            index
                .index_document(
                    serde_json::from_value(json!({
                        "title":format!("alpha record {i}"),"body":body
                    }))
                    .unwrap(),
                )
                .await
                .unwrap();
        }
        for _ in 0..1000 {
            if index.read().await.indexed_doc_count().await == 2400 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(index.read().await.indexed_doc_count().await, 2400);
        for shard in &index.read().await.shard_vec {
            let shard = shard.read().await;
            assert!(
                shard.postings_buffer.len() > 64 * 1024,
                "must exercise growth"
            );
            assert!(shard.postings_buffer.len() < 16 * 1024 * 1024);
            assert!(
                shard
                    .segments_level0
                    .iter()
                    .filter(|s| s.positions_compressed.capacity() > 0)
                    .count()
                    < 100
            );
        }
        assert_eq!(count(&index, "alpha").await, 2400);
        index.commit().await;
        for shard in &index.read().await.shard_vec {
            assert_eq!(shard.read().await.postings_buffer.capacity(), 0);
        }
        assert_eq!(count(&index, "alpha").await, 2400);
        index
            .index_document(
                serde_json::from_value(json!({"title":"alpha extra", "body":body})).unwrap(),
            )
            .await
            .unwrap();
        index.commit().await; // Restores and merges a partially filled committed block.
        assert_eq!(count(&index, "alpha").await, 2401);
        assert_eq!(count(&index, "\"alpha and beta\"").await, 2401);
        for shard in &index.read().await.shard_vec {
            let shard = shard.read().await;
            assert_eq!(shard.postings_buffer.capacity(), 0);
            assert!(
                shard
                    .segments_level0
                    .iter()
                    .filter(|s| s.positions_compressed.capacity() > 0)
                    .count()
                    < 100,
                "partial-block restoration must also allocate scratch lazily"
            );
        }
        index.close().await;
        drop(index);
        let reopened = open_index(&directory).await.unwrap();
        assert_eq!(count(&reopened, "alpha").await, 2401);
        reopened.write().await.clear_index().await;
        for shard in &reopened.read().await.shard_vec {
            assert_eq!(shard.read().await.postings_buffer.capacity(), 0);
        }
        assert_eq!(count(&reopened, "alpha").await, 0);
        reopened.close().await;
        drop(reopened);
        fs::remove_dir_all(directory).unwrap();
    }
}

async fn count(index: &crate::index::IndexArc, query: &str) -> usize {
    index
        .search(
            query.into(),
            None,
            QueryType::Union,
            SearchMode::Lexical,
            false,
            0,
            10,
            ResultType::Count,
            true,
            vec![],
            vec![],
            vec![],
        )
        .await
        .result_count_total
}
