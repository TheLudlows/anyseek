//! Exercise real file errors in the existing commit chain on both platforms.
use crate::{
    commit::Commit,
    index::{
        AccessType, Clustering, DocumentCompression, FieldLengthNormalization, FrequentwordType,
        IndexArc, IndexDocuments, IndexMetaObject, LexicalSimilarity, StopwordType,
        TextNormalization, TokenizerType, create_index,
    },
    vector::Inference,
};
use std::{fs, path::PathBuf};

async fn fixture(vector: bool) -> (PathBuf, IndexArc) {
    let base = std::env::var_os("SEEKPOWER_TEST_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir);
    fs::create_dir_all(&base).unwrap();
    let path = base.join(format!(
        "seekpower-commit-io-{:032x}",
        rand::random::<u128>()
    ));
    let inference: Inference = if vector {
        serde_json::from_value(serde_json::json!({"External":{"dimensions":2,"precision":"F32","quantization":"None","similarity":"Cosine"}})).unwrap()
    } else {
        Inference::None
    };
    let meta = IndexMetaObject {
        id: 0,
        name: "commit-io".into(),
        lexical_similarity: LexicalSimilarity::Bm25f,
        tokenizer: TokenizerType::UnicodeAlphanumeric,
        text_normalization: TextNormalization::None,
        field_length_normalization: FieldLengthNormalization::Shared,
        stop_words: StopwordType::None,
        frequent_words: FrequentwordType::None,
        ngram_indexing: 0,
        document_compression: DocumentCompression::Snappy,
        access_type: AccessType::Mmap,
        clustering: Clustering::None,
        inference,
    };
    let mut schema =
        serde_json::json!([{"field":"body","field_type":"Text","store":true,"index_lexical":true}]);
    if vector {
        schema.as_array_mut().unwrap().push(serde_json::json!({"field":"vector","field_type":"Json","store":false,"index_lexical":false,"index_vector":true}));
    }
    let index = create_index(
        &path,
        meta,
        &serde_json::from_value(schema).unwrap(),
        11,
        true,
        Some(1),
    )
    .await
    .unwrap();
    index
        .index_documents(vec![
            serde_json::from_value(serde_json::json!({"body":"alpha beta", "vector":[1.0,0.0]}))
                .unwrap(),
        ])
        .await
        .unwrap();
    // No polling: successful indexing now waits for application.
    assert_eq!(index.read().await.indexed_doc_count().await, 1);
    (path, index)
}

async fn release(index: IndexArc, path: PathBuf) {
    // Failed mutable legacy index files are not safe to commit again. Release
    // the internal backreference/mappings, without trying to repair the files.
    for shard in &index.read().await.shard_vec {
        shard.write().await.index_option = None;
    }
    drop(index);
    fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn commit_io_errors_reach_public_caller() {
    for component in ["index.bin", "docstore.bin", "vector.bin"] {
        let (path, index) = fixture(component == "vector.bin").await;
        {
            let shard = index.read().await.shard_vec[0].clone();
            let mut shard = shard.write().await;
            let readonly = fs::File::open(path.join("shards/0").join(component)).unwrap();
            match component {
                "index.bin" => shard.index_file = readonly,
                "docstore.bin" => shard.docstore_file = readonly,
                "vector.bin" => shard.vector_file = readonly,
                _ => unreachable!(),
            }
        }
        assert!(
            index.commit().await.is_err(),
            "{component} error was swallowed"
        );
        release(index, path).await;
    }
}

#[tokio::test]
async fn repeated_partial_commits_release_mappings_and_reopen() {
    let (path, index) = fixture(true).await;
    index.commit().await.unwrap();
    index
        .index_documents(vec![
            serde_json::from_value(serde_json::json!({"body":"second", "vector":[0.0,1.0]}))
                .unwrap(),
        ])
        .await
        .unwrap();
    index.commit().await.unwrap();
    for shard in &index.read().await.shard_vec {
        shard.write().await.index_option = None;
    }
    drop(index);
    let reopened = crate::index::open_index(&path).await.unwrap();
    assert_eq!(reopened.read().await.indexed_doc_count().await, 2);
    release(reopened, path).await;
}
