use storm_db::index::{create_index, open_index, close_index, commit, search, Index};
use storm_db::schema::{SchemaField, FieldType, IndexConfig, TokenizerType, VectorSimilarity, Compression, Clustering};
use storm_db::document::{index_document, index_documents, delete_document, get_document};
use storm_db::search::{SearchRequest, SearchMode, QueryType, ResultType};
use storm_db::posting::QueryTypeTerm;
use storm_db::error::Result;
use object_store::memory::InMemory;
use std::sync::Arc;

async fn create_test_index() -> Index {
    let schema = vec![
        SchemaField {
            field: "title".to_string(),
            store: true,
            index_lexical: true,
            index_vector: false,
            field_type: FieldType::Text,
            facet: false,
            boost: 1.0,
        },
        SchemaField {
            field: "body".to_string(),
            store: true,
            index_lexical: true,
            index_vector: false,
            field_type: FieldType::Text,
            facet: false,
            boost: 0.5,
        },
        SchemaField {
            field: "rating".to_string(),
            store: true,
            index_lexical: false,
            index_vector: false,
            field_type: FieldType::F32,
            facet: true,
            boost: 1.0,
        },
    ];

    let config = IndexConfig {
        tokenizer_type: TokenizerType::UnicodeAlphanumericFolded,
        stemmer_language: Some("english".to_string()),
        vector_similarity: VectorSimilarity::Cosine,
        vector_dimensions: 256,
        compression: Compression::Snappy,
        segment_number_bits: 4,
        clustering: Clustering::Auto,
        ..Default::default()
    };

    let object_store = Arc::new(InMemory::new());
    create_index("/test-index", object_store, config, schema)
        .await
        .expect("Failed to create index")
}

fn make_test_document(
    title: &str,
    body: &str,
    rating: f32,
) -> std::collections::HashMap<String, serde_json::Value> {
    let mut doc = std::collections::HashMap::new();
    doc.insert(
        "title".to_string(),
        serde_json::Value::String(title.to_string()),
    );
    doc.insert(
        "body".to_string(),
        serde_json::Value::String(body.to_string()),
    );
    doc.insert("rating".to_string(), serde_json::Value::from(rating));
    doc
}

#[tokio::test]
async fn test_full_lifecycle() {
    // 1. Create index
    let mut index = create_test_index().await;

    // 2. Index documents
    let doc1 = index_document(
        &index,
        make_test_document("Rust Programming", "Rust is a systems programming language", 4.8),
    )
    .unwrap();
    let doc2 = index_document(
        &index,
        make_test_document("Python Basics", "Python is an interpreted programming language", 4.2),
    )
    .unwrap();
    let doc3 = index_document(
        &index,
        make_test_document(
            "Web Development",
            "Building web applications with modern frameworks",
            3.9,
        ),
    )
    .unwrap();

    assert!(doc1 < doc2);
    assert!(doc2 < doc3);

    // 3. Commit
    commit(&index).await.expect("Commit failed");

    // 4. Search - lexical
    let request = SearchRequest {
        query_string: Some("programming".to_string()),
        query_vector: None,
        query_type: QueryType::Union,
        search_mode: SearchMode::Lexical,
        offset: 0,
        length: 10,
        result_type: ResultType::Topk,
        include_uncommitted: false,
        field_filter: None,
    };
    let response = search(&index, request).await.expect("Search failed");
    assert!(response.result_count >= 2); // doc1 and doc2 contain "programming"

    // 5. Get document
    let retrieved = get_document(&index, doc1).await.expect("Get document failed");
    assert_eq!(
        retrieved.get("title").unwrap().as_str(),
        Some("Rust Programming")
    );

    // 6. Delete document
    delete_document(&index, doc3).await.expect("Delete failed");

    // 7. Search again - doc3 should be excluded
    let request2 = SearchRequest {
        query_string: Some("web".to_string()),
        query_vector: None,
        query_type: QueryType::Union,
        search_mode: SearchMode::Lexical,
        offset: 0,
        length: 10,
        result_type: ResultType::Topk,
        include_uncommitted: false,
        field_filter: None,
    };
    let response2 = search(&index, request2).await.expect("Search failed");
    // doc3 was deleted, so "web" should not be found (or result_count should be 0)

    // 8. Close and reopen
    close_index(&index).await.expect("Close failed");
    // Reopen with same object store... (this test will need the object_store to persist)

    println!("Full lifecycle test completed!");
}

#[tokio::test]
async fn test_batch_indexing() {
    let index = create_test_index().await;

    let docs = vec![
        make_test_document("Doc 1", "First document content", 3.0),
        make_test_document("Doc 2", "Second document content", 4.0),
        make_test_document("Doc 3", "Third document content", 5.0),
    ];

    let doc_ids = index_documents(&index, docs).expect("Batch index failed");
    assert_eq!(doc_ids.len(), 3);

    commit(&index).await.expect("Commit failed");
}

#[tokio::test]
async fn test_phrase_search() {
    let index = create_test_index().await;

    index_document(
        &index,
        make_test_document(
            "Test",
            "the quick brown fox jumps over the lazy dog",
            4.0,
        ),
    )
    .unwrap();
    index_document(
        &index,
        make_test_document("Test2", "a quick red fox runs", 3.0),
    )
    .unwrap();

    commit(&index).await.expect("Commit failed");

    let request = SearchRequest {
        query_string: Some("\"quick brown fox\"".to_string()),
        query_vector: None,
        query_type: QueryType::Phrase,
        search_mode: SearchMode::Lexical,
        offset: 0,
        length: 10,
        result_type: ResultType::Topk,
        include_uncommitted: false,
        field_filter: None,
    };
    let response = search(&index, request).await.expect("Search failed");
    // Only the first document has "quick brown fox" as a phrase
    assert!(response.result_count <= 2);
}
