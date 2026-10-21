use crate::api_types::{
    CreateIndexRequest, DeleteQueryRequest, IndexResponseObject, SearchRequestObject,
    SearchResultObject,
};
use crate::http_server::ApiError;
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    time::Instant,
};

use itertools::Itertools;
use std::collections::HashSet;

use crate::keys::ApikeyObject;

use anyseek::{
    commit::Commit,
    index::{
        AccessType, Clustering, DeleteDocument, DeleteDocuments, Document, DocumentCompression,
        FrequentwordType, IS_AVX2, IS_NEON, IndexArc, IndexDocuments, IndexMetaObject,
        LexicalSimilarity, SchemaField, UpdateDocuments, create_index,
    },
    iterator::{GetIterator, IteratorResult},
    search::Search,
    vector::Inference,
};

use crate::VERSION;

pub(crate) fn live_api() -> String {
    let simd = if *IS_AVX2 {
        " (AVX2 enabled)"
    } else if *IS_NEON {
        " (NEON enabled)"
    } else {
        " (SIMD disabled)"
    };
    "anyseek server ".to_owned() + VERSION + simd
}

pub(crate) async fn create_index_api(
    index_path: &PathBuf,
    request: CreateIndexRequest,
    force_shard_number: Option<usize>,
    apikey_object: &mut ApikeyObject,
) -> Result<u64, ApiError> {
    let mut names = HashSet::new();
    if request.index_name.trim().is_empty() || request.schema.is_empty() {
        return Err(ApiError::BadRequest(
            "index_name and schema must be nonempty".into(),
        ));
    }
    for field in &request.schema {
        if field.field.trim().is_empty() || !names.insert(&field.field) {
            return Err(ApiError::BadRequest(
                "schema field names must be nonempty and unique".into(),
            ));
        }
        if !field.boost.is_finite() || field.boost <= 0.0 {
            return Err(ApiError::BadRequest(
                "field boost must be finite and positive".into(),
            ));
        }
    }
    let schema: Vec<SchemaField> = request.schema.into_iter().map(Into::into).collect();
    let inference = request.vector.map(Inference::from).unwrap_or_default();
    anyseek::vector::validate_vector_schema(&inference, &schema)?;
    let mut index_id: u64 = 0;
    for id in apikey_object.index_list.keys().sorted() {
        if *id == index_id {
            index_id = id + 1;
        } else {
            break;
        }
    }

    let index_id_path = Path::new(&index_path)
        .join(apikey_object.id.to_string())
        .join(index_id.to_string());
    fs::create_dir_all(&index_id_path)?;

    let meta = IndexMetaObject {
        id: index_id,
        name: request.index_name,
        lexical_similarity: LexicalSimilarity::Bm25fProximity,
        tokenizer: request.tokenizer,
        text_normalization: request.text_normalization,
        field_length_normalization: request.field_length_normalization,
        stop_words: request.stop_words,
        frequent_words: FrequentwordType::English,
        ngram_indexing: 9,
        document_compression: DocumentCompression::Snappy,
        access_type: AccessType::Mmap,
        clustering: Clustering::Auto,
        inference,
    };

    let index_arc = create_index(&index_id_path, meta, &schema, 11, true, force_shard_number)
        .await
        .map_err(|error| {
            log::error!("Creating index failed: {error}");
            ApiError::Internal
        })?;

    apikey_object.index_list.insert(index_id, index_arc);

    Ok(index_id)
}

pub(crate) async fn delete_index_api(
    index_id: u64,
    index_list: &mut HashMap<u64, IndexArc>,
) -> Result<u64, String> {
    if let Some(index_arc) = index_list.get(&index_id) {
        let mut index_mut = index_arc.write().await;
        index_mut.delete_index();
        drop(index_mut);
        index_list.remove(&index_id);

        Ok(index_list.len() as u64)
    } else {
        Err("index_id not found".to_string())
    }
}

pub(crate) async fn commit_index_api(index_arc: &IndexArc) -> std::io::Result<u64> {
    index_arc.commit().await?;
    Ok(index_arc.read().await.indexed_doc_count().await as u64)
}

async fn index_info(index: &IndexArc) -> IndexResponseObject {
    let index = index.read().await;
    IndexResponseObject {
        text_normalization: index.meta.text_normalization,
        field_length_normalization: index.meta.field_length_normalization,
        version: VERSION.to_string(),
        schema: index
            .schema_map
            .values()
            .sorted_by_key(|f| &f.field)
            .map(Into::into)
            .collect(),
        id: index.meta.id,
        name: index.meta.name.clone(),
        indexed_doc_count: index.indexed_doc_count().await,
        committed_doc_count: index.committed_doc_count().await,
    }
}

pub(crate) async fn get_index_info_api(
    index_id: u64,
    indexes: &HashMap<u64, IndexArc>,
) -> Result<IndexResponseObject, String> {
    let index = indexes.get(&index_id).ok_or("index_id not found")?;
    Ok(index_info(index).await)
}

pub(crate) async fn get_apikey_indices_info_api(
    indexes: &HashMap<u64, IndexArc>,
) -> Result<Vec<IndexResponseObject>, String> {
    let mut result = Vec::with_capacity(indexes.len());
    for index in indexes.values() {
        result.push(index_info(index).await);
    }
    Ok(result)
}

pub(crate) async fn index_documents_api(
    index_arc: &IndexArc,
    document_vec: Vec<Document>,
) -> Result<usize, String> {
    index_arc.index_documents(document_vec).await?;
    Ok(index_arc.read().await.indexed_doc_count().await)
}

pub(crate) async fn get_document_api(
    index_arc: &IndexArc,
    document_id: usize,
    fields: Vec<String>,
) -> Option<Document> {
    if !index_arc.read().await.stored_field_names.is_empty() {
        index_arc
            .read()
            .await
            .get_document(document_id, true, &HashSet::from_iter(fields))
            .await
            .ok()
    } else {
        None
    }
}

pub(crate) async fn update_documents_api(
    index_arc: &IndexArc,
    id_document_vec: Vec<(u64, Document)>,
) -> Result<u64, String> {
    index_arc.update_documents(id_document_vec).await?;
    Ok(index_arc.read().await.indexed_doc_count().await as u64)
}

pub(crate) async fn delete_document_api(
    index_arc: &IndexArc,
    document_id: u64,
) -> Result<u64, String> {
    index_arc.delete_document(document_id).await;
    Ok(index_arc.read().await.indexed_doc_count().await as u64)
}

pub(crate) async fn delete_documents_by_object_api(
    index_arc: &IndexArc,
    document_id_vec: Vec<u64>,
) -> Result<u64, String> {
    index_arc.delete_documents(document_id_vec).await;
    Ok(index_arc.read().await.indexed_doc_count().await as u64)
}

pub(crate) async fn delete_documents_by_query_api(
    index_arc: &IndexArc,
    request: DeleteQueryRequest,
) -> Result<u64, String> {
    if request.query.trim().is_empty() {
        return Err(
            "deletion by query requires nonempty text; use all:true to delete all documents".into(),
        );
    }
    let length = index_arc.read().await.indexed_doc_count().await;
    let result = index_arc
        .search(
            request.query,
            None,
            request.operator.query_type(),
            anyseek::search::SearchMode::Lexical,
            false,
            0,
            length,
            anyseek::search::ResultType::Topk,
            true,
            request.search_fields,
            request.filter,
            Vec::new(),
        )
        .await;
    if let Some(error) = result.error {
        return Err(error);
    }
    delete_documents_by_object_api(
        index_arc,
        result
            .results
            .into_iter()
            .map(|r| r.doc_id as u64)
            .collect(),
    )
    .await
}

pub(crate) async fn get_iterator_api_post(
    index_arc: &IndexArc,
    document_id: Option<u64>,
    skip: usize,
    take: isize,
    include_deleted: bool,
    include_document: bool,
    fields: Vec<String>,
) -> IteratorResult {
    index_arc
        .get_iterator(
            document_id,
            skip,
            take,
            include_deleted,
            include_document,
            fields,
        )
        .await
}

pub(crate) async fn clear_index_api(index_arc: &IndexArc) -> Result<u64, String> {
    let mut index_mut = index_arc.write().await;
    index_mut.clear_index().await;
    Ok(index_mut.indexed_doc_count().await as u64)
}

pub(crate) async fn query_index_api(
    index_arc: &IndexArc,
    search_request: SearchRequestObject,
    default_nprobe: std::num::NonZeroUsize,
) -> Result<SearchResultObject, String> {
    let start_time = Instant::now();
    if search_request
        .offset
        .checked_add(search_request.length)
        .is_none()
    {
        return Err("offset plus length is too large".into());
    }
    if search_request.query_vector.is_none() && search_request.similarity_threshold.is_some() {
        return Err("similarity_threshold requires query_vector".into());
    }
    let mode = search_request.search_mode(default_nprobe)?;
    let result_type = search_request.result_type();
    let query_vector = match search_request.query_vector.as_ref() {
        Some(value) => Some(anyseek::vector::parse_external_embedding(
            value,
            &index_arc.read().await.meta.inference,
        )?),
        _ => None,
    };

    let result_object = index_arc
        .search(
            search_request.query_string.to_owned(),
            query_vector,
            search_request.query_type(),
            mode,
            true,
            search_request.offset,
            search_request.length,
            result_type,
            true,
            search_request.field_filter,
            search_request.facet_filter,
            search_request.result_sort,
        )
        .await;

    if let Some(error) = result_object.error {
        return Err(error);
    }
    let elapsed_time = start_time.elapsed().as_nanos();

    let mut results: Vec<Document> = Vec::new();

    if !index_arc.read().await.stored_field_names.is_empty() {
        let return_fields_filter = HashSet::from_iter(search_request.fields);

        for result in result_object.results.iter() {
            match index_arc
                .read()
                .await
                .get_document(result.doc_id, true, &return_fields_filter)
                .await
            {
                Ok(doc) => {
                    let mut doc = doc;
                    doc.insert("_id".to_string(), result.doc_id.into());
                    doc.insert("_score".to_string(), result.score.into());

                    results.push(doc);
                }
                Err(_e) => {}
            }
        }
    }

    Ok(SearchResultObject {
        original_query: result_object.original_query.to_owned(),
        query: result_object.query.to_owned(),
        time: elapsed_time,
        offset: search_request.offset,
        length: search_request.length,
        count: result_object.results.len(),
        count_total: search_request
            .count
            .then_some(result_object.result_count_total),
        query_terms: result_object.query_terms,
        results,
    })
}
