use crate::api_types::{SearchRequestObject, SearchResultObject, GetDocumentRequest, IndexResponseObject};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    time::Instant,
};

use itertools::Itertools;
use std::collections::HashSet;

use crate::multi_tenancy::ApikeyObject;

use seekpower::{
    commit::Commit,
    index::{
        AccessType, Clustering, DeleteDocument, DeleteDocuments, DeleteDocumentsByQuery, Document, DocumentCompression,
        FrequentwordType, IS_AVX2, IS_NEON,
        IndexArc, IndexDocument, IndexDocuments, IndexMetaObject,
        LexicalSimilarity, SchemaField, StopwordType, TokenizerType,
        UpdateDocument, UpdateDocuments, create_index, open_index,
    },
    iterator::{GetIterator, IteratorResult},
    search::{Search, SearchMode},
    vector::Inference,
};

use crate::{VERSION, multi_tenancy::calculate_hash};

const APIKEY_PATH: &str = "apikey.json";

/// Save file atomically
pub(crate) fn save_file_atomically(path: &PathBuf, content: String) {
    let mut temp_path = path.clone();
    temp_path.set_extension("bak");
    fs::write(&temp_path, content).unwrap();
    match fs::rename(temp_path, path) {
        Ok(_) => {}
        Err(e) => println!("error: {e:?}"),
    }
}

pub(crate) fn save_apikey_data(apikey: &ApikeyObject, index_path: &PathBuf) {
    let apikey_id: u64 = apikey.id;

    let apikey_id_path = Path::new(&index_path).join(apikey_id.to_string());
    let apikey_persistence_json = serde_json::to_string(&apikey).unwrap();
    let apikey_persistence_path = Path::new(&apikey_id_path).join(APIKEY_PATH);
    save_file_atomically(&apikey_persistence_path, apikey_persistence_json);
}

pub(crate) fn live_api() -> String {
    let simd = if *IS_AVX2 {
        " (AVX2 enabled)"
    } else if *IS_NEON {
        " (NEON enabled)"
    } else {
        " (SIMD disabled)"
    };
    "Seekpower server ".to_owned() + VERSION + simd
}

pub(crate) fn create_apikey_api<'a>(
    index_path: &'a PathBuf,
    apikey: &[u8],
    apikey_list: &'a mut HashMap<u128, ApikeyObject>,
) -> &'a mut ApikeyObject {
    let apikey_hash_u128 = calculate_hash(&apikey) as u128;
    // A repeated identity must not replace its scope or orphan indices.
    if apikey_list.contains_key(&apikey_hash_u128) {
        return apikey_list.get_mut(&apikey_hash_u128).unwrap();
    }

    let mut apikey_id: u64 = 0;
    let mut apikey_list_vec: Vec<(&u128, &ApikeyObject)> = apikey_list.iter().collect();
    apikey_list_vec.sort_by_key(|a| a.1.id);
    for value in apikey_list_vec {
        if value.1.id == apikey_id {
            apikey_id = value.1.id + 1;
        } else {
            break;
        }
    }

    let apikey_object = ApikeyObject {
        id: apikey_id,
        apikey_hash: apikey_hash_u128,
        index_list: HashMap::new(),
    };

    let apikey_id_path = Path::new(&index_path).join(apikey_id.to_string());
    fs::create_dir_all(apikey_id_path).unwrap();

    save_apikey_data(&apikey_object, index_path);

    apikey_list.insert(apikey_hash_u128, apikey_object);
    apikey_list.get_mut(&apikey_hash_u128).unwrap()
}

pub(crate) fn delete_apikey_api(
    index_path: &PathBuf,
    apikey_list: &mut HashMap<u128, ApikeyObject>,
    apikey_hash: u128,
) -> Result<u64, String> {
    if let Some(apikey_object) = apikey_list.get(&apikey_hash) {
        let apikey_id_path = Path::new(&index_path).join(apikey_object.id.to_string());
        println!("delete path {}", apikey_id_path.to_string_lossy());
        fs::remove_dir_all(&apikey_id_path).unwrap();

        apikey_list.remove(&apikey_hash);
        Ok(apikey_list.len() as u64)
    } else {
        Err("not found".to_string())
    }
}

/// Open all indices below a single apikey
pub(crate) async fn open_all_indices(
    index_path: &PathBuf,
    index_list: &mut HashMap<u64, IndexArc>,
) {
    if !Path::exists(index_path) {
        fs::create_dir_all(index_path).unwrap();
    }

    for result in fs::read_dir(index_path).unwrap() {
        let path = result.unwrap();
        if path.path().is_dir() {
            let single_index_path = path.path();

            let index_arc = match open_index(&single_index_path).await {
                Ok(index_arc) => index_arc,
                Err(err) => {
                    println!("{} {}", err, single_index_path.display());
                    continue;
                }
            };

            let index_id = index_arc.read().await.meta.id;

            index_list.insert(index_id, index_arc);
        }
    }
}

/// Open api key
pub(crate) async fn open_apikey(
    index_path: &PathBuf,
    apikey_list: &mut HashMap<u128, ApikeyObject>,
) -> bool {
    let apikey_path = Path::new(&index_path).join(APIKEY_PATH);
    match fs::read_to_string(apikey_path) {
        Ok(apikey_string) => {
            let mut apikey_object: ApikeyObject = serde_json::from_str(&apikey_string).unwrap();

            open_all_indices(index_path, &mut apikey_object.index_list).await;
            apikey_list.insert(apikey_object.apikey_hash, apikey_object);

            true
        }
        Err(_) => false,
    }
}

/// Open all apikeys in the specified path
pub(crate) async fn open_all_apikeys(
    index_path: &PathBuf,
    apikey_list: &mut HashMap<u128, ApikeyObject>,
) -> bool {
    let mut test_index_flag = false;
    if !Path::exists(index_path) {
        println!("index path not found: {} ", index_path.to_string_lossy());
        fs::create_dir_all(index_path).unwrap();
    }

    for result in fs::read_dir(index_path).unwrap() {
        let path = result.unwrap();
        if path.path().is_dir() {
            let single_index_path = path.path();
            test_index_flag |= open_apikey(&single_index_path, apikey_list).await;
        }
    }
    test_index_flag
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_index_api<'a>(
    index_path: &'a PathBuf,
    index_name: String,
    schema: Vec<SchemaField>,
    lexical_similarity: LexicalSimilarity,
    tokenizer: TokenizerType,
    stop_words: StopwordType,
    frequent_words: FrequentwordType,
    ngram_indexing: u8,
    document_compression: DocumentCompression,
    force_shard_number: Option<usize>,
    apikey_object: &'a mut ApikeyObject,
    mute: bool,
    clustering: Clustering,
    inference: Inference,
) -> Result<u64, String> {
    seekpower::vector::validate_vector_schema(&inference, &schema)?;
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
    fs::create_dir_all(&index_id_path).map_err(|e| e.to_string())?;

    let meta = IndexMetaObject {
        id: index_id,
        name: index_name,
        lexical_similarity,
        tokenizer,
        stop_words,
        frequent_words,
        ngram_indexing,
        document_compression,
        access_type: AccessType::Mmap,
        clustering,
        inference,
    };

    let index_arc = create_index(
        &index_id_path,
        meta,
        &schema,
        11,
        mute,
        force_shard_number,
    )
    .await?;

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

pub(crate) async fn commit_index_api(index_arc: &IndexArc) -> Result<u64, String> {
    index_arc.commit().await;
    Ok(index_arc.read().await.indexed_doc_count().await as u64)
}

pub(crate) async fn get_index_info_api(
    index_id: u64,
    index_list: &HashMap<u64, IndexArc>,
) -> Result<IndexResponseObject, String> {
    if let Some(index_arc) = index_list.get(&index_id) {
        let index_ref = index_arc.read().await;

        Ok(IndexResponseObject {
            version: VERSION.to_string(),
            schema: index_ref.schema_map.clone(),
            id: index_ref.meta.id,
            name: index_ref.meta.name.clone(),
            indexed_doc_count: index_ref.indexed_doc_count().await,
            committed_doc_count: index_ref.committed_doc_count().await,
            operations_count: 0,
            query_count: 0,

        })
    } else {
        Err("index_id not found".to_string())
    }
}

pub(crate) async fn get_apikey_indices_info_api(
    index_list: &HashMap<u64, IndexArc>,
) -> Result<Vec<IndexResponseObject>, String> {
    let mut index_response_object_vec: Vec<IndexResponseObject> = Vec::new();
    for index in index_list.iter() {
        let index_ref = index.1.read().await;

        index_response_object_vec.push(IndexResponseObject {
            version: VERSION.to_string(),
            schema: index_ref.schema_map.clone(),
            id: index_ref.meta.id,
            name: index_ref.meta.name.clone(),
            indexed_doc_count: index_ref.indexed_doc_count().await,
            committed_doc_count: index_ref.committed_doc_count().await,
            operations_count: 0,
            query_count: 0,

        });
    }

    Ok(index_response_object_vec)
}

pub(crate) async fn index_document_api(
    index_arc: &IndexArc,
    document: Document,
) -> Result<usize, String> {
    index_arc.index_document(document).await?;

    Ok(index_arc.read().await.indexed_doc_count().await)
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
    get_document_request: GetDocumentRequest,
) -> Option<Document> {
    if !index_arc.read().await.stored_field_names.is_empty() {
        index_arc
            .read()
            .await
            .get_document(
                document_id,
                true,
                &HashSet::from_iter(get_document_request.fields),
            )
            .await
            .ok()
    } else {
        None
    }
}

pub(crate) async fn update_document_api(
    index_arc: &IndexArc,
    id_document: (u64, Document),
) -> Result<u64, String> {
    index_arc.update_document(id_document).await?;
    Ok(index_arc.read().await.indexed_doc_count().await as u64)
}

pub(crate) async fn update_documents_api(
    index_arc: &IndexArc,
    id_document_vec: Vec<(u64, Document)>,
) -> Result<u64, String> {
    index_arc.update_documents(id_document_vec).await?;
    Ok(index_arc.read().await.indexed_doc_count().await as u64)
}

pub(crate) async fn delete_document_by_parameter_api(
    index_arc: &IndexArc,
    document_id: u64,
) -> Result<u64, String> {
    index_arc.delete_document(document_id).await;
    Ok(index_arc.read().await.indexed_doc_count().await as u64)
}

pub(crate) async fn delete_document_by_object_api(
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
    search_request: SearchRequestObject,
) -> Result<u64, String> {
    seekpower::search::validate_metadata_filters(index_arc, &search_request.facet_filter).await?;
    index_arc
        .delete_documents_by_query(
            search_request.query_string.to_owned(),
            search_request.query_type_default,
            search_request.offset,
            search_request.length,
            search_request.realtime,
            search_request.field_filter,
            search_request.facet_filter,
            search_request.result_sort,
        )
        .await;

    Ok(index_arc.read().await.indexed_doc_count().await as u64)
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
) -> Result<SearchResultObject, String> {
    let start_time = Instant::now();

    let query_vector = match search_request.query_vector.as_ref() {
        Some(value) if search_request.search_mode != SearchMode::Lexical => Some(
            seekpower::vector::parse_external_embedding(value, &index_arc.read().await.meta.inference)?
        ),
        _ => None,
    };

    let result_object = index_arc
        .search(
            search_request.query_string.to_owned(),
            query_vector,
            search_request.query_type_default,
            search_request.search_mode,
            search_request.enable_empty_query,
            search_request.offset,
            search_request.length,
            search_request.result_type,
            search_request.realtime,
            search_request.field_filter,

            search_request.facet_filter,
            search_request.result_sort,

        )
        .await;

    if let Some(error) = result_object.error { return Err(error); }
    let elapsed_time = start_time.elapsed().as_nanos();

    let mut results: Vec<Document> = Vec::new();

    if !index_arc.read().await.stored_field_names.is_empty() {
        let return_fields_filter = HashSet::from_iter(search_request.fields);

        for result in result_object.results.iter() {
            match index_arc
                .read()
                .await
                .get_document(
                    result.doc_id,
                    search_request.realtime,
                    &return_fields_filter,
                )
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
        count_total: result_object.result_count_total,
        query_terms: result_object.query_terms,
        results,

    })
}
