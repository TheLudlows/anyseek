use crate::api_types::{SearchRequestObject, CreateIndexRequest, GetIteratorRequest, GetDocumentRequest};
use hyper_util::server::conn::auto;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;
use std::str;
use std::sync::Arc;
use std::{convert::Infallible, net::SocketAddr};

use rand::TryRng;
use rand::rngs::SysRng;

use http_body_util::{BodyExt, Full, combinators::BoxBody};
use hyper::{
    Method, Request, Response, StatusCode,
    body::{Bytes, Incoming},
    header::HeaderValue,
    service::service_fn,
};
use hyper_util::rt::{TokioExecutor, TokioIo};

use seekstorm::INDEX_RUNTIME;
use seekstorm::index::{
    Document,
    Synonym,
};

use sha2::Digest;
use sha2::Sha256;

use tokio::net::TcpListener;

use base64::{Engine as _, engine::general_purpose};

use crate::api_endpoints::create_index_api;
use crate::api_endpoints::delete_apikey_api;
use crate::api_endpoints::update_documents_api;
use crate::api_endpoints::{add_synonyms_api, get_index_info_api, set_synonyms_api};
use crate::api_endpoints::{clear_index_api, close_index_api};
use crate::api_endpoints::{commit_index_api, create_apikey_api};
use crate::api_endpoints::{
    delete_document_by_object_api, delete_document_by_parameter_api, index_documents_api,
};
use crate::api_endpoints::{delete_documents_by_object_api, delete_documents_by_query_api};
use crate::api_endpoints::delete_index_api;
use crate::api_endpoints::get_apikey_indices_info_api;
use crate::api_endpoints::{get_document_api, get_synonyms_api};
use crate::api_endpoints::{
    get_iterator_api_post, live_api, update_document_api,
};
use crate::api_endpoints::{index_document_api, query_index_api_post};
use crate::multi_tenancy::{ApikeyObject, CreateApikeyRequest, DeleteApikeyRequest, get_apikey_hash};
use crate::{MASTER_KEY_SECRET, VERSION};

const INDEX_HTML: &str = include_str!("../web/index.html");
const FLEXBOX_CSS: &str = include_str!("../web/css/flexboxgrid.min.css");
const MASTER_CSS: &str = include_str!("../web/css/master.css");
const SETUP_JS: &str = include_str!("../web/js/setup.js");
const MASTER_JS: &str = include_str!("../web/js/master.js");
const JQUERY_JS: &str = include_str!("../web/js/jquery-3.7.1.min.js");
const LOGO_SVG: &[u8] = include_bytes!("../web/svg/logo.svg");
const FAVICON_16: &[u8] = include_bytes!("../web/favicon-16x16.png");
const FAVICON_32: &[u8] = include_bytes!("../web/favicon-32x32.png");

pub(crate) fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

pub(crate) fn status(
    status: StatusCode,
    error_message: String,
) -> Response<BoxBody<Bytes, Infallible>> {
    Response::builder()
        .status(status)
        .body(BoxBody::new(Full::new(error_message.into())))
        .unwrap()
}

fn with_cors_headers(
    mut response: Response<BoxBody<Bytes, Infallible>>,
) -> Response<BoxBody<Bytes, Infallible>> {
    response
        .headers_mut()
        .insert("Access-Control-Allow-Origin", HeaderValue::from_static("*"));
    response
}

enum HttpServerError {
    IndexNotFound,
    ApiKeyNotFound,
    SynonymsNotFound,
    Unauthorized,
    BadRequest(String),
    InternalServerError,
    NotImplemented,
    DocumentNotFound,
}

impl From<HttpServerError> for Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
    fn from(error: HttpServerError) -> Self {
        Ok(match error {
            HttpServerError::IndexNotFound => {
                status(StatusCode::NOT_FOUND, "index does not exist".to_string())
            }
            HttpServerError::ApiKeyNotFound => {
                status(StatusCode::NOT_FOUND, "apikey does not exist".to_string())
            }
            HttpServerError::SynonymsNotFound => {
                status(StatusCode::NOT_FOUND, "synonyms not found".to_string())
            }
            HttpServerError::Unauthorized => status(
                StatusCode::UNAUTHORIZED,
                "apikey invalid or missing".to_string(),
            ),
            HttpServerError::BadRequest(error_message) => status(
                StatusCode::BAD_REQUEST,
                format!("bad request:{}", error_message),
            ),
            HttpServerError::InternalServerError => status(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal server error".to_string(),
            ),
            HttpServerError::NotImplemented => {
                status(StatusCode::NOT_IMPLEMENTED, "not implemented".to_string())
            }
            HttpServerError::DocumentNotFound => {
                status(StatusCode::NOT_FOUND, "document not found".to_string())
            }
        })
    }
}

pub(crate) async fn http_request_handler(
    index_path: PathBuf,
    apikey_list: Arc<tokio::sync::RwLock<HashMap<u128, ApikeyObject>>>,
    req: Request<Incoming>,
    _remote_addr: SocketAddr,
    force_shard_number: Option<usize>,
) -> Result<Response<BoxBody<Bytes, Infallible>>, Infallible> {
    let apikey_header = req
        .headers()
        .get("apikey")
        .map(|v| v.to_str().unwrap_or("").to_string());

    let mut parts: [&str; 6] = ["", "", "", "", "", ""];
    let mut i = 0;
    let path = req.uri().path();
    for part in path.split('/') {
        if part.is_empty() {
            continue;
        }
        parts[i] = part;

        i += 1;

        if i >= parts.len() {
            break;
        }
    }

    // Retired binary protocol has no deserializer or execution path.
    if parts[0..3] == ["api", "v2", "index"] && parts[4] == "query" {
        return Ok(status(StatusCode::NOT_FOUND,
            "binary query endpoint removed; use POST /api/v1/index/{index_id}/query with JSON".to_string()));
    }
    let post_json_endpoint = parts[0..3] == ["api", "v1", "index"]
        && matches!(parts[4], "query" | "iterator") && parts[5].is_empty();
    if req.method() == Method::OPTIONS {
        return Ok(Response::builder()
            .status(StatusCode::NO_CONTENT)
            .header("Access-Control-Allow-Origin", "*")
            .header("Access-Control-Allow-Methods", if post_json_endpoint { "POST, OPTIONS" } else { "POST, GET, OPTIONS" })
            .header("Access-Control-Allow-Headers", "content-type, apikey")
            .body(BoxBody::new(Full::new(Bytes::new())))
            .unwrap());
    }
    if post_json_endpoint {
        if req.method() != Method::POST {
            let mut response = status(StatusCode::METHOD_NOT_ALLOWED,
                "use POST with a JSON request body for search and document iteration".to_string());
            response.headers_mut().insert("Allow", HeaderValue::from_static("POST, OPTIONS"));
            return Ok(response);
        }
        if req.uri().query().is_some_and(|query| !query.is_empty()) {
            return HttpServerError::BadRequest("put all query/iterator parameters in the JSON request body, not the URL".to_string()).into();
        }
    }

    match (
        parts[0],
        parts[1],
        parts[2],
        parts[3],
        parts[4],
        parts[5],
        req.method(),
    ) {
        ("api", "v1", "live", _, _, _, &Method::GET) => {
            let live_message = serde_json::to_vec(&live_api()).unwrap();
            Ok(Response::new(BoxBody::new(Full::new(live_message.into()))))
        }

        ("api", "v1", "index", _, "query", _, method)
            if *method == Method::POST =>
        {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::IndexNotFound.into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };

            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };

            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);

            let Ok(collected_body) = req.into_body().collect().await else {
                return HttpServerError::BadRequest("failed to read request body".to_string())
                    .into();
            };
            let request_bytes = collected_body.to_bytes();

            let search_request = match serde_json::from_slice::<SearchRequestObject>(&request_bytes)
            {
                Ok(search_request) => search_request,
                Err(e) => {
                    return HttpServerError::BadRequest(e.to_string()).into();
                }
            };

            let search_result_local = match query_index_api_post(&index_arc_clone, search_request).await {
                Ok(result) => result,
                Err(error) => return HttpServerError::BadRequest(error).into(),
            };

            let search_result_json = serde_json::to_vec(&search_result_local).unwrap();
            Ok(Response::new(BoxBody::new(Full::new(
                search_result_json.into(),
            ))))
        }

        ("api", "v1", "index", "", _, _, &Method::POST) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let Ok(collected_body) = req.into_body().collect().await else {
                return HttpServerError::BadRequest("failed to read request body".to_string())
                    .into();
            };
            let request_bytes = collected_body.to_bytes();

            let create_index_request_object =
                match serde_json::from_slice::<CreateIndexRequest>(&request_bytes) {
                    Ok(create_index_request_object) => create_index_request_object,
                    Err(e) => {
                        return HttpServerError::BadRequest(e.to_string()).into();
                    }
                };

            let mut apikey_list_mut = apikey_list.write().await;
            let Some(apikey_object) = apikey_list_mut.get_mut(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let index_id = create_index_api(
                &index_path,
                create_index_request_object.index_name,
                create_index_request_object.schema,
                create_index_request_object.similarity,
                create_index_request_object.tokenizer,
                create_index_request_object.stemmer,
                create_index_request_object.stop_words,
                create_index_request_object.frequent_words,
                create_index_request_object.ngram_indexing,
                create_index_request_object.document_compression,
                create_index_request_object.synonyms,
                force_shard_number,
                apikey_object,

                true,
                create_index_request_object.clustering,
                create_index_request_object.inference,
            )
            .await;
            drop(apikey_list_mut);
            let index_id = match index_id { Ok(id) => id, Err(error) => return HttpServerError::BadRequest(error).into() };
            Ok(Response::new(BoxBody::new(Full::new(
                index_id.to_string().into(),
            ))))
        }

        ("api", "v1", "index", _, "", "", &Method::DELETE) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::IndexNotFound.into();
            };

            let mut apikey_list_mut = apikey_list.write().await;
            if let Some(apikey_object) = apikey_list_mut.get_mut(&apikey_hash) {
                let Ok(_) = delete_index_api(index_id, &mut apikey_object.index_list).await else {
                    return HttpServerError::IndexNotFound.into();
                };

                let index_count = apikey_object.index_list.len();
                drop(apikey_list_mut);

                Ok(Response::new(BoxBody::new(Full::new(
                    index_count.to_string().into(),
                ))))
            } else {
                HttpServerError::Unauthorized.into()
            }
        }

        ("api", "v1", "index", _, "", "", &Method::PATCH) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::IndexNotFound.into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };

            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);

            let task_result = std::thread::spawn(move || {
                INDEX_RUNTIME.block_on(async move { commit_index_api(&index_arc_clone).await })
            });

            let join_result = match task_result.join() {
                Ok(join_result) => join_result,
                Err(_) => return HttpServerError::InternalServerError.into(),
            };
            match join_result {
                Ok(status_object_json) => Ok(Response::new(BoxBody::new(Full::new(
                    status_object_json.to_string().into(),
                )))),
                Err(e) => HttpServerError::BadRequest(e.to_string()).into(),
            }
        }

        ("api", "v1", "index", _, "", "", &Method::PUT) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::IndexNotFound.into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };

            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);
            let result = close_index_api(&index_arc_clone).await;

            Ok(Response::new(BoxBody::new(Full::new(
                result.unwrap().to_string().into(),
            ))))
        }

        ("api", "v1", "apikey", "", "", "", &Method::GET) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let status_object = get_apikey_indices_info_api(&apikey_object.index_list).await;
            drop(apikey_list_ref);
            let status_object_json = serde_json::to_vec(&status_object).unwrap();
            Ok(Response::new(BoxBody::new(Full::new(
                status_object_json.into(),
            ))))
        }

        ("api", "v1", "index", _, "", _, &Method::GET) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::BadRequest("index_id invalid or missing".to_string())
                    .into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let status_object = get_index_info_api(index_id, &apikey_object.index_list).await;

            drop(apikey_list_ref);

            match status_object {
                Ok(status_object) => {
                    let status_object_json = serde_json::to_vec(&status_object).unwrap();
                    Ok(Response::new(BoxBody::new(Full::new(
                        status_object_json.into(),
                    ))))
                }
                Err(_e) => HttpServerError::IndexNotFound.into(),
            }
        }

        ("api", "v1", "index", _, "synonyms", _, &Method::POST) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Ok(index_id) = parts[3].parse::<u64>() else {
                return HttpServerError::BadRequest("index_id invalid or missing".to_string())
                    .into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };
            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);

            let Ok(collected_body) = req.into_body().collect().await else {
                return HttpServerError::BadRequest("failed to read request body".to_string())
                    .into();
            };
            let request_bytes = collected_body.to_bytes();
            let synonyms = match serde_json::from_slice::<Vec<Synonym>>(&request_bytes) {
                Ok(create_index_request_object) => create_index_request_object,
                Err(e) => {
                    return HttpServerError::BadRequest(e.to_string()).into();
                }
            };

            if let Ok(result) = add_synonyms_api(&index_arc_clone, synonyms).await {
                let status_object_json = serde_json::to_vec(&result).unwrap();
                Ok(Response::new(BoxBody::new(Full::new(
                    status_object_json.into(),
                ))))
            } else {
                HttpServerError::SynonymsNotFound.into()
            }
        }

        ("api", "v1", "index", _, "synonyms", _, &Method::PUT) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Ok(index_id) = parts[3].parse::<u64>() else {
                return HttpServerError::BadRequest("index_id invalid or missing".to_string())
                    .into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };
            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);

            let Ok(collected_body) = req.into_body().collect().await else {
                return HttpServerError::BadRequest("failed to read request body".to_string())
                    .into();
            };
            let request_bytes = collected_body.to_bytes();
            let synonyms = match serde_json::from_slice::<Vec<Synonym>>(&request_bytes) {
                Ok(create_index_request_object) => create_index_request_object,
                Err(e) => {
                    return HttpServerError::BadRequest(e.to_string()).into();
                }
            };

            if let Ok(result) = set_synonyms_api(&index_arc_clone, synonyms).await {
                let status_object_json = serde_json::to_vec(&result).unwrap();
                Ok(Response::new(BoxBody::new(Full::new(
                    status_object_json.into(),
                ))))
            } else {
                HttpServerError::SynonymsNotFound.into()
            }
        }

        ("api", "v1", "index", _, "synonyms", _, &Method::GET) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Ok(index_id) = parts[3].parse::<u64>() else {
                return HttpServerError::BadRequest("index_id invalid or missing".to_string())
                    .into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };
            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);
            let result = get_synonyms_api(&index_arc_clone).await;
            let status_object_json = serde_json::to_vec(&result).unwrap();
            Ok(Response::new(BoxBody::new(Full::new(
                status_object_json.into(),
            ))))
        }

        ("api", "v1", "index", _, "doc", _, &Method::POST) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::BadRequest("index_id invalid or missing".to_string())
                    .into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };
            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);

            let Ok(collected_body) = req.into_body().collect().await else {
                return HttpServerError::BadRequest("failed to read request body".to_string())
                    .into();
            };
            let request_bytes = collected_body.to_bytes();

            let task_result = std::thread::spawn(move || {
                INDEX_RUNTIME.block_on(async move {
                    let request_string = str::from_utf8(&request_bytes).map_err(|e| e.to_string())?;
                    if !request_string.trim().starts_with('[') {
                        let document = serde_json::from_str(request_string).map_err(|e| e.to_string())?;
                        index_document_api(&index_arc_clone, document).await
                    } else {
                        let documents = serde_json::from_str(request_string).map_err(|e| e.to_string())?;
                        index_documents_api(&index_arc_clone, documents).await
                    }
                })
            });

            let join_result = match task_result.join() {
                Ok(join_result) => join_result,
                Err(_) => return HttpServerError::InternalServerError.into(),
            };
            match join_result {
                Ok(count) => Ok(Response::new(BoxBody::new(Full::new(
                    serde_json::to_vec(&Ok::<usize, String>(count)).unwrap().into(),
                )))),
                Err(e) => HttpServerError::BadRequest(e.to_string()).into(),
            }
        }

        ("api", "v1", "index", _, "doc", _, &Method::PATCH) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::BadRequest("index_id invalid or missing".to_string())
                    .into();
            };
            let Ok(collected_body) = req.into_body().collect().await else {
                return HttpServerError::BadRequest("failed to read request body".to_string())
                    .into();
            };
            let request_bytes = collected_body.to_bytes();
            let Ok(request_string) = str::from_utf8(&request_bytes) else {
                return HttpServerError::BadRequest("request body is not valid UTF-8".to_string())
                    .into();
            };
            let request_string = request_string.trim();

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };
            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);

            let is_doc_vector = if let Some(p0) = request_string.find('[') {
                if p0 + 1 < request_string.len()
                    && let Some(p1) = request_string[p0 + 1..].find('{')
                {
                    request_string[p0 + 1..p0 + 1 + p1].find('[').is_some()
                } else {
                    false
                }
            } else {
                return HttpServerError::BadRequest(String::new()).into();
            };

            let status_object = if !is_doc_vector {
                let document_object: (u64, Document) = match serde_json::from_str(request_string) {
                    Ok(id_document_object) => id_document_object,
                    Err(e) => {
                        return HttpServerError::BadRequest(e.to_string()).into();
                    }
                };

                update_document_api(&index_arc_clone, document_object).await
            } else {
                let id_document_object_vec: Vec<(u64, Document)> =
                    match serde_json::from_str(request_string) {
                        Ok(document_object_vec) => document_object_vec,
                        Err(e) => {
                            return HttpServerError::BadRequest(e.to_string()).into();
                        }
                    };

                update_documents_api(&index_arc_clone, id_document_object_vec).await
            };

            if let Err(error) = &status_object { return HttpServerError::BadRequest(error.clone()).into(); }
            let status_object_json = serde_json::to_vec(&status_object).unwrap();
            Ok(Response::new(BoxBody::new(Full::new(
                status_object_json.into(),
            ))))
        }

        ("api", "v1", "index", _, "doc", _, &Method::GET) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::BadRequest("index_id invalid or missing".to_string())
                    .into();
            };
            let Ok(doc_id) = parts[5].parse() else {
                return HttpServerError::BadRequest("doc_id invalid or missing".to_string()).into();
            };

            if req.uri().query().is_some_and(|query| !query.is_empty()) {
                return HttpServerError::BadRequest("document reads accept fields in the JSON body, not URL parameters".into()).into();
            }

            let Ok(collected_body) = req.into_body().collect().await else {
                return HttpServerError::BadRequest("failed to read request body".to_string())
                    .into();
            };
            let request_bytes = collected_body.to_bytes();

            let get_document_request = if !request_bytes.is_empty() {
                let get_document_request: GetDocumentRequest =
                    match serde_json::from_slice(&request_bytes) {
                        Ok(document_object) => document_object,
                        Err(e) => {
                            return HttpServerError::BadRequest(e.to_string()).into();
                        }
                    };
                get_document_request
            } else {
                GetDocumentRequest {
                    fields: Vec::new(),
                }
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };
            let status_object = get_document_api(index_arc, doc_id, get_document_request).await;
            drop(apikey_list_ref);

            if let Some(status_object) = status_object {
                let status_object_json = serde_json::to_vec(&status_object).unwrap();
                Ok(Response::new(BoxBody::new(Full::new(
                    status_object_json.into(),
                ))))
            } else {
                HttpServerError::DocumentNotFound.into()
            }
        }

        ("api", "v1", "index", _, "doc", _, &Method::DELETE) => {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::BadRequest("index_id invalid or missing".to_string())
                    .into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };
            let index_arc_clone = index_arc.clone();
            drop(apikey_list_ref);

            match parts[5].parse() {
                Ok(document_id) => {
                    let status_object =
                        delete_document_by_parameter_api(&index_arc_clone, document_id).await;
                    let status_object_json = serde_json::to_vec(&status_object).unwrap();
                    Ok(Response::new(BoxBody::new(Full::new(
                        status_object_json.into(),
                    ))))
                }

                Err(_) => {
                    let Ok(collected_body) = req.into_body().collect().await else {
                        return HttpServerError::BadRequest(
                            "failed to read request body".to_string(),
                        )
                        .into();
                    };
                    let request_bytes = collected_body.to_bytes();

                    if *request_bytes == *b"clear" {
                        let status_object = clear_index_api(&index_arc_clone).await;
                        let status_object_json = serde_json::to_vec(&status_object).unwrap();
                        Ok(Response::new(BoxBody::new(Full::new(
                            status_object_json.into(),
                        ))))
                    } else {
                        match serde_json::from_slice::<SearchRequestObject>(&request_bytes) {
                            Ok(search_request) => {
                                let status_object =
                                    delete_documents_by_query_api(&index_arc_clone, search_request)
                                        .await;
                                if let Err(error) = &status_object {
                                    return HttpServerError::BadRequest(error.clone()).into();
                                }
                                let status_object_json =
                                    serde_json::to_vec(&status_object).unwrap();
                                Ok(Response::new(BoxBody::new(Full::new(
                                    status_object_json.into(),
                                ))))
                            }
                            Err(_) => {
                                let Ok(request_string) = str::from_utf8(&request_bytes) else {
                                    return HttpServerError::BadRequest(
                                        "request body is not valid UTF-8".to_string(),
                                    )
                                    .into();
                                };
                                let is_doc_vector = request_string.trim().starts_with('[');
                                let status_object = if !is_doc_vector {
                                    let document_id = match serde_json::from_str(request_string) {
                                        Ok(document_id) => document_id,
                                        Err(e) => {
                                            return HttpServerError::BadRequest(e.to_string())
                                                .into();
                                        }
                                    };
                                    delete_document_by_object_api(&index_arc_clone, document_id)
                                        .await
                                } else {
                                    let document_id_vec = match serde_json::from_str(request_string)
                                    {
                                        Ok(document_id_vec) => document_id_vec,
                                        Err(e) => {
                                            return HttpServerError::BadRequest(e.to_string())
                                                .into();
                                        }
                                    };
                                    delete_documents_by_object_api(
                                        &index_arc_clone,
                                        document_id_vec,
                                    )
                                    .await
                                };
                                let status_object_json =
                                    serde_json::to_vec(&status_object).unwrap();
                                Ok(Response::new(BoxBody::new(Full::new(
                                    status_object_json.into(),
                                ))))
                            }
                        }
                    }
                }
            }
        }

        ("api", "v1", "index", _, "iterator", "", method)
            if *method == Method::POST =>
        {
            let Some(apikey) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(apikey_hash) = get_apikey_hash(apikey, &apikey_list).await else {
                return HttpServerError::Unauthorized.into();
            };

            let Ok(index_id) = parts[3].parse() else {
                return HttpServerError::BadRequest("index_id invalid or missing".to_string())
                    .into();
            };

            let Ok(collected_body) = req.into_body().collect().await else {
                return HttpServerError::BadRequest("failed to read request body".to_string())
                    .into();
            };
            let request_bytes = collected_body.to_bytes();

            let get_iterator_request = if !request_bytes.is_empty() {
                let get_iterator_request: GetIteratorRequest =
                    match serde_json::from_slice(&request_bytes) {
                        Ok(document_id_object) => document_id_object,
                        Err(e) => {
                            return HttpServerError::BadRequest(e.to_string()).into();
                        }
                    };
                get_iterator_request
            } else {
                return HttpServerError::BadRequest("Request body is empty".to_string()).into();
            };

            let apikey_list_ref = apikey_list.read().await;
            let Some(apikey_object) = apikey_list_ref.get(&apikey_hash) else {
                return HttpServerError::Unauthorized.into();
            };
            let Some(index_arc) = apikey_object.index_list.get(&index_id) else {
                return HttpServerError::IndexNotFound.into();
            };
            let status_object = get_iterator_api_post(
                index_arc,
                get_iterator_request.document_id,
                get_iterator_request.skip,
                get_iterator_request.take,
                get_iterator_request.include_deleted,
                get_iterator_request.include_document,
                get_iterator_request.fields.clone(),
            )
            .await;
            drop(apikey_list_ref);

            let status_object_json = serde_json::to_vec(&status_object).unwrap();
            Ok(Response::new(BoxBody::new(Full::new(
                status_object_json.into(),
            ))))
        }

        ("api", "v1", "apikey", "", "", "", &Method::POST) => {
            let Some(apikey_header) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let mut hasher = Sha256::new();
            hasher.update(MASTER_KEY_SECRET.to_string());
            let master_apikey = hasher.finalize();
            let master_apikey_base64 = general_purpose::STANDARD.encode(master_apikey);

            if apikey_header != master_apikey_base64 {
                return HttpServerError::Unauthorized.into();
            };
            if req.uri().query().is_some_and(|query| !query.is_empty()) {
                return HttpServerError::BadRequest("API key creation accepts only a JSON body with optional demo".into()).into();
            }
            let Ok(collected_body) = req.into_body().collect().await else {
                return HttpServerError::BadRequest("failed to read request body".to_string())
                    .into();
            };
            let request_bytes = collected_body.to_bytes();
            let create_request: CreateApikeyRequest =
                match serde_json::from_slice(&request_bytes) {
                    Ok(create_request) => create_request,
                    Err(e) => {
                        return Ok(status(StatusCode::BAD_REQUEST, e.to_string()));
                    }
                };

            let mut apikey = [0u8; 32];
            if !create_request.demo {
                SysRng.try_fill_bytes(&mut apikey).unwrap();
            }
            let api_key_base64 = general_purpose::STANDARD.encode(apikey);

            let mut apikey_list_mut = apikey_list.write().await;
            create_apikey_api(
                &index_path,
                &apikey,
                &mut apikey_list_mut,
            );
            drop(apikey_list_mut);

            Ok(Response::new(BoxBody::new(Full::new(
                api_key_base64.into(),
            ))))
        }

        ("api", "v1", "apikey", "", "", "", &Method::DELETE) => {
            let Some(apikey_header) = apikey_header else {
                return HttpServerError::Unauthorized.into();
            };
            let mut hasher = Sha256::new();
            hasher.update(MASTER_KEY_SECRET.to_string());
            let master_apikey = hasher.finalize();
            let master_apikey_base64 = general_purpose::STANDARD.encode(master_apikey);

            if apikey_header != master_apikey_base64 {
                return HttpServerError::Unauthorized.into();
            };
            let Ok(collected_body) = req.into_body().collect().await else {
                return HttpServerError::BadRequest("failed to read request body".to_string())
                    .into();
            };
            let request_bytes = collected_body.to_bytes();

            let request_object: DeleteApikeyRequest = match serde_json::from_slice(&request_bytes) {
                Ok(request_object) => request_object,
                Err(e) => {
                    return HttpServerError::BadRequest(e.to_string()).into();
                }
            };

            let Ok(apikey) = general_purpose::STANDARD.decode(&request_object.apikey_base64) else {
                return HttpServerError::Unauthorized.into();
            };
            let apikey_hash = calculate_hash(&apikey) as u128;

            let mut apikey_list_mut = apikey_list.write().await;
            let result = delete_apikey_api(&index_path, &mut apikey_list_mut, apikey_hash);
            drop(apikey_list_mut);

            match result {
                Ok(count) => Ok(Response::new(BoxBody::new(Full::new(
                    count.to_string().into(),
                )))),
                Err(_) => HttpServerError::ApiKeyNotFound.into(),
            }
        }

        ("api", "v1", "index", _, "file", _, _) => Ok(status(
            StatusCode::NOT_FOUND,
            "file endpoints have been removed; use the JSON document API and external source URLs".to_string(),
        )),

        (_, _, _, _, _, _, &Method::GET) => match path {
            "/" => Ok(Response::new(BoxBody::new(INDEX_HTML.to_string()))),
            "/css/flexboxgrid.min.css" => Ok(Response::new(BoxBody::new(FLEXBOX_CSS.to_string()))),
            "/css/master.css" => Ok(Response::new(BoxBody::new(MASTER_CSS.to_string()))),
            "/js/setup.js" => Ok(Response::new(BoxBody::new(SETUP_JS.to_string()))),
            "/js/master.js" => Ok(Response::new(BoxBody::new(MASTER_JS.to_string()))),
            "/js/jquery-3.7.1.min.js" => Ok(Response::new(BoxBody::new(JQUERY_JS.to_string()))),

            "/svg/logo.svg" => {
                let response = Response::builder()
                    .header("Content-Type", "image/svg+xml")
                    .header("content-length", LOGO_SVG.len())
                    .body(BoxBody::new(Full::new(LOGO_SVG.into())))
                    .unwrap();
                Ok(response)
            }

            "/favicon-16x16.png" => Ok(Response::new(BoxBody::new(Full::new(FAVICON_16.into())))),
            "/favicon-32x32.png" => Ok(Response::new(BoxBody::new(Full::new(FAVICON_32.into())))),
            "/version" => Ok(Response::new(BoxBody::new(Full::new(VERSION.into())))),
            _ => HttpServerError::NotImplemented.into(),
        },
        _ => HttpServerError::NotImplemented.into(),
    }
}

pub(crate) async fn http_server(
    index_path: &Path,
    apikey_list: Arc<tokio::sync::RwLock<HashMap<u128, ApikeyObject>>>,
    local_ip: &String,
    local_port: &u16,
    force_shard_number: &Option<usize>,
) {
    let local_address = SocketAddr::new(local_ip.parse().expect("validated local_ip"), *local_port);

    match TcpListener::bind(local_address).await {
        Ok(listener) => {
            io::stdout().flush().unwrap();

            let index_path = index_path.to_path_buf();

            let auto_server = auto::Builder::new(TokioExecutor::new());

            loop {
                let (stream, remote_address) = listener.accept().await.unwrap();

                match stream.nodelay() {
                    Ok(false) => {
                        if let Err(e) = stream.set_nodelay(true) {
                            eprintln!("Error setting TCP_NODELAY for {}: {}", remote_address, e);
                        }
                    }
                    Ok(true) => {}
                    Err(e) => eprintln!(
                        "Could not read TCP_NODELAY status for {}: {}",
                        remote_address, e
                    ),
                }

                let io = TokioIo::new(stream);
                let server = auto_server.clone();

                let index_path = index_path.clone();
                let apikey_list = apikey_list.clone();
                let force_shard_number = *force_shard_number;

                tokio::spawn(async move {
                    if let Err(err) = server
                        .serve_connection(
                            io,
                            service_fn(move |request: Request<Incoming>| {
                                let index_path = index_path.clone();
                                let apikey_list = apikey_list.clone();
                                let force_shard_number = force_shard_number;
                                async move {
                                    let response = http_request_handler(
                                        index_path,
                                        apikey_list,
                                        request,
                                        remote_address,
                                        force_shard_number,
                                    )
                                    .await?;

                                    Ok::<_, Infallible>(with_cors_headers(response))
                                }
                            }),
                        )
                        .await
                    {
                        eprintln!("Error processing connection {}: {:?}", remote_address, err);
                    }
                });
            }
        }

        Err(_e) => {
            println!(
                "Starting the server at {:?} failed. \
            Check if there is another SeekStorm server instance running on the same port. \
            Try changing the port.",
                local_address
            );
            process::exit(1)
        }
    }
}

