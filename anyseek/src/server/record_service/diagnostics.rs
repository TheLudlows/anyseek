//! Bounded, content-free observations. Progress is sampled after the query.
use super::*;

tokio::task_local! { pub(super) static REQUEST_ID: String; }

pub(super) fn new_id() -> String {
    format!("{:032x}", rand::random::<u128>())
}

pub(super) async fn request_id(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let id = new_id();
    let started = std::time::Instant::now();
    let mut response = REQUEST_ID.scope(id.clone(), next.run(request)).await;
    log::debug!(
        "response_prepared request_id={} status={} elapsed_us={} body_delivery=unconfirmed",
        id,
        response.status().as_u16(),
        started.elapsed().as_micros()
    );
    response
        .headers_mut()
        .insert("x-anyseek-request-id", id.parse().expect("hex request ID"));
    response
}

#[derive(Serialize)]
pub(super) struct QueryDiagnostics {
    pub format_version: u32,
    pub request_id: String,
    pub engine_version: &'static str,
    pub mode: &'static str,
    pub elapsed_us: u64,
    pub analysis_fingerprint: String,
    pub embedding_space_sha256: Option<String>,
    pub index_progress: Progress,
}

#[derive(Serialize)]
pub(super) struct Progress {
    pub committed_seq: Option<u64>,
    pub lexical_seq: u64,
    pub vector_seq: u64,
    // Raw backend diagnostics may contain source text or filesystem details.
    pub index_failed: bool,
    pub recovery_required: bool,
    pub sampling: &'static str,
}

impl QueryDiagnostics {
    pub(super) fn new(
        request_id: String,
        mode: &'static str,
        elapsed: std::time::Duration,
        analysis_fingerprint: String,
        embedding_space_id: Option<&str>,
        progress: anyseek::record::CommittedProgress,
    ) -> Self {
        Self {
            format_version: 2,
            request_id,
            engine_version: crate::VERSION,
            mode,
            elapsed_us: elapsed.as_micros().min(u64::MAX as u128) as u64,
            analysis_fingerprint,
            embedding_space_sha256: embedding_space_id.map(|value| {
                Sha256::digest(value.as_bytes())
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect()
            }),
            index_progress: Progress {
                committed_seq: progress.committed_seq,
                lexical_seq: progress.lexical_seq,
                vector_seq: progress.vector_seq,
                index_failed: progress.index_error.is_some(),
                recovery_required: progress.recovery_required,
                sampling: "after_query_not_atomic_with_result",
            },
        }
    }
}

pub(super) fn describe_openapi(doc: &mut Value) {
    let hash = json!({"type":"string","pattern":"^[a-f0-9]{64}$"});
    doc["components"]["schemas"]["AnalysisContract"] = json!({"type":"object",
        "required":["format","fingerprint","tokenizer","tokenizer_identity","stop_words","stop_words_fingerprint"],
        "properties":{"format":{"const":"native-package-analysis:v1"},"fingerprint":hash,
            "tokenizer":{"type":"string"},"tokenizer_identity":{"type":"string"},
            "stop_words":{"enum":["None","English","German","French","Spanish","Custom"]},"stop_words_fingerprint":hash}});
    doc["components"]["schemas"]["QueryDiagnostics"] = json!({"type":"object",
        "required":["format_version","request_id","engine_version","mode","elapsed_us","analysis_fingerprint","embedding_space_sha256","index_progress"],
        "properties":{"format_version":{"const":2},"request_id":{"type":"string","pattern":"^[a-f0-9]{32}$"},
            "engine_version":{"type":"string"},"mode":{"enum":["browse","lexical","vector","hybrid"]},
            "elapsed_us":{"type":"integer","minimum":0},"analysis_fingerprint":hash,
            "embedding_space_sha256":{"type":["string","null"],"pattern":"^[a-f0-9]{64}$"},
            "index_progress":{"type":"object","required":["committed_seq","lexical_seq","vector_seq","index_failed","recovery_required","sampling"],
                "properties":{"committed_seq":{"type":["integer","null"],"minimum":0},
                    "lexical_seq":{"type":"integer","minimum":0},"vector_seq":{"type":"integer","minimum":0},
                    "index_failed":{"type":"boolean"},"recovery_required":{"type":"boolean"},
                    "sampling":{"const":"after_query_not_atomic_with_result"}}}}});
    for path in doc["paths"].as_object_mut().unwrap().values_mut() {
        for operation in path.as_object_mut().unwrap().values_mut() {
            for response in operation["responses"].as_object_mut().unwrap().values_mut() {
                response["headers"] = json!({"x-anyseek-request-id":{"description":"Server-generated correlation ID, including errors; client input is not reused","schema":{"type":"string"}},
                    "x-anyseek-error-code":{"description":"Safe machine code on service errors; raw message is not a diagnostics field","schema":{"type":"string"}}});
            }
        }
    }
    doc["paths"]["/api/v1/index/{id}/query"]["post"]["responses"]["200"]["content"] = json!({
        "application/json":{"schema":{"oneOf":[
            {"type":"object","required":["diagnostics"],"properties":{"diagnostics":{"$ref":"#/components/schemas/QueryDiagnostics"}}},
            {"$ref":"#/components/schemas/TimeVectorResult"}
        ]}}
    });
    doc["paths"]["/api/v1/index/{id}"]["get"]["responses"]["200"]["content"] = json!({"application/json":{"schema":{"type":"object","required":["analysis"],"properties":{"analysis":{"$ref":"#/components/schemas/AnalysisContract"}}}}});
}
