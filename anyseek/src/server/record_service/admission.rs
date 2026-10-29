//! Request classification never substitutes for catalog authorization.
use super::*;
use anyseek::resources::Lane;
use axum::{extract::{FromRequestParts, MatchedPath}, http::Method};
use std::io::Write;

pub(super) struct OutputBytes { pub bytes: Bytes, pub _scope: ResourceScope }
impl AsRef<[u8]> for OutputBytes { fn as_ref(&self) -> &[u8] { &self.bytes } }

// Refuse before Vec growth, including serializers which write many small pieces.
pub(super) fn encode<T: Serialize>(value: &T) -> serde_json::Result<Vec<u8>> {
    struct Writer(Vec<u8>);
    impl Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > MAX_RESPONSE.saturating_sub(self.0.len()) {
                return Err(std::io::Error::other("response budget exceeded"));
            }
            let length = self.0.len() + bytes.len();
            if length > self.0.capacity() {
                let target = length.max(self.0.capacity().saturating_mul(2)).min(MAX_RESPONSE);
                self.0.try_reserve_exact(target - self.0.len()).map_err(std::io::Error::other)?;
            }
            self.0.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    let mut writer = Writer(Vec::new());
    serde_json::to_writer(&mut writer, value)?;
    Ok(writer.0)
}
fn classify(path: &str, method: &Method) -> Lane {
    if !path.starts_with("/api/v1/") || path == "/api/v1/live" { return Lane::Public; }
    match (path, method) {
        ("/api/v1/index", &Method::POST) | ("/api/v1/restore" | "/api/v1/service-restores", &Method::POST) => Lane::Recovery,
        ("/api/v1/service-backups" | "/api/v1/index/{id}/backup", &Method::POST)
            | ("/api/v1/index/{id}", &Method::PATCH) | ("/api/v1/backups/{id}", &Method::DELETE) => Lane::Background,
        ("/api/v1/apikey", &Method::POST | &Method::DELETE) | ("/api/v1/index/{id}", &Method::DELETE)
            | ("/api/v1/index/{id}/grants", &Method::POST) | ("/api/v1/backups", &Method::GET)
            | ("/api/v1/catalog/operation/{epoch}/{nonce}", &Method::GET) => Lane::Control,
        _ => Lane::Foreground,
    }
}
pub(super) async fn request(service: &Service, request: axum::extract::Request) -> Result<(axum::extract::Request, ResourceScope), Error> {
    let (mut parts, body) = request.into_parts();
    let path = parts.extensions.get::<MatchedPath>().map(MatchedPath::as_str).unwrap_or("").to_owned();
    let lane = classify(&path, &parts.method);
    let preflight = if lane == Lane::Control { &service.control_preflight } else { &service.preflight };
    let _preflight = preflight.clone().try_acquire_owned().map_err(|_| Error::from(CatalogError::Busy))?;
    if lane != Lane::Public {
        // Authenticate before granting reserved control capacity or allocating per-ID state.
        // The handlers recheck current grants at their existing linearization points.
        if lane == Lane::Control { service.admin(&parts.headers).await?; }
        else { service.authenticate(&parts.headers).await?; }
    }
    let mut subject = "service".to_owned();
    if path.starts_with("/api/v1/index/{id}") {
        let Path(params) = Path::<BTreeMap<String, String>>::from_request_parts(&mut parts, &()).await.map_err(bad)?;
        if let Some(id) = params.get("id") {
            // Axum performs exactly the same percent decoding used by handlers.
            // Unknown/user supplied identities do not grow the resource registry.
            if let Some((id, _)) = service.records.read().await.get_key_value(id) { subject = format!("collection:{id}"); }
        }
    }
    let scope = service.resources.admit(subject, lane, (MAX_BODY + MAX_RESPONSE) as u64)
        .map_err(|error| Error(StatusCode::TOO_MANY_REQUESTS, "busy", error.to_string()))?;
    Ok((axum::extract::Request::from_parts(parts, body), scope))
}
