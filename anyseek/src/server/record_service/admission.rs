//! Service authentication precedes namespace routing and resource admission.
use super::*;
use anyseek::resources::Lane;
use axum::{extract::{FromRequestParts, MatchedPath}, http::Method};
use std::io::Write;

#[derive(Clone)]
pub(super) struct RoutedCollection {
    pub tenant: String,
    pub id: String,
    pub generation: Option<u64>,
}

pub(super) struct OutputBytes {
    pub bytes: Bytes,
    pub _scope: ResourceScope,
    // A transport frame may outlive its Body after poll_frame hands it off.
    pub _activity: Option<anyseek::resources::Activity>,
}
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
        ("/api/v1/service-backups" | "/api/v1/index/{id}/backup" | EVENT_TIME_CHECKPOINT_PATH | entity::CHECKPOINT_PATH, &Method::POST)
            | ("/api/v1/index/{id}", &Method::PATCH) | ("/api/v1/backups/{id}", &Method::DELETE) => Lane::Background,
        ("/api/v1/index/{id}", &Method::DELETE) | ("/api/v1/backups", &Method::GET)
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
    let mut tenant=None;
    if lane != Lane::Public || (path == "/api/v1/live" && parts.headers.contains_key("apikey")) {
        let principal=service.authenticate_credentials(&parts.headers).await?;
        if path == EVENT_TIME_CHECKPOINT_PATH && principal != MASTER {
            return Err(Error(StatusCode::FORBIDDEN, "unauthorized", "event-time checkpoints require service administration".into()));
        }
        if path == entity::CHECKPOINT_PATH && principal != MASTER {
            return Err(Error(StatusCode::FORBIDDEN, "unauthorized", "entity checkpoints require service administration".into()));
        }
        let global = matches!(path.as_str(), "/api/v1/live" | "/api/v1/backups" | "/api/v1/backups/{id}"
            | "/api/v1/service-backups" | "/api/v1/service-restores" | EVENT_TIME_CHECKPOINT_PATH | entity::CHECKPOINT_PATH
            | "/api/v1/catalog/operation/{epoch}/{nonce}");
        if !global && principal == MASTER {
            return Err(bad("X-AnySeek-Tenant is required for tenant data requests"));
        }
        parts.extensions.insert(Principal(principal.clone()));
        if principal!=MASTER {tenant=Some(principal);}
    }
    let mut subject = "service".to_owned();
    if path.starts_with("/api/v1/index/{id}") {
        let Path(params) = Path::<BTreeMap<String, String>>::from_request_parts(&mut parts, &()).await.map_err(bad)?;
        if let Some(id) = params.get("id") {
            anyseek::catalog::validate_name(id)?;
            let tenant = tenant.ok_or_else(||Error(StatusCode::FORBIDDEN,"unauthorized","X-AnySeek-Tenant required".into()))?;
            // Admit a known cold collection under the same qualified subject
            // as a resident one. Unknown IDs still share the service budget.
            #[cfg(test)]
            service.cold.test.preflight.wait().await;
            let generation = match service.catalog.collection_entry(&tenant, id).await {
                Ok(entry) => {
                    subject = format!("collection:{}", entry.scope());
                    Some(entry.generation)
                }
                Err(CatalogError::Unauthorized) => None,
                Err(error) => return Err(error.into()),
            };
            parts.extensions.insert(RoutedCollection { tenant, id: id.clone(), generation });
        }
    }
    let scope = service.resources.admit(subject, lane, (MAX_BODY + MAX_RESPONSE) as u64)
        .map_err(|error| Error(StatusCode::TOO_MANY_REQUESTS, "busy", error.to_string()))?;
    Ok((axum::extract::Request::from_parts(parts, body), scope))
}
