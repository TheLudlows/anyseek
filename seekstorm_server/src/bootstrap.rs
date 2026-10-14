//! First-run credentials belong to the service, never to the search core.
use base64::{Engine, engine::general_purpose};
use rand::{TryRng, rngs::SysRng};
use std::{collections::HashMap, fs, io, path::PathBuf};

use crate::{api_endpoints::create_apikey_api, multi_tenancy::ApikeyObject};

const MARKER: &str = "bootstrap.complete";

/// Return a plaintext key only on first initialization. The marker survives key
/// revocation; existing installations are marked without adding another tenant.
pub(crate) fn initialize_key(
    path: &PathBuf,
    keys: &mut HashMap<u128, ApikeyObject>,
) -> io::Result<Option<String>> {
    if path.join(MARKER).try_exists()? {
        return Ok(None);
    }
    let key = if keys.is_empty() {
        // Do not mistake an unrecognized/nonempty data directory for a new install.
        if fs::read_dir(path)?.next().is_some() {
            return Err(io::Error::other(
                "nonempty data directory has no API keys; refusing automatic initialization",
            ));
        }
        let mut bytes = [0u8; 32];
        SysRng
            .try_fill_bytes(&mut bytes)
            .map_err(io::Error::other)?;
        let identity = create_apikey_api(path, &bytes, keys);
        let key_path = path.join(identity.id.to_string()).join("apikey.json");
        let persisted: ApikeyObject = serde_json::from_slice(&fs::read(&key_path)?)?;
        if persisted.apikey_hash != identity.apikey_hash || persisted.id != identity.id {
            return Err(io::Error::other("bootstrap API key was not persisted"));
        }
        fs::OpenOptions::new()
            .write(true)
            .open(key_path)?
            .sync_all()?;
        Some(general_purpose::STANDARD.encode(bytes))
    } else {
        None
    };
    // This contains no credential. Never remove it when revoking a key.
    fs::write(path.join(MARKER), b"1\n")?;
    fs::OpenOptions::new()
        .write(true)
        .open(path.join(MARKER))?
        .sync_all()?;
    Ok(key)
}
