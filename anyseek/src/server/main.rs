#![recursion_limit = "256"]
#![crate_name = "anyseek_server"]
#![allow(clippy::approx_constant)]

//! AnySeek HTTP API service. The standalone console lives in console/.
//! Build from this repository; see anyseek/src/server/README.md for startup and authentication,
//! generated OpenAPI for the complete route inventory, and test_api.rest for HTTP examples.

use std::collections::HashMap;
use std::env;
use std::error::Error;
use std::str;
use std::sync::LazyLock;
#[cfg(target_os = "windows")]
use winapi::um::{
    processthreadsapi::{GetCurrentProcess, SetPriorityClass},
    winbase::ABOVE_NORMAL_PRIORITY_CLASS,
};

use crate::server::initialize;

/// Startup validates this before opening storage or accepting requests.
#[doc(hidden)]
#[cfg(not(test))]
pub static MASTER_KEY_SECRET: LazyLock<String> =
    LazyLock::new(|| read_master_secret(env::var("MASTER_KEY_SECRET")).expect("validated at startup"));

// Unit routers do not run main; this credential exists only in the test binary.
#[cfg(test)]
pub static MASTER_KEY_SECRET: LazyLock<String> = LazyLock::new(|| "anyseek-unit-test-secret".into());

fn read_master_secret(value: Result<String, env::VarError>) -> Result<String, &'static str> {
    let secret = value.map_err(|_| "MASTER_KEY_SECRET must be set to a private UTF-8 secret; see anyseek/src/server/README.md")?;
    if secret.trim().is_empty() {
        return Err("MASTER_KEY_SECRET must not be empty or whitespace; see anyseek/src/server/README.md");
    }
    // Preserve the exact bytes and existing key derivation, including legacy secrets.
    Ok(secret)
}

use anyseek::server::api_types;
mod record_service;
#[doc(hidden)]
mod server;
#[doc(hidden)]
pub(crate) const VERSION: &str = env!("CARGO_PKG_VERSION");
#[doc(hidden)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    #[cfg(target_os = "windows")]
    unsafe {
        let process = GetCurrentProcess();
        SetPriorityClass(process, ABOVE_NORMAL_PRIORITY_CLASS);
    }

    let params = match parse_params(env::args().skip(1)) {
        Ok(params) => params,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };

    if params
        .get("generate_openapi")
        .is_some_and(|value| value == "true")
    {
        record_service::generate_openapi();
        return Ok(());
    }
    if let Err(error) = read_master_secret(env::var("MASTER_KEY_SECRET")) {
        eprintln!("{error}");
        std::process::exit(2);
    }
    if params.get("print_master_key").is_some_and(|value| value == "true") {
        use base64::Engine;
        use sha2::{Digest, Sha256};
        println!("{}", base64::engine::general_purpose::STANDARD.encode(Sha256::digest(MASTER_KEY_SECRET.as_bytes())));
        return Ok(());
    }
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp(None)
        .init();
    initialize(params).await;

    Ok(())
}

fn parse_params(args: impl Iterator<Item = String>) -> Result<HashMap<String, String>, String> {
    let mut params = HashMap::new();
    for arg in args {
        let (name, value) = arg
            .split_once('=')
            .ok_or_else(|| format!("expected name=value: {arg}"))?;
        let (name, value) = (name.trim(), value.trim());
        let value = if name == "local_ip" {
            value
                .strip_prefix('[')
                .and_then(|v| v.strip_suffix(']'))
                .unwrap_or(value)
        } else {
            value
        };
        if name == "ingest_path" {
            return Err("ingest_path has been removed; use the JSON document API".into());
        }
        match name {
            "index_path" if !value.is_empty() => {}
            "local_ip" if value.parse::<std::net::IpAddr>().is_ok() => {}
            "local_port" if value.parse::<u16>().is_ok() => {}
            "default_nprobe"
                if value.parse::<usize>().is_ok_and(|n| n > 0) => {}
            "generate_openapi" | "print_master_key" if value.parse::<bool>().is_ok() => {}
            _ => return Err(format!("unsupported or invalid parameter: {name}")),
        }
        if params.insert(name.to_string(), value.to_string()).is_some() {
            return Err(format!("duplicate parameter: {name}"));
        }
    }
    if params.get("generate_openapi").is_some_and(|v| v == "true")
        && params.get("print_master_key").is_some_and(|v| v == "true") {
        return Err("generate_openapi and print_master_key cannot both be true".into());
    }
    Ok(params)
}

#[cfg(test)]
mod tests {
    use super::{parse_params, read_master_secret};
    #[test]
    fn master_secret_is_required_and_preserved_exactly() {
        assert!(read_master_secret(Err(std::env::VarError::NotPresent)).is_err());
        for value in ["", " ", "\n\t"] {
            assert!(read_master_secret(Ok(value.into())).is_err());
        }
        for value in ["1234", " existing-secret ", "私有密钥"] {
            assert_eq!(read_master_secret(Ok(value.into())).unwrap(), value);
        }
    }
    #[test]
    fn cli_options_are_explicit_and_validated() {
        let parse = |args: &[&str]| parse_params(args.iter().map(|s| s.to_string()));
        assert!(parse(&[]).is_ok());
        assert_eq!(
            parse(&["default_nprobe=37"]).unwrap()["default_nprobe"],
            "37"
        );
        assert_eq!(
            parse(&[
                "index_path=a=b",
                "local_port=0",
                "local_ip=::1"
            ])
            .unwrap()["index_path"],
            "a=b"
        );
        assert_eq!(parse(&["local_ip=[::1]"]).unwrap()["local_ip"], "::1");
        for args in [
            vec!["unknown=x"],
            vec!["ingest_path=x"],
            vec!["--help"],
            vec!["local_port=bad"],
            vec!["default_nprobe=0"],
            vec!["default_nprobe=-1"],
            vec!["default_nprobe=1.5"],
            vec!["default_nprobe=18446744073709551616"],
            vec!["local_ip=bad"],
            vec!["force_shard_number=1"],
            vec!["index_path="],
            vec!["generate_openapi=yes"],
            vec!["print_master_key=yes"],
            vec!["generate_openapi=true", "print_master_key=true"],
            vec!["local_port=80", "local_port=81"],
        ] {
            assert!(parse(&args).is_err(), "{args:?}");
        }
    }
}
