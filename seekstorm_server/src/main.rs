#![recursion_limit = "256"]
#![crate_name = "seekstorm_server"]
#![doc(html_logo_url = "http://seekstorm.com/assets/logo.svg")]
#![allow(clippy::approx_constant)]

//! SeekPower HTTP service and embedded search UI.
//! Build from this repository; see seekstorm_server/README.md for startup and authentication,
//! FEATURES.md for the complete route inventory, and src/test_api.rest for HTTP examples.

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

/// &#x26A0; **WARNING**: make sure to set the MASTER_KEY_SECRET environment variable to a secret, otherwise your generated API keys will be compromised.
#[doc(hidden)]
pub static MASTER_KEY_SECRET: LazyLock<String> =
    LazyLock::new(|| env::var("MASTER_KEY_SECRET").unwrap_or("1234".to_string()));

#[doc(hidden)]
mod api_endpoints;
use seekstorm_server::{api_types, api_schema};
mod info;
#[doc(hidden)]
mod http_server;
#[doc(hidden)]
mod multi_tenancy;
#[doc(hidden)]
mod server;
mod bootstrap;
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
        Err(error) => { eprintln!("{error}"); std::process::exit(2); }
    };

    if params.get("generate_openapi").is_some_and(|value| value == "true") {
        api_endpoints::generate_openapi();
        return Ok(());
    }
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp(None)
        .init();
    initialize(params).await;

    Ok(())
}


fn parse_params(args: impl Iterator<Item=String>) -> Result<HashMap<String, String>, String> {
    let mut params = HashMap::new();
    for arg in args {
        let (name, value) = arg.split_once('=').ok_or_else(|| format!("expected name=value: {arg}"))?;
        let (name, value) = (name.trim(), value.trim());
        let value = if name == "local_ip" { value.strip_prefix('[').and_then(|v| v.strip_suffix(']')).unwrap_or(value) } else { value };
        if name == "ingest_path" { return Err("ingest_path has been removed; use the JSON document API".into()); }
        match name {
            "index_path" if !value.is_empty() => {},
            "local_ip" if value.parse::<std::net::IpAddr>().is_ok() => {},
            "local_port" if value.parse::<u16>().is_ok() => {},
            "force_shard_number" if value.parse::<usize>().is_ok_and(|n| n > 0) => {},
            "generate_openapi" if value.parse::<bool>().is_ok() => {},
            _ => return Err(format!("unsupported or invalid parameter: {name}")),
        }
        if params.insert(name.to_string(), value.to_string()).is_some() {
            return Err(format!("duplicate parameter: {name}"));
        }
    }
    Ok(params)
}

#[cfg(test)]
mod tests {
    use super::parse_params;
    #[test]
    fn cli_options_are_explicit_and_validated() {
        let parse = |args: &[&str]| parse_params(args.iter().map(|s| s.to_string()));
        assert!(parse(&[]).is_ok());
        assert_eq!(parse(&["index_path=a=b", "local_port=0", "local_ip=::1", "force_shard_number=1"]).unwrap()["index_path"], "a=b");
        assert_eq!(parse(&["local_ip=[::1]"]).unwrap()["local_ip"], "::1");
        for args in [vec!["unknown=x"],vec!["ingest_path=x"],vec!["--help"],vec!["local_port=bad"],
            vec!["local_ip=bad"],vec!["force_shard_number=0"],vec!["index_path="],vec!["generate_openapi=yes"],
            vec!["local_port=80","local_port=81"]] {
            assert!(parse(&args).is_err(), "{args:?}");
        }
    }
}
