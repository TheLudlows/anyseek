use base64::{Engine, engine::general_purpose};
use anyseek::index::{IS_AVX2, IS_NEON};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, env::current_exe, path::Path};

use crate::{MASTER_KEY_SECRET, VERSION};

pub(crate) async fn initialize(params: HashMap<String, String>) {
    let force_shard_number = params
        .get("force_shard_number")
        .and_then(|n| n.parse().ok());
    let default_nprobe = params
        .get("default_nprobe")
        .map(|n| {
            n.parse::<std::num::NonZeroUsize>()
                .expect("validated default_nprobe")
        })
        .unwrap_or_else(|| std::num::NonZeroUsize::new(crate::api_types::DEFAULT_NPROBE).unwrap());
    let path = params
        .get("index_path")
        .map(String::as_str)
        .unwrap_or("anyseek_index");
    let index_path = if Path::new(path).is_absolute() {
        Path::new(path).to_path_buf()
    } else {
        current_exe()
            .expect("executable path")
            .parent()
            .unwrap()
            .join(path)
    };
    let local_ip = params
        .get("local_ip")
        .cloned()
        .unwrap_or_else(|| "0.0.0.0".into());
    let local_port = params
        .get("local_port")
        .map(|port| port.parse().expect("validated port"))
        .unwrap_or(8080);
    let master_apikey =
        general_purpose::STANDARD.encode(Sha256::digest(MASTER_KEY_SECRET.as_bytes()));
    let simd = if *IS_AVX2 {
        "AVX2"
    } else if *IS_NEON {
        "NEON"
    } else {
        "disabled"
    };
    log::info!(
        "anyseek {VERSION}; index path: {}; SIMD: {simd}; CPUs: {} logical, {} physical",
        index_path.display(),
        num_cpus::get(),
        num_cpus::get_physical()
    );
    log::info!("Web UI and REST API: http://{local_ip}:{local_port}; Ctrl-C to shut down");
    println!("Master API key: {master_apikey}");

    if force_shard_number.is_some_and(|n: usize| n != 1) {
        log::error!(
            "Reliable P1 service supports exactly one partition; force_shard_number must be 1"
        );
        std::process::exit(1);
    }
    let service = crate::record_service::Service::open(&index_path, default_nprobe)
        .await
        .unwrap_or_else(|error| {
            log::error!("Recovering reliable service failed: {error}");
            std::process::exit(1)
        });
    let address =
        std::net::SocketAddr::new(local_ip.parse().expect("validated local_ip"), local_port);
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .unwrap_or_else(|error| {
            log::error!("Starting server failed: {error}");
            std::process::exit(1)
        });
    if let Err(error) = axum::serve(listener, crate::record_service::router(service))
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        log::error!("HTTP service failed: {error}");
    }

    log::info!("Shutdown complete");
}

pub(crate) async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut interrupt = signal(SignalKind::interrupt()).expect("install SIGINT handler");
        let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! { _ = interrupt.recv() => {}, _ = terminate.recv() => {} }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .expect("install Ctrl-C handler");
}
