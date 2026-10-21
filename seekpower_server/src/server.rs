use base64::{Engine, engine::general_purpose};
use seekpower::{
    index::{Close, IS_AVX2, IS_NEON},
    info::collect_index_info,
    utils::dir_size,
};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, env::current_exe, fs, path::Path, sync::Arc, time::Instant};
use tokio::sync::RwLock;

use crate::keys::create_apikey_api;
use crate::{MASTER_KEY_SECRET, VERSION, http_server::http_server, keys::open_all_apikeys};

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
        .unwrap_or("seekstorm_index");
    let index_path = if Path::new(path).is_absolute() {
        Path::new(path).to_path_buf()
    } else {
        current_exe()
            .expect("executable path")
            .parent()
            .unwrap()
            .join(path)
    };
    if let Err(error) = fs::create_dir_all(&index_path) {
        log::error!("Cannot create index path {}: {error}", index_path.display());
        std::process::exit(1);
    }
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
        "Seekpower {VERSION}; index path: {}; SIMD: {simd}; CPUs: {} logical, {} physical",
        index_path.display(),
        num_cpus::get(),
        num_cpus::get_physical()
    );
    log::info!("Web UI and REST API: http://{local_ip}:{local_port}; Ctrl-C to shut down");
    println!("Master API key: {master_apikey}");

    let apikey_list = Arc::new(RwLock::new(HashMap::new()));
    let start = Instant::now();
    {
        let mut keys = apikey_list.write().await;
        open_all_apikeys(&index_path, &mut keys).await;
        // The master key owns a persisted index scope and can also manage other keys.
        let master_key_bytes = Sha256::digest(MASTER_KEY_SECRET.as_bytes());
        create_apikey_api(&index_path, master_key_bytes.as_slice(), &mut keys).unwrap_or_else(
            |error| {
                log::error!("Persisting master key failed: {error}");
                std::process::exit(1)
            },
        );
        let index_count: usize = keys.values().map(|key| key.index_list.len()).sum();
        log::info!(
            "Loaded {} API key scopes, {index_count} indices, {} bytes on disk in {:?}",
            keys.len(),
            dir_size(&index_path).unwrap_or(0),
            start.elapsed()
        );
        for key in keys.values() {
            for index in key.index_list.values() {
                let info = collect_index_info(index).await;
                log::info!(
                    "Scope {} index {} {:?}: {} documents, {} shards, lexical={}, vector={}, dimensions={}, {} bytes",
                    key.id,
                    info.meta.id,
                    info.meta.name,
                    info.indexed_documents,
                    info.shards,
                    info.lexical,
                    info.vector,
                    info.vector_dimensions,
                    info.index_size_bytes
                );
            }
        }
    }

    // The HTTP server stops accepting and drains connections before indexes are closed.
    http_server(
        &index_path,
        apikey_list.clone(),
        &local_ip,
        &local_port,
        &force_shard_number,
        default_nprobe,
    )
    .await;
    log::info!("Committing and closing all indices");
    let keys = apikey_list.write().await;
    let mut close_failed = false;
    for key in keys.values() {
        for index in key.index_list.values() {
            if let Err(error) = index.close().await {
                close_failed = true;
                log::error!("Index close failed: {error}");
            }
        }
    }
    if close_failed {
        log::error!("Shutdown incomplete: one or more indices failed to close");
        std::process::exit(1);
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
