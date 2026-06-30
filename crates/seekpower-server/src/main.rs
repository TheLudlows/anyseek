use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use seekpower_server::create_app;

#[derive(Debug, serde::Deserialize)]
struct Config {
    #[serde(default)]
    server: ServerConfig,
    #[serde(default)]
    storage: StorageConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            storage: StorageConfig::default(),
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct ServerConfig {
    #[serde(default = "default_addr")]
    addr: String,
    #[serde(default = "default_log_level")]
    log_level: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            addr: default_addr(),
            log_level: default_log_level(),
        }
    }
}

#[derive(Debug, serde::Deserialize)]
struct StorageConfig {
    #[serde(default = "default_storage_kind")]
    kind: String,
    #[serde(default = "default_storage_path")]
    path: String,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            kind: default_storage_kind(),
            path: default_storage_path(),
        }
    }
}

fn default_addr() -> String {
    "127.0.0.1:3000".into()
}
fn default_log_level() -> String {
    "info".into()
}
fn default_storage_kind() -> String {
    "inmemory".into()
}
fn default_storage_path() -> String {
    "./data".into()
}

fn load_config(path: &PathBuf) -> Config {
    if path.exists() {
        let content = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("failed to read config file {:?}: {}", path, e));
        toml::from_str(&content)
            .unwrap_or_else(|e| panic!("failed to parse config file {:?}: {}", path, e))
    } else {
        Config::default()
    }
}

#[tokio::main]
async fn main() {
    let config_path = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var("SEEKPOWER_CONFIG")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("seekpower.toml"))
        });

    let config = load_config(&config_path);

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| config.server.log_level.clone().into()),
        )
        .init();

    let addr: SocketAddr = config
        .server
        .addr
        .parse()
        .unwrap_or_else(|e| panic!("invalid listen address '{}': {}", config.server.addr, e));

    let state = Arc::new(Mutex::new(None));
    let app = create_app(state);

    tracing::info!("SeekPower server listening on {}", addr);
    tracing::info!("Storage: kind={}, path={}", config.storage.kind, config.storage.path);

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| panic!("failed to bind to {}: {}", addr, e));

    if let Err(e) = axum::serve(listener, app).await {
        tracing::error!("server error: {}", e);
        std::process::exit(1);
    }
}
