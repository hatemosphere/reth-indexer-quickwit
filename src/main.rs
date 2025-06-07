mod csv;
mod datasource;
mod datasource_factory;
mod decode_events;
mod error;
mod indexer;
mod parquet;
mod provider;
mod quickwit;
mod rpc;
mod types;

use crate::error::IndexerError;
use crate::indexer::sync;
use std::path::Path;
use tracing::{debug, error, info};
use types::IndexerConfig;

// We use jemalloc for performance reasons
#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

/// Loads the indexer configuration from the specified file path.
/// Returns the loaded `IndexerConfig` if successful.
fn load_indexer_config(file_path: &Path) -> Result<IndexerConfig, IndexerError> {
    let content = std::fs::read_to_string(file_path).map_err(|e| {
        IndexerError::Config(format!(
            "Failed to read config file at {:?}: {}",
            file_path, e
        ))
    })?;

    let config: IndexerConfig = serde_json::from_str(&content)
        .map_err(|e| IndexerError::Config(format!("Failed to parse config JSON: {}", e)))?;

    Ok(config)
}

#[tokio::main]
async fn main() {
    // Initialize the logger
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_thread_ids(true)
        .with_thread_names(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set the global tracing subscriber");
    tracing_log::LogTracer::init().expect("Failed to initialize log tracer");

    // Log startup information
    info!("Starting reth-indexer");
    debug!("Jemalloc allocator: {}", cfg!(feature = "jemalloc"));

    // Check for RPC mode
    let rpc_mode: bool = std::env::var("RPC").map(|_| true).unwrap_or(false);
    let config_path: String =
        std::env::var("CONFIG").unwrap_or("./reth-indexer-config.json".to_string());
    info!("Loading configuration from: {}", config_path);

    let indexer_config: IndexerConfig = match load_indexer_config(Path::new(&config_path)) {
        Ok(config) => {
            debug!("Configuration loaded successfully");
            debug!("Event mappings: {} configured", config.event_mappings.len());
            debug!("From block: {}", config.from_block);
            if let Some(to_block) = config.to_block {
                debug!("To block: {}", to_block);
            }
            if let Some(ref quickwit) = config.quickwit {
                debug!("Quickwit enabled: {}", quickwit.api_endpoint);
            }
            if let Some(ref parquet) = config.parquet {
                debug!("Parquet output enabled: {}", parquet.data_directory);
            }
            config
        }
        Err(e) => {
            error!("Failed to load configuration from {}: {}", config_path, e);
            std::process::exit(1);
        }
    };

    if rpc_mode {
        // Start RPC server using existing Quickwit instance
        if let Some(quickwit_conf) = &indexer_config.quickwit {
            let rpc_port = std::env::var("RPC_PORT")
                .unwrap_or("8545".to_string())
                .parse::<u16>()
                .unwrap_or(8545);

            info!("Starting RPC server on port {}", rpc_port);
            debug!("Quickwit endpoint: {}", quickwit_conf.api_endpoint);
            debug!("Index prefix: {}", quickwit_conf.index_prefix);

            // Initialize Quickwit client (without recreating indexes)
            let mut quickwit_conf_for_rpc = quickwit_conf.clone();
            quickwit_conf_for_rpc.recreate_indexes = false;

            let quickwit_client = match quickwit::init_quickwit_db(
                &quickwit_conf_for_rpc,
                &indexer_config.event_mappings,
            )
            .await
            {
                Ok(client) => {
                    info!("Quickwit client initialized successfully");
                    client
                }
                Err(e) => {
                    error!("Failed to initialize Quickwit client: {:?}", e);
                    std::process::exit(1);
                }
            };

            rpc::start_rpc_server(
                quickwit_client,
                quickwit_conf.index_prefix.clone(),
                rpc_port,
            )
            .await;
        } else {
            error!("Cannot start RPC server: Quickwit configuration is missing");
            std::process::exit(1);
        }
    } else {
        info!("Starting indexer in sync mode");
        sync(&indexer_config).await;
    }

    info!("Reth indexer shutdown complete");
}
