mod csv;
mod datasource;
mod decode_events;
mod error;
mod indexer;
mod parquet;
mod provider;
mod quickwit;
mod rpc;
mod types;

use crate::indexer::sync;
use std::path::Path;
use types::IndexerConfig;

// We use jemalloc for performance reasons
#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

/// Loads the indexer configuration from the specified file path.
/// Returns the loaded `IndexerConfig` if successful.
fn load_indexer_config(file_path: &Path) -> Result<IndexerConfig, Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(file_path)
        .map_err(|e| format!("Failed to read config file at {:?}: {}", file_path, e))?;

    let config: IndexerConfig = serde_json::from_str(&content)
        .map_err(|e| format!("Failed to parse config JSON: {}", e))?;

    Ok(config)
}

#[tokio::main]
async fn main() {
    // Initialize the logger
    env_logger::init();

    // Check for RPC mode
    let rpc_mode: bool = std::env::var("RPC").map(|_| true).unwrap_or(false);
    let config: String =
        std::env::var("CONFIG").unwrap_or("./reth-indexer-config.json".to_string());
    println!("config: {}", config);

    let indexer_config: IndexerConfig = match load_indexer_config(Path::new(&config)) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Error loading configuration: {}", e);
            std::process::exit(1);
        }
    };

    if rpc_mode {
        // Start RPC server using existing Quickwit instance
        if let Some(quickwit_conf) = &indexer_config.quickwit {
            println!("Starting RPC server...");
            
            // Initialize Quickwit client (without recreating indexes)
            let mut quickwit_conf_for_rpc = quickwit_conf.clone();
            quickwit_conf_for_rpc.recreate_indexes = false;
            
            let quickwit_client = quickwit::init_quickwit_db(&quickwit_conf_for_rpc, &indexer_config.event_mappings)
                .await
                .expect("Failed to initialize Quickwit client for RPC");
            
            let rpc_port = std::env::var("RPC_PORT")
                .unwrap_or("8545".to_string())
                .parse::<u16>()
                .unwrap_or(8545);
            
            rpc::start_rpc_server(quickwit_client, quickwit_conf.index_prefix.clone(), rpc_port).await;
        } else {
            eprintln!("RPC mode requires Quickwit configuration");
            std::process::exit(1);
        }
    } else {
        // Normal indexing mode
        sync(&indexer_config).await;
    }
}
