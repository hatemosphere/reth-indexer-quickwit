use std::path::Path;
use std::sync::Arc;

use reth_db::{open_db_read_only, DatabaseEnv, mdbx::DatabaseArguments};
use reth_chainspec::ChainSpecBuilder;
use reth_provider::{ProviderFactory, providers::StaticFileProvider};
use reth_node_ethereum::EthereumNode;
use reth_node_types::NodeTypesWithDBAdapter;

pub fn get_reth_factory(path: &Path) -> Result<ProviderFactory<NodeTypesWithDBAdapter<EthereumNode, Arc<DatabaseEnv>>>, String> {
    // Check database version
    let db_version_path = path.join("db").join("database.version");
    if db_version_path.exists() {
        if let Ok(version) = std::fs::read_to_string(&db_version_path) {
            println!("Database version: {}", version.trim());
        }
    }
    
    let db = open_db_read_only(path.join("db").as_path(), DatabaseArguments::default()).map_err(|e| {
        format!("Could not open database: {:?}. Make sure reth node is syncing and readable", e)
    })?;

    // Use dev chainspec for local development
    let spec = ChainSpecBuilder::mainnet()
        .cancun_activated()
        .build();
    let static_file_provider = StaticFileProvider::read_only(path.join("static_files"), true)
        .map_err(|e| format!("Failed to create static file provider: {}", e))?;
    
    let factory = ProviderFactory::<NodeTypesWithDBAdapter<EthereumNode, Arc<DatabaseEnv>>>::new(
        db.into(),
        spec.into(),
        static_file_provider,
    );

    Ok(factory)
}
