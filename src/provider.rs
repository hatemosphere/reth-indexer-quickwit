use std::path::Path;
use std::sync::Arc;

use reth_chainspec::ChainSpecBuilder;
use reth_db::{mdbx::DatabaseArguments, open_db_read_only, DatabaseEnv};
use reth_node_ethereum::EthereumNode;
use reth_node_types::NodeTypesWithDBAdapter;
use reth_provider::{providers::StaticFileProvider, ProviderFactory};
use tracing::{debug, info};

use crate::error::IndexerError;

pub fn get_reth_db(path: &Path) -> Result<Arc<DatabaseEnv>, IndexerError> {
    let db_path = path.join("db");
    info!("Opening Reth database at: {:?}", db_path);

    let db_args = DatabaseArguments::default().with_exclusive(Some(false));
    debug!("Database arguments: read-only mode, non-exclusive");

    let db = open_db_read_only(db_path.as_path(), db_args)
        .map_err(|e| IndexerError::Database(format!("Could not open database: {}", e)))?;

    debug!("Database opened successfully");
    Ok(db.into())
}

pub fn get_reth_factory_with_db(
    path: &Path,
    db: Arc<DatabaseEnv>,
) -> Result<ProviderFactory<NodeTypesWithDBAdapter<EthereumNode, Arc<DatabaseEnv>>>, IndexerError> {
    // Use dev chainspec for local development
    let spec = ChainSpecBuilder::mainnet().cancun_activated().build();
    debug!("Using mainnet chain spec with Cancun activated");

    let static_files_path = path.join("static_files");
    debug!(
        "Initializing static file provider at: {:?}",
        static_files_path
    );

    let static_file_provider =
        StaticFileProvider::read_only(static_files_path, true).map_err(|e| {
            IndexerError::Provider(format!("Failed to create static file provider: {}", e))
        })?;

    let factory = ProviderFactory::<NodeTypesWithDBAdapter<EthereumNode, Arc<DatabaseEnv>>>::new(
        db,
        spec.into(),
        static_file_provider,
    );

    debug!("Provider factory created successfully");
    Ok(factory)
}
