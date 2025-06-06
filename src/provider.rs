use std::path::Path;
use std::sync::Arc;

use log::info;
use reth_chainspec::ChainSpecBuilder;
use reth_db::{mdbx::DatabaseArguments, open_db_read_only, DatabaseEnv};
use reth_node_ethereum::EthereumNode;
use reth_node_types::NodeTypesWithDBAdapter;
use reth_provider::{providers::StaticFileProvider, ProviderFactory};

use crate::error::IndexerError;

pub fn get_reth_db(path: &Path) -> Result<Arc<DatabaseEnv>, IndexerError> {
    let db_path = path.join("db");
    info!("Opening database at: {:?}", db_path);

    let db_args = DatabaseArguments::default().with_exclusive(Some(false));

    let db = open_db_read_only(db_path.as_path(), db_args)
        .map_err(|e| IndexerError::Database(format!("Could not open database: {}", e)))?;

    Ok(db.into())
}

pub fn get_reth_factory_with_db(
    path: &Path,
    db: Arc<DatabaseEnv>,
) -> Result<ProviderFactory<NodeTypesWithDBAdapter<EthereumNode, Arc<DatabaseEnv>>>, IndexerError> {
    // Use dev chainspec for local development
    let spec = ChainSpecBuilder::mainnet().cancun_activated().build();
    let static_file_provider = StaticFileProvider::read_only(path.join("static_files"), true)
        .map_err(|e| IndexerError::Provider(format!("Failed to create static file provider: {}", e)))?;

    let factory = ProviderFactory::<NodeTypesWithDBAdapter<EthereumNode, Arc<DatabaseEnv>>>::new(
        db,
        spec.into(),
        static_file_provider,
    );

    Ok(factory)
}

