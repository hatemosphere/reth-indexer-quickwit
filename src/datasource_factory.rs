use crate::{
    datasource::DatasourceWritable,
    error::IndexerError,
    types::{IndexerConfig, IndexerContractMapping},
};
use async_trait::async_trait;
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Factory trait for creating datasource instances
#[async_trait]
pub trait DatasourceFactory: Send + Sync {
    /// Create a new datasource instance from configuration
    async fn create(
        &self,
        config: &IndexerConfig,
        event_mappings: &[IndexerContractMapping],
    ) -> Result<Box<dyn DatasourceWritable>, IndexerError>;

    /// Check if this factory can handle the given configuration
    fn can_handle(&self, config: &IndexerConfig) -> bool;

    /// Get the name of this datasource type
    fn name(&self) -> &'static str;
}

/// Registry for datasource factories
pub struct DatasourceRegistry {
    factories: HashMap<String, Box<dyn DatasourceFactory>>,
}

impl DatasourceRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            factories: HashMap::new(),
        }
    }

    /// Register a datasource factory
    pub fn register(&mut self, factory: Box<dyn DatasourceFactory>) {
        debug!("Registering datasource factory: {}", factory.name());
        self.factories.insert(factory.name().to_string(), factory);
    }

    /// Create all applicable datasources from configuration
    pub async fn create_datasources(
        &self,
        config: &IndexerConfig,
    ) -> Result<Vec<Box<dyn DatasourceWritable>>, IndexerError> {
        let mut writers = Vec::new();
        debug!(
            "Creating datasources from {} registered factories",
            self.factories.len()
        );

        for factory in self.factories.values() {
            if factory.can_handle(config) {
                info!("Initializing {} datasource", factory.name());
                match factory.create(config, &config.event_mappings).await {
                    Ok(writer) => {
                        debug!("{} datasource initialized successfully", factory.name());
                        writers.push(writer);
                    }
                    Err(e) => {
                        warn!("Failed to initialize {} datasource: {}", factory.name(), e);
                    }
                }
            } else {
                debug!("{} datasource not configured, skipping", factory.name());
            }
        }

        if writers.is_empty() {
            return Err(IndexerError::Config(
                "No datasources configured. Please configure at least one datasource (parquet, quickwit, etc.)".to_string()
            ));
        }

        info!("Initialized {} datasource(s)", writers.len());
        Ok(writers)
    }
}

/// Parquet datasource factory
pub struct ParquetFactory;

#[async_trait]
impl DatasourceFactory for ParquetFactory {
    async fn create(
        &self,
        config: &IndexerConfig,
        event_mappings: &[IndexerContractMapping],
    ) -> Result<Box<dyn DatasourceWritable>, IndexerError> {
        let parquet_conf = config
            .parquet
            .as_ref()
            .ok_or_else(|| IndexerError::Config("Parquet configuration missing".to_string()))?;

        let client = crate::parquet::init_parquet_db(parquet_conf, event_mappings)
            .await
            .map_err(|e| IndexerError::Config(format!("Failed to initialize Parquet: {:?}", e)))?;

        Ok(Box::new(client))
    }

    fn can_handle(&self, config: &IndexerConfig) -> bool {
        config.parquet.is_some()
    }

    fn name(&self) -> &'static str {
        "parquet"
    }
}

/// Quickwit datasource factory
pub struct QuickwitFactory;

#[async_trait]
impl DatasourceFactory for QuickwitFactory {
    async fn create(
        &self,
        config: &IndexerConfig,
        event_mappings: &[IndexerContractMapping],
    ) -> Result<Box<dyn DatasourceWritable>, IndexerError> {
        let quickwit_conf = config
            .quickwit
            .as_ref()
            .ok_or_else(|| IndexerError::Config("Quickwit configuration missing".to_string()))?;

        let client = crate::quickwit::init_quickwit_db(quickwit_conf, event_mappings)
            .await
            .map_err(|e| IndexerError::Config(format!("Failed to initialize Quickwit: {:?}", e)))?;

        Ok(Box::new(client))
    }

    fn can_handle(&self, config: &IndexerConfig) -> bool {
        config.quickwit.is_some()
    }

    fn name(&self) -> &'static str {
        "quickwit"
    }
}

/// Create a default registry with all built-in datasources
pub fn create_default_registry() -> DatasourceRegistry {
    let mut registry = DatasourceRegistry::new();

    // Register built-in datasources
    registry.register(Box::new(ParquetFactory));
    registry.register(Box::new(QuickwitFactory));

    registry
}

/// Initialize datasource writers using the registry pattern
pub async fn init_datasource_writers(
    config: &IndexerConfig,
) -> Result<Vec<Box<dyn DatasourceWritable>>, IndexerError> {
    debug!("Initializing datasource writers");
    let registry = create_default_registry();
    registry.create_datasources(config).await
}
