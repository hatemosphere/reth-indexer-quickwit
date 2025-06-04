use alloy_primitives::Address;
use serde::Deserialize;
use std::path::PathBuf;

/// Represents an input parameter in the ABI.
#[derive(Debug, Deserialize, Clone)]
pub struct ABIInput {
    /// Indicates if the input parameter is indexed.
    pub indexed: bool,

    /// The internal type of the input parameter.
    #[serde(rename = "internalType")]
    #[allow(dead_code)]
    pub internal_type: String,

    /// The name of the input parameter.
    pub name: String,

    /// The type of the input parameter.
    #[serde(rename = "type")]
    pub type_: String,

    #[serde(
        // deserialize_with = "deserialize_regex_option",
        rename = "rethRegexMatch"
    )]
    pub regex: Option<String>,
}

/// Represents an item in the ABI.
#[derive(Debug, Deserialize, Clone)]
pub struct ABIItem {
    /// The list of input parameters for the ABI item.
    pub inputs: Vec<ABIInput>,

    /// The name of the ABI item.
    pub name: String,

    // Apply custom indexes to the database
    #[allow(dead_code)]
    pub custom_db_indexes: Option<Vec<Vec<String>>>,
}

/// Represents a contract mapping in the Indexer.
#[derive(Debug, Deserialize, Clone)]
pub struct IndexerContractMapping {
    /// The contract address.
    #[serde(rename = "filterByContractAddress")]
    // pub contract_address: Option<Address>,
    pub filter_by_contract_addresses: Option<Vec<Address>>,

    /// How often you should sync back to the postgres db.
    #[serde(rename = "syncBackRoughlyEveryNLogs")]
    pub sync_back_every_n_log: u64,

    /// The list of ABI items to decode.
    #[serde(rename = "decodeAbiItems")]
    pub decode_abi_items: Vec<ABIItem>,
}

fn default_false() -> bool {
    false
}


#[derive(Debug, Deserialize)]
pub struct IndexerParquetConfig {
    #[serde(rename = "dropTableBeforeSync")]
    pub drop_tables: bool,

    #[serde(rename = "dataDirectory")]
    pub data_directory: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct IndexerQuickwitConfig {
    /// Quickwit API endpoint (e.g., "http://localhost:7280")
    #[serde(rename = "apiEndpoint")]
    pub api_endpoint: String,

    /// Index name prefix (will be appended with event name)
    #[serde(rename = "indexPrefix")]
    pub index_prefix: String,

    /// Local data directory for Quickwit storage
    #[serde(rename = "dataDirectory")]
    pub data_directory: String,

    /// Batch size for document ingestion
    #[serde(rename = "batchSize", default = "default_batch_size")]
    pub batch_size: usize,

    /// Whether to recreate indexes on startup
    #[serde(rename = "recreateIndexes", default = "default_false")]
    pub recreate_indexes: bool,

    /// Whether to use Elasticsearch bulk API instead of native Quickwit API
    #[serde(rename = "useEsBulkApi", default = "default_false")]
    pub use_es_bulk_api: bool,
}

fn default_batch_size() -> usize {
    1000
}

#[derive(Debug, Deserialize)]
pub struct IndexerConfig {
    /// The location of the rethDB.
    #[serde(rename = "rethDBLocation")]
    pub reth_db_location: PathBuf,

    /// The location of the CSV.
    #[serde(rename = "csvLocation")]
    pub csv_location: PathBuf,

    /// Include ETH transfers in indexing
    #[serde(rename = "ethTransfers", default = "default_false")]
    pub include_eth_transfers: bool,

    /// The starting block number.
    #[serde(rename = "fromBlockNumber")]
    pub from_block: u64,

    /// The ending block number.
    #[serde(rename = "toBlockNumber")]
    pub to_block: Option<u64>,

    /// parquet configuration, if exists
    #[serde(rename = "parquet", skip_serializing_if = "Option::is_none")]
    pub parquet: Option<IndexerParquetConfig>,

    /// Quickwit configuration, if exists
    #[serde(rename = "quickwit", skip_serializing_if = "Option::is_none")]
    pub quickwit: Option<IndexerQuickwitConfig>,

    /// The list of contract mappings.
    #[serde(rename = "eventMappings")]
    pub event_mappings: Vec<IndexerContractMapping>,
}
