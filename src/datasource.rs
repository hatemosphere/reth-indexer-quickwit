use crate::{csv::CsvWriter, types::IndexerContractMapping};
use async_trait::async_trait;

use csv::ReaderBuilder;
use indexmap::IndexMap;
use phf::phf_ordered_map;
use polars::prelude::*;
use std::{any::Any, collections::HashMap, fs::File};

///  Common trait for writeable datasources
///  This interface will be implemented by each writer to allow for
///  different storage backends (Parquet, Quickwit, etc.)

#[async_trait]
pub trait DatasourceWritable {
    // Legacy CSV-based interface (for backwards compatibility)
    async fn write_data(&self, table_name: &str, csv_writer: &CsvWriter);

    // New direct data interface (preferred for new implementations)
    #[allow(dead_code)]
    async fn write_documents(
        &self,
        _table_name: &str,
        _documents: Vec<serde_json::Value>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Default implementation returns error - implementations should override
        Err("write_documents not implemented for this datasource".into())
    }

    // Check if this datasource prefers direct document writes
    #[allow(dead_code)]
    fn supports_direct_writes(&self) -> bool {
        false
    }

    #[allow(dead_code)]
    fn as_any(&self) -> &dyn Any;
}

/// Columns common to all tablesi
/// Each table's columns will include the following commmon fields
///
pub static COMMON_COLUMNS: phf::OrderedMap<&'static str, &'static str> = phf_ordered_map! {
    "indexed_id" => "string",  // unique identifier generated in rust
    "contract_address" => "string",
    "tx_hash" => "string",
    "block_number" => "int",
    "block_hash" => "string",
    "timestamp" => "int"
};

///
/// This function merges the common columns specified in COMMON_COLUMNS with the
/// columns specified in the reth-indexer-config file for the particular event type to parse
/// The types are ordered to allow for consistent column order within the GCP tables
/// This is done for each table specified in the indexer
///
/// # Arguments
///
/// * `indexer_event_mappings` - list of all event mappings
pub fn load_table_configs(
    indexer_event_mappings: &[IndexerContractMapping],
) -> HashMap<String, IndexMap<String, String>> {
    //  Config map for all tables
    //  Ordered by common columns, then by configured mapped
    let mut all_tables: HashMap<String, IndexMap<String, String>> = HashMap::new();

    for mapping in indexer_event_mappings {
        for abi_item in mapping.decode_abi_items.iter() {
            let table_name = abi_item.name.to_lowercase();
            let column_type_map: IndexMap<String, String> = abi_item
                .inputs
                .iter()
                .map(|input| {
                    (
                        input.name.clone(),
                        solidity_type_to_db_type(input.type_.clone().as_str()).to_string(),
                    )
                })
                .collect();

            let merged_column_types: IndexMap<String, String> = COMMON_COLUMNS
                .into_iter()
                .map(|it| (it.0.to_string(), it.1.to_string()))
                .chain(column_type_map)
                .collect();

            all_tables.insert(table_name, merged_column_types);
        }
    }

    all_tables
}

///
/// Maps solidity types to generic database types.
/// Used for creating appropriate column types in various storage backends.
///
/// # Arguments
///
/// * `abi_type` - the ABI type, specified as a string
pub fn solidity_type_to_db_type(abi_type: &str) -> &str {
    match abi_type {
        "address" => "string",
        "bool" | "bytes" | "string" | "int256" | "uint256" => "string",
        "uint8" | "uint16" | "uint32" | "uint64" | "uint128" | "int8" | "int16" | "int32"
        | "int64" | "int128" => "int",
        _ => panic!("Unsupported type {}", abi_type),
    }
}

///
/// Read CSV to Polars dataframe
/// Given CSV file, read into type-enforce polars dataframe
/// We enforce the type of the column based on the table configuration rather than
/// utilize polars defaults, due to the large numbers involved in some values
/// Polars' default type imputation will yield wrong values
///
/// # Arguments
///
/// * `table_name` - name of table for dataset
/// * `path` - path to csv file
pub fn read_csv_to_polars(path: &str, column_map: &IndexMap<String, String>) -> DataFrame {
    //  Get list of columns in order, from csv file
    let file = File::open(path).unwrap();
    let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);
    let headers = rdr.headers().unwrap();
    let column_names: Vec<String> = headers.iter().map(String::from).collect();

    //  Build column data types for polars dataframe, from column mapping
    let plr_col_types = Some(Arc::new(
        column_names
            .iter()
            .map(|name| match column_map[name].as_str() {
                "int" => Field::new(name.clone().into(), DataType::Int64),
                "string" => Field::new(name.clone().into(), DataType::String),
                _ => panic!("incompatible type found"),
            })
            .collect(),
    ));

    //  Read polars dataframe, w/ specified schema
    CsvReadOptions::default()
        .with_has_header(true)
        .with_schema(plr_col_types)
        .try_into_reader_with_file_path(Some(path.into()))
        .unwrap()
        .finish()
        .unwrap()
}
