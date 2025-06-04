use std::{
    fs::{self, File},
    io::BufWriter,
    path::{Path, PathBuf},
};

use csv::{Writer, WriterBuilder};

use crate::{
    types::{ABIItem, IndexerContractMapping},
};

// Temporary constant
const ETH_TRANSFER_TABLE_NAME: &str = "eth_transfer";

/// Creates CSV writers based on the provided indexer contract mappings and configuration options.
///
/// # Arguments
///
/// * `write_path` - The path to write the CSV files.
/// * `indexer_contract_mappings` - The list of indexer contract mappings.
/// * `include_eth_transfers` - Indicates whether to include ETH transfers in the writers.
///
/// # Returns
///
/// Returns a vector of `CsvWriter` instances.
pub fn create_csv_writers(
    write_path: &Path,
    indexer_contract_mappings: &[IndexerContractMapping],
    include_eth_transfers: bool,
) -> Vec<CsvWriter> {
    let mut writers: Vec<CsvWriter> = indexer_contract_mappings
        .iter()
        .flat_map(|mapping| {
            mapping.decode_abi_items.iter().map(|abi_item| {
                CsvWriter::new(
                    abi_item.name.clone(),
                    write_path,
                    csv_event_columns(abi_item),
                )
            })
        })
        .collect();

    if include_eth_transfers {
        writers.push(CsvWriter::new(
            ETH_TRANSFER_TABLE_NAME.to_string(),
            write_path,
            vec![
                "from".to_string(),
                "to".to_string(),
                "value".to_string(),
                "block_number".to_string(),
                "block_hash".to_string(),
                "timestamp".to_string(),
            ],
        ));
    }

    writers
}

/// Creates a CSV writer with the provided path and columns.
///
/// # Arguments
///
/// * `path_to_csv` - The path to the CSV file.
/// * `columns` - The column headers for the CSV file.
///
/// # Returns
///
/// Returns a `csv::Writer` instance.
fn create_writer(path_to_csv: &Path, columns: &Vec<String>) -> Writer<BufWriter<File>> {
    let file = File::create(path_to_csv).expect("Failed to create CSV file");
    let file = BufWriter::new(file);

    let mut writer = WriterBuilder::new().from_writer(file);

    writer
        .write_record(columns)
        .expect("Failed to write CSV header record");
    writer.flush().expect("Failed to flush CSV writer");

    writer
}

/// Generates the column names for an event CSV based on the ABI item.
///
/// The column names are generated in the following order:
/// - Sorted input names, sorted by the indexed field in descending order.
/// - Additional common fields: "record_id", "contract_address", "tx_hash", "block_number", "block_hash", "timestamp".
///
/// # Arguments
///
/// * `abi_item` - The ABI item representing the event.
///
/// # Returns
///
/// Returns a vector of column names for the event CSV.
fn csv_event_columns(abi_item: &ABIItem) -> Vec<String> {
    let mut sorted_inputs = abi_item.inputs.clone();
    sorted_inputs.sort_by_key(|input| !input.indexed); // Sort by indexed field in descending order

    let columns_prefix = vec![
        "indexed_id".to_string(), // the column in database created is "indexed_id", not "record_id"
        "contract_address".to_string(),
    ];
    
    // Add raw log fields for eth_getLogs compatibility
    let columns_log_fields = vec![
        "log_index".to_string(),
        "transaction_index".to_string(),
        "topic0".to_string(), // Always the event signature
        "topic1".to_string(), // Optional indexed parameter
        "topic2".to_string(), // Optional indexed parameter
        "topic3".to_string(), // Optional indexed parameter
        "data".to_string(),   // Non-indexed parameters encoded
    ];
    
    let columns_suffix = vec![
        "tx_hash".to_string(),
        "block_number".to_string(),
        "block_hash".to_string(),
        "timestamp".to_string(),
    ];
    let columns_abi: Vec<String> = sorted_inputs
        .iter()
        .map(|input| input.name.clone())
        .collect();

    columns_prefix
        .into_iter()
        .chain(columns_log_fields)
        .chain(columns_abi)
        .chain(columns_suffix)
        .collect()
}

/// A struct representing a CSV writer with batched flushing for better performance.
pub struct CsvWriter {
    /// The name
    pub name: String,

    /// The underlying CSV writer.
    writer: Writer<BufWriter<File>>,

    /// The path to the CSV file.
    path_to_csv: PathBuf,

    columns: Vec<String>,
    
    /// Number of records written since last flush
    records_since_flush: usize,
    
    /// Flush every N records for better performance
    flush_interval: usize,
}

impl CsvWriter {
    /// Creates a new `CsvWriter` instance.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the CSV writer.
    /// * `path_folder` - The path to the folder where the CSV file will be created.
    /// * `columns` - The columns of the CSV file.
    ///
    /// # Returns
    ///
    /// A new `CsvWriter` instance.
    pub fn new(name: String, path_folder: &Path, columns: Vec<String>) -> Self {
        let path_to_csv = path_folder.join(&name).with_extension("csv");

        // remove csv file if it exists (ignore result)
        let _ = fs::remove_file(&path_to_csv);

        CsvWriter {
            name,
            writer: create_writer(path_to_csv.as_path(), &columns),
            path_to_csv,
            columns,
            records_since_flush: 0,
            flush_interval: 1000, // Flush every 1000 records for better performance
        }
    }

    /// Writes a batch of records to the CSV file with batched flushing.
    ///
    /// # Arguments
    ///
    /// * `records` - The records to write to the CSV file.
    ///
    /// # Panics
    ///
    /// This function will panic if there is an error writing to the CSV file or flushing the writer.
    pub fn write(&mut self, records: Vec<String>) {
        self.writer
            .write_record(&records)
            .expect("Failed to write records to CSV");
        
        self.records_since_flush += 1;
        
        // Only flush periodically for better performance
        if self.records_since_flush >= self.flush_interval {
            self.writer.flush().expect("Failed to flush CSV writer");
            self.records_since_flush = 0;
        }
    }
    
    /// Force a flush of the CSV writer.
    pub fn flush(&mut self) {
        self.writer.flush().expect("Failed to flush CSV writer");
        self.records_since_flush = 0;
    }

    /// Deletes the CSV file associated with this CsvWriter.
    ///
    /// # Panics
    ///
    /// This function will panic if there is an error deleting the CSV file.
    pub fn delete(&mut self) {
        fs::remove_file(&self.path_to_csv).unwrap();
    }

    /// Resets the CsvWriter by deleting the existing CSV file and creating a new one.
    ///
    /// # Panics
    ///
    /// This function will panic if there is an error deleting the existing CSV file or
    /// creating a new one.
    pub fn reset(&mut self) {
        self.delete();
        self.writer = create_writer(&self.path_to_csv, &self.columns);
    }

    /// Returns the path to the CSV file.
    ///
    /// # Returns
    ///
    /// The path to the CSV file as a string slice.
    pub fn path(&self) -> &str {
        self.path_to_csv.to_str().unwrap()
    }

    /// Returns the size of a CSV file in kilobytes.
    ///
    /// # Arguments
    ///
    /// * `path_to_csv` - The path to the CSV file.
    ///
    /// # Returns
    ///
    /// The size of the CSV file in kilobytes.
    pub fn get_kb_file_size(&self) -> u64 {
        let metadata = fs::metadata(&self.path_to_csv).expect("Failed to retrieve file metadata");
        let size = metadata.len();
        size / 1024
    }
}
