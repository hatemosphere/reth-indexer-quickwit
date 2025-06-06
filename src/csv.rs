use std::{
    fs::{self, File},
    io::BufWriter,
    path::{Path, PathBuf},
};

use csv::{Writer, WriterBuilder};
use tracing::{debug, error, trace};

use crate::types::{ABIItem, IndexerContractMapping};

// Temporary constant
const ETH_TRANSFER_TABLE_NAME: &str = "eth_transfer";

/// Creates CSV writers based on the provided indexer contract mappings and configuration options.
///
/// # Arguments
///
/// * `write_path` - The path to write the CSV files.
/// * `indexer_contract_mappings` - The list of indexer contract mappings.
/// * `include_eth_transfers` - Indicates whether to include ETH transfers in the writers.
/// * `sync_threshold` - How many records to write before syncing to database.
///
/// # Returns
///
/// Returns a vector of `CsvWriter` instances.
pub fn create_csv_writers(
    write_path: &Path,
    indexer_contract_mappings: &[IndexerContractMapping],
    include_eth_transfers: bool,
    sync_threshold: usize,
) -> Vec<CsvWriter> {
    debug!(
        "Creating CSV writers at path: {:?}, sync_threshold: {}",
        write_path, sync_threshold
    );

    let mut writers: Vec<CsvWriter> = indexer_contract_mappings
        .iter()
        .flat_map(|mapping| {
            mapping.decode_abi_items.iter().map(|abi_item| {
                debug!("Creating CSV writer for event: {}", abi_item.name);
                CsvWriter::new(
                    abi_item.name.clone(),
                    write_path,
                    csv_event_columns(abi_item),
                    sync_threshold,
                )
            })
        })
        .collect();

    if include_eth_transfers {
        debug!("Creating CSV writer for ETH transfers");
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
            sync_threshold,
        ));
    }

    debug!("Created {} CSV writers", writers.len());
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
    trace!("Creating CSV file at: {:?}", path_to_csv);

    let file = match File::create(path_to_csv) {
        Ok(f) => f,
        Err(e) => {
            error!("Failed to create CSV file at {:?}: {}", path_to_csv, e);
            panic!("Failed to create CSV file: {}", e);
        }
    };

    let file = BufWriter::new(file);
    let mut writer = WriterBuilder::new().from_writer(file);

    if let Err(e) = writer.write_record(columns) {
        error!("Failed to write CSV header: {}", e);
        panic!("Failed to write CSV header record: {}", e);
    }

    if let Err(e) = writer.flush() {
        error!("Failed to flush CSV writer: {}", e);
        panic!("Failed to flush CSV writer: {}", e);
    }

    trace!("CSV file created with {} columns", columns.len());
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

    /// Total number of records written to this CSV
    total_records: usize,

    /// Sync to database every N records
    sync_threshold: usize,
}

impl CsvWriter {
    /// Creates a new `CsvWriter` instance.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the CSV writer.
    /// * `path_folder` - The path to the folder where the CSV file will be created.
    /// * `columns` - The columns of the CSV file.
    /// * `sync_threshold` - How often to sync to database.
    ///
    /// # Returns
    ///
    /// A new `CsvWriter` instance.
    pub fn new(
        name: String,
        path_folder: &Path,
        columns: Vec<String>,
        sync_threshold: usize,
    ) -> Self {
        let path_to_csv = path_folder.join(&name).with_extension("csv");

        // remove csv file if it exists
        if path_to_csv.exists() {
            debug!("Removing existing CSV file: {:?}", path_to_csv);
            if let Err(e) = fs::remove_file(&path_to_csv) {
                error!("Failed to remove existing CSV file: {}", e);
            }
        }

        CsvWriter {
            name,
            writer: create_writer(path_to_csv.as_path(), &columns),
            path_to_csv,
            columns,
            total_records: 0,
            sync_threshold,
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
        trace!("Writing record to CSV: {}", self.name);

        if let Err(e) = self.writer.write_record(&records) {
            error!("Failed to write record to CSV {}: {}", self.name, e);
            panic!("Failed to write records to CSV: {}", e);
        }

        self.total_records += 1;

        // Flush every time for data integrity
        if let Err(e) = self.writer.flush() {
            error!("Failed to flush CSV writer {}: {}", self.name, e);
            panic!("Failed to flush CSV writer: {}", e);
        }

        if self.total_records % 10000 == 0 {
            debug!("CSV {}: {} records written", self.name, self.total_records);
        }
    }

    /// Get a mutable reference to the underlying CSV writer for direct field writing.
    /// This is more efficient than building a Vec<String> when you have many fields.
    pub fn get_writer(&mut self) -> &mut Writer<BufWriter<File>> {
        &mut self.writer
    }

    /// Increment the record count by a specific amount.
    pub fn increment_record_count_by(&mut self, amount: usize) {
        self.total_records += amount;
    }

    /// Force a flush of the CSV writer.
    pub fn flush(&mut self) {
        trace!("Flushing CSV writer: {}", self.name);
        if let Err(e) = self.writer.flush() {
            error!("Failed to flush CSV writer {}: {}", self.name, e);
            panic!("Failed to flush CSV writer: {}", e);
        }
    }

    /// Check if we should sync to database based on record count
    pub fn should_sync(&self) -> bool {
        self.total_records > 0 && self.total_records % self.sync_threshold == 0
    }

    /// Get the total number of records written
    pub fn get_total_records(&self) -> usize {
        self.total_records
    }

    /// Deletes the CSV file associated with this CsvWriter.
    ///
    /// # Panics
    ///
    /// This function will panic if there is an error deleting the CSV file.
    pub fn delete(&mut self) {
        debug!("Deleting CSV file: {:?}", self.path_to_csv);
        if let Err(e) = fs::remove_file(&self.path_to_csv) {
            error!("Failed to delete CSV file {:?}: {}", self.path_to_csv, e);
            panic!("Failed to delete CSV file: {}", e);
        }
    }

    /// Resets the CsvWriter by deleting the existing CSV file and creating a new one.
    ///
    /// # Panics
    ///
    /// This function will panic if there is an error deleting the existing CSV file or
    /// creating a new one.
    pub fn reset(&mut self) {
        debug!("Resetting CSV writer: {}", self.name);
        self.delete();
        self.writer = create_writer(&self.path_to_csv, &self.columns);
        self.total_records = 0;
        debug!("CSV writer {} reset complete", self.name);
    }

    /// Returns the path to the CSV file.
    ///
    /// # Returns
    ///
    /// The path to the CSV file as a string slice.
    pub fn path(&self) -> &str {
        self.path_to_csv.to_str().unwrap()
    }
}
