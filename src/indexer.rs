use std::{
    str::FromStr,
    time::{Duration, Instant},
};

use alloy_consensus::{Transaction, TxReceipt};
use alloy_primitives::{hex::ToHexExt, Address, Bloom, Sealable, B256 as H256};
use alloy_rpc_types::{FilterSet, FilteredParams};
use log::{debug, error, info, warn};
use reth_primitives::{Header, Log};
use reth_primitives_traits::{SignedTransaction, SignerRecoverable};
use reth_provider::{
    BlockBodyIndicesProvider, BlockNumReader, HeaderProvider, ReceiptProvider, TransactionsProvider,
};
// use uuid::Uuid; // Removed - using uuid directly

use crate::{
    csv::{create_csv_writers, CsvWriter}, // Re-enabled for testing
    datasource::DatasourceWritable,
    decode_events::{abi_item_to_topic_id, decode_logs, DecodedLog},
    provider::{get_reth_db, get_reth_factory_with_db},
    types::{IndexerConfig, IndexerContractMapping},
};

/// Writes a state record to a CSV file.
///
/// This function writes a state record to a CSV file using the provided `CsvWriter`. The state
/// record consists of decoded logs, transaction information, and block information.
///
/// # Arguments
///
/// * `csv_writer` - A mutable reference to a `CsvWriter` for writing the state record.
/// * `decoded_logs` - A reference to a vector of tuples containing `DecodedLog` and raw `Log`.
/// * `header_tx_info` - A reference to the `Header` containing transaction information.
/// * `tx_hash` - The transaction hash.
/// * `block_hash` - The block hash.
/// * `tx_index` - The transaction index in the block.
fn write_csv_state_record(
    csv_writer: &mut CsvWriter,
    decoded_logs: &[(DecodedLog, Log)],
    header_tx_info: &Header,
    tx_hash: H256,
    block_hash: H256,
    tx_index: u64,
) {
    use csv::ByteRecord;

    let writer = csv_writer.get_writer();

    for (log_index, (decoded_log, raw_log)) in decoded_logs.iter().enumerate() {
        let mut record = ByteRecord::new();

        // Write fields directly to avoid intermediate allocations
        record.push_field(uuid::Uuid::new_v4().to_string().as_bytes());
        record.push_field(format!("{:?}", decoded_log.address).as_bytes());

        // Add raw log fields for eth_getLogs compatibility
        record.push_field(log_index.to_string().as_bytes());
        record.push_field(tx_index.to_string().as_bytes());

        // Add topics (topic0 is always present, others may be empty)
        let topics = raw_log.topics();
        for i in 0..4 {
            if let Some(topic) = topics.get(i) {
                record.push_field(format!("{:?}", topic).as_bytes());
            } else {
                record.push_field(b"");
            }
        }

        // Add raw data field - more efficient hex encoding
        if raw_log.data.data.is_empty() {
            record.push_field(b"0x");
        } else {
            let hex_data = format!("0x{}", raw_log.data.data.encode_hex());
            record.push_field(hex_data.as_bytes());
        }

        // Add decoded values
        for input in &decoded_log.topics {
            record.push_field(input.value.as_bytes());
        }

        // Write common information every table has
        record.push_field(format!("{:?}", tx_hash).as_bytes());
        record.push_field(header_tx_info.number.to_string().as_bytes());
        record.push_field(format!("{:?}", block_hash).as_bytes());
        record.push_field(header_tx_info.timestamp.to_string().as_bytes());

        writer
            .write_byte_record(&record)
            .expect("Failed to write record to CSV");
        csv_writer.increment_record_count();
    }

    // Flush after writing all records for this batch
    writer.flush().expect("Failed to flush CSV writer");
}

/// Checks if a contract address is present in the logs bloom filter.
///
/// This function takes a contract address and a logs bloom filter and checks if the contract
/// address is present in the logs bloom filter. It uses the `FilteredParams::address_filter`
/// method to create an address filter and then checks if the filter matches the logs bloom.
///
/// # Arguments
///
/// * `contract_address` - The contract address to check.
/// * `logs_bloom` - The logs bloom filter to match against.
fn contract_in_bloom(contract_address: Address, logs_bloom: Bloom) -> bool {
    let filter_set = FilterSet::from(contract_address);
    let address_filter = FilteredParams::address_filter(&filter_set);
    FilteredParams::matches_address(logs_bloom, &address_filter)
}

/// Checks if a topic ID is present in the logs bloom filter.
///
/// This function takes a topic ID and a logs bloom filter and checks if the topic ID is present
/// in the logs bloom filter. It uses the `FilteredParams::topics_filter` method to create a
/// topics filter and then checks if the filter matches the logs bloom.
///
/// # Arguments
///
/// * `topic_id` - The topic ID to check.
/// * `logs_bloom` - The logs bloom filter to match against.
fn topic_in_bloom(topic_id: H256, logs_bloom: Bloom) -> bool {
    let filter_set = FilterSet::from(topic_id);
    let topic_filter = FilteredParams::topics_filter(&[filter_set]);
    FilteredParams::matches_topics(logs_bloom, &topic_filter)
}

/// Syncs the state from a CSV writer to the configured datasources.
///
/// # Arguments
///
/// * `name` - The name of the table/index.
/// * `csv_writer` - A mutable reference to the CSV writer.
/// * `db_writers` - A reference to the configured datasource writers.
async fn sync_state_to_db(
    name: String,
    csv_writer: &mut CsvWriter,
    db_writers: &Vec<Box<dyn DatasourceWritable>>,
) {
    let record_count = csv_writer.get_total_records();
    info!(
        "Executing sync_state_to_db for table: {:?} with {} records",
        name, record_count
    );

    // Ensure all data is flushed before syncing
    csv_writer.flush();

    // info!("Executing datasource insertion / sync...");
    for datasource in db_writers {
        datasource.write_data(name.as_str(), csv_writer).await;
    }

    //  Reset csv writer
    csv_writer.reset();
}

/// Synchronizes all states from CSV files to the configured datasources.
///
/// This function iterates over the event mappings specified in the `indexer_config`
/// and synchronizes the state data from CSV files to the configured datasources.
/// For each ABI item in the event mappings, it finds the corresponding CSV writer
/// and invokes the `sync_state_to_db` function to perform the synchronization.
///
/// # Arguments
///
/// * `indexer_config` - A reference to the `IndexerConfig` containing the configuration settings.
/// * `csv_writers` - A mutable slice of `CsvWriter` instances representing the CSV writers for each ABI item.
/// * `db_writers` - A ref to vector of DatasourceWritable objects - to interact w/ database(s)
async fn sync_all_states_to_db(
    indexer_config: &IndexerConfig,
    _reached_head: bool,
    csv_writers: &mut [CsvWriter],
    db_writers: &Vec<Box<dyn DatasourceWritable>>,
) {
    info!("Syncing all remaining CSV data to database...");
    for mapping in &indexer_config.event_mappings {
        for abi_item in &mapping.decode_abi_items {
            if let Some(csv_writer) = csv_writers.iter_mut().find(|w| w.name == abi_item.name) {
                let record_count = csv_writer.get_total_records();
                if record_count > 0 {
                    info!(
                        "  Syncing {} with {} total records",
                        abi_item.name, record_count
                    );
                    sync_state_to_db(abi_item.name.to_lowercase(), csv_writer, db_writers).await;
                } else {
                    info!("  Skipping {} (no records)", abi_item.name);
                }
            }
        }
    }
}

/// Initialize datasource writers using the extensible factory pattern.
///
/// This function uses a registry-based approach to create datasource instances,
/// making it easy to add new datasource types without modifying this function.
///
/// # Arguments
///
/// * `indexer_config` - The full configuration
pub async fn init_datasource_writers(
    indexer_config: &IndexerConfig,
) -> Vec<Box<dyn DatasourceWritable>> {
    crate::datasource_factory::init_datasource_writers(indexer_config)
        .await
        .unwrap_or_else(|e| {
            panic!("Failed to initialize datasources: {}", e);
        })
}

/// Synchronizes the indexer by processing blocks and writing the decoded logs to CSV files and
/// configured datasources.
///
/// This function performs the following steps:
///
/// 1. Initializes the datasource writers based on the provided configuration.
/// 2. Initializes the reth provider for accessing the reth database.
/// 3. Creates CSV writers for each ABI item based on the provided configuration.
/// 4. Iterates over blocks starting from the `from_block` specified in the configuration up to the
///    `to_block` if provided (or until reaching the maximum block number).
/// 5. Retrieves the block headers for each block number from the reth provider.
/// 6. Checks if the block matches any contract addresses specified in the mapping and the RPC bloom
///    filter.
/// 7. Invokes the `process_block` function to process the block and write the logs to CSV files and
///    configured datasources.
/// 8. Performs clean-up operations for any remaining CSV files.
///
/// # Arguments
///
/// * `indexer_config` - The `IndexerConfig` containing the configuration details for the indexer.
pub async fn sync(indexer_config: &IndexerConfig) {
    // info!("Starting indexer");
    info!("Starting indexer");
    info!("Initializing database writers");
    let db_writers = init_datasource_writers(indexer_config).await;

    let mut csv_writers = create_csv_writers(
        indexer_config.csv_location.as_path(),
        &indexer_config.event_mappings,
        indexer_config.include_eth_transfers,
        indexer_config.csv_sync_threshold,
    );

    let mut block_number = indexer_config.from_block;

    // Open database environment once
    let db = get_reth_db(&indexer_config.reth_db_location).expect("Failed to open database");

    let factory = get_reth_factory_with_db(&indexer_config.reth_db_location, db)
        .expect("Failed to initialize reth factory");

    // Create a provider once and reuse it
    let mut provider = factory
        .provider()
        .expect("Failed to initialize reth provider");

    // If toBlock is not specified, get the latest block number
    let to_block = if let Some(to) = indexer_config.to_block {
        to
    } else {
        // Get the latest block number from the database
        match provider.best_block_number() {
            Ok(best) => {
                info!(
                    "No toBlock specified, indexing up to current block: {}",
                    best
                );
                best
            }
            Err(e) => {
                error!("Failed to get best block number: {:?}", e);
                error!("Please specify toBlockNumber in config");
                return;
            }
        }
    };

    info!(
        "Starting indexer sync from block {} to {}",
        block_number, to_block
    );

    let start = Instant::now();

    // Process blocks from fromBlock to toBlock
    while block_number <= to_block {
        let header_result = provider.header_by_number(block_number);

        match header_result {
            Err(e) => {
                // Database errors can occur during heavy syncing
                info!("Error reading header for block {}: {:?}", block_number, e);

                // If we get a database error, we might need a new provider
                // Try to create a new provider to see fresh data
                provider = factory
                    .provider()
                    .expect("Failed to create new provider after error");

                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            Ok(header_opt) => match header_opt {
                None => {
                    // Block doesn't exist yet, we've reached the end
                    info!("Block {} not found, reached end of chain", block_number);
                    break;
                }
                Some(header_tx_info) => {
                    debug!("Checking block: {}", block_number);

                    // Debug: Print bloom filter status
                    if !header_tx_info.logs_bloom.is_zero() {
                        info!("  Block {} has non-zero bloom filter", block_number);
                    }

                    for mapping in &indexer_config.event_mappings {
                        let rpc_bloom: Bloom =
                            Bloom::from_str(&format!("{:?}", header_tx_info.logs_bloom)).unwrap();

                        if let Some(contract_addresses) = &mapping.filter_by_contract_addresses {
                            if !contract_addresses.is_empty() {
                                // check at least 1 matches bloom in mapping file
                                if !contract_addresses
                                    .iter()
                                    .any(|address| contract_in_bloom(*address, rpc_bloom))
                                {
                                    continue;
                                }
                            }
                        }

                        let topic_match = mapping.decode_abi_items.iter().any(|item| {
                            let topic = abi_item_to_topic_id(item);
                            let in_bloom = topic_in_bloom(topic, rpc_bloom);
                            if !header_tx_info.logs_bloom.is_zero() {
                                info!("    Checking topic {} in bloom: {}", topic, in_bloom);
                            }
                            in_bloom
                        });

                        if !topic_match {
                            continue;
                        }

                        info!("  Processing block {} for mapping", block_number);
                        process_block(
                            &provider,
                            &mut csv_writers,
                            &db_writers,
                            mapping,
                            rpc_bloom,
                            block_number,
                            &header_tx_info,
                            indexer_config.include_eth_transfers,
                        )
                        .await;
                    }

                    block_number += 1;
                }
            },
        }
    }

    // Sync any remaining data in CSV buffers
    sync_all_states_to_db(indexer_config, false, &mut csv_writers, &db_writers).await;

    info!("Indexer sync is now complete");

    let duration = start.elapsed();
    info!("Elapsed time: {:.2?}", duration);
}

/// Processes a block by iterating over its transactions, filtering them based on contract addresses,
/// and invoking the `process_transaction` function for each eligible transaction.
///
/// This function performs the following steps:
///
/// 1. Retrieves the block body indices from the provided `NodeDb` for the given block number. If the
///    indices are not available, indicating that the state of the `NodeDb` is not caught up with the
///    target block number, it returns early.
/// 2. Iterates over the transaction IDs within the block based on the retrieved indices.
/// 3. Retrieves the transaction and receipt for each transaction ID from the `NodeDb`.
/// 4. Filters the receipt logs based on the contract addresses specified in the mapping, if any.
/// 5. Invokes the `process_transaction` function to decode and write the logs to CSV files and
///    configured datasources.
///
/// # Arguments
///
/// * `provider` - The reth-provider instance
/// * `csv_writers` - A mutable slice of `CsvWriter` instances representing the CSV writers for each
///   ABI item.
/// * `db_writers` - A reference to list of database writers
/// * `mapping` - A reference to the `IndexerContractMapping` containing the ABI items and other mapping
///   details.
/// * `rpc_bloom` - The bloom filter associated with the block's RPC logs.
/// * `block_number` - The block number being processed.
/// * `header_tx_info` - A reference to the `Header` containing transaction-related information.
async fn process_block<P>(
    provider: &P,
    csv_writers: &mut [CsvWriter],
    db_writers: &Vec<Box<dyn DatasourceWritable>>,
    mapping: &IndexerContractMapping,
    rpc_bloom: Bloom,
    block_number: u64,
    header_tx_info: &Header,
    include_eth_transfers: bool,
) where
    P: BlockBodyIndicesProvider + TransactionsProvider + ReceiptProvider,
{
    // Compute block hash once
    let block_hash = header_tx_info.clone().seal_slow().hash();
    let block_body_indices = match provider.block_body_indices(block_number) {
        Ok(indices) => indices,
        Err(_) => {
            // This is normal during heavy database writes - just skip this block
            // It will be retried later when the database is less busy
            return;
        }
    };
    if let Some(block_body_indices) = block_body_indices {
        if block_body_indices.tx_count > 0 {
            info!(
                "  Block {} has {} transactions",
                block_number, block_body_indices.tx_count
            );
        }
        let mut tx_index = 0u64;
        for tx_id in block_body_indices.first_tx_num
            ..block_body_indices.first_tx_num + block_body_indices.tx_count
        {
            match provider.transaction_by_id(tx_id) {
                Ok(Some(transaction)) => {
                    match provider.receipt(tx_id) {
                        Ok(Some(receipt)) => {
                            let logs: Vec<Log> = if let Some(contract_addresses) =
                                &mapping.filter_by_contract_addresses
                            {
                                if contract_addresses.is_empty() {
                                    // Empty array means no filtering - include all logs
                                    receipt.logs().to_vec()
                                } else {
                                    // Filter by the specified contract addresses
                                    receipt
                                        .logs()
                                        .iter()
                                        .filter(|log| {
                                            contract_addresses
                                                .iter()
                                                .any(|address| address == &log.address)
                                        })
                                        .cloned()
                                        .collect()
                                }
                            } else {
                                // None means no filtering - include all logs
                                receipt.logs().to_vec()
                            };

                            if logs.is_empty() {
                                tx_index += 1;
                                continue;
                            }

                            info!("    Found {} logs in tx {}", logs.len(), tx_id);

                            process_transaction(
                                csv_writers,
                                db_writers,
                                mapping,
                                rpc_bloom,
                                &logs,
                                &transaction,
                                header_tx_info,
                                block_hash,
                                tx_index,
                            )
                            .await;

                            // Track ETH transfers if enabled
                            // Note: ETH transfers are NOT logs and won't be returned by eth_getLogs
                            // They're tracked separately for analytics/monitoring purposes
                            if include_eth_transfers {
                                let tx_value = transaction.value();
                                if !tx_value.is_zero() {
                                    // Write ETH transfer record to separate table
                                    if let Some(eth_writer) =
                                        csv_writers.iter_mut().find(|w| w.name == "eth_transfer")
                                    {
                                        let mut records = Vec::new();
                                        records.push(format!(
                                            "{:?}",
                                            transaction.recover_signer().unwrap_or_default()
                                        ));
                                        records.push(format!(
                                            "{:?}",
                                            transaction.to().unwrap_or_default()
                                        ));
                                        records.push(tx_value.to_string());
                                        records.push(header_tx_info.number.to_string());
                                        records.push(format!("{:?}", block_hash));
                                        records.push(header_tx_info.timestamp.to_string());
                                        eth_writer.write(records);
                                    }
                                }
                            }

                            tx_index += 1;
                        }
                        Ok(None) => {
                            // No receipt for this transaction
                            tx_index += 1;
                        }
                        Err(e) => {
                            info!(
                                "    Warning: Failed to get receipt for tx {}: {:?}",
                                tx_id, e
                            );
                            tx_index += 1;
                        }
                    }
                }
                Ok(None) => {
                    // Transaction not found
                    tx_index += 1;
                }
                Err(e) => {
                    info!("    Warning: Failed to get transaction {}: {:?}", tx_id, e);
                    tx_index += 1;
                }
            }
        }
    }
}

/// Processes a transaction by decoding logs and writing them to CSV files and configured datasources.
///
/// This function iterates over the `decode_abi_items` of the provided `IndexerContractMapping` and
/// performs the following steps:
///
/// 1. Checks if the topic ID of the ABI item is present in the provided RPC bloom filter. If not,
///    it skips the ABI item and proceeds to the next one.
/// 2. Searches for a corresponding CSV writer based on the ABI item's name. If found, it proceeds
///    with decoding the logs and writing them to the CSV file.
/// 3. After writing a certain number of logs, determined by the `sync_back_every_n_log` value in
///    the mapping, it syncs the CSV file to the configured datasources.
///
/// # Arguments
///
/// * `csv_writers` - A mutable slice of `CsvWriter` instances representing the CSV writers for each
///   ABI item.
/// * `db_writers` - A reference to vector of database writers, whatever sources are in config
/// * `mapping` - A reference to the `IndexerContractMapping` containing the ABI items and other mapping
///   details.
/// * `rpc_bloom` - The bloom filter associated with the transaction's RPC logs.
/// * `logs` - A slice of `Log` instances representing the logs associated with the transaction.
/// * `transaction` - The `TransactionSigned` instance representing the transaction being processed.
/// * `header_tx_info` - A reference to the `Header` containing transaction-related information.
/// * `tx_index` - The transaction index in the block.
async fn process_transaction<T>(
    csv_writers: &mut [CsvWriter],
    db_writers: &Vec<Box<dyn DatasourceWritable>>,
    mapping: &IndexerContractMapping,
    rpc_bloom: Bloom,
    logs: &[Log],
    transaction: &T,
    header_tx_info: &Header,
    block_hash: H256,
    tx_index: u64,
) where
    T: SignedTransaction,
{
    for abi_item in &mapping.decode_abi_items {
        let topic_id = abi_item_to_topic_id(abi_item);

        if !topic_in_bloom(topic_id, rpc_bloom) {
            continue;
        }

        if let Some(csv_writer) = csv_writers.iter_mut().find(|w| w.name == abi_item.name) {
            info!(
                "    Processing {} logs for event {}",
                logs.len(),
                abi_item.name
            );
            let decoded_logs = decode_logs(topic_id, logs, abi_item);
            if !decoded_logs.is_empty() {
                info!(
                    "    Found {} decoded logs for {}",
                    decoded_logs.len(),
                    abi_item.name
                );
                write_csv_state_record(
                    csv_writer,
                    &decoded_logs,
                    header_tx_info,
                    *transaction.tx_hash(),
                    block_hash,
                    tx_index,
                );

                // Check if we should sync based on record count
                if csv_writer.should_sync() {
                    info!(
                        "CSV sync threshold reached for {} (records: {})",
                        abi_item.name,
                        csv_writer.get_total_records()
                    );
                    sync_state_to_db(abi_item.name.to_lowercase(), csv_writer, db_writers).await;
                }
            } else {
                info!(
                    "    No decoded logs for {} (topic: {:?})",
                    abi_item.name, topic_id
                );
            }
        } else {
            info!("    No CSV writer found for event {}", abi_item.name);
        }
    }
}
