use std::{
    str::FromStr,
    time::{Duration, Instant},
};

use log::info;
use alloy_primitives::{Address, Bloom, B256 as H256, Sealable, hex::ToHexExt};
use reth_primitives::{Header, Log};
use reth_provider::{
    BlockReader, HeaderProvider, ReceiptProvider, TransactionsProvider,
};
use alloy_rpc_types::{FilteredParams, FilterSet};
use alloy_consensus::{TxReceipt, Transaction};
use reth_primitives_traits::{SignedTransaction, SignerRecoverable};
// use uuid::Uuid; // Removed - using uuid directly

use crate::{
    csv::{create_csv_writers, CsvWriter}, // Re-enabled for testing
    datasource::DatasourceWritable,
    decode_events::{abi_item_to_topic_id, decode_logs, DecodedLog},
    provider::get_reth_factory,
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
    for (log_index, (decoded_log, raw_log)) in decoded_logs.iter().enumerate() {
        // Pre-allocate with estimated capacity to avoid reallocations
        let mut records = Vec::with_capacity(12 + decoded_log.topics.len());

        records.push(uuid::Uuid::new_v4().to_string());
        records.push(format!("{:?}", decoded_log.address));

        // Add raw log fields for eth_getLogs compatibility
        records.push(log_index.to_string());
        records.push(tx_index.to_string());

        // Add topics (topic0 is always present, others may be empty)
        let topics = raw_log.topics();
        // Use a loop to reduce code duplication
        for i in 0..4 {
            records.push(topics.get(i).map(|t| format!("{:?}", t)).unwrap_or_default());
        }

        // Add raw data field - more efficient hex encoding
        let mut data_hex = String::with_capacity(2 + raw_log.data.data.len() * 2);
        data_hex.push_str("0x");
        data_hex.push_str(&raw_log.data.data.encode_hex());
        records.push(data_hex);

        // Add decoded values - avoid intermediate vector
        records.extend(decoded_log.topics.iter().map(|input| input.value.clone()));

        // write common information every table has
        records.push(format!("{:?}", tx_hash));
        records.push(header_tx_info.number.to_string());
        records.push(format!("{:?}", block_hash));
        records.push(header_tx_info.timestamp.to_string());

        csv_writer.write(records);
    }
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
    println!("Executing sync_state_to_db for table: {:?}", name);

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
    for mapping in &indexer_config.event_mappings {
        for abi_item in &mapping.decode_abi_items {
            if let Some(csv_writer) = csv_writers.iter_mut().find(|w| w.name == abi_item.name) {
                sync_state_to_db(abi_item.name.to_lowercase(), csv_writer, db_writers).await;
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
    println!("Starting indexer");
    println!("Initializing database writers");
    let db_writers = init_datasource_writers(indexer_config).await;

    let mut csv_writers = create_csv_writers(
        indexer_config.csv_location.as_path(),
        &indexer_config.event_mappings,
        indexer_config.include_eth_transfers,
    );

    let mut block_number = indexer_config.from_block;
    let to_block = indexer_config.to_block.unwrap_or(u64::MAX);

    let factory = get_reth_factory(&indexer_config.reth_db_location)
        .expect("Failed to initialize reth factory");
    let mut provider = factory
        .provider()
        .expect("Failed to initialize reth provider");

    println!("Starting indexer sync...");

    let start = Instant::now();

    let mut reached_head = false;

    // unlimited loop to handle all cases
    loop {
        match provider.header_by_number(block_number).unwrap() {
            None => {
                // means it should stay alive when at head
                if to_block == u64::MAX {
                    if !reached_head {
                        sync_all_states_to_db(
                            indexer_config,
                            reached_head,
                            &mut csv_writers,
                            &db_writers,
                        )
                        .await;
                        println!("Synced all data, waiting for new blocks to index as they come in.");
                        let duration = start.elapsed();
                        println!("Elapsed time: {:.2?}", duration);
                        reached_head = true;
                    }

                    // as the block not been seen and its +1 on it we should make sure
                    // we do not skip a block
                    let last_seen_block = block_number - 1;

                    loop {
                        // If the db changes we need a new read tx otherwise it will see the old version - that's how MVCC works
                        match factory.provider() {
                            Ok(new_provider) => {
                                provider = new_provider;

                                // Get latest block by binary search for efficiency
                                let mut latest_block_number = last_seen_block;
                                let mut step = 1000u64; // Start with larger steps

                                // First, find rough upper bound
                                while provider.header_by_number(latest_block_number + step).unwrap().is_some() {
                                    latest_block_number += step;
                                    step = step.saturating_mul(2); // Exponential growth
                                }

                                // Then binary search for exact block
                                let mut low = latest_block_number;
                                let mut high = latest_block_number + step;
                                while low < high {
                                    let mid = low + (high - low) / 2;
                                    if provider.header_by_number(mid + 1).unwrap().is_some() {
                                        low = mid + 1;
                                    } else {
                                        high = mid;
                                    }
                                }
                                latest_block_number = low;

                                info!("latest block number: {}", latest_block_number);
                                info!("last seen block number: {}", last_seen_block);

                                if latest_block_number > last_seen_block {
                                    // block_number already set so break out
                                    println!("new block(s) found check from: {}... last seen: {}... latest block: {}", block_number, last_seen_block, latest_block_number);
                                    break;
                                }
                            }
                            Err(e) => {
                                println!("Failed to get new provider (database might be locked): {:?}. Retrying in 0.5 seconds...", e);
                                tokio::time::sleep(Duration::from_millis(500)).await;
                                continue;
                            }
                        }

                        // Use async sleep for better resource usage
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                } else {
                    sync_all_states_to_db(
                        indexer_config,
                        reached_head,
                        &mut csv_writers,
                        &db_writers,
                    )
                    .await;
                    break;
                }
            }
            Some(header_tx_info) => {
                println!("Checking block: {}", block_number);
                info!("checking block: {}", block_number);

                for mapping in &indexer_config.event_mappings {
                    let rpc_bloom: Bloom =
                        Bloom::from_str(&format!("{:?}", header_tx_info.logs_bloom)).unwrap();

                    // Debug: check if bloom filter has any data
                    if !header_tx_info.logs_bloom.is_zero() {
                        println!("  Block {} has non-zero bloom filter", block_number);
                    }

                    if let Some(contract_address) = &mapping.filter_by_contract_addresses {
                        // check at least 1 matches bloom in mapping file
                        if !contract_address
                            .iter()
                            .any(|address| contract_in_bloom(*address, rpc_bloom))
                        {
                            continue;
                        }
                    }

                    let topic_match = mapping
                        .decode_abi_items
                        .iter()
                        .any(|item| {
                            let topic = abi_item_to_topic_id(item);
                            let in_bloom = topic_in_bloom(topic, rpc_bloom);
                            if !header_tx_info.logs_bloom.is_zero() {
                                println!("    Checking topic {} in bloom: {}", topic, in_bloom);
                            }
                            in_bloom
                        });

                    if !topic_match {
                        continue;
                    }

                    println!("  Processing block {} for mapping", block_number);
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
                // if we have reached the head, we want to keep going
                // if we are higher then the defined block number we write and exist
                if block_number == to_block || reached_head {
                    sync_all_states_to_db(
                        indexer_config,
                        reached_head,
                        &mut csv_writers,
                        &db_writers,
                    )
                    .await;

                    // only exit if we have reached the head as it should continue to run and wait for new blocks
                    if !reached_head {
                        break;
                    }
                }
            }
        }
    }

    println!("Indexer sync is now complete");

    let duration = start.elapsed();
    println!("Elapsed time: {:.2?}", duration);
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
async fn process_block<T: ReceiptProvider + TransactionsProvider + HeaderProvider + BlockReader>(
    provider: T,
    csv_writers: &mut [CsvWriter],
    db_writers: &Vec<Box<dyn DatasourceWritable>>,
    mapping: &IndexerContractMapping,
    rpc_bloom: Bloom,
    block_number: u64,
    header_tx_info: &Header,
    include_eth_transfers: bool,
) {
    let block_body_indices = provider.block_body_indices(block_number).unwrap();
    if let Some(block_body_indices) = block_body_indices {
        if block_body_indices.tx_count > 0 {
            println!("  Block {} has {} transactions", block_number, block_body_indices.tx_count);
        }
        let mut tx_index = 0u64;
        for tx_id in block_body_indices.first_tx_num
            ..block_body_indices.first_tx_num + block_body_indices.tx_count
        {
            if let Some(transaction) = provider.transaction_by_id(tx_id).unwrap() {
                if let Some(receipt) = provider.receipt(tx_id).unwrap() {
                    let logs: Vec<Log> =
                        if let Some(contract_address) = &mapping.filter_by_contract_addresses {
                            receipt
                                .logs()
                                .iter()
                                .filter(|log| {
                                    contract_address
                                        .iter()
                                        .any(|address| address == &log.address)
                                })
                                .cloned()
                                .collect()
                        } else {
                            receipt.logs().to_vec()
                        };

                    if logs.is_empty() {
                        tx_index += 1;
                        continue;
                    }

                    println!("    Found {} logs in tx {}", logs.len(), tx_id);

                    process_transaction(
                        csv_writers,
                        db_writers,
                        mapping,
                        rpc_bloom,
                        &logs,
                        transaction.clone(),
                        header_tx_info,
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
                            if let Some(eth_writer) = csv_writers.iter_mut().find(|w| w.name == "eth_transfer") {
                                let mut records = Vec::new();
                                records.push(format!("{:?}", transaction.recover_signer().unwrap_or_default()));
                                records.push(format!("{:?}", transaction.to().unwrap_or_default()));
                                records.push(tx_value.to_string());
                                records.push(header_tx_info.number.to_string());
                                records.push(format!("{:?}", header_tx_info.clone().seal_slow().hash()));
                                records.push(header_tx_info.timestamp.to_string());
                                eth_writer.write(records);
                            }
                        }
                    }

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
    transaction: T,
    header_tx_info: &Header,
    tx_index: u64,
)
where
    T: SignedTransaction,
{
    for abi_item in &mapping.decode_abi_items {
        let topic_id = abi_item_to_topic_id(abi_item);

        if !topic_in_bloom(topic_id, rpc_bloom) {
            continue;
        }

        if let Some(csv_writer) = csv_writers.iter_mut().find(|w| w.name == abi_item.name) {
            let decoded_logs = decode_logs(topic_id, logs, abi_item);
            if !decoded_logs.is_empty() {
                write_csv_state_record(
                    csv_writer,
                    &decoded_logs,
                    header_tx_info,
                    *transaction.tx_hash(),
                    header_tx_info.clone().seal_slow().hash(),
                    tx_index,
                );

                // Calculate file size and sync back if necessary
                let kb_file_size = csv_writer.get_kb_file_size();
                let sync_back_threshold = (0.3333 * mapping.sync_back_every_n_log as f64) as u64;
                if kb_file_size >= sync_back_threshold {
                    sync_state_to_db(abi_item.name.to_lowercase(), csv_writer, db_writers).await;
                }
            }
        }
    }
}
