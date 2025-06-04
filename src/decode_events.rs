use alloy_sol_types::{sol_data, SolType};
use regex::RegexBuilder;
use alloy_primitives::{hex::{self, ToHexExt}, keccak256, Address, B256 as H256};
use reth_primitives::Log;

use crate::types::{ABIInput, ABIItem};

use once_cell::sync::Lazy;
use std::sync::Mutex;
use std::collections::HashMap;

// Cache for event signatures to avoid recomputation
static TOPIC_ID_CACHE: Lazy<Mutex<HashMap<String, H256>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});

/// Converts an ABI item to a topic ID by calculating the Keccak-256 hash
/// of the item's name concatenated with its input types.
///
/// # Arguments
///
/// * `item` - The ABI item to convert.
///
/// # Returns
///
/// The topic ID as a `reth_primitives::H256` hash.
pub fn abi_item_to_topic_id(item: &ABIItem) -> H256 {
    // Create a cache key from the ABI item
    let input_types: Vec<String> = item
        .inputs
        .iter()
        .map(|input| input.type_.clone())
        .collect();
    let signature = format!("{}({})", item.name, input_types.join(","));

    // Check cache first
    {
        let cache = TOPIC_ID_CACHE.lock().unwrap();
        if let Some(&topic_id) = cache.get(&signature) {
            return topic_id;
        }
    }

    // Compute and cache
    let topic_id = keccak256(&signature);
    {
        let mut cache = TOPIC_ID_CACHE.lock().unwrap();
        cache.insert(signature, topic_id);
    }

    topic_id
}

#[derive(Debug)]
pub struct DecodedTopic {
    pub name: String,
    pub value: String,
}

/// Represents a decoded structure with a name and a corresponding value.
#[derive(Debug)]
pub struct DecodedLog {
    pub address: Address,
    pub topics: Vec<DecodedTopic>,
}

/// Decodes logs that match the specified topic ID using the given ABI.
///
/// # Arguments
///
/// * `topic_id` - The topic ID to match against the logs' first topic.
/// * `logs` - The logs to decode.
/// * `abi` - The ABI item used for decoding.
///
/// # Returns
///
/// A vector of tuples containing decoded logs and their raw log data.
///
/// # Examples
///
/// ```
/// use your_crate::{H256, Log, ABIItem, DecodedLog};
///
/// let topic_id = H256::from_low_u64_be(123);
/// let logs: Vec<Log> = vec![/* logs here */];
/// let abi: ABIItem = /* ABI item here */;
///
/// let decoded_logs = decode_logs(topic_id, &logs, &abi);
/// ```
pub fn decode_logs(topic_id: H256, logs: &[Log], abi: &ABIItem) -> Vec<(DecodedLog, Log)> {
    logs.iter()
        .filter_map(|log| {
            if log.topics().get(0) == Some(&topic_id) {
                decode_log(log, abi).ok().map(|decoded| (decoded, log.clone()))
            } else {
                None
            }
        })
        .collect()
}

/// Decodes the value of a topic using the provided ABI input definition.
///
/// # Arguments
///
/// * `topic` - The topic value to decode as a byte slice.
/// * `abi` - The ABI input definition for the topic.
///
/// # Panics
///
/// This function will panic if the ABI input type is unknown.
///
/// # Returns
///
/// The decoded value as a string.
fn decode_topic_value(topic: &[u8], abi: &ABIInput) -> String {
    // Generic helper to decode and convert to string
    macro_rules! decode_and_stringify {
        ($type:ty) => {
            <$type>::abi_decode(topic, false)
                .map(|v| v.to_string())
                .unwrap_or_else(|_| format!("0x{}", hex::encode(topic)))
        };
    }

    match abi.type_.as_str() {
        "address" => sol_data::Address::abi_decode(topic, false)
            .map(|addr| addr.to_checksum(None))
            .unwrap_or_else(|_| format!("0x{}", hex::encode(topic))),
        "bool" => decode_and_stringify!(sol_data::Bool),
        "bytes" => sol_data::Bytes::abi_decode(topic, false)
            .map(|b| b.encode_hex())
            .unwrap_or_else(|_| hex::encode(topic)),
        "string" => sol_data::String::abi_decode(topic, false)
            .unwrap_or_else(|_| format!("0x{}", hex::encode(topic))),
        // Unsigned integers
        "uint8" => decode_and_stringify!(sol_data::Uint::<8>),
        "uint16" => decode_and_stringify!(sol_data::Uint::<16>),
        "uint32" => decode_and_stringify!(sol_data::Uint::<32>),
        "uint64" => decode_and_stringify!(sol_data::Uint::<64>),
        "uint128" => decode_and_stringify!(sol_data::Uint::<128>),
        "uint256" => decode_and_stringify!(sol_data::Uint::<256>),
        // Signed integers
        "int8" => decode_and_stringify!(sol_data::Int::<8>),
        "int16" => decode_and_stringify!(sol_data::Int::<16>),
        "int32" => decode_and_stringify!(sol_data::Int::<32>),
        "int64" => decode_and_stringify!(sol_data::Int::<64>),
        "int128" => decode_and_stringify!(sol_data::Int::<128>),
        "int256" => decode_and_stringify!(sol_data::Int::<256>),
        // Handle bytes32 as a common type
        "bytes32" => {
            if topic.len() == 32 {
                format!("0x{}", hex::encode(topic))
            } else {
                panic!("Invalid bytes32 length: {}", topic.len())
            }
        }
        _ => panic!("Unknown type: {}", abi.type_),
    }
}

use std::sync::RwLock;

// Cache compiled regexes to avoid recompilation
static REGEX_CACHE: Lazy<RwLock<HashMap<String, regex::Regex>>> = Lazy::new(|| {
    RwLock::new(HashMap::new())
});

/// Matches a value against a regular expression pattern, ignoring case.
///
/// # Arguments
///
/// * `abi_name` - The name of the ABI item.
/// * `regex` - The regular expression pattern to match against.
/// * `value` - The value to match against the regular expression.
///
/// # Returns
///
/// Returns `true` if the value matches the regular expression pattern, ignoring case.
/// Otherwise, returns `false`.
fn regex_match(abi_name: &str, regex: &str, value: &str) -> Result<bool, String> {
    // Try to get from cache first
    {
        let cache = REGEX_CACHE.read().unwrap();
        if let Some(compiled_regex) = cache.get(regex) {
            return Ok(compiled_regex.is_match(value));
        }
    }

    // Not in cache, compile and store
    let compiled_regex = RegexBuilder::new(regex)
        .case_insensitive(true)
        .build()
        .map_err(|e| format!("Invalid regex for {}: {}", abi_name, e))?;

    let is_match = compiled_regex.is_match(value);

    // Store in cache
    {
        let mut cache = REGEX_CACHE.write().unwrap();
        cache.insert(regex.to_string(), compiled_regex);
    }

    Ok(is_match)
}

/// Decodes a log using the provided ABI and returns the decoded log.
///
/// This function decodes the indexed topics and non-indexed data of the log using the given ABI
/// definition. The decoded log is sorted based on the order of inputs in the ABI.
///
/// # Arguments
///
/// * `log` - The log to decode.
/// * `abi` - The ABI definition used for decoding.
///
/// # Returns
///
/// A `Result` containing the decoded log if decoding is successful, or an empty `()` if decoding fails.
///
/// # Examples
///
/// ```
/// use my_abi_lib::{Log, ABIItem, DecodedLog};
///
/// let log = Log { /* log data */ };
/// let abi = ABIItem { /* ABI definition */ };
///
/// match decode_log(&log, &abi) {
///     Ok(decoded_log) => {
///         // Handle the decoded log
///         println!("Decoded log: {:?}", decoded_log);
///     }
///     Err(_) => {
///         // Handle decoding error
///         println!("Failed to decode log");
///     }
/// }
/// ```
fn decode_log(log: &Log, abi: &ABIItem) -> Result<DecodedLog, ()> {
    let decoded_indexed_topics = decode_log_topics(log, abi)?;
    let decoded_non_indexed_data = decode_log_data(log, abi)?;

    let mut topics: Vec<DecodedTopic> = decoded_indexed_topics
        .into_iter()
        .chain(decoded_non_indexed_data)
        .collect();

    topics.sort_by_key(|item| abi.inputs.iter().position(|input| input.name == item.name));

    Ok(DecodedLog {
        address: log.address,
        topics,
    })
}

/// Decodes a topic value based on the provided ABI input definition.
///
/// This function decodes the raw bytes of a topic into a human-readable value
/// based on the data type specified in the ABI input. It also performs a regex
/// match against the decoded value if a regex pattern is specified in the ABI input.
///
/// # Arguments
///
/// * `topic` - The raw bytes of the topic.
/// * `abi_input` - The ABI input definition.
///
/// # Returns
///
/// * `Result<DecodedTopic, ()>` - The decoded topic value wrapped in a `Result`.
///   - `Ok(DecodedTopic)` if the decoding and regex match are successful.
///   - `Err(())` if there is an error during decoding or the regex match fails.
///
fn decode_topic_log(topic: &[u8], abi_input: &ABIInput) -> Result<DecodedTopic, ()> {
    let value = decode_topic_value(topic, abi_input);

    // check regex and bail out if it doesn't match
    if let Some(regex) = &abi_input.regex {
        match regex_match(&abi_input.name, regex, &value) {
            Ok(true) => {},
            Ok(false) | Err(_) => return Err(()),
        }
    }

    Ok(DecodedTopic {
        name: abi_input.name.clone(),
        value,
    })
}

/// Decodes the indexed topics of a log based on the provided ABI item.
///
/// This function decodes the indexed topics of a log event based on the ABI item's
/// input definitions. It returns a vector of decoded topics, where each topic
/// consists of the name and value. The decoding is performed by calling the
/// `decode_topic_log` function for each topic.
///
/// # Arguments
///
/// * `log` - The log containing the indexed topics.
/// * `abi` - The ABI item definition.
///
/// # Returns
///
/// * `Result<Vec<DecodedTopic>, ()>` - The vector of decoded topics wrapped in a `Result`.
///   - `Ok(Vec<DecodedTopic>)` if the decoding is successful.
///   - `Err(())` if there is an error during decoding.
fn decode_log_topics(log: &Log, abi: &ABIItem) -> Result<Vec<DecodedTopic>, ()> {
    let indexed_inputs: Vec<&ABIInput> = abi
        .inputs
        .iter()
        .filter(|input| input.indexed)
        .collect::<Vec<_>>();

    if indexed_inputs.len() != log.topics().len() - 1 {
        // -1 because the first topic is the event signature
        return Err(());
    }

    let mut results: Vec<DecodedTopic> = Vec::<DecodedTopic>::new();

    for (i, topic) in log.topics().iter().enumerate().skip(1) {
        let abi_input = indexed_inputs[i - 1];
        results.push(decode_topic_log(&topic.0, abi_input)?);
    }

    Ok(results)
}

/// Decodes the non-indexed data of a log based on the provided ABI item.
///
/// This function decodes the non-indexed data of a log event based on the ABI item's
/// input definitions. It returns a vector of decoded topics, where each topic
/// consists of the name and value. The decoding is performed by calling the
/// `decode_topic_log` function for each data chunk.
///
/// # Arguments
///
/// * `log` - The log containing the non-indexed data.
/// * `abi` - The ABI item definition.
///
/// # Returns
///
/// * `Result<Vec<DecodedTopic>, ()>` - The vector of decoded topics wrapped in a `Result`.
///   - `Ok(Vec<DecodedTopic>)` if the decoding is successful.
///   - `Err(())` if there is an error during decoding.
///
fn decode_log_data(log: &Log, abi: &ABIItem) -> Result<Vec<DecodedTopic>, ()> {
    let non_indexed_inputs: Vec<&ABIInput> = abi
        .inputs
        .iter()
        .filter(|input| !input.indexed)
        .collect::<Vec<_>>();

    let topics = log.data.data.chunks_exact(32);

    if non_indexed_inputs.len() != topics.len() {
        return Err(());
    }

    let mut results = Vec::<DecodedTopic>::new();

    for (i, topic) in topics.enumerate() {
        let abi_input = non_indexed_inputs[i];
        results.push(decode_topic_log(topic, abi_input)?);
    }

    Ok(results)
}
