use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::post,
    Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{sync::Arc, str::FromStr};
use alloy_primitives::{Address, B256};
use alloy_rpc_types::{BlockNumberOrTag, FilterBlockOption, Log as RpcLog, Filter};

use crate::quickwit::{QuickwitClient, QuickwitSearchRequest};

#[derive(Clone)]
pub struct RpcState {
    quickwit_client: Arc<QuickwitClient>,
    #[allow(dead_code)]
    index_prefix: String,
}

impl RpcState {
    pub fn new(quickwit_client: QuickwitClient, index_prefix: String) -> Self {
        Self {
            quickwit_client: Arc::new(quickwit_client),
            index_prefix,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    pub params: Value,
    pub id: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub result: Option<Value>,
    pub error: Option<JsonRpcError>,
    pub id: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    pub data: Option<Value>,
}

impl JsonRpcResponse {
    fn success(id: Option<Value>, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: Some(result),
            error: None,
            id,
        }
    }

    fn error(id: Option<Value>, code: i64, message: String) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code,
                message,
                data: None,
            }),
            id,
        }
    }
}

pub fn create_rpc_router(state: RpcState) -> Router {
    Router::new()
        .route("/", post(handle_rpc))
        .with_state(state)
}

async fn handle_rpc(
    State(state): State<RpcState>,
    Json(request): Json<JsonRpcRequest>,
) -> Result<Json<JsonRpcResponse>, StatusCode> {
    match request.method.as_str() {
        "eth_getLogs" => handle_get_logs(state, request).await,
        "eth_getBlockByNumber" => handle_get_block_by_number(state, request).await,
        "eth_getTransactionReceipt" => handle_get_transaction_receipt(state, request).await,
        _ => Ok(Json(JsonRpcResponse::error(
            request.id,
            -32601,
            "Method not found".to_string(),
        ))),
    }
}

async fn handle_get_logs(
    state: RpcState,
    request: JsonRpcRequest,
) -> Result<Json<JsonRpcResponse>, StatusCode> {
    // Parse the filter from params
    let params_array = request.params.as_array()
        .ok_or_else(|| StatusCode::BAD_REQUEST)?;
    let filter: Filter = match serde_json::from_value(params_array.get(0).cloned().unwrap_or_default()) {
        Ok(f) => f,
        Err(e) => {
            return Ok(Json(JsonRpcResponse::error(
                request.id,
                -32602,
                format!("Invalid params: {}", e),
            )))
        }
    };

    // Build Quickwit query
    let query_json = build_quickwit_query(&filter);

    // Create search request
    let search_request = QuickwitSearchRequest {
        query: query_json["query"].as_str().unwrap_or("*").to_string(),
        start_timestamp: query_json["start_timestamp"].as_i64().unwrap_or(0),
        end_timestamp: query_json["end_timestamp"].as_i64().unwrap_or(i64::MAX),
        max_hits: query_json["max_hits"].as_u64().unwrap_or(1000) as usize,
    };

    // Search across all event types
    let search_result = match state.quickwit_client.search_all_logs(search_request).await {
        Ok(result) => result,
        Err(e) => {
            return Ok(Json(JsonRpcResponse::error(
                request.id,
                -32603,
                format!("Internal error: {}", e),
            )))
        }
    };

    // Transform Quickwit results to RPC logs
    let mut logs: Vec<RpcLog> = vec![];
    for hit in search_result.hits {
        if let Some(log) = quickwit_hit_to_rpc_log(hit) {
            logs.push(log);
        }
    }

    Ok(Json(JsonRpcResponse::success(
        request.id,
        json!(logs),
    )))
}

/// Convert a Quickwit search hit to an RPC log
fn quickwit_hit_to_rpc_log(hit: Value) -> Option<RpcLog> {
    // Extract fields from Quickwit document
    let address = Address::from_str(hit.get("contract_address")?.as_str()?).ok()?;
    let block_number = hit.get("block_number")?.as_u64()?;
    let tx_hash = B256::from_str(hit.get("tx_hash")?.as_str()?).ok()?;
    let tx_index = hit.get("transaction_index")?.as_u64()?;
    let block_hash = B256::from_str(hit.get("block_hash")?.as_str()?).ok()?;
    let log_index = hit.get("log_index")?.as_u64()?;

    // Extract topics (handling potential null/empty values)
    let mut topics = vec![];
    for i in 0..4 {
        let topic_field = format!("topic{}", i);
        if let Some(topic_str) = hit.get(&topic_field).and_then(|v| v.as_str()) {
            if !topic_str.is_empty() {
                if let Ok(topic) = B256::from_str(topic_str) {
                    topics.push(topic);
                }
            }
        }
    }

    // Extract data
    let data_str = hit.get("data")?.as_str()?;
    let data = alloy_primitives::Bytes::from_str(data_str).ok()?;

    // Create the inner log
    let inner = alloy_primitives::Log {
        address,
        data: alloy_primitives::LogData::new_unchecked(topics, data),
    };

    Some(RpcLog {
        inner,
        block_hash: Some(block_hash),
        block_number: Some(block_number),
        block_timestamp: None,
        transaction_hash: Some(tx_hash),
        transaction_index: Some(tx_index),
        log_index: Some(log_index),
        removed: false,
    })
}

async fn handle_get_block_by_number(
    state: RpcState,
    request: JsonRpcRequest,
) -> Result<Json<JsonRpcResponse>, StatusCode> {
    // Parse params: [blockNumberOrTag, includeTransactions]
    let params_array = request.params.as_array()
        .ok_or_else(|| StatusCode::BAD_REQUEST)?;

    let block_number_or_tag: BlockNumberOrTag = match params_array.get(0) {
        Some(v) => serde_json::from_value(v.clone()).map_err(|_| StatusCode::BAD_REQUEST)?,
        None => BlockNumberOrTag::Latest,
    };

    let include_transactions = params_array.get(1)
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Convert to block number
    let block_number = match block_number_or_tag {
        BlockNumberOrTag::Number(n) => n,
        BlockNumberOrTag::Latest => {
            // Query Quickwit for the highest block number
            let search_req = QuickwitSearchRequest {
                query: "*".to_string(),
                start_timestamp: 0,
                end_timestamp: i64::MAX,
                max_hits: 1,
            };

            match state.quickwit_client.search_all_logs(search_req).await {
                Ok(result) => {
                    result.hits.get(0)
                        .and_then(|hit| hit.get("block_number"))
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0)
                }
                Err(_) => 0,
            }
        }
        BlockNumberOrTag::Earliest => 0,
        _ => {
            return Ok(Json(JsonRpcResponse::error(
                request.id,
                -32602,
                "Unsupported block tag".to_string(),
            )))
        }
    };

    // Query logs for this block
    let search_req = QuickwitSearchRequest {
        query: format!("block_number:{}", block_number),
        start_timestamp: 0,
        end_timestamp: i64::MAX,
        max_hits: 10000, // Get all logs for the block
    };

    let logs = match state.quickwit_client.search_all_logs(search_req).await {
        Ok(result) => result.hits,
        Err(e) => {
            return Ok(Json(JsonRpcResponse::error(
                request.id,
                -32603,
                format!("Internal error: {}", e),
            )))
        }
    };

    if logs.is_empty() {
        return Ok(Json(JsonRpcResponse::success(request.id, json!(null))));
    }

    // Extract block info from first log
    let first_log = &logs[0];
    let block_hash = first_log.get("block_hash")
        .and_then(|v| v.as_str())
        .unwrap_or("0x0000000000000000000000000000000000000000000000000000000000000000");
    let timestamp = first_log.get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.timestamp() as u64)
        .unwrap_or(0);

    // Extract unique transaction hashes for the block
    let mut tx_hashes: Vec<String> = logs.iter()
        .filter_map(|log| log.get("tx_hash").and_then(|v| v.as_str()))
        .map(|s| s.to_string())
        .collect();
    tx_hashes.sort();
    tx_hashes.dedup();

    // Build complete ETH RPC block response
    let block = json!({
        "number": format!("0x{:x}", block_number),
        "hash": block_hash,
        "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000", // Would need to store this
        "nonce": "0x0000000000000000",
        "sha3Uncles": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
        "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
        "transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
        "stateRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "receiptsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
        "miner": "0x0000000000000000000000000000000000000000",
        "difficulty": "0x0",
        "totalDifficulty": "0x0",
        "extraData": "0x",
        "size": "0x0",
        "gasLimit": "0x1c9c380",
        "gasUsed": "0x0",
        "timestamp": format!("0x{:x}", timestamp),
        "transactions": if include_transactions {
            // For full transactions, we'd need to store more tx data
            // For now, just return hashes
            json!(tx_hashes)
        } else {
            json!(tx_hashes) // Always return tx hashes array
        },
        "uncles": [],
        "baseFeePerGas": "0x0",
        "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    });

    Ok(Json(JsonRpcResponse::success(request.id, block)))
}

async fn handle_get_transaction_receipt(
    state: RpcState,
    request: JsonRpcRequest,
) -> Result<Json<JsonRpcResponse>, StatusCode> {
    // Parse params: [transactionHash]
    let params_array = request.params.as_array()
        .ok_or_else(|| StatusCode::BAD_REQUEST)?;

    let tx_hash = match params_array.get(0).and_then(|v| v.as_str()) {
        Some(hash) => hash,
        None => {
            return Ok(Json(JsonRpcResponse::error(
                request.id,
                -32602,
                "Missing transaction hash".to_string(),
            )))
        }
    };

    // Query logs for this transaction
    let search_req = QuickwitSearchRequest {
        query: format!("tx_hash:{}", tx_hash),
        start_timestamp: 0,
        end_timestamp: i64::MAX,
        max_hits: 1000, // Get all logs for the transaction
    };

    let logs = match state.quickwit_client.search_all_logs(search_req).await {
        Ok(result) => result.hits,
        Err(e) => {
            return Ok(Json(JsonRpcResponse::error(
                request.id,
                -32603,
                format!("Internal error: {}", e),
            )))
        }
    };

    if logs.is_empty() {
        return Ok(Json(JsonRpcResponse::success(request.id, json!(null))));
    }

    // Extract receipt info from first log
    let first_log = &logs[0];
    let block_number = first_log.get("block_number")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let block_hash = first_log.get("block_hash")
        .and_then(|v| v.as_str())
        .unwrap_or("0x0000000000000000000000000000000000000000000000000000000000000000");
    let transaction_index = first_log.get("transaction_index")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    // Convert logs to RPC format
    let rpc_logs: Vec<Value> = logs.iter()
        .filter_map(|hit| quickwit_hit_to_rpc_log(hit.clone()))
        .map(|log| serde_json::to_value(log).unwrap_or(json!({})))
        .collect();

    // Build transaction receipt
    let receipt = json!({
        "transactionHash": tx_hash,
        "transactionIndex": format!("0x{:x}", transaction_index),
        "blockHash": block_hash,
        "blockNumber": format!("0x{:x}", block_number),
        "from": "0x0000000000000000000000000000000000000000", // Would need to store this
        "to": first_log.get("contract_address").and_then(|v| v.as_str()).unwrap_or("0x0000000000000000000000000000000000000000"),
        "cumulativeGasUsed": "0x0",
        "gasUsed": "0x0",
        "contractAddress": null,
        "logs": rpc_logs,
        "logsBloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
        "status": "0x1", // Assume success since we have logs
        "effectiveGasPrice": "0x0",
        "type": "0x0"
    });

    Ok(Json(JsonRpcResponse::success(request.id, receipt)))
}

/// Build a Quickwit query from an Ethereum filter
fn build_quickwit_query(filter: &Filter) -> Value {
    let mut query_parts = vec![];

    // Block range filter
    match &filter.block_option {
        FilterBlockOption::Range { from_block, to_block } => {
            if let Some(from) = from_block {
                query_parts.push(format!("block_number:[{} TO *]", block_number_to_u64(from)));
            }
            if let Some(to) = to_block {
                query_parts.push(format!("block_number:[* TO {}]", block_number_to_u64(to)));
            }
        }
        FilterBlockOption::AtBlockHash(_) => {
            // Block hash filtering not supported in this simple implementation
        }
    }

    // Address filter - filter.address is a FilterSet
    if !filter.address.is_empty() {
        let addrs: Vec<_> = filter.address.iter().collect();
        let address_query = if addrs.len() == 1 {
            format!("contract_address:{}", addrs[0])
        } else {
            let addr_list = addrs.iter()
                .map(|a| format!("contract_address:{}", a))
                .collect::<Vec<_>>()
                .join(" OR ");
            format!("({})", addr_list)
        };
        query_parts.push(address_query);
    }

    // Topics filter - supports null values
    // filter.topics is an array of FilterSet
    for (i, topic_filter) in filter.topics.iter().enumerate() {
        if !topic_filter.is_empty() {
            let topics: Vec<_> = topic_filter.iter().collect();
            let topic_field = format!("topic{}", i);
            let topic_query = if topics.len() == 1 {
                format!("{}:{}", topic_field, topics[0])
            } else {
                let topic_list = topics.iter()
                    .map(|t| format!("{}:{}", topic_field, t))
                    .collect::<Vec<_>>()
                    .join(" OR ");
                format!("({})", topic_list)
            };
            query_parts.push(topic_query);
        }
        // empty FilterSet means "any value" for this topic position, so we don't add a filter
    }

    // Combine all query parts with AND
    let query_string = if query_parts.is_empty() {
        "*".to_string()
    } else {
        query_parts.join(" AND ")
    };

    json!({
        "query": query_string,
        "start_timestamp": 0,
        "end_timestamp": i64::MAX,
        "max_hits": 1000,
    })
}

fn block_number_to_u64(block: &BlockNumberOrTag) -> u64 {
    match block {
        BlockNumberOrTag::Number(n) => *n,
        BlockNumberOrTag::Latest => u64::MAX, // Will need to query for latest
        BlockNumberOrTag::Earliest => 0,
        BlockNumberOrTag::Pending => u64::MAX,
        BlockNumberOrTag::Safe => u64::MAX - 1,
        BlockNumberOrTag::Finalized => u64::MAX - 2,
    }
}

pub async fn start_rpc_server(
    quickwit_client: QuickwitClient,
    index_prefix: String,
    port: u16,
) {
    let state = RpcState::new(quickwit_client, index_prefix);
    let app = create_rpc_router(state);

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    println!("RPC server listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap();

    axum::serve(listener, app)
        .await
        .unwrap();
}
