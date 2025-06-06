use crate::{
    csv::CsvWriter,
    datasource::{load_table_configs, DatasourceWritable},
    types::{IndexerContractMapping, IndexerQuickwitConfig},
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use csv::ReaderBuilder;
use indexmap::IndexMap;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{any::Any, collections::HashMap, error::Error, fs, fs::File, path::Path};

#[derive(Clone)]
pub struct QuickwitClient {
    pub(crate) api_endpoint: String,
    pub(crate) index_prefix: String,
    batch_size: usize,
    recreate_indexes: bool,
    http_client: Client,
    table_map: HashMap<String, IndexMap<String, String>>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum QuickwitClientError {
    QuickwitOperationError(String),
}

#[derive(Debug, Serialize, Deserialize)]
struct QuickwitIndexConfig {
    version: String,
    index_id: String,
    doc_mapping: DocMapping,
    indexing_settings: IndexingSettings,
    search_settings: SearchSettings,
}

#[derive(Debug, Serialize, Deserialize)]
struct DocMapping {
    field_mappings: Vec<FieldMapping>,
    timestamp_field: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct FieldMapping {
    name: String,
    #[serde(rename = "type")]
    field_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    stored: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    indexed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fast: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexingSettings {
    commit_timeout_secs: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct SearchSettings {
    default_search_fields: Vec<String>,
}

/// Factory function to initialize Quickwit client
pub async fn init_quickwit_db(
    indexer_quickwit_config: &IndexerQuickwitConfig,
    event_mappings: &[IndexerContractMapping],
) -> Result<QuickwitClient, QuickwitClientError> {
    let quickwit_client = QuickwitClient::new(indexer_quickwit_config, event_mappings).await?;

    if indexer_quickwit_config.recreate_indexes {
        if let Err(err) = quickwit_client.delete_indexes().await {
            return Err(quickwit_operation_error(err));
        }
    }

    if let Err(err) = quickwit_client.create_indexes().await {
        return Err(quickwit_operation_error(err));
    }

    Ok(quickwit_client)
}

fn quickwit_operation_error(err: Box<dyn Error>) -> QuickwitClientError {
    QuickwitClientError::QuickwitOperationError(format!(
        "Quickwit client operation error: {:?}",
        err
    ))
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QuickwitSearchRequest {
    pub query: String,
    pub start_timestamp: i64,
    pub end_timestamp: i64,
    pub max_hits: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QuickwitSearchResponse {
    pub hits: Vec<Value>,
    pub num_hits: usize,
}

impl QuickwitClient {
    pub async fn new(
        indexer_quickwit_config: &IndexerQuickwitConfig,
        indexer_contract_mappings: &[IndexerContractMapping],
    ) -> Result<Self, QuickwitClientError> {
        let table_map = load_table_configs(indexer_contract_mappings);

        // Check data directory exists - if not, create it
        let root_data_dir: &Path = Path::new(indexer_quickwit_config.data_directory.as_str());
        if !root_data_dir.exists() {
            if let Err(err) = fs::create_dir_all(root_data_dir) {
                return Err(QuickwitClientError::QuickwitOperationError(format!(
                    "Failed to create data directory: {:?}",
                    err,
                )));
            }
        }

        Ok(QuickwitClient {
            api_endpoint: indexer_quickwit_config.api_endpoint.clone(),
            index_prefix: indexer_quickwit_config.index_prefix.clone(),
            batch_size: indexer_quickwit_config.batch_size,
            recreate_indexes: indexer_quickwit_config.recreate_indexes,
            http_client: Client::builder()
                .timeout(std::time::Duration::from_secs(60))
                .pool_max_idle_per_host(50)
                .pool_idle_timeout(std::time::Duration::from_secs(90))
                .http2_adaptive_window(true)
                .build()
                .unwrap(),
            table_map,
        })
    }

    /// Delete existing indexes if recreate_indexes is true
    pub async fn delete_indexes(&self) -> Result<(), Box<dyn Error>> {
        if self.recreate_indexes {
            println!("WARNING: recreate_indexes is true, deleting existing indexes...");
            for table_name in self.table_map.keys() {
                let index_id = self.get_index_id(table_name);
                let url = format!("{}/api/v1/indexes/{}", self.api_endpoint, index_id);

                match self.http_client.delete(&url).send().await {
                    Ok(response) => {
                        if response.status().is_success() || response.status() == 404 {
                            println!("Deleted index: {}", index_id);
                        } else {
                            println!("Failed to delete index {}: {}", index_id, response.status());
                        }
                    }
                    Err(e) => {
                        println!("Error deleting index {}: {:?}", index_id, e);
                    }
                }
            }
        }
        Ok(())
    }

    /// Create indexes for each event type
    pub async fn create_indexes(&self) -> Result<(), Box<dyn Error>> {
        for (table_name, column_map) in &self.table_map {
            let index_id = self.get_index_id(table_name);
            let index_config = self.build_index_config(&index_id, column_map);

            let url = format!("{}/api/v1/indexes", self.api_endpoint);
            let response = self
                .http_client
                .post(&url)
                .json(&index_config)
                .send()
                .await?;

            if response.status().is_success() {
                println!("Created index: {}", index_id);
            } else if response.status() == 400 {
                // Index might already exist
                println!("Index {} may already exist", index_id);
            } else {
                let error_text = response.text().await?;
                return Err(format!("Failed to create index {}: {}", index_id, error_text).into());
            }
        }
        Ok(())
    }

    /// Build Quickwit index configuration
    fn build_index_config(
        &self,
        index_id: &str,
        column_map: &IndexMap<String, String>,
    ) -> QuickwitIndexConfig {
        let mut field_mappings = vec![];

        // Add all fields from the column map
        for (field_name, field_type) in column_map {
            let quickwit_type = match (field_name.as_str(), field_type.as_str()) {
                ("timestamp", _) => "datetime", // Timestamp fields must be datetime
                ("log_index", _) => "i64",      // Log index is numeric
                ("transaction_index", _) => "i64", // Transaction index is numeric
                (name, _) if name.starts_with("topic") => "text", // Topics are hex strings
                ("data", _) => "text",          // Data field is hex string
                (_, "int") => "i64",
                (_, "string") => "text",
                _ => "text",
            };

            let mut field = FieldMapping {
                name: field_name.clone(),
                field_type: quickwit_type.to_string(),
                stored: Some(true),
                indexed: Some(true),
                fast: None,
            };

            // Make numeric fields fast for efficient filtering/aggregation
            if quickwit_type == "i64" {
                field.fast = Some(true);
            }

            // Make timestamp field fast for time-based queries
            if quickwit_type == "datetime" {
                field.fast = Some(true);
            }

            // Make topic fields fast for efficient filtering in eth_getLogs
            if field_name.starts_with("topic") {
                field.fast = Some(true);
            }

            field_mappings.push(field);
        }

        QuickwitIndexConfig {
            version: "0.8".to_string(),
            index_id: index_id.to_string(),
            doc_mapping: DocMapping {
                field_mappings,
                timestamp_field: "timestamp".to_string(),
            },
            indexing_settings: IndexingSettings {
                commit_timeout_secs: 60, // Increase for better bulk performance
            },
            search_settings: SearchSettings {
                default_search_fields: vec!["contract_address".to_string(), "tx_hash".to_string()],
            },
        }
    }

    /// Get the index ID for a table
    fn get_index_id(&self, table_name: &str) -> String {
        format!("{}-{}", self.index_prefix, table_name.to_lowercase())
    }

    /// Convert CSV data to Quickwit documents and ingest them
    pub async fn write_csv_to_quickwit(&self, table_name: &str, csv_writer: &CsvWriter) {
        let index_id = self.get_index_id(table_name);
        let column_map = &self.table_map[table_name];

        // Read CSV and convert to JSON documents
        match self.csv_to_documents(csv_writer.path(), column_map) {
            Ok(documents) => {
                if documents.is_empty() {
                    println!("No documents to ingest for {}", table_name);
                    return;
                }

                // Ingest documents in batches
                for chunk in documents.chunks(self.batch_size) {
                    if let Err(e) = self.ingest_documents_native(&index_id, chunk).await {
                        println!("Failed to ingest batch for {}: {:?}", table_name, e);
                    }
                }

                println!(
                    "Successfully ingested {} documents to {}",
                    documents.len(),
                    index_id
                );
            }
            Err(e) => {
                println!("Failed to convert CSV to documents: {:?}", e);
            }
        }
    }

    /// Convert CSV to JSON documents
    fn csv_to_documents(
        &self,
        csv_path: &str,
        column_map: &IndexMap<String, String>,
    ) -> Result<Vec<Value>, Box<dyn Error + Send + Sync>> {
        let file = File::open(csv_path)?;
        let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(file);
        let headers = rdr.headers()?.clone();

        let mut documents = Vec::new();

        for result in rdr.records() {
            let record = result?;
            let mut doc = json!({});

            for (i, value) in record.iter().enumerate() {
                if let Some(header) = headers.get(i) {
                    // Skip empty topic fields to save storage and improve performance
                    if header.starts_with("topic") && value.is_empty() {
                        continue;
                    }

                    let field_type = column_map
                        .get(header)
                        .map(|s| s.as_str())
                        .unwrap_or("string");

                    // Convert value based on field type
                    let json_value = match (header, field_type) {
                        ("log_index", _) | ("transaction_index", _) => value
                            .parse::<i64>()
                            .map(|v| json!(v))
                            .unwrap_or_else(|_| json!(0)),
                        (_, "int") => value
                            .parse::<i64>()
                            .map(|v| json!(v))
                            .unwrap_or_else(|_| json!(value)),
                        _ => json!(value),
                    };

                    doc[header] = json_value;
                }
            }

            // Convert timestamp to RFC3339 format for Quickwit
            if let Some(timestamp) = doc.get("timestamp").and_then(|v| v.as_i64()) {
                let dt =
                    DateTime::<Utc>::from_timestamp(timestamp, 0).unwrap_or_else(|| Utc::now());
                doc["timestamp"] = json!(dt.to_rfc3339());
            }

            documents.push(doc);
        }

        Ok(documents)
    }

    /// Ingest documents using native Quickwit API
    async fn ingest_documents_native(
        &self,
        index_id: &str,
        documents: &[Value],
    ) -> Result<(), Box<dyn Error>> {
        let start = std::time::Instant::now();
        let url = format!("{}/api/v1/{}/ingest", self.api_endpoint, index_id);

        // Convert documents to NDJSON format
        let ndjson = documents
            .iter()
            .map(|doc| serde_json::to_string(doc))
            .collect::<Result<Vec<_>, _>>()?
            .join("\n");

        let response = self
            .http_client
            .post(&url)
            .header("Content-Type", "application/x-ndjson")
            .body(ndjson)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(format!("Failed to ingest documents: {}", error_text).into());
        }

        let elapsed = start.elapsed();
        println!("    Ingested {} docs in {:?}", documents.len(), elapsed);

        Ok(())
    }

    /// Search for logs in Quickwit
    pub async fn search_logs(
        &self,
        index_suffix: &str,
        request: QuickwitSearchRequest,
    ) -> Result<QuickwitSearchResponse, Box<dyn Error>> {
        let index_id = self.get_index_id(index_suffix);
        let url = format!("{}/api/v1/{}/search", self.api_endpoint, index_id);

        // Quickwit search API doesn't use start/end timestamp in the body
        // Timestamps should be part of the query string if needed
        let search_body = json!({
            "query": request.query,
            "max_hits": request.max_hits,
        });

        let response = self
            .http_client
            .post(&url)
            .json(&search_body)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(format!("Search failed: {}", error_text).into());
        }

        let search_result: Value = response.json().await?;

        // Extract hits from response
        let hits = search_result
            .get("hits")
            .and_then(|h| h.as_array())
            .map(|arr| arr.to_vec())
            .unwrap_or_default();

        Ok(QuickwitSearchResponse {
            num_hits: hits.len(),
            hits,
        })
    }

    /// Search across all event indexes
    pub async fn search_all_logs(
        &self,
        request: QuickwitSearchRequest,
    ) -> Result<QuickwitSearchResponse, Box<dyn Error>> {
        let mut all_hits = Vec::new();

        // Search in each event type index
        for table_name in self.table_map.keys() {
            match self.search_logs(table_name, request.clone()).await {
                Ok(response) => {
                    all_hits.extend(response.hits);
                }
                Err(e) => {
                    // Log error but continue searching other indexes
                    eprintln!("Error searching index {}: {:?}", table_name, e);
                }
            }
        }

        // Sort by block number and log index for consistent ordering
        all_hits.sort_by(|a, b| {
            let a_block = a.get("block_number").and_then(|v| v.as_u64()).unwrap_or(0);
            let b_block = b.get("block_number").and_then(|v| v.as_u64()).unwrap_or(0);
            let a_log_idx = a.get("log_index").and_then(|v| v.as_u64()).unwrap_or(0);
            let b_log_idx = b.get("log_index").and_then(|v| v.as_u64()).unwrap_or(0);

            a_block.cmp(&b_block).then(a_log_idx.cmp(&b_log_idx))
        });

        // Limit results to max_hits
        all_hits.truncate(request.max_hits);

        Ok(QuickwitSearchResponse {
            num_hits: all_hits.len(),
            hits: all_hits,
        })
    }

    /// Get all event table names
    #[allow(dead_code)]
    pub fn get_event_types(&self) -> Vec<String> {
        self.table_map.keys().cloned().collect()
    }
}

/// Implement DatasourceWritable trait
#[async_trait]
impl DatasourceWritable for QuickwitClient {
    async fn write_data(&self, table_name: &str, csv_writer: &CsvWriter) {
        println!("  writing / sync to quickwit index: {:?}", table_name);
        self.write_csv_to_quickwit(table_name, csv_writer).await;
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
