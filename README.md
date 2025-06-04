# reth-indexer

A high-performance Ethereum event indexer that reads directly from Reth node's database and provides a fully compatible ETH RPC API through Quickwit search engine integration.

## Overview

reth-indexer is reading directly from Reth's database. It indexes blockchain events into Quickwit (a distributed search engine) and exposes them through a standard Ethereum RPC interface.

### Key Features

- **Direct Database Access**: Reads directly from Reth node's database for maximum performance
- **ETH RPC Compatible**: Implements `eth_getLogs`, `eth_getBlockByNumber`, and `eth_getTransactionReceipt`
- **Quickwit Integration**: Uses Quickwit as a high-performance search backend
- **Bloom Filter Optimization**: Skips irrelevant blocks efficiently (~10,000 empty blocks in 400ms)
- **Configuration-Driven**: Flexible event filtering and indexing through JSON configuration
- **High Throughput**: Processes ~30,000 events/second

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌──────────┐
│  Reth Node  │────▶│ reth-indexer │────▶│  Quickwit   │────▶│ RPC API  │
│  Database   │     │              │     │   Engine    │     │          │
└─────────────┘     └──────────────┘     └─────────────┘     └──────────┘
```

## Installation

### Prerequisites

- Rust 1.82.0 or later
- Running Reth node with database v2 (on the same machine)
  - Reth database v2 is fully supported
  - Reth database v1 compatibility is not guaranteed
- Docker and Docker Compose (for Quickwit)

### Building from Source

```bash
# Clone the repository
git clone https://github.com/your-org/reth-indexer.git
cd reth-indexer

# Build with performance optimizations
cargo build --profile maxperf --features jemalloc
```

## Configuration

Create a configuration file (e.g., `reth-indexer-config.json`):

```json
{
  "rethDBLocation": "/path/to/reth/db",
  "csvLocation": "/tmp/reth-indexer-csv",
  "quickwit": {
    "apiEndpoint": "http://localhost:7280",
    "dataDirectory": "./data/quickwit",
    "indexPrefix": "reth-events",
    "batchSize": 1000,
    "recreateIndexes": false,
    "useEsBulkApi": false
  },
  "fromBlockNumber": 0,
  "toBlockNumber": 1000000,
  "eventMappings": [
    {
      "contractAddresses": ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"],
      "decodeAbiItems": [
        {
          "name": "Transfer",
          "type": "event",
          "inputs": [
            {
              "name": "from",
              "type": "address",
              "indexed": true
            },
            {
              "name": "to",
              "type": "address",
              "indexed": true
            },
            {
              "name": "value",
              "type": "uint256",
              "indexed": false
            }
          ]
        }
      ]
    }
  ]
}
```

### Configuration Options

- **rethDBLocation**: Path to Reth node's database
- **csvLocation**: Temporary directory for CSV buffering
- **quickwit**: Quickwit-specific configuration
  - **apiEndpoint**: Quickwit API URL
  - **dataDirectory**: Local data storage for Quickwit
  - **indexPrefix**: Prefix for Quickwit indexes
  - **batchSize**: Documents per batch for ingestion
  - **recreateIndexes**: Whether to delete and recreate indexes on startup
  - **useEsBulkApi**: Use Elasticsearch bulk API format (for compatibility)
- **fromBlockNumber**: Starting block for indexing
- **toBlockNumber**: Ending block for indexing
- **eventMappings**: Array of contract events to index

## Running

### 1. Start Quickwit

```bash
docker-compose -f docker-compose.quickwit.yml up -d
```

### 2. Run the Indexer

```bash
# Index events
RUSTFLAGS="-C target-cpu=native" CONFIG="./reth-indexer-config.json" \
  cargo run --profile maxperf --features jemalloc

# Or using the built binary
RUSTFLAGS="-C target-cpu=native" CONFIG="./reth-indexer-config.json" \
  ./target/release/reth-indexer
```

### 3. Start RPC Server

```bash
# Enable RPC mode with API=true
RUSTFLAGS="-C target-cpu=native" API=true CONFIG="./reth-indexer-config.json" \
  cargo run --profile maxperf --features jemalloc
```

The RPC server will start on `http://0.0.0.0:8545`

## RPC API Usage

### eth_getLogs

Retrieve logs matching the specified filter:

```bash
# Get all logs
curl -X POST http://localhost:8545 \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "eth_getLogs",
    "params": [{}],
    "id": 1
  }'

# Filter by address and topics
curl -X POST http://localhost:8545 \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "eth_getLogs",
    "params": [{
      "address": ["0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"],
      "topics": [
        ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
        ["0x000000000000000000000000sender_address"],
        null
      ],
      "fromBlock": "0x0",
      "toBlock": "0x1000"
    }],
    "id": 1
  }'
```

### eth_getBlockByNumber

Get block information by number:

```bash
curl -X POST http://localhost:8545 \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "eth_getBlockByNumber",
    "params": ["0x1b4", false],
    "id": 1
  }'
```

### eth_getTransactionReceipt

Get transaction receipt with logs:

```bash
curl -X POST http://localhost:8545 \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "eth_getTransactionReceipt",
    "params": ["0xtransaction_hash"],
    "id": 1
  }'
```

### Optimization Tips

1. **Use Performance Build Profile**: Always use `--profile maxperf`
2. **Enable jemalloc**: Use `--features jemalloc` for better memory management
3. **Native CPU Instructions**: Set `RUSTFLAGS="-C target-cpu=native"`
4. **Batch Size**: Adjust `quickwit.batchSize` based on your hardware
5. **Run on Same Machine**: Indexer must run on the same machine as Reth node

## Development

### Project Structure

```
src/
├── main.rs           # Entry point and CLI handling
├── indexer.rs        # Core indexing logic
├── rpc.rs            # ETH RPC implementation
├── quickwit.rs       # Quickwit integration
├── provider.rs       # Reth database interface
├── decode_events.rs  # Event decoding logic
├── datasource.rs     # Storage abstraction
├── csv.rs            # CSV buffering
├── parquet.rs        # Parquet file support
└── types.rs          # Shared type definitions
```

### Building for Development

```bash
# Debug build
cargo build

# Run tests
cargo test

# Check code
cargo clippy
```

### Docker Support

Build and run with Docker:

```bash
# Build image
docker build -t reth-indexer .

# Run with docker-compose
docker-compose -f docker-compose.local.yml up
```

## Limitations

- Must run on the same machine as the Reth node (requires direct database access)
- Optimized for local, high-throughput indexing (not distributed)
- Currently indexes events only (not transactions or state)

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Built on [Reth](https://github.com/paradigmxyz/reth) - Ethereum execution client
- Uses [Quickwit](https://quickwit.io/) - Cloud-native search engine
- Forked from [joshstevens19/reth-indexer](https://github.com/joshstevens19/reth-indexer)
- Inspired by the need for faster blockchain data indexing
