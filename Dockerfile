# Build stage
FROM rust:1.87-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    clang \
    libclang-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src

# Build dependencies first (for caching)
RUN cargo build --release --locked

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from builder
COPY --from=builder /app/target/release/reth-indexer /usr/local/bin/reth-indexer

# Create necessary directories
RUN mkdir -p /csv-data /parquet-data /config

# Set the entrypoint
ENTRYPOINT ["reth-indexer"]