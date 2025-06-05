#!/bin/sh
set -eux

echo "Waiting for chain..."
sleep 10

echo "Testing connection..."
cast block-number --rpc-url http://reth-dev:8545

echo "Deploying with cast..."
cd /contracts

# Build first
forge build

# Get bytecode using grep and sed instead of jq
BYTECODE=$(cat out/SimpleToken.sol/SimpleToken.json | grep -o '"bytecode":{"object":"[^"]*"' | sed 's/.*"object":"\([^"]*\)".*/\1/')

# Deploy using cast with raw bytecode - constructor will use default values
DEPLOY_OUTPUT=$(cast send --rpc-url http://reth-dev:8545 \
  --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
  --create $BYTECODE \
  --json)

TX_HASH=$(echo "$DEPLOY_OUTPUT" | grep -o '"transactionHash":"[^"]*"' | head -1 | sed 's/.*":"\([^"]*\)".*/\1/')

echo "Deploy TX: $TX_HASH"
sleep 5

# Get contract address from receipt
RECEIPT=$(cast receipt --rpc-url http://reth-dev:8545 "$TX_HASH" --json)
CONTRACT=$(echo "$RECEIPT" | grep -o '"contractAddress":"[^"]*"' | sed 's/.*":"\([^"]*\)".*/\1/')
echo "Contract deployed at: $CONTRACT"

# Now make some transfers
for i in $(seq 1 10000); do
  echo "Transfer $i..."
  # Remove --async and add error handling
  if cast send --rpc-url http://reth-dev:8545 \
    --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
    $CONTRACT \
    "transfer(address,uint256)" \
    "0x70997970C51812dc3A010C7d01b50e0d17dc79C8" \
    "1000000000000000000"; then
    echo "  ✓ Transfer $i successful"
  else
    echo "  ✗ Transfer $i failed, retrying..."
    sleep 0.1
    # Retry once
    cast send --rpc-url http://reth-dev:8545 \
      --private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
      $CONTRACT \
      "transfer(address,uint256)" \
      "0x70997970C51812dc3A010C7d01b50e0d17dc79C8" \
      "1000000000000000000" || echo "  ✗ Transfer $i failed on retry"
  fi

  # Longer pause every 100 transfers
  if [ $((i % 100)) -eq 0 ]; then
    echo "Pausing at transfer $i (100 transfers completed)..."
    sleep 0.5
  fi
done

echo "Done!"
sleep infinity
