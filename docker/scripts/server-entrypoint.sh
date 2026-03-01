#!/bin/bash
set -e

echo "=== TunnelMesh Server Starting (with mesh join) ==="

# Generate SSH keys if they don't exist
KEY_PATH="/root/.tunnelmesh/id_ed25519"
if [ ! -f "$KEY_PATH" ]; then
    echo "Generating SSH keys..."
    tunnelmesh init
fi

# Validate auth token is set (required for security - passed via environment variable)
if [ -z "$TUNNELMESH_TOKEN" ]; then
    echo "ERROR: TUNNELMESH_TOKEN environment variable is not set"
    echo "Generate a token with: openssl rand -hex 32"
    exit 1
fi

# Validate token format (must be 64 hex characters)
if ! echo "$TUNNELMESH_TOKEN" | grep -qE '^[0-9a-fA-F]{64}$'; then
    echo "ERROR: TUNNELMESH_TOKEN must be exactly 64 hexadecimal characters"
    echo "Current token length: ${#TUNNELMESH_TOKEN}"
    echo "Generate a valid token with: openssl rand -hex 32"
    exit 1
fi

# Build command
CMD="tunnelmesh join --config /etc/tunnelmesh/server.yaml"

# Set log level from environment (default: info)
LOG_LEVEL="${LOG_LEVEL:-info}"
CMD="$CMD --log-level $LOG_LEVEL"

# Append OTLP endpoint if configured (set via docker-compose environment)
if [ -n "${OTLP_ENDPOINT:-}" ]; then
    CMD="$CMD --otlp-endpoint $OTLP_ENDPOINT"
fi

# Start the server (which will also join the mesh as a client)
echo "Starting mesh server..."
exec $CMD
