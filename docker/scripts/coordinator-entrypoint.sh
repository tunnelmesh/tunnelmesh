#!/bin/bash
set -e

# Use hostname as coordinator name (hostname is already "coordinator-N")
COORD_NAME="$(hostname)"
export COORD_NAME

# European cities with coordinates (lat,lon,name) - same as peers
LOCATIONS=(
    "51.5074,-0.1278,London"
    "48.8566,2.3522,Paris"
    "52.5200,13.4050,Berlin"
    "41.9028,12.4964,Rome"
    "40.4168,-3.7038,Madrid"
    "52.3676,4.9041,Amsterdam"
    "50.8503,4.3517,Brussels"
    "59.3293,18.0686,Stockholm"
    "55.6761,12.5683,Copenhagen"
    "60.1699,24.9384,Helsinki"
    "59.9139,10.7522,Oslo"
    "53.3498,-6.2603,Dublin"
    "48.2082,16.3738,Vienna"
    "50.0755,14.4378,Prague"
    "47.4979,19.0402,Budapest"
    "52.2297,21.0122,Warsaw"
    "44.4268,26.1025,Bucharest"
    "42.6977,23.3219,Sofia"
    "37.9838,23.7275,Athens"
    "38.7223,-9.1393,Lisbon"
    "46.2044,6.1432,Geneva"
    "45.4642,9.1900,Milan"
    "41.3851,2.1734,Barcelona"
    "53.5511,9.9937,Hamburg"
    "48.1351,11.5820,Munich"
)

# Get deterministic location based on hostname
HOSTNAME_HASH=$(echo -n "$(hostname)" | cksum | cut -d' ' -f1)
LOCATION_INDEX=$((HOSTNAME_HASH % ${#LOCATIONS[@]}))
LOCATION="${LOCATIONS[$LOCATION_INDEX]}"

# Parse location
LATITUDE=$(echo "$LOCATION" | cut -d',' -f1)
LONGITUDE=$(echo "$LOCATION" | cut -d',' -f2)
CITY_NAME=$(echo "$LOCATION" | cut -d',' -f3)

echo "=== TunnelMesh Coordinator Starting ==="
echo "Coordinator Name: $COORD_NAME"
echo "Hostname: $(hostname)"
echo "Location: $CITY_NAME ($LATITUDE, $LONGITUDE)"

# Generate SSH keys if they don't exist
KEY_PATH="/root/.tunnelmesh/id_ed25519"
if [ ! -f "$KEY_PATH" ]; then
    echo "Generating SSH keys..."
    tunnelmesh init
fi

# Validate auth token is set
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

# Generate config from template
envsubst < /etc/tunnelmesh/coordinator.yaml.template > /etc/tunnelmesh/coordinator.yaml

echo "Generated config:"
cat /etc/tunnelmesh/coordinator.yaml

# Build command - coord-1 bootstraps, others join coord-1
LOG_LEVEL="${LOG_LEVEL:-info}"

if [ "$(hostname)" = "coordinator-1" ]; then
    # coordinator-1 is the bootstrap coordinator
    echo "Starting as BOOTSTRAP coordinator..."
    CMD="tunnelmesh join --config /etc/tunnelmesh/coordinator.yaml --log-level $LOG_LEVEL --latitude $LATITUDE --longitude $LONGITUDE --city $CITY_NAME"
else
    # coordinator-2 and coordinator-3 join coordinator-1's mesh
    echo "Waiting for coordinator-1 to be ready..."
    MAX_WAIT=60
    WAIT_COUNT=0
    until curl -sf http://coordinator-1:8080/health > /dev/null 2>&1; do
        WAIT_COUNT=$((WAIT_COUNT + 1))
        if [ "$WAIT_COUNT" -ge "$MAX_WAIT" ]; then
            echo "ERROR: coordinator-1 did not become ready after $((MAX_WAIT * 2))s, exiting"
            exit 1
        fi
        echo "  coordinator-1 not ready (attempt $WAIT_COUNT/$MAX_WAIT), waiting..."
        sleep 2
    done
    echo "coordinator-1 is ready! Waiting 5s for coordinator to complete self-registration..."
    sleep 5
    echo "Joining mesh as secondary coordinator..."

    # Allow HTTP for Docker testing environment
    export TUNNELMESH_ALLOW_HTTP=true
    CMD="tunnelmesh join http://coordinator-1:8080 --config /etc/tunnelmesh/coordinator.yaml --log-level $LOG_LEVEL --latitude $LATITUDE --longitude $LONGITUDE --city $CITY_NAME"
fi

# Start the coordinator
echo "Starting mesh coordinator..."
echo "Command: $CMD"
exec $CMD
