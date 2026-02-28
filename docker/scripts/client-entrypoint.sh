#!/bin/bash
set -e

# Use NODE_NAME env var if set, otherwise generate from hostname
NODE_NAME="${NODE_NAME:-node-$(hostname)}"
export NODE_NAME

# Random first names for user registration
FIRST_NAMES=(
    "Alice" "Bob" "Charlie" "Diana" "Eve" "Frank" "Grace" "Henry"
    "Ivy" "Jack" "Kate" "Leo" "Mia" "Noah" "Olivia" "Paul"
    "Quinn" "Ruby" "Sam" "Tara" "Uma" "Victor" "Wendy" "Xander"
    "Yara" "Zoe" "Alex" "Beth" "Carl" "Dana" "Eli" "Fiona"
)

# European cities with coordinates (lat,lon,name)
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

# Get node index from hostname (e.g., "abc123def" -> hash to index)
# Use checksum of hostname to get a deterministic but distributed index
HOSTNAME_HASH=$(echo -n "$(hostname)" | cksum | cut -d' ' -f1)
LOCATION_INDEX=$((HOSTNAME_HASH % ${#LOCATIONS[@]}))
LOCATION="${LOCATIONS[$LOCATION_INDEX]}"

# Parse location
LATITUDE=$(echo "$LOCATION" | cut -d',' -f1)
LONGITUDE=$(echo "$LOCATION" | cut -d',' -f2)
CITY_NAME=$(echo "$LOCATION" | cut -d',' -f3)

echo "=== TunnelMesh Client Starting ==="
echo "Node Name: $NODE_NAME"
echo "Location: $CITY_NAME ($LATITUDE, $LONGITUDE)"
echo "Server URL: $SERVER_URL"

# Wait for server to be available
echo "Waiting for coordination server..."
until curl -sf "${SERVER_URL}/health" > /dev/null 2>&1; do
    echo "  Server not ready, waiting..."
    sleep 2
done
echo "Server is ready!"

# Generate SSH keys if they don't exist
KEY_PATH="/root/.tunnelmesh/id_ed25519"
if [ ! -f "$KEY_PATH" ]; then
    echo "Generating SSH keys..."
    tunnelmesh init
fi

# Generate peer config from template
envsubst < /etc/tunnelmesh/peer.yaml.template > /etc/tunnelmesh/peer.yaml

echo "Generated peer config:"
cat /etc/tunnelmesh/peer.yaml

# Set auth token from AUTH_TOKEN env var (for both mesh join and API calls)
# Note: Peer identity is separate - based on ed25519 SSH keys in ~/.tunnelmesh/
export TUNNELMESH_TOKEN="${AUTH_TOKEN:-docker-test-token-123}"

# Function to start the mesh daemon
start_daemon() {
    echo "Starting mesh daemon..."
    OTLP_FLAG=""
    [ -n "${OTLP_ENDPOINT:-}" ] && OTLP_FLAG="--otlp-endpoint $OTLP_ENDPOINT"
    # shellcheck disable=SC2086
    tunnelmesh join "$SERVER_URL" --config /etc/tunnelmesh/peer.yaml --latitude "$LATITUDE" --longitude "$LONGITUDE" --city "$CITY_NAME" $OTLP_FLAG &
    MESH_PID=$!
    echo "Daemon started with PID $MESH_PID"
}

# Start the mesh client in background
start_daemon

# Wait for TUN device to be created
echo "Waiting for TUN device..."
for i in $(seq 1 30); do
    if ip link show tun-mesh0 > /dev/null 2>&1; then
        echo "TUN device created!"
        ip addr show tun-mesh0
        break
    fi
    sleep 1
done

# Configure DNS to use TunnelMesh resolver for .tunnelmesh domains
echo "Configuring DNS resolver..."
# Backup original resolv.conf and add TunnelMesh DNS as primary
cp /etc/resolv.conf /etc/resolv.conf.backup
echo "nameserver 127.0.0.53" > /etc/resolv.conf
echo "search tunnelmesh" >> /etc/resolv.conf
cat /etc/resolv.conf.backup >> /etc/resolv.conf
echo "DNS configured:"
cat /etc/resolv.conf

# User identity is now derived from SSH key - no separate setup needed
# User is automatically registered when peer joins the mesh

# Give time for initial peer discovery (Go code handles jitter and fast retries)
echo "Waiting for initial peer discovery..."
sleep 5

# Peer list and ping loop
echo "Starting ping test loop..."
PING_INTERVAL="${PING_INTERVAL:-2}"
DISCOVERY_INTERVAL="${DISCOVERY_INTERVAL:-20}"

counter=0
PEER_IPS=""

while true; do
    # Check if daemon is still running, restart if dead
    if ! kill -0 "$MESH_PID" 2>/dev/null; then
        echo ""
        echo "!!! Daemon (PID $MESH_PID) died, restarting..."
        start_daemon
        # Wait for TUN device to be recreated
        for i in $(seq 1 30); do
            if ip link show tun-mesh0 > /dev/null 2>&1; then
                echo "TUN device recreated!"
                break
            fi
            sleep 1
        done
        # Reconfigure DNS
        cp /etc/resolv.conf.backup /etc/resolv.conf.tmp 2>/dev/null || true
        echo "nameserver 127.0.0.53" > /etc/resolv.conf
        echo "search tunnelmesh" >> /etc/resolv.conf
        cat /etc/resolv.conf.tmp >> /etc/resolv.conf 2>/dev/null || true
        sleep 5  # Give time for peer discovery
    fi

    # Refresh peer list periodically
    if [ $((counter % DISCOVERY_INTERVAL)) -eq 0 ] || [ -z "$PEER_IPS" ]; then
        echo ""
        echo "--- Refreshing peer list ---"
        PEERS_JSON=$(curl -sf -H "Authorization: Bearer $AUTH_TOKEN" "${SERVER_URL}/api/v1/peers" 2>/dev/null || echo '{"peers":[]}')

        # Extract mesh IPs of other peers (excluding self)
        PEER_IPS=$(echo "$PEERS_JSON" | jq -r ".peers[] | select(.name != \"$NODE_NAME\") | .mesh_ip" 2>/dev/null | grep -v "^$" || true)

        if [ -n "$PEER_IPS" ]; then
            echo "Discovered peers:"
            echo "$PEERS_JSON" | jq -r ".peers[] | select(.name != \"$NODE_NAME\") | \"  \\(.name) -> \\(.mesh_ip)\"" 2>/dev/null || true
        else
            echo "No other peers discovered yet"
        fi
    fi

    # Pick a random peer and ping it (alternating between IP and DNS alias)
    if [ -n "$PEER_IPS" ]; then
        # Convert to array and pick random
        readarray -t PEER_ARRAY <<< "$PEER_IPS"
        PEER_COUNT=${#PEER_ARRAY[@]}

        if [ $PEER_COUNT -gt 0 ]; then
            RANDOM_INDEX=$((RANDOM % PEER_COUNT))
            TARGET_IP="${PEER_ARRAY[$RANDOM_INDEX]}"
            TARGET_NAME=$(echo "$PEERS_JSON" | jq -r ".peers[] | select(.mesh_ip == \"$TARGET_IP\") | .name" 2>/dev/null || echo "unknown")

            # Randomly choose to ping by IP or by DNS alias
            if [ $((RANDOM % 2)) -eq 0 ]; then
                # Ping by IP
                echo "[$(date '+%H:%M:%S')] Pinging $TARGET_NAME ($TARGET_IP) via mesh IP..."
                if ping -c 1 -W 2 "$TARGET_IP" > /dev/null 2>&1; then
                    echo "  SUCCESS: $TARGET_NAME is reachable via IP!"
                else
                    echo "  FAILED: Cannot reach $TARGET_NAME via IP"
                fi
            else
                # Ping by DNS alias (alt.node-xxx.tunnelmesh)
                # Use getent to resolve (musl's ping doesn't use custom resolvers properly)
                TARGET_DNS="alt.${TARGET_NAME}.tunnelmesh"
                RESOLVED_IP=$(getent hosts "$TARGET_DNS" 2>/dev/null | awk '{print $1}')
                if [ -n "$RESOLVED_IP" ]; then
                    echo "[$(date '+%H:%M:%S')] Pinging $TARGET_NAME via DNS alias ($TARGET_DNS -> $RESOLVED_IP)..."
                    if ping -c 1 -W 2 "$RESOLVED_IP" > /dev/null 2>&1; then
                        echo "  SUCCESS: $TARGET_NAME is reachable via DNS alias!"
                    else
                        echo "  FAILED: Cannot reach $TARGET_NAME via DNS alias (ping failed)"
                    fi
                else
                    echo "[$(date '+%H:%M:%S')] Pinging $TARGET_NAME via DNS alias ($TARGET_DNS)..."
                    echo "  FAILED: Cannot resolve DNS alias $TARGET_DNS"
                fi
            fi
        fi
    fi

    counter=$((counter + PING_INTERVAL))
    sleep "$PING_INTERVAL"
done
