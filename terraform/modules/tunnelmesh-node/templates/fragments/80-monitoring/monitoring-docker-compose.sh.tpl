# === MONITORING STACK SETUP (Docker Compose) ===
echo "Setting up monitoring stack via Docker Compose..."

# Create host directories
mkdir -p /opt/monitoring/prometheus
mkdir -p /opt/monitoring/loki
mkdir -p /opt/monitoring/grafana/provisioning/datasources
mkdir -p /opt/monitoring/grafana/provisioning/dashboards
mkdir -p /opt/monitoring/grafana/dashboards
mkdir -p /opt/monitoring/targets

# Write prometheus config
cat > /opt/monitoring/prometheus/prometheus.yml <<'PROMCONFIG'
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - '/etc/prometheus/alerts.yml'

scrape_configs:
  - job_name: 'prometheus'
    metrics_path: /prometheus/metrics
    static_configs:
      - targets: ['localhost:9090']

  - job_name: 'tunnelmesh-peers'
    scheme: https
    tls_config:
      insecure_skip_verify: true
    file_sd_configs:
      - files:
          - '/etc/prometheus/targets/peers.json'
        refresh_interval: 30s
PROMCONFIG

# Write alerts config
cat > /opt/monitoring/prometheus/alerts.yml <<'ALERTS'
groups:
  - name: tunnelmesh-warnings
    rules:
      - alert: TunnelMeshElevatedPacketDrops
        expr: sum(rate(tunnelmesh_dropped_no_route_total[5m])) > 1 or sum(rate(tunnelmesh_dropped_no_tunnel_total[5m])) > 1
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Elevated packet drops detected"
          description: "Packet drop rate exceeds 1/s for 2 minutes"

      - alert: TunnelMeshReconnectionAttempts
        expr: sum(rate(tunnelmesh_reconnects_total[5m])) > 0.1
        for: 3m
        labels:
          severity: warning
        annotations:
          summary: "Frequent reconnection attempts"
          description: "Reconnection rate indicates unstable connections"

      - alert: TunnelMeshPeerDisconnected
        expr: count(tunnelmesh_connection_state == 0) > 0
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Peer(s) disconnected"
          description: "One or more peers have been disconnected for 2+ minutes"

      - alert: TunnelMeshUnhealthyTunnels
        expr: (sum(tunnelmesh_active_tunnels) - sum(tunnelmesh_healthy_tunnels)) > 2
        for: 3m
        labels:
          severity: warning
        annotations:
          summary: "Unhealthy tunnels detected"
          description: "Multiple tunnels are active but not healthy"

      - alert: TunnelMeshForwarderErrors
        expr: sum(rate(tunnelmesh_forwarder_errors_total[5m])) > 0.1
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Forwarder errors detected"
          description: "Forwarder error rate is elevated"

  - name: tunnelmesh-critical
    rules:
      - alert: TunnelMeshMultiplePeersDown
        expr: count(tunnelmesh_connection_state == 0) >= 3
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Multiple peers disconnected"
          description: "3+ peers are currently disconnected - mesh connectivity severely impacted"

      - alert: TunnelMeshHighErrorRate
        expr: sum(rate(tunnelmesh_forwarder_errors_total[1m])) > 1
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "High forwarder error rate"
          description: "Forwarder errors exceeding 1/s - service is degraded"

      - alert: TunnelMeshSignificantPacketLoss
        expr: (sum(rate(tunnelmesh_dropped_no_route_total[1m])) + sum(rate(tunnelmesh_dropped_no_tunnel_total[1m]))) / (sum(rate(tunnelmesh_packets_sent_total[1m])) + 1) > 0.05
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Significant packet loss detected"
          description: "Packet drop rate exceeds 5% of total traffic"

      - alert: TunnelMeshNoHealthyTunnels
        expr: tunnelmesh_active_tunnels > 0 and tunnelmesh_healthy_tunnels == 0
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "Peer has no healthy tunnels"
          description: "Active tunnels exist but none are healthy - connectivity impaired"

      - alert: TunnelMeshRelayDisconnected
        expr: tunnelmesh_relay_connected == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Relay connection lost"
          description: "Peer lost connection to relay server - NAT traversal may fail"

  - name: tunnelmesh-page
    rules:
      - alert: TunnelMeshAllTunnelsDown
        expr: sum(tunnelmesh_active_tunnels) == 0 and count(tunnelmesh_peer_info) > 1
        for: 30s
        labels:
          severity: page
        annotations:
          summary: "All mesh tunnels are down"
          description: "Complete mesh network failure - no active tunnels between any peers"

      - alert: TunnelMeshMajorOutage
        expr: count(tunnelmesh_connection_state == 0) / count(tunnelmesh_connection_state) > 0.5
        for: 1m
        labels:
          severity: page
        annotations:
          summary: "Majority of peers unreachable"
          description: "Over 50% of peers are disconnected - major outage"

      - alert: TunnelMeshRelayCompleteFailure
        expr: count(tunnelmesh_relay_connected == 0) == count(tunnelmesh_relay_connected) and count(tunnelmesh_relay_connected) > 0
        for: 1m
        labels:
          severity: page
        annotations:
          summary: "All peers lost relay connection"
          description: "Complete relay server failure - NAT traversal unavailable for all peers"

      - alert: TunnelMeshNoPeersResponding
        expr: absent(tunnelmesh_peer_info)
        for: 2m
        labels:
          severity: page
        annotations:
          summary: "No TunnelMesh peers responding"
          description: "Cannot reach any TunnelMesh peer metrics - possible complete network outage"

ALERTS

# Write loki config (retention expressed in hours)
# NOTE: This heredoc is intentionally unquoted (<<LOKICONFIG not <<'LOKICONFIG')
# so Terraform can substitute $${loki_retention_days * 24}h. Any future values
# containing $ or %%{ must either be escaped or moved to a quoted heredoc section.
# http_listen_address binds Loki to localhost only. With network_mode: host,
# this is equivalent to the old systemd setup — Loki is reachable from the host
# at 127.0.0.1:3100 (via nginx proxy) but not from the external network.
cat > /opt/monitoring/loki/local-config.yaml <<LOKICONFIG
auth_enabled: false

server:
  http_listen_port: 3100
  grpc_listen_port: 9096
  http_listen_address: 127.0.0.1

common:
  instance_addr: 127.0.0.1
  path_prefix: /loki
  storage:
    filesystem:
      chunks_directory: /loki/chunks
      rules_directory: /loki/rules
  replication_factor: 1
  ring:
    kvstore:
      store: inmemory

query_range:
  results_cache:
    cache:
      embedded_cache:
        enabled: true
        max_size_mb: 100

schema_config:
  configs:
    - from: 2020-10-24
      store: tsdb
      object_store: filesystem
      schema: v13
      index:
        prefix: index_
        period: 24h

compactor:
  working_directory: /loki/compactor
  compaction_interval: 10m
  retention_enabled: true
  retention_delete_delay: 2h
  retention_delete_worker_count: 150
  delete_request_store: filesystem

limits_config:
  reject_old_samples: true
  reject_old_samples_max_age: 168h
  ingestion_rate_mb: 10
  ingestion_burst_size_mb: 20
  retention_period: ${loki_retention_days * 24}h
LOKICONFIG

# Download grafana provisioning files from repo
echo "Downloading Grafana provisioning files..."
curl -sL "https://raw.githubusercontent.com/${github_owner}/tunnelmesh/main/monitoring/grafana/provisioning/datasources/datasource.yml" \
  -o /opt/monitoring/grafana/provisioning/datasources/datasource.yml
curl -sL "https://raw.githubusercontent.com/${github_owner}/tunnelmesh/main/monitoring/grafana/provisioning/dashboards/dashboard.yml" \
  -o /opt/monitoring/grafana/provisioning/dashboards/dashboard.yml

# Download grafana dashboards via GitHub API
echo "Downloading Grafana dashboards..."
if [ -n "$GITHUB_TOKEN" ]; then
  DASHBOARD_LIST=$(curl -sf -H "Authorization: token $GITHUB_TOKEN" "https://api.github.com/repos/${github_owner}/tunnelmesh/contents/monitoring/grafana/dashboards")
else
  DASHBOARD_LIST=$(curl -sf "https://api.github.com/repos/${github_owner}/tunnelmesh/contents/monitoring/grafana/dashboards")
fi
if echo "$DASHBOARD_LIST" | jq -e 'type == "array"' > /dev/null 2>&1; then
  echo "$DASHBOARD_LIST" | jq -r '.[] | select(.name | endswith(".json")) | .download_url' | \
    while read -r url; do
      filename=$(basename "$url")
      echo "  Downloading $filename"
      curl -sL "$url" -o "/opt/monitoring/grafana/dashboards/$filename"
    done
else
  echo "Warning: GitHub API did not return a valid dashboard list, skipping dashboard download"
fi

# Download SD generator binary from GitHub releases
echo "Downloading TunnelMesh Prometheus SD Generator..."
ARCH=$(dpkg --print-architecture)
%{ if binary_version == "latest" ~}
SD_DOWNLOAD_URL="https://github.com/${github_owner}/tunnelmesh/releases/latest/download/tunnelmesh-prometheus-sd-generator-linux-$ARCH"
%{ else ~}
SD_DOWNLOAD_URL="https://github.com/${github_owner}/tunnelmesh/releases/download/${binary_version}/tunnelmesh-prometheus-sd-generator-linux-$ARCH"
%{ endif ~}
echo "Downloading SD generator from $SD_DOWNLOAD_URL"
curl -sL "$SD_DOWNLOAD_URL" -o /opt/monitoring/sd-generator
chmod +x /opt/monitoring/sd-generator

# Create initial empty targets file
echo "[]" > /opt/monitoring/targets/peers.json

# Write sd-generator env file (auth token kept out of docker-compose.yml)
cat > /opt/monitoring/sd-generator.env <<SDENV
AUTH_TOKEN=${auth_token}
SDENV
chmod 600 /opt/monitoring/sd-generator.env

# Write docker-compose.yml
cat > /opt/monitoring/docker-compose.yml <<COMPOSEEOF
services:
  prometheus:
    image: prom/prometheus:${prometheus_image_tag}
    network_mode: host
    restart: unless-stopped
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.listen-address=127.0.0.1:9090'
      - '--web.external-url=/prometheus/'
      - '--web.route-prefix=/prometheus/'
      - '--storage.tsdb.retention.time=${prometheus_retention_days}d'
    volumes:
      - /opt/monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - /opt/monitoring/prometheus/alerts.yml:/etc/prometheus/alerts.yml:ro
      - /opt/monitoring/targets:/etc/prometheus/targets:ro
      - prometheus-data:/prometheus
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://127.0.0.1:9090/prometheus/-/healthy"]
      interval: 10s
      timeout: 5s
      retries: 3
      start_period: 10s

  sd-generator:
    image: alpine:3.23
    network_mode: host
    restart: unless-stopped
    user: "nobody"
    read_only: true
    entrypoint: ["/app/sd-generator"]
    environment:
      - COORD_SERVER_URL=http://127.0.0.1:${coordinator_port}
      - POLL_INTERVAL=30s
      - OUTPUT_FILE=/targets/peers.json
      - METRICS_PORT=9443
      - TLS_SKIP_VERIFY=true
    env_file:
      - /opt/monitoring/sd-generator.env
    volumes:
      - /opt/monitoring/sd-generator:/app/sd-generator:ro
      - /opt/monitoring/targets:/targets
    depends_on:
      prometheus:
        condition: service_healthy

  loki:
    image: grafana/loki:${loki_image_tag}
    network_mode: host
    restart: unless-stopped
    command: -config.file=/etc/loki/local-config.yaml
    volumes:
      - /opt/monitoring/loki/local-config.yaml:/etc/loki/local-config.yaml:ro
      - loki-data:/loki

  grafana:
    image: grafana/grafana:${grafana_image_tag}
    network_mode: host
    restart: unless-stopped
    environment:
      - GF_SERVER_HTTP_PORT=3000
      - GF_SERVER_HTTP_ADDR=127.0.0.1
      - GF_SERVER_ROOT_URL=%(protocol)s://%(domain)s/grafana/
      - GF_SERVER_SERVE_FROM_SUB_PATH=true
      - GF_AUTH_ANONYMOUS_ENABLED=true
      - GF_AUTH_ANONYMOUS_ORG_ROLE=Viewer
      - GF_DASHBOARDS_DEFAULT_HOME_DASHBOARD_PATH=/var/lib/grafana/dashboards/tunnelmesh.json
    volumes:
      - /opt/monitoring/grafana/provisioning:/etc/grafana/provisioning:ro
      - /opt/monitoring/grafana/dashboards:/var/lib/grafana/dashboards:ro
      - grafana-data:/var/lib/grafana
    depends_on:
      prometheus:
        condition: service_healthy

volumes:
  prometheus-data:
  loki-data:
  grafana-data:
COMPOSEEOF

# Start the monitoring stack
echo "Starting monitoring stack..."
docker compose -f /opt/monitoring/docker-compose.yml --project-name tunnelmesh-monitoring up -d
echo "Monitoring stack started"

# Wait for Loki to be ready (grafana/loki image has no shell or wget, so check from host)
echo "Waiting for Loki to become ready..."
for i in $(seq 1 36); do
  if curl -sf http://127.0.0.1:3100/ready > /dev/null 2>&1; then
    echo "Loki is ready"
    break
  fi
  if [ "$i" -eq 36 ]; then
    echo "Error: Loki did not become ready after 3 minutes"
    exit 1
  fi
  sleep 5
done
