%{ if coordinator_enabled ~}
# === COORDINATOR PEER CONFIGURATION ===
cat > /etc/tunnelmesh/coordinator.yaml <<'COORDCONF'
name: "${node_name}"
servers: []  # First coordinator (standalone)
auth_token: "${auth_token}"
%{ if peer_enabled ~}
ssh_port: ${ssh_tunnel_port}
private_key: /etc/tunnelmesh/peer.key
%{ endif ~}
%{ if length(admin_peers) > 0 ~}
admin_peers: [${join(", ", [for p in admin_peers : "\"${p}\""])}]
%{ endif ~}

# Enable coordinator services
coordinator:
  enabled: true
  listen: ":${coordinator_port}"
  memberlist_seeds: []  # Standalone coordinator (TODO: add clustering support)

  admin:
    enabled: true
%{ if peer_enabled ~}
    port: 443  # Mesh-internal admin on port 443 (nginx uses 8443 for external API)
%{ endif ~}
%{ if monitoring_enabled ~}
    monitoring:
      prometheus_url: "${prometheus_url}"
      grafana_url: "${grafana_url}"
%{ endif ~}
    # Admin accessible at https://this.tunnelmesh/ from mesh peers

  relay:
    enabled: ${relay_enabled}
    pair_timeout: "90s"

  locations: ${locations_enabled}

%{ if peer_enabled ~}
# TUN interface (coordinator is also a peer)
tun:
  name: tun-mesh
  mtu: 1400

# DNS resolver
dns:
  enabled: true
  listen: "127.0.0.1:5353"
%{ endif ~}

%{ if exit_node != "" ~}
# Route internet through exit peer
exit_peer: "${exit_node}"
%{ endif ~}
%{ if allow_exit_traffic ~}
# Allow peers to use this as exit node
allow_exit_traffic: true
%{ endif ~}

%{ if location_latitude != null && location_longitude != null ~}
# Manual location override
location:
  latitude: ${location_latitude}
  longitude: ${location_longitude}
%{ if location_city != "" ~}
  city: "${location_city}"
%{ endif ~}
%{ if location_country != "" ~}
  country: "${location_country}"
%{ endif ~}
%{ endif ~}

%{ if monitoring_enabled && loki_enabled ~}
# Loki logging
loki:
  enabled: true
  url: "http://127.0.0.1:3100"
%{ endif ~}

%{ if wireguard_enabled ~}
# WireGuard concentrator
wireguard:
  enabled: true
  listen_port: ${wg_listen_port}
  data_dir: /var/lib/tunnelmesh/wireguard
  endpoint: "${wg_endpoint}"
%{ endif ~}
COORDCONF

# Install coordinator service (serve mode; no TUNNELMESH_TOKEN required)
echo "y" | /usr/local/bin/tunnelmesh service install --mode serve --config /etc/tunnelmesh/coordinator.yaml
%{ endif ~}
