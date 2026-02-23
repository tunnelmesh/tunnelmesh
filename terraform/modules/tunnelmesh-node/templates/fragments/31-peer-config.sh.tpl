%{ if !coordinator_enabled && peer_enabled ~}
# === PEER-ONLY CONFIGURATION ===
cat > /etc/tunnelmesh/peer.yaml <<'PEERCONF'
name: "${node_name}"
servers:
  - "${peer_server}"
auth_token: "${auth_token}"
ssh_port: ${ssh_tunnel_port}
private_key: /etc/tunnelmesh/peer.key

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

# TUN interface
tun:
  name: tun-mesh
  mtu: 1400

# DNS resolver
dns:
  enabled: true
  listen: "127.0.0.1:5353"

%{ if wireguard_enabled ~}
# WireGuard concentrator
wireguard:
  enabled: true
  listen_port: ${wg_listen_port}
  data_dir: /var/lib/tunnelmesh/wireguard
  endpoint: "${wg_endpoint}"
%{ endif ~}
PEERCONF

# Create default context pointing to config file, then install service
# Context name "default" → service name "tunnelmesh" (required by service start in next step)
# TUNNELMESH_SERVER must be exported so service install can embed it in the service environment;
# PeerConfig.Servers is yaml:"-" so the server URL cannot be read from the config file.
export TUNNELMESH_TOKEN="${auth_token}"
export TUNNELMESH_SERVER="${peer_server}"
/usr/local/bin/tunnelmesh context create default --config /etc/tunnelmesh/peer.yaml
echo "y" | /usr/local/bin/tunnelmesh service install --context default
%{ endif ~}
