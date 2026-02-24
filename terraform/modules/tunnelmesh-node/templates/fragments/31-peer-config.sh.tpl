%{ if !coordinator_enabled && peer_enabled ~}
# === PEER-ONLY CONFIGURATION ===
cat > /etc/tunnelmesh/peer.yaml <<'PEERCONF'
name: "${node_name}"
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
# Manual geolocation override
geolocation:
  latitude: ${location_latitude}
  longitude: ${location_longitude}
%{ if location_city != "" ~}
  city: "${location_city}"
%{ endif ~}
%{ endif ~}

# TUN interface
tun:
  name: tun-mesh
  mtu: 1400

# DNS resolver
dns:
  listen: "127.0.0.1:5353"

PEERCONF

# TUNNELMESH_SERVER is read by 'service install' and baked into the service's env vars
# so it is available to the join command at runtime without appearing in process listings.
# TUNNELMESH_TOKEN is consumed by 'service install' for join mode (not stored anywhere).
export TUNNELMESH_SERVER="${peer_server}"
export TUNNELMESH_TOKEN="${auth_token}"

# Create default context pointing to config file, then install service
# Context name "default" → service name "tunnelmesh" (required by service start in next step)
/usr/local/bin/tunnelmesh context create default --config /etc/tunnelmesh/peer.yaml
echo "y" | /usr/local/bin/tunnelmesh service install --context default
%{ endif ~}
