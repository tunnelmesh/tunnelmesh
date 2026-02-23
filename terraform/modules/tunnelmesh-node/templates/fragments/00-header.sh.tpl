#!/bin/bash
set -e

# Cloud-init runs without a login shell so HOME may be unset.
# tunnelmesh's context store requires it to locate ~/.tunnelmesh/.
export HOME=/root

echo "=== TunnelMesh Node Setup ==="
echo "Coordinator: ${coordinator_enabled}"
echo "Peer: ${peer_enabled}"
echo "WireGuard: ${wireguard_enabled}"
echo "SSL: ${ssl_enabled}"
%{ if monitoring_enabled ~}
echo "Monitoring: ${monitoring_enabled}"
%{ endif ~}
