# Configure firewall
ufw allow 22/tcp comment 'SSH'

%{ if peer_enabled || coordinator_enabled ~}
ufw allow ${ssh_tunnel_port}/tcp comment 'TunnelMesh SSH'
ufw allow ${ssh_tunnel_port + 1}/udp comment 'TunnelMesh UDP'
%{ endif ~}

%{ if coordinator_enabled ~}
ufw allow 80/tcp comment 'HTTP'
ufw allow ${external_api_port}/tcp comment 'HTTPS API'
%{ endif ~}
%{ if peer_enabled ~}
# tun-mesh traffic is authenticated by Noise protocol — mesh packet filter
# handles per-service access control, no OS-level port filtering needed here
ufw allow in on tun-mesh comment 'Mesh (Noise-authenticated)'
%{ endif ~}

ufw --force enable
