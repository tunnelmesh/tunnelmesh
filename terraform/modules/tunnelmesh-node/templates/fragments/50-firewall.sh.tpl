# Configure firewall
ufw allow 22/tcp comment 'SSH'

%{ if peer_enabled || coordinator_enabled ~}
ufw allow ${ssh_tunnel_port}/tcp comment 'TunnelMesh SSH'
ufw allow ${ssh_tunnel_port + 1}/udp comment 'TunnelMesh UDP'
%{ endif ~}

%{ if coordinator_enabled ~}
ufw allow 80/tcp comment 'HTTP'
ufw allow ${external_api_port}/tcp comment 'HTTPS API'
%{ if peer_enabled ~}
ufw allow in on tun-mesh to any port 443 proto tcp comment 'Mesh Admin HTTPS'
%{ endif ~}
%{ endif ~}

ufw --force enable
