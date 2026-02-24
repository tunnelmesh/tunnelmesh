%{ if peer_enabled ~}
# Enable IP forwarding
cat >> /etc/sysctl.conf <<'SYSCTL'
net.ipv4.ip_forward = 1
net.ipv6.conf.all.forwarding = 1
SYSCTL
sysctl -p
%{ endif ~}
