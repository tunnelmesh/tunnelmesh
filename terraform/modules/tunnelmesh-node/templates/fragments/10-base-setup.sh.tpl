# Make apt fully non-interactive
export DEBIAN_FRONTEND=noninteractive
export NEEDRESTART_MODE=a

# Update system (keep existing configs on conflicts)
apt-get update
apt-get -o Dpkg::Options::="--force-confold" -o Dpkg::Options::="--force-confdef" upgrade -y

# Install base dependencies
apt-get install -y -q curl wget jq fail2ban

%{ if coordinator_enabled ~}
# Install Docker CE — required for monitoring stack and Docker socket integration.
# Peer-only nodes skip this to avoid the ~200MB overhead unless they run containers.
# Detect distro for the correct apt repo (supports Ubuntu and Debian).
# VERSION_CODENAME from os-release is more reliable than lsb_release on minimal images.
DISTRO=$(. /etc/os-release && echo "$ID")
CODENAME=$(. /etc/os-release && echo "$VERSION_CODENAME")
install -m 0755 -d /etc/apt/keyrings
curl -fsSL "https://download.docker.com/linux/$DISTRO/gpg" \
  | gpg --batch --yes --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/$DISTRO $CODENAME stable" \
  > /etc/apt/sources.list.d/docker.list
apt-get update
apt-get install -y -q docker-ce docker-ce-cli containerd.io docker-compose-plugin
systemctl enable docker
systemctl start docker
%{ endif ~}

# Configure fail2ban for SSH protection
cat > /etc/fail2ban/jail.local <<'FAIL2BAN'
[DEFAULT]
bantime = 1h
findtime = 10m
maxretry = 5
backend = systemd

[sshd]
enabled = true
port = ssh
filter = sshd
logpath = %(sshd_log)s
maxretry = 3
bantime = 1h
FAIL2BAN
systemctl enable fail2ban
systemctl start fail2ban

%{ if coordinator_enabled && ssl_enabled ~}
# Install nginx and certbot for SSL termination
apt-get install -y -q nginx certbot python3-certbot-nginx
%{ endif ~}

# Configure journald for reasonable log retention on small VPS
mkdir -p /etc/systemd/journald.conf.d
cat > /etc/systemd/journald.conf.d/tunnelmesh.conf <<'JOURNALDCONF'
[Journal]
# Limit persistent log storage (reasonable for small VPS)
SystemMaxUse=200M
SystemKeepFree=100M
SystemMaxFileSize=50M
# Limit runtime logs
RuntimeMaxUse=50M
# Keep logs for max 3 days
MaxRetentionSec=3day
# Compress logs
Compress=yes
JOURNALDCONF
systemctl restart systemd-journald

# Create tunnelmesh directories
mkdir -p /etc/tunnelmesh
mkdir -p /var/lib/tunnelmesh

