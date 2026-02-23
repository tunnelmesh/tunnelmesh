# TunnelMesh Node Module Variables
# Supports: coordinator-only, coordinator+peer, peer+wireguard, peer-only

# --- Required Variables ---

variable "name" {
  description = "Node name (used for droplet name, DNS record, and mesh peer name)"
  type        = string
}

variable "domain" {
  description = "Domain name for DNS records"
  type        = string
}

variable "auth_token" {
  description = "Authentication token for the mesh"
  type        = string
  sensitive   = true
}

variable "admin_peers" {
  description = "List of peer names or peer IDs (SHA256 of SSH key, first 8 bytes = 16 hex chars) granted the admins group"
  type        = list(string)
  default     = []
}

variable "region" {
  description = "DigitalOcean region"
  type        = string
}

# --- Feature Flags ---

variable "coordinator_enabled" {
  description = "Run the coordination server"
  type        = bool
  default     = false
}

variable "peer_enabled" {
  description = "Run as a mesh peer"
  type        = bool
  default     = false
}

variable "wireguard_enabled" {
  description = "Enable WireGuard concentrator (requires peer_enabled=true)"
  type        = bool
  default     = false
}

# --- Coordinator Settings ---

variable "coordinator_port" {
  description = "HTTP port for the coordination server (internal)"
  type        = number
  default     = 8080
}

variable "external_api_port" {
  description = "HTTPS port for external API (nginx). Port 443 is reserved for mesh-internal admin."
  type        = number
  default     = 8443
}

variable "locations_enabled" {
  description = "Enable node location tracking (uses external IP geolocation API)"
  type        = bool
  default     = false
}

# --- Peer Settings ---

variable "peer_server_url" {
  description = "Coordination server URL (required if peer_enabled=true and coordinator_enabled=false)"
  type        = string
  default     = ""
}

variable "ssh_tunnel_port" {
  description = "SSH tunnel port for mesh connections"
  type        = number
  default     = 2222
}

# --- Exit Node Settings ---

variable "exit_node" {
  description = "Name of peer to route internet traffic through (split-tunnel VPN)"
  type        = string
  default     = ""
}

variable "allow_exit_traffic" {
  description = "Allow this node to act as an exit node for other peers"
  type        = bool
  default     = false
}

# --- Location Settings ---

variable "location_latitude" {
  description = "Manual GPS latitude for this node (overrides IP geolocation)"
  type        = number
  default     = null
}

variable "location_longitude" {
  description = "Manual GPS longitude for this node (overrides IP geolocation)"
  type        = number
  default     = null
}

variable "location_city" {
  description = "City name for this node location"
  type        = string
  default     = ""
}


# --- WireGuard Settings ---

variable "wg_listen_port" {
  description = "WireGuard UDP listen port"
  type        = number
  default     = 51820
}

variable "wg_endpoint" {
  description = "WireGuard public endpoint (defaults to {name}.{domain}:{wg_listen_port})"
  type        = string
  default     = ""
}

# --- Droplet Settings ---

variable "droplet_size" {
  description = "Droplet size slug"
  type        = string
  default     = "s-1vcpu-512mb-10gb" # $4/month
}

variable "droplet_image" {
  description = "Droplet image slug"
  type        = string
  default     = "ubuntu-24-04-x64"
}

variable "ssh_key_ids" {
  description = "List of SSH key IDs for droplet access"
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Tags to apply to resources"
  type        = list(string)
  default     = ["tunnelmesh"]
}

# --- Binary Settings ---

variable "github_owner" {
  description = "GitHub owner for downloading tunnelmesh binary"
  type        = string
  default     = "zombar"
}

variable "binary_version" {
  description = "TunnelMesh version to install (latest or specific version)"
  type        = string
  default     = "latest"
}

# --- SSL Settings ---

variable "ssl_enabled" {
  description = "Enable SSL via Let's Encrypt (requires coordinator_enabled=true)"
  type        = bool
  default     = true
}

variable "ssl_email" {
  description = "Email for Let's Encrypt certificate notifications"
  type        = string
  default     = ""
}

# --- Auto-Update Settings ---

variable "auto_update_enabled" {
  description = "Enable automatic updates via systemd timer"
  type        = bool
  default     = true
}

variable "auto_update_schedule" {
  description = "Schedule for auto-updates (systemd OnCalendar format: hourly, daily, weekly)"
  type        = string
  default     = "hourly"
}

# --- Monitoring Settings ---

variable "monitoring_enabled" {
  description = "Enable monitoring stack (Prometheus, Grafana, Loki, SD Generator) on coordinator nodes"
  type        = bool
  default     = false
}

variable "prometheus_image_tag" {
  description = "Prometheus Docker image tag (e.g. v3.2.1)"
  type        = string
  default     = "v3.2.1"
}

variable "loki_image_tag" {
  description = "Loki Docker image tag (e.g. 3.5.0)"
  type        = string
  default     = "3.5.0"
}

variable "grafana_image_tag" {
  description = "Grafana Docker image tag (e.g. 11.6.0)"
  type        = string
  default     = "11.6.0"
}

variable "loki_retention_days" {
  description = "Loki log retention in days"
  type        = number
  default     = 3
}

variable "prometheus_retention_days" {
  description = "Prometheus data retention in days"
  type        = number
  default     = 3
}
