# TunnelMesh Terraform Variables

# ============================================================================
# REQUIRED
# ============================================================================

variable "do_token" {
  description = "DigitalOcean API token"
  type        = string
  sensitive   = true
}

variable "domain" {
  description = "Base domain name (must be managed in DigitalOcean DNS)"
  type        = string
}

variable "auth_token" {
  description = "Authentication token for mesh peers (generate with: openssl rand -hex 32)"
  type        = string
  sensitive   = true
}

variable "admin_peers" {
  description = "List of peer names or peer IDs granted admin role (e.g., [\"honker\", \"abc123def456...\"]). Peer IDs (SHA256 of SSH key, first 16 hex chars) are preferred for security."
  type        = list(string)
  default     = []
}

# ============================================================================
# NODE DEFINITIONS
# ============================================================================

variable "peers" {
  description = <<-EOF
    Map of nodes to deploy. Each node can have:
    - coordinator: bool - Run coordination server (multiple allowed for replication)
    - peer: bool - Run as mesh peer (default: true, set false for coordinator-only)
    - region: string - Override default region
    - size: string - Override default droplet size
    - ssh_port: number - Override SSH tunnel port
    - exit_node: string - Route internet traffic through this peer (split-tunnel VPN)
    - allow_exit_traffic: bool - Allow this node to be an exit node for other peers
    - location: object - Manual GPS location { latitude, longitude, city, country }
    - tags: list(string) - Additional tags
  EOF
  type        = map(any)
  default = {
    # Default: all-in-one coordinator + peer
    "tunnelmesh" = {
      coordinator = true
      peer        = true
    }
  }

}

variable "external_coordinator_url" {
  description = "URL of external coordinator (if not deploying one)"
  type        = string
  default     = ""
}

# ============================================================================
# DEFAULT SETTINGS (can be overridden per-node)
# ============================================================================

variable "default_region" {
  description = "Default DigitalOcean region"
  type        = string
  default     = "ams3"
}

variable "default_droplet_size" {
  description = "Default droplet size"
  type        = string
  default     = "s-1vcpu-1gb" # $6/month
}

variable "default_ssh_port" {
  description = "Default SSH tunnel port"
  type        = number
  default     = 2222
}

variable "external_api_port" {
  description = "HTTPS port for external coordinator API. Port 443 is reserved for mesh-internal admin."
  type        = number
  default     = 8443
}

# ============================================================================
# GLOBAL SETTINGS
# ============================================================================

variable "locations_enabled" {
  description = "Enable node location tracking (uses external IP geolocation API)"
  type        = bool
  default     = false
}

variable "ssh_key_name" {
  description = "Name of SSH key in DigitalOcean (for droplet access)"
  type        = string
  default     = ""
}

variable "github_owner" {
  description = "GitHub owner for downloading tunnelmesh binary"
  type        = string
  default     = "zombar"
}

variable "binary_version" {
  description = "TunnelMesh version to install"
  type        = string
  default     = "latest"
}

variable "ssl_email" {
  description = "Email for SSL certificate notifications"
  type        = string
  default     = ""
}

variable "zerossl_eab_kid" {
  description = "ZeroSSL EAB Key ID (from app.zerossl.com > Developer > ACME Credentials). When set, ZeroSSL is used instead of Let's Encrypt."
  type        = string
  default     = ""
  sensitive   = true
}

variable "zerossl_eab_hmac_key" {
  description = "ZeroSSL EAB HMAC Key (from app.zerossl.com > Developer > ACME Credentials)."
  type        = string
  default     = ""
  sensitive   = true
}

variable "auto_update_enabled" {
  description = "Enable automatic updates on all nodes"
  type        = bool
  default     = true
}

variable "auto_update_schedule" {
  description = "Schedule for auto-updates (systemd OnCalendar format: hourly, daily, weekly)"
  type        = string
  default     = "hourly"
}

# ============================================================================
# MONITORING SETTINGS
# ============================================================================

variable "monitoring_enabled" {
  description = "Enable monitoring stack (Prometheus, Grafana, Loki) on coordinator"
  type        = bool
  default     = false
}

variable "prometheus_retention_days" {
  description = "Prometheus data retention in days"
  type        = number
  default     = 3
}

variable "loki_retention_days" {
  description = "Loki log retention in days"
  type        = number
  default     = 3
}
