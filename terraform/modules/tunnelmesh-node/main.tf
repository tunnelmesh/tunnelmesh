# TunnelMesh Node Module
# Deploys a DigitalOcean droplet configured as coordinator, peer, or both

terraform {
  required_providers {
    digitalocean = {
      source  = "digitalocean/digitalocean"
      version = "~> 2.0"
    }
  }
}

locals {
  # Computed values
  wg_endpoint = var.wg_endpoint != "" ? var.wg_endpoint : "${var.name}.${var.domain}:${var.wg_listen_port}"

  # Server URL for peer mode
  # If coordinator is also enabled, peer connects to localhost
  # Otherwise, use the provided peer_server_url
  peer_server = var.coordinator_enabled ? "http://127.0.0.1:${var.coordinator_port}" : var.peer_server_url

  # Determine which services to run
  run_coordinator = var.coordinator_enabled
  run_peer        = var.peer_enabled || (var.coordinator_enabled && var.peer_enabled)
  # WireGuard concentrator only runs on peer nodes (coordinator just needs admin panel)
  run_wireguard_concentrator = var.wireguard_enabled && var.peer_enabled

  # Tags based on enabled features
  feature_tags = concat(
    var.tags,
    var.coordinator_enabled ? ["coordinator"] : [],
    var.peer_enabled ? ["peer"] : [],
    var.wireguard_enabled ? ["wireguard"] : [],
    var.monitoring_enabled ? ["monitoring"] : []
  )

  # SSL email defaults to admin@domain if not specified
  ssl_email = var.ssl_email != "" ? var.ssl_email : "admin@${var.domain}"

  # Common template variables for all fragments
  common_vars = {
    coordinator_enabled = var.coordinator_enabled
    peer_enabled        = var.peer_enabled
    wireguard_enabled   = var.wireguard_enabled
    ssl_enabled         = var.ssl_enabled && var.coordinator_enabled
    monitoring_enabled  = var.monitoring_enabled

    # Names and domains
    node_name     = var.name
    domain        = var.domain

    # Coordinator settings
    coordinator_port  = var.coordinator_port
    external_api_port = var.external_api_port
    relay_enabled     = var.relay_enabled
    auth_token        = var.auth_token
    locations_enabled = var.locations_enabled

    # Peer settings
    peer_server     = local.peer_server
    ssh_tunnel_port = var.ssh_tunnel_port

    # Exit node settings
    exit_node          = var.exit_node
    allow_exit_traffic = var.allow_exit_traffic

    # Location settings
    location_latitude  = var.location_latitude
    location_longitude = var.location_longitude
    location_city      = var.location_city
    location_country   = var.location_country

    # WireGuard settings
    wg_listen_port = var.wg_listen_port
    wg_endpoint    = local.wg_endpoint

    # Binary settings
    github_owner   = var.github_owner
    binary_version = var.binary_version

    # SSL settings
    ssl_email = local.ssl_email

    # Auto-update settings
    auto_update_enabled  = var.auto_update_enabled
    auto_update_schedule = var.auto_update_schedule
  }

  # Monitoring-specific variables
  monitoring_vars = merge(local.common_vars, {
    prometheus_url            = var.monitoring_enabled ? "http://localhost:9090" : ""
    grafana_url               = var.monitoring_enabled ? "http://localhost:3000" : ""
    prometheus_retention_days = var.prometheus_retention_days
    loki_retention_days       = var.loki_retention_days
    loki_enabled              = var.monitoring_enabled
  })
}

# The droplet
resource "digitalocean_droplet" "node" {
  name     = var.name
  region   = var.region
  size     = var.droplet_size
  image    = var.droplet_image
  ssh_keys = var.ssh_key_ids

  monitoring = true
  tags       = local.feature_tags

  user_data = join("\n", [
    templatefile("${path.module}/templates/fragments/00-header.sh.tpl", local.common_vars),
    templatefile("${path.module}/templates/fragments/10-base-setup.sh.tpl", local.common_vars),
    templatefile("${path.module}/templates/fragments/20-binary-download.sh.tpl", local.common_vars),

    # Coordinator config (includes monitoring URLs when enabled)
    var.coordinator_enabled ? templatefile("${path.module}/templates/fragments/30-coordinator-config.sh.tpl", local.monitoring_vars) : "",

    # Peer-only config (when coordinator not enabled)
    templatefile("${path.module}/templates/fragments/31-peer-config.sh.tpl", local.common_vars),

    # Nginx/SSL (coordinator only)
    var.coordinator_enabled && var.ssl_enabled ? templatefile("${path.module}/templates/fragments/40-nginx-ssl.sh.tpl", local.common_vars) : "",

    # Firewall
    templatefile("${path.module}/templates/fragments/50-firewall.sh.tpl", local.common_vars),

    # Sysctl (IP forwarding)
    templatefile("${path.module}/templates/fragments/60-sysctl.sh.tpl", local.common_vars),

    # Service start
    templatefile("${path.module}/templates/fragments/70-service-install.sh.tpl", local.common_vars),

    # Monitoring (coordinator only, when enabled)
    var.coordinator_enabled && var.monitoring_enabled ? templatefile("${path.module}/templates/fragments/80-monitoring/monitoring-docker-compose.sh.tpl", local.monitoring_vars) : "",

    # SSL certificate (after services are running)
    var.coordinator_enabled && var.ssl_enabled ? templatefile("${path.module}/templates/fragments/90-ssl-cert.sh.tpl", local.common_vars) : "",

    # Auto-update timer
    templatefile("${path.module}/templates/fragments/99-auto-update.sh.tpl", local.common_vars),

    "echo '=== TunnelMesh Node Setup Complete ==='",
  ])
}

# Reserved IP for static addressing
resource "digitalocean_reserved_ip" "node" {
  region = var.region
}

# Assign the reserved IP to the droplet
resource "digitalocean_reserved_ip_assignment" "node" {
  ip_address = digitalocean_reserved_ip.node.ip_address
  droplet_id = digitalocean_droplet.node.id
}

# Firewall
resource "digitalocean_firewall" "node" {
  name        = "${var.name}-firewall"
  droplet_ids = [digitalocean_droplet.node.id]

  # SSH access
  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = ["0.0.0.0/0", "::/0"]
  }

  # TunnelMesh SSH tunnel port (if peer enabled)
  dynamic "inbound_rule" {
    for_each = var.peer_enabled || var.coordinator_enabled ? [1] : []
    content {
      protocol         = "tcp"
      port_range       = tostring(var.ssh_tunnel_port)
      source_addresses = ["0.0.0.0/0", "::/0"]
    }
  }

  # HTTP (for SSL cert issuance and redirect)
  dynamic "inbound_rule" {
    for_each = var.coordinator_enabled ? [1] : []
    content {
      protocol         = "tcp"
      port_range       = "80"
      source_addresses = ["0.0.0.0/0", "::/0"]
    }
  }

  # HTTPS (external coordinator API)
  dynamic "inbound_rule" {
    for_each = var.coordinator_enabled ? [1] : []
    content {
      protocol         = "tcp"
      port_range       = tostring(var.external_api_port)
      source_addresses = ["0.0.0.0/0", "::/0"]
    }
  }

  # WireGuard UDP (only for peer nodes running concentrator)
  dynamic "inbound_rule" {
    for_each = local.run_wireguard_concentrator ? [1] : []
    content {
      protocol         = "udp"
      port_range       = tostring(var.wg_listen_port)
      source_addresses = ["0.0.0.0/0", "::/0"]
    }
  }

  # UDP transport port (SSH port + 1)
  dynamic "inbound_rule" {
    for_each = var.peer_enabled || var.coordinator_enabled ? [1] : []
    content {
      protocol         = "udp"
      port_range       = tostring(var.ssh_tunnel_port + 1)
      source_addresses = ["0.0.0.0/0", "::/0"]
    }
  }

  # Allow all outbound
  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}

# DNS record (points to reserved IP for stability)
resource "digitalocean_record" "node" {
  domain = var.domain
  type   = "A"
  name   = var.name
  value  = digitalocean_reserved_ip.node.ip_address
  ttl    = 300
}
