# TunnelMesh Infrastructure
#
# Deploy any number of TunnelMesh nodes with varying configurations.
# Define your nodes in the `nodes` variable - each node can have its own settings.
#
# Example configurations in terraform.tfvars:
#
# peers = {
#   # All-in-one: coordinator + peer + wireguard
#   "tunnelmesh" = {
#     coordinator = true
#     peer        = true
#     wireguard   = true
#   }
#
#   # Additional WireGuard peer in different region
#   "tm-eu" = {
#     peer      = true
#     wireguard = true
#     region    = "fra1"
#   }
#
#   # Exit node in Asia (allows other peers to route internet traffic through it)
#   "tm-asia" = {
#     peer               = true
#     region             = "sgp1"
#     allow_exit_traffic = true
#     location = {
#       latitude  = 1.3521
#       longitude = 103.8198
#       city      = "Singapore"
#       country   = "Singapore"
#     }
#   }
#
#   # Client that routes internet traffic through tm-asia exit node
#   "tm-client" = {
#     peer      = true
#     region    = "lon1"
#     exit_node = "tm-asia"  # Route internet through Singapore exit
#   }
# }

terraform {
  required_version = ">= 1.0"

  required_providers {
    digitalocean = {
      source  = "digitalocean/digitalocean"
      version = "~> 2.0"
    }
  }
}

provider "digitalocean" {
  token = var.do_token
}

# Look up SSH key if specified
data "digitalocean_ssh_key" "main" {
  count = var.ssh_key_name != "" ? 1 : 0
  name  = var.ssh_key_name
}

locals {
  ssh_key_ids = var.ssh_key_name != "" ? [data.digitalocean_ssh_key.main[0].id] : []

  # Find the coordinator node (there should be exactly one if any nodes need it)
  coordinator_name = one([for name, cfg in var.peers : name if lookup(cfg, "coordinator", false)])
  # External API on configurable port (port 443 reserved for mesh-internal admin)
  coordinator_url = local.coordinator_name != null ? "https://${local.coordinator_name}.${var.domain}:${var.external_api_port}" : var.external_coordinator_url

  # Monitoring requires at least 1 GB RAM; upgrade nodes that are below this threshold
  monitoring_min_size = "s-1vcpu-1gb"
  sub_1gb_sizes       = toset(["s-1vcpu-512mb-10gb"])
}

# Deploy all nodes using the tunnelmesh-node module
module "node" {
  source   = "./modules/tunnelmesh-node"
  for_each = var.peers

  name        = each.key
  domain      = var.domain
  auth_token  = var.auth_token
  admin_peers = var.admin_peers
  region      = lookup(each.value, "region", var.default_region)

  # Feature flags from node config
  coordinator_enabled = lookup(each.value, "coordinator", false)
  peer_enabled        = lookup(each.value, "peer", false)
  wireguard_enabled   = lookup(each.value, "wireguard", false)

  # Coordinator settings
  locations_enabled = lookup(each.value, "locations_enabled", var.locations_enabled)
  external_api_port = var.external_api_port

  # Peer server URL (for non-coordinator nodes)
  # If this node is the coordinator, it connects to localhost
  # Otherwise, connect to the coordinator node or external URL
  peer_server_url = lookup(each.value, "coordinator", false) ? "" : local.coordinator_url

  # Exit node settings
  exit_node          = lookup(each.value, "exit_node", "")
  allow_exit_traffic = lookup(each.value, "allow_exit_traffic", false)

  # Location settings (manual GPS override)
  location_latitude  = try(each.value.location.latitude, null)
  location_longitude = try(each.value.location.longitude, null)
  location_city      = try(each.value.location.city, "")

  # WireGuard settings
  wg_listen_port = lookup(each.value, "wg_port", var.default_wg_port)
  wg_endpoint    = lookup(each.value, "wireguard", false) ? "${each.key}.${var.domain}:${lookup(each.value, "wg_port", var.default_wg_port)}" : ""

  # Droplet settings
  # Monitoring requires ≥1 GB RAM; upgrade sub-1 GB nodes automatically
  droplet_size = (
    lookup(each.value, "monitoring_enabled", false) &&
    contains(local.sub_1gb_sizes, lookup(each.value, "size", var.default_droplet_size))
    ? local.monitoring_min_size
    : lookup(each.value, "size", var.default_droplet_size)
  )
  ssh_key_ids     = local.ssh_key_ids
  ssh_tunnel_port = lookup(each.value, "ssh_port", var.default_ssh_port)

  # Binary settings
  github_owner   = var.github_owner
  binary_version = var.binary_version

  # SSL (only for coordinator)
  ssl_enabled = lookup(each.value, "coordinator", false)
  ssl_email   = var.ssl_email

  # Auto-update settings
  auto_update_enabled  = var.auto_update_enabled
  auto_update_schedule = var.auto_update_schedule

  # Monitoring (coordinator only)
  monitoring_enabled        = lookup(each.value, "coordinator", false) && lookup(each.value, "monitoring_enabled", var.monitoring_enabled)
  prometheus_retention_days = var.prometheus_retention_days
  loki_retention_days       = var.loki_retention_days

  tags = concat(
    ["tunnelmesh"],
    lookup(each.value, "coordinator", false) ? ["coordinator"] : [],
    lookup(each.value, "peer", false) ? ["peer"] : [],
    lookup(each.value, "wireguard", false) ? ["wireguard"] : [],
    lookup(each.value, "allow_exit_traffic", false) ? ["exit-node"] : [],
    try(each.value.tags, [])
  )
}
