# TunnelMesh Node Module Outputs

output "droplet_id" {
  description = "The ID of the droplet"
  value       = digitalocean_droplet.node.id
}

output "ipv4_address" {
  description = "The static reserved IPv4 address"
  value       = digitalocean_reserved_ip.node.ip_address
}

output "reserved_ip" {
  description = "The reserved IP address (static)"
  value       = digitalocean_reserved_ip.node.ip_address
}

output "droplet_ipv4_address" {
  description = "The dynamic IPv4 address of the droplet (use reserved_ip for stable addressing)"
  value       = digitalocean_droplet.node.ipv4_address
}

output "ipv6_address" {
  description = "The public IPv6 address of the droplet"
  value       = digitalocean_droplet.node.ipv6_address
}

output "hostname" {
  description = "The full hostname of the node"
  value       = "${var.name}.${var.domain}"
}

output "ssh_command" {
  description = "SSH command to connect to the droplet"
  value       = "ssh root@${digitalocean_reserved_ip.node.ip_address}"
}

# Coordinator-specific outputs
output "coordinator_url" {
  description = "URL of the coordination server (if enabled)"
  value       = var.coordinator_enabled ? (var.ssl_enabled ? "https://${var.name}.${var.domain}" : "http://${var.name}.${var.domain}:${var.coordinator_port}") : null
}

output "coordinator_api_url" {
  description = "API URL for peers to connect to"
  value       = var.coordinator_enabled ? (var.ssl_enabled ? "https://${var.name}.${var.domain}" : "http://${digitalocean_reserved_ip.node.ip_address}:${var.coordinator_port}") : null
}

# Peer-specific outputs
output "peer_name" {
  description = "The mesh peer name"
  value       = var.peer_enabled || var.coordinator_enabled ? var.name : null
}

# Exit node outputs
output "is_exit_node" {
  description = "Whether this node allows exit traffic"
  value       = var.allow_exit_traffic
}

output "exit_node" {
  description = "Name of the exit node this peer routes through (if configured)"
  value       = var.exit_node != "" ? var.exit_node : null
}
