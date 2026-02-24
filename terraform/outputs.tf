# TunnelMesh Terraform Outputs

# ============================================================================
# ALL NODES
# ============================================================================

output "nodes" {
  description = "All deployed nodes with their details"
  value = {
    for name, node in module.node : name => {
      ip              = node.ipv4_address
      hostname        = node.hostname
      ssh             = node.ssh_command
      coordinator_url = node.coordinator_url
    }
  }
}

output "node_ips" {
  description = "Map of node names to IP addresses"
  value       = { for name, node in module.node : name => node.ipv4_address }
}

output "ssh_commands" {
  description = "SSH commands for all nodes"
  value       = { for name, node in module.node : name => node.ssh_command }
}

# ============================================================================
# COORDINATOR
# ============================================================================

output "coordinator_name" {
  description = "Name of the coordinator node"
  value       = local.coordinator_name
}

output "coordinator_url" {
  description = "Coordination server URL"
  value       = local.coordinator_name != null ? module.node[local.coordinator_name].coordinator_url : var.external_coordinator_url
}


# ============================================================================
# CONFIGURATION HELPERS
# ============================================================================

output "auth_token" {
  description = "Authentication token for mesh peers"
  value       = var.auth_token
  sensitive   = true
}

output "peer_config_example" {
  description = "Example peer configuration for connecting to this mesh"
  sensitive   = true
  value = local.coordinator_name != null ? join("\n", [
    "# ~/.tunnelmesh/peer.yaml",
    "name: \"my-device\"",
    "server: \"${module.node[local.coordinator_name].coordinator_url}\"",
    "auth_token: \"${var.auth_token}\"",
    "ssh_port: 2222",
    "private_key: ~/.tunnelmesh/id_ed25519",
    "",
    "tun:",
    "  name: tun-mesh",
    "  mtu: 1400",
    "",
    "dns:",
    "  enabled: true",
    "  listen: \"127.0.0.1:5353\""
  ]) : "# No coordinator deployed - set external_coordinator_url"
}
