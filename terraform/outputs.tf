output "node_names" {
  value = azurerm_linux_virtual_machine.node[*].name
}

output "node_private_ips" {
  value = azurerm_network_interface.node[*].private_ip_address
}
