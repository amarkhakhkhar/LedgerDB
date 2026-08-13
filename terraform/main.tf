resource "azurerm_resource_group" "ledgerdb" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_virtual_network" "ledgerdb" {
  name                = "ledgerdb-vnet"
  address_space       = ["10.42.0.0/16"]
  location            = azurerm_resource_group.ledgerdb.location
  resource_group_name = azurerm_resource_group.ledgerdb.name
}

resource "azurerm_subnet" "ledgerdb" {
  name                 = "ledgerdb-subnet"
  resource_group_name  = azurerm_resource_group.ledgerdb.name
  virtual_network_name = azurerm_virtual_network.ledgerdb.name
  address_prefixes     = ["10.42.1.0/24"]
}

resource "azurerm_network_security_group" "ledgerdb" {
  name                = "ledgerdb-nsg"
  location            = azurerm_resource_group.ledgerdb.location
  resource_group_name = azurerm_resource_group.ledgerdb.name

  security_rule {
    name                       = "ssh"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
}

resource "azurerm_network_interface" "node" {
  count               = 3
  name                = "ledgerdb-node-${count.index + 1}-nic"
  location            = azurerm_resource_group.ledgerdb.location
  resource_group_name = azurerm_resource_group.ledgerdb.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.ledgerdb.id
    private_ip_address_allocation = "Dynamic"
  }
}

resource "azurerm_network_interface_security_group_association" "node" {
  count                     = 3
  network_interface_id      = azurerm_network_interface.node[count.index].id
  network_security_group_id = azurerm_network_security_group.ledgerdb.id
}

resource "azurerm_linux_virtual_machine" "node" {
  count               = 3
  name                = "ledgerdb-node-${count.index + 1}"
  resource_group_name = azurerm_resource_group.ledgerdb.name
  location            = azurerm_resource_group.ledgerdb.location
  size                = var.vm_size
  admin_username      = var.admin_username
  network_interface_ids = [
    azurerm_network_interface.node[count.index].id
  ]
  disable_password_authentication = true

  admin_ssh_key {
    username   = var.admin_username
    public_key = var.ssh_public_key
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "Standard_LRS"
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "ubuntu-24_04-lts"
    sku       = "server"
    version   = "latest"
  }

  tags = {
    project = "ledgerdb"
    role    = "raft-node"
  }
}
