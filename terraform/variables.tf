variable "location" {
  description = "Azure region for the three LedgerDB nodes."
  type        = string
  default     = "East US"
}

variable "resource_group_name" {
  description = "Resource group for the Raft cluster infrastructure."
  type        = string
  default     = "ledgerdb-raft-rg"
}

variable "admin_username" {
  description = "Linux admin username."
  type        = string
  default     = "ledgerdb"
}

variable "ssh_public_key" {
  description = "SSH public key used to access the VMs."
  type        = string
}

variable "vm_size" {
  description = "Azure VM size for each Raft node."
  type        = string
  default     = "Standard_B1s"
}
