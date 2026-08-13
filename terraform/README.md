# LedgerDB Day 5 infrastructure

This Terraform configuration provisions exactly three Azure Linux VMs for the Day 6 three-node Raft cluster.

## Prerequisites

- Terraform >= 1.6
- Azure CLI authenticated with `az login`, or an equivalent AzureRM service-principal environment
- An SSH public key
- An Azure subscription with permission to create resource groups, networking, and VMs

## Commands

```powershell
cd terraform
terraform init
terraform fmt -check
terraform validate
terraform plan -var="ssh_public_key=$(Get-Content $HOME/.ssh/id_ed25519.pub -Raw)"
terraform apply -var="ssh_public_key=$(Get-Content $HOME/.ssh/id_ed25519.pub -Raw)"
terraform output
terraform destroy -var="ssh_public_key=$(Get-Content $HOME/.ssh/id_ed25519.pub -Raw)"
```

Alternatively copy `terraform.tfvars.example` to `terraform.tfvars`, replace the SSH key, and run `terraform apply` / `terraform destroy` without repeating variables.

Terraform state should not be committed. Cloud resources are intentionally not created by normal CI; `apply` and `destroy` are operator actions because they incur cloud costs.
