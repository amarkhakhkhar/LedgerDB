# LedgerDB Day 7 Kubernetes bootstrap

The headless `ledgerdb` Service provides stable StatefulSet DNS identities. A
fresh cluster needs no manually generated peer list: each node derives
`node-0`, `node-1`, and `node-2` from `RAFT_PEER_SERVICE` and `RAFT_REPLICAS`.

Set the image before applying:

```powershell
(Get-Content k8s/ledgerdb.yaml) -replace 'ghcr.io/REPLACE_OWNER/ledgerdb:latest','ghcr.io/YOUR_GITHUB_OWNER/ledgerdb:latest' | Set-Content k8s/ledgerdb.local.yaml
kubectl apply -f k8s/ledgerdb.local.yaml
```

Or use the bootstrap scripts after replacing the image once in the manifest:

```bash
./scripts/recreate-cluster.sh
./scripts/teardown-cluster.sh
```

PowerShell:

```powershell
.\scripts\recreate-cluster.ps1
.\scripts\teardown-cluster.ps1
```

The bootstrap waits for all three stable pods and then runs the leader-election
proof. The script does not claim a live Kubernetes proof unless `kubectl` is
connected to a real cluster.
