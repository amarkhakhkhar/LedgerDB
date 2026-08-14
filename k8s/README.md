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

## Day 8 SQL and Raft-aware query readiness

`POST /query` accepts SQL text and returns both the physical plan and rows. The
supported subset is `SELECT`, equality and `BETWEEN` predicates, one-key
`GROUP BY` with `SUM`/`COUNT`/`AVG`, and equi-`JOIN` of `ledger` to itself.
The normal `ledgerdb-query` ClusterIP Service includes only ready endpoints;
the headless `ledgerdb` Service continues to publish all peers for Raft DNS.

`/livez` only confirms that the node process is serving HTTP. `/readyz` returns
200 only for a leader, or for a follower with fresh leader contact whose local
log is caught up to the leader's reported log index. Kubernetes therefore does
not send query-Service traffic to a lagging replica.

Run the live Kubernetes catch-up proof after deployment:

```powershell
python benchmarks/raft_readiness_k8s_demo.py --namespace default
```

It temporarily enables a 100 ms replay delay, deletes one follower, writes 40
records while it is absent, and proves that its EndpointSlice entry is removed
while lagging and restored only after it catches up.

## Day 9 chaos metrics and dashboard

Deploy the self-contained Prometheus and Grafana resources after the LedgerDB
StatefulSet is running:

```powershell
kubectl apply -f k8s/observability.yaml
kubectl port-forward service/ledgerdb-grafana 3000:3000
```

Open `http://localhost:3000` and sign in as `admin` / `admin`. The provisioned
**LedgerDB Day 9 — Raft Chaos** dashboard shows leader-election counter spikes,
per-peer replication lag, the current leader, and mean query latency. Start a
live pod-kill run in another terminal:

```powershell
while ($true) { kubectl delete pod ("ledgerdb-" + (Get-Random -Minimum 0 -Maximum 3)) --wait=false; Start-Sleep -Seconds 12 }
```

Use `Ctrl+C` to stop the loop. This targets one pod at a time, leaving a quorum.
