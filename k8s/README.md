# LedgerDB 3-node Raft cluster on Kubernetes

The StatefulSet provides stable identities and DNS names required by Raft:

- `ledgerdb-0.ledgerdb:8000`
- `ledgerdb-1.ledgerdb:8000`
- `ledgerdb-2.ledgerdb:8000`

Replace `REPLACE_OWNER` in `ledgerdb.yaml` with the GitHub Container Registry owner that contains the Day 6 image.

## Deploy

```bash
kubectl apply -f k8s/ledgerdb.yaml
kubectl rollout status statefulset/ledgerdb
kubectl get pods -l app=ledgerdb -o wide
```

## Inspect leader election

```bash
kubectl exec ledgerdb-0 -- python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8000/status').read().decode())"
```

Repeat for `ledgerdb-1` and `ledgerdb-2`. Exactly one should report `"role": "leader"` after election converges.

To kill the leader, find its pod from `/status` and run:

```bash
kubectl delete pod ledgerdb-<leader>
```

The StatefulSet recreates the same ordinal identity. The surviving two nodes must elect a leader within the configured election timeout bound.

## Cleanup

```bash
kubectl delete -f k8s/ledgerdb.yaml
```

## Recorded Kubernetes leader-failure proof

After the StatefulSet is running, run from the repository root:

```bash
python benchmarks/raft_k8s_demo.py --bound-seconds 5
```

The script queries all three stable pods, records the current leader, deletes that leader pod, waits for the StatefulSet to recreate it while the surviving quorum elects a replacement, and records the new leader and elapsed election time. A successful run prints `k8s_leader_re_election=PASS`.
