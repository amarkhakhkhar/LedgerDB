#!/usr/bin/env bash
set -euo pipefail
kubectl apply -f k8s/ledgerdb.yaml
kubectl rollout status statefulset/ledgerdb --timeout=120s
kubectl wait --for=condition=ready pod/ledgerdb-0 pod/ledgerdb-1 pod/ledgerdb-2 --timeout=120s
python benchmarks/raft_k8s_demo.py --bound-seconds 5
