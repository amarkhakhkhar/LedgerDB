#!/usr/bin/env bash
set -euo pipefail
IMAGE="${1:?usage: deploy.sh ghcr.io/OWNER/REPO:TAG}"
kubectl apply -f k8s/ledgerdb.yaml
kubectl apply -f k8s/observability.yaml
kubectl -n default set image statefulset/ledgerdb ledgerdb="$IMAGE"
kubectl rollout status statefulset/ledgerdb --timeout=180s
kubectl get pods -l app=ledgerdb -o wide
kubectl get endpoints ledgerdb-query
