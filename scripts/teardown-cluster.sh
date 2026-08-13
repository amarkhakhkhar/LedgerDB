#!/usr/bin/env bash
set -euo pipefail
kubectl delete -f k8s/ledgerdb.yaml --ignore-not-found
