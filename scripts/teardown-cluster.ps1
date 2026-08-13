$ErrorActionPreference = "Stop"
kubectl delete -f k8s/ledgerdb.yaml --ignore-not-found
