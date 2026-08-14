param([Parameter(Mandatory=$true)][string]$Image)
$ErrorActionPreference = "Stop"
kubectl apply -f k8s/ledgerdb.yaml
kubectl apply -f k8s/observability.yaml
kubectl set image statefulset/ledgerdb ledgerdb=$Image
kubectl rollout status statefulset/ledgerdb --timeout=180s
kubectl get pods -l app=ledgerdb -o wide
kubectl get endpoints ledgerdb-query
