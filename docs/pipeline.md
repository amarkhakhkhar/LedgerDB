# LedgerDB v1.0 CI/CD Pipeline

```text
git push
   |
   v
GitHub Actions
   |
   +--> Python install
   +--> unit/integration suite
   +--> crash recovery proof
   +--> concurrency proof
   +--> Raft election + replication
   +--> chaos proof
   +--> Terraform validate
   |
   v
Docker build
   |
   v
GHCR tagged image
   |
   v
Kubernetes StatefulSet
   |
   +--> headless peer discovery
   +--> 3 durable Raft nodes
   +--> readiness/liveness
   |
   v
Prometheus --> Grafana
```

## Local equivalent

```powershell
python -m unittest discover -s tests -v
python benchmarks/concurrent_load.py --readers 6 --writes 100
python benchmarks/raft_chaos_demo.py --duration-seconds 8
python benchmarks/full_benchmark.py --rows 100000
python benchmarks/v1_demo.py
```

## GitHub Actions

Pushes and pull requests run the verification suite. Merges to `main` build and push `ghcr.io/<owner>/<repo>` with immutable `sha-<short-sha>` and `latest` tags. A deployment script then updates the StatefulSet image in a configured Kubernetes cluster. The workflow keeps deployment credentials outside the repository.

## Automatic Kubernetes deployment

The repository supports a gated production deployment in the same workflow. Set the repository variable `K8S_DEPLOY_ENABLED=true` and add a base64-encoded kubeconfig as the `KUBE_CONFIG_DATA` Actions secret. After a successful `main` build, the deploy job applies the StatefulSet/observability manifests, updates the StatefulSet to the immutable commit image, waits for rollout, and prints the three-node cluster and query Service state. With the variable unset, CI still proves build/push without requiring production credentials.
