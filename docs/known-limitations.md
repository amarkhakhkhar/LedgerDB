# Known Limitations — LedgerDB v1.0

These are deliberate v1.0 boundaries, not hidden features.

- **No sharding.** Every node stores the complete dataset; horizontal partitioning is out of scope.
- **Three-node Raft topology.** The shipped Kubernetes deployment is fixed at three replicas for this milestone.
- **Single-region.** The reference deployment assumes one Kubernetes region/cluster. Cross-region quorum and WAN-aware Raft are out of scope.
- **Subset of SQL.** Supported queries are `SELECT` over `ledger`, equality and `BETWEEN` filters, single-key `GROUP BY` with `SUM`/`COUNT`/`AVG`, and equi-joins. UPDATE/DELETE, subqueries, window functions, CTEs, arbitrary expressions, and full SQL grammar are out of scope.
- **No cost-based optimizer.** Physical planning is rule-based.
- **No snapshots/compaction for Raft logs.** Logs remain append-only for the current milestone.
- **No automatic membership changes.** Raft cluster membership is static.
- **No multi-region failover SLA.** Recovery bounds demonstrated by the local chaos tests are measurements, not production SLAs.
- **Application transactions are intentionally narrow.** The ledger transaction API implements balanced debit/credit posting and idempotency; it is not a general distributed transaction coordinator.
- **Observability is lightweight.** Prometheus text metrics and the supplied Grafana dashboard cover core signals but are not a full production telemetry stack.
