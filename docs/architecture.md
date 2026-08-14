# LedgerDB v1.0 Architecture

```mermaid
flowchart TB
    C[Client / SQL] --> Q[Raft HTTP Query API]
    C --> W[Raft HTTP Write API]
    Q --> R[RWLock read boundary]
    W --> L[Leader]
    L --> LOG[Durable Raft log + WAL]
    L --> F1[Follower 1]
    L --> F2[Follower 2]
    F1 --> S1[Column store + indexes + ledger]
    F2 --> S2[Column store + indexes + ledger]
    L --> S0[Column store + indexes + ledger]
    S0 --> P[Prometheus]
    S1 --> P
    S2 --> P
    P --> G[Grafana]
    CI[Git push] --> A[GitHub Actions]
    A --> T[Tests: unit + recovery + concurrency + chaos]
    T --> D[Docker build/push]
    D --> K[Kubernetes StatefulSet]
    K --> L
```

## Request path

1. SQL is parsed into a deliberately bounded query AST.
2. The planner selects equality/range indexes, hash aggregation, or sort-merge join.
3. Reads execute under the reader side of the global RW lock.
4. Writes enter through the Raft leader.
5. The leader appends the command to its durable log and replicates it to followers.
6. Each node materializes committed commands into durable column/index/ledger storage.
7. Prometheus scrapes node health, leadership, lag, query latency, and transaction counters.

## Deployment topology

Kubernetes uses a three-replica StatefulSet and a headless Service. StatefulSet identities provide stable Raft peer addresses. Query readiness excludes followers that have lost recent leader contact or have not caught up.
