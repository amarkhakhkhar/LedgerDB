# LedgerDB

LedgerDB is a small durable storage engine built as measured systems increments.

## Day 3: persistent equality indexes

LedgerDB now maintains a hash-based inverted index for equality predicates:

```text
column value -> row IDs

account="cash" -> [0, 2, 7, ...]
```

`filter_eq(column, value)` uses the index by default. Passing
`use_index=False` performs the full-column scan baseline used by the benchmark.

The index is persisted directly under `data/indexes/` as JSONL. We deliberately
removed per-row `fsync` from index updates: the WAL remains the durability source
of truth, while the derived index is flushed at controlled persistence points.
On restart, WAL recovery runs first. If an index is missing, corrupt, or has a
row count different from the durable column watermark, it is rebuilt from the
column data. This makes a WAL-only row visible in the index immediately after
recovery without making every insert pay for an index `fsync`.

The benchmark uses batched setup so the measured section contains only the
filtered query itself, not one million durable setup writes:

```powershell
python benchmarks/filter_benchmark.py --rows 1000000 --distinct 10000 --target 42
```

It proves that indexed and full-scan results are identical and reports the
query speedup.

## CI

![CI](https://github.com/OWNER/REPOSITORY/actions/workflows/ci.yml/badge.svg)

GitHub Actions runs the complete test suite on every push and pull request. The
suite includes the original subprocess crash-recovery proof and the Day 3
index-correctness-after-crash proof. A regression in either guarantee fails CI.

## Storage contract

```text
insert row -> fsync WAL -> append columns -> update derived equality index
                                             -> controlled index flush
```

The WAL and column watermark remain authoritative. The equality index is an
acceleration structure that can always be reconstructed.

## Day 4: concurrency and sort-merge join

LedgerDB now uses a writer-preference reader-writer lock around its commit and query boundaries. Multiple readers may run concurrently, while a writer gets exclusive access to the WAL, column store, and derived equality indexes. A reader therefore observes either the state before a commit or the fully committed state, never a half-committed combination of column and index data.

`sort_merge_join(left, right, left_column, right_column)` acquires read locks for both databases in deterministic object order, preventing cross-database lock-order deadlocks. It sorts both inputs on the join key and merges equal-key runs. Duplicate keys produce all matching pairs, with result fields prefixed by `left.` and `right.`.

The concurrency proof runs multiple reader threads while a writer repeatedly commits batches:

```powershell
python -m unittest tests.test_concurrency -v
```

The readers validate indexed and full-scan equality under one read lock and verify that every observed row has a complete schema. The join stress test also verifies that concurrent writes cannot leak partially committed rows into a join.

## CI/CD

CI runs the full test suite plus the explicit concurrent-load proof. Successful pushes to `main` then build and push a Docker image to GitHub Container Registry (GHCR), tagged with the commit SHA and `latest`. The workflow uses the built-in `GITHUB_TOKEN` with `packages: write`; no long-lived registry secret is required.

```text
push / pull request
        -> full tests
        -> concurrency stress test
        -> pass
        -> merge to main
        -> Docker build
        -> GHCR push: sha-<commit> and latest
```

## Day 5: double-entry ledger and crash-safe transactions

`post_transaction()` writes a balanced debit/credit pair as one transaction. The
transaction WAL stores the complete pair before either ledger entry is
materialized. Recovery replays any WAL transaction that is not already fully
materialized, so a process killed after the first ledger entry still recovers a
complete balanced pair.

Idempotency keys are persisted with each ledger entry. Repeating a transaction
with the same key returns the original transaction ID and does not append a
second pair.

The crash proof is executable with:

```powershell
python tests/test_day5_crash.py
```

It kills a subprocess after the first side of a transaction has been persisted,
reopens the database, and asserts `total_debits == total_credits` and exactly one
transaction pair. The full test suite also covers retries and restart recovery.

The simplified suspicious-key validator uses a 3Sum/4Sum-style complement
search. It flags unique combinations of persisted transaction keys that reach a
configured target; it is an algorithmic demonstration, not a production fraud
model.

## Day 5: Terraform for the Day 6 Raft cluster

`terraform/` provisions exactly three Azure Linux VMs plus a shared virtual
network, subnet, NICs, and SSH access controls. No cloud resources are created
by CI. From a clean checkout, use `terraform init`, `terraform fmt -check`,
`terraform validate`, `terraform plan`, `terraform apply`, and finally
`terraform destroy` as documented in `terraform/README.md`.


## Day 6 — Raft leader election

LedgerDB now contains a focused Raft leader-election layer. Each node exposes HTTP endpoints for `RequestVote`, heartbeat `AppendEntries`, and `/status`. Election timeouts are randomized within a fixed bounded window and leaders send periodic heartbeats. Term and `voted_for` state are persisted atomically. This increment deliberately stops at leader election; log replication and commit-index advancement are Day 7 work.

Run a local three-process election proof:

```bash
python benchmarks/raft_election_demo.py --bound-seconds 3
```

The demo records the initial leader, kills it, and records the replacement leader plus elapsed election time.

### Kubernetes

`k8s/ledgerdb.yaml` deploys three LedgerDB nodes as a StatefulSet behind a headless Service. StatefulSet ordinals provide stable identities and DNS names required by Raft. See `k8s/README.md` for deployment and cleanup commands. Replace the GHCR owner placeholder with the repository owner containing the Day 6 image.

## Day 7: Raft log replication and automatic peer discovery

The leader now persists application commands in `raft-log.jsonl` and replicates
those entries with `AppendEntries`. Each follower persists an entry before
acknowledging it and applies the command to its local LedgerDB. The leader
tracks `next_index` and `match_index` per follower and retries from the missing
prefix after a reconnect. `/status` exposes current, average, and maximum
replication lag over a rolling 60-sample window; `/state` exposes a deterministic
log digest for convergence proofs.

Run the real local follower-failure proof:

```powershell
python benchmarks/raft_replication_demo.py
```

The proof starts three processes, elects a leader, writes five entries, kills a
follower, writes twenty more entries, restarts the follower, and asserts that its
Raft log digest and materialized LedgerDB rows exactly equal the leader.

Kubernetes uses the headless `ledgerdb` Service and StatefulSet DNS identities.
The node derives peers from `RAFT_PEER_SERVICE` and `RAFT_REPLICAS`, so no
manual peer list is required. Use `scripts/bootstrap-cluster.sh` (or the
PowerShell equivalent) to recreate the cluster and verify automatic election;
use the teardown script to remove it.
