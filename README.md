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
