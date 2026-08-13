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
