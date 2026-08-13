# LedgerDB

LedgerDB is a small durable storage engine built as measured systems increments.
Day 1 establishes recovery, rather than merely persistence: every row is first
committed to a fsynced write-ahead log (WAL), then applied to append-only,
disk-backed column files.

## Storage contract

```text
insert row → fsync WAL record → append each column → fsync row watermark
```

The metadata row-count watermark controls which column values are visible. At
startup, LedgerDB replays WAL records after that watermark. Therefore a process
death after a WAL commit but before column application recovers the committed
row exactly once.

Data layout:

```text
data/
  wal/ledger.wal                 append-only committed mutations
  columns/metadata.json          schema + durable visible-row watermark
  columns/<name>.column.jsonl    one append-only file per column
```

## Crash-recovery proof

The test starts a separate Python process, tells it to terminate immediately
after WAL `fsync`, then opens a new engine instance against the same directory.
It asserts both the existing row and the WAL-only row are recovered, and that a
second restart does not duplicate the recovered row.

```powershell
python -m unittest discover -s tests -v
```

## Container lifecycle proof

The multi-stage image exposes `/var/lib/ledgerdb` as a Docker volume, so the
WAL and column files survive container replacement.

```powershell
docker build --tag ledgerdb:day-01 .
docker volume create ledgerdb-day-01
docker run --rm -v ledgerdb-day-01:/var/lib/ledgerdb ledgerdb:day-01 insert '{"account":"cash","amount":100}'
docker run --rm -v ledgerdb-day-01:/var/lib/ledgerdb ledgerdb:day-01 rows
```

Expected final output:

```json
[{"account":"cash","amount":100}]
```

To prove the volume also preserves crash recovery, force the post-WAL crash,
then restart with the same mount:

```powershell
docker run --rm -e LEDGERDB_CRASH_AFTER_WAL=1 -v ledgerdb-day-01:/var/lib/ledgerdb ledgerdb:day-01 insert '{"account":"wal-only","amount":25}'
docker run --rm -v ledgerdb-day-01:/var/lib/ledgerdb ledgerdb:day-01 rows
```

The first command exits `137`; the second reports both `cash` and `wal-only`.
