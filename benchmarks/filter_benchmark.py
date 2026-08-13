"""Benchmark equality filtering with and without the persistent hash index."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ledgerdb import LedgerDB


def build_fixture(root: Path, rows: int, distinct: int, chunk_size: int) -> None:
    """Create durable column files efficiently; query setup is not benchmarked."""
    columns = root / "columns"
    columns.mkdir(parents=True, exist_ok=True)
    (root / "wal").mkdir(parents=True, exist_ok=True)
    (root / "indexes").mkdir(parents=True, exist_ok=True)
    (root / "wal" / "ledger.wal").touch()
    (columns / "metadata.json").write_text(
        json.dumps({"columns": ["key", "value"], "row_count": rows}, separators=(",", ":")),
        encoding="utf-8",
    )
    key_path = columns / "key.column.jsonl"
    value_path = columns / "value.column.jsonl"
    with key_path.open("wb") as key_file, value_path.open("wb") as value_file:
        for start in range(0, rows, chunk_size):
            stop = min(start + chunk_size, rows)
            key_file.write(b"".join(f"{row_id % distinct}\n".encode() for row_id in range(start, stop)))
            value_file.write(b"".join(f"{row_id}\n".encode() for row_id in range(start, stop)))
        key_file.flush()
        value_file.flush()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--distinct", type=int, default=10_000)
    parser.add_argument("--target", type=int, default=42)
    parser.add_argument("--chunk-size", type=int, default=100_000)
    args = parser.parse_args()
    if args.rows < 1 or args.distinct < 1 or not 0 <= args.target < args.distinct:
        raise SystemExit("rows/distinct must be positive and target must be in [0, distinct)")

    root = Path(tempfile.mkdtemp(prefix="ledgerdb-filter-bench-"))
    try:
        build_fixture(root, args.rows, args.distinct, args.chunk_size)
        db = LedgerDB(root)
        db.close()
        db = LedgerDB(root)

        started = time.perf_counter()
        scan = db.filter_eq("key", args.target, use_index=False)
        scan_seconds = time.perf_counter() - started

        started = time.perf_counter()
        indexed = db.filter_eq("key", args.target, use_index=True)
        index_seconds = time.perf_counter() - started

        if scan != indexed:
            raise AssertionError("indexed and full-scan results differ")

        print(f"rows={args.rows:,}")
        print(f"distinct={args.distinct:,}")
        print(f"target={args.target}")
        print(f"matches={len(indexed):,}")
        print(f"full_scan_seconds={scan_seconds:.6f}")
        print(f"hash_index_seconds={index_seconds:.6f}")
        print(f"speedup={scan_seconds / index_seconds:.2f}x")
    finally:
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
