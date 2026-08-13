"""Scripted Day 4 concurrent reader/writer proof for CI and local runs."""

from __future__ import annotations

import argparse
import tempfile
import threading
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ledgerdb import LedgerDB


def run(readers: int, writes: int) -> None:
    with tempfile.TemporaryDirectory() as temporary:
        database = LedgerDB(Path(temporary))
        database.bulk_insert([{"key": 0, "value": 0}, {"key": 1, "value": 1}])
        failures: list[BaseException] = []
        stop = threading.Event()
        barrier = threading.Barrier(readers + 1)

        def reader() -> None:
            try:
                barrier.wait()
                while not stop.is_set():
                    if not database.validate_filter_index("key", 0):
                        raise AssertionError("indexed and scanned states diverged")
                    if any(set(row) != {"key", "value"} for row in database.rows()):
                        raise AssertionError("reader observed a partial row")
            except BaseException as error:
                failures.append(error)
                stop.set()

        def writer() -> None:
            try:
                barrier.wait()
                for sequence in range(writes):
                    database.bulk_insert([
                        {"key": 0, "value": sequence},
                        {"key": 1, "value": sequence},
                    ])
            except BaseException as error:
                failures.append(error)
            finally:
                stop.set()

        threads = [threading.Thread(target=reader, name=f"reader-{i}") for i in range(readers)]
        threads.append(threading.Thread(target=writer, name="writer"))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        if any(thread.is_alive() for thread in threads):
            raise RuntimeError("concurrent-load test deadlocked")
        if failures:
            raise AssertionError(f"concurrent-load test failed: {failures!r}")
        expected_rows = 2 + writes * 2
        if database.row_count != expected_rows:
            raise AssertionError(f"expected {expected_rows} rows, got {database.row_count}")
        print(f"concurrent_load=PASS readers={readers} writes={writes} rows={expected_rows}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--readers", type=int, default=6)
    parser.add_argument("--writes", type=int, default=100)
    args = parser.parse_args()
    run(args.readers, args.writes)
