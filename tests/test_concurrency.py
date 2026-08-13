"""Concurrency correctness tests for readers racing with committed writers."""

from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path

from ledgerdb import LedgerDB


class ConcurrencyTests(unittest.TestCase):
    def test_readers_never_observe_a_half_committed_write(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = LedgerDB(Path(temporary))
            database.bulk_insert([{"key": 0, "value": 0}, {"key": 1, "value": 1}])
            failures: list[BaseException] = []
            stop = threading.Event()
            barrier = threading.Barrier(7)

            def reader() -> None:
                try:
                    barrier.wait()
                    while not stop.is_set():
                        if not database.validate_filter_index("key", 0):
                            raise AssertionError("indexed and scanned states diverged")
                        rows = database.rows()
                        if any(set(row) != {"key", "value"} for row in rows):
                            raise AssertionError("reader observed a partial row")
                except BaseException as error:
                    failures.append(error)
                    stop.set()

            def writer() -> None:
                try:
                    barrier.wait()
                    for sequence in range(100):
                        database.bulk_insert([
                            {"key": 0, "value": sequence},
                            {"key": 1, "value": sequence},
                        ])
                except BaseException as error:
                    failures.append(error)
                finally:
                    stop.set()

            threads = [threading.Thread(target=reader) for _ in range(6)]
            threads.append(threading.Thread(target=writer))
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=15)
            self.assertFalse(any(thread.is_alive() for thread in threads), "concurrency test deadlocked")
            self.assertEqual(failures, [])
            self.assertEqual(database.row_count, 202)

    def test_sort_merge_join_is_consistent_under_concurrent_writer(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            left = LedgerDB(Path(temporary) / "left")
            right = LedgerDB(Path(temporary) / "right")
            left.bulk_insert([{"key": 1, "left_value": "a"}, {"key": 2, "left_value": "b"}])
            right.bulk_insert([{"key": 1, "right_value": "x"}, {"key": 2, "right_value": "y"}])
            failures: list[BaseException] = []
            stop = threading.Event()

            def writer() -> None:
                try:
                    for value in range(50):
                        right.insert({"key": 3, "right_value": value})
                except BaseException as error:
                    failures.append(error)
                finally:
                    stop.set()

            thread = threading.Thread(target=writer)
            thread.start()
            while not stop.is_set():
                result = left.sort_merge_join(right, "key", "key")
                if len(result) != 2:
                    failures.append(AssertionError("join observed a partial right-hand state"))
                    stop.set()
                    break
            thread.join(timeout=15)
            self.assertFalse(thread.is_alive(), "join concurrency test deadlocked")
            self.assertEqual(failures, [])


class SortMergeJoinCorrectnessTests(unittest.TestCase):
    def test_sort_merge_join_matches_duplicate_keys(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            left = LedgerDB(Path(temporary) / "left")
            right = LedgerDB(Path(temporary) / "right")
            left.bulk_insert([
                {"key": 2, "left_value": "b"},
                {"key": 1, "left_value": "a1"},
                {"key": 1, "left_value": "a2"},
            ])
            right.bulk_insert([
                {"key": 1, "right_value": "x"},
                {"key": 2, "right_value": "y"},
                {"key": 1, "right_value": "z"},
            ])
            result = left.sort_merge_join(right, "key", "key")
            self.assertEqual(len(result), 5)
            self.assertEqual(
                sorted((row["left.key"], row["left.left_value"], row["right.right_value"]) for row in result),
                [
                    (1, "a1", "x"), (1, "a1", "z"),
                    (1, "a2", "x"), (1, "a2", "z"),
                    (2, "b", "y"),
                ],
            )
