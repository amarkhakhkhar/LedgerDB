"""End-to-end durability tests, including abrupt subprocess termination."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np

from ledgerdb import LedgerDB


class CrashRecoveryTests(unittest.TestCase):
    def test_replays_a_wal_committed_row_after_process_death(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data_directory = Path(temporary) / "data"
            database = LedgerDB(data_directory)
            database.insert({"account": "opening", "amount": 100})

            environment = os.environ | {"LEDGERDB_CRASH_AFTER_WAL": "1"}
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "ledgerdb.cli",
                    "--data-dir",
                    str(data_directory),
                    "insert",
                    '{"account":"crash-committed","amount":25}',
                ],
                check=False,
                env=environment,
            )
            self.assertEqual(result.returncode, 137)

            recovered = LedgerDB(data_directory)
            self.assertEqual(
                recovered.rows(),
                [{"account": "opening", "amount": 100}, {"account": "crash-committed", "amount": 25}],
            )

            # Restarting after replay remains idempotent: no duplicate row.
            self.assertEqual(LedgerDB(data_directory).rows(), recovered.rows())

    def test_rejects_schema_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = LedgerDB(temporary)
            database.insert({"account": "a", "amount": 1})
            with self.assertRaises(ValueError):
                database.insert({"account": "a", "currency": "USD"})

    def test_equality_index_matches_scan_and_survives_restart(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = LedgerDB(temporary)
            database.bulk_insert([
                {"account": "cash", "amount": 100},
                {"account": "bank", "amount": 200},
                {"account": "cash", "amount": 300},
            ])
            expected = database.filter_eq("account", "cash", use_index=False)
            self.assertEqual(database.filter_eq("account", "cash"), expected)
            self.assertTrue((Path(temporary) / "indexes" / "account.eqindex.jsonl").exists())

            recovered = LedgerDB(temporary)
            self.assertEqual(recovered.filter_eq("account", "cash"), expected)

    def test_equality_index_is_rebuilt_after_crash_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = LedgerDB(temporary)
            database.insert({"account": "opening", "amount": 100})
            environment = os.environ | {"LEDGERDB_CRASH_AFTER_WAL": "1"}
            result = subprocess.run(
                [sys.executable, "-m", "ledgerdb.cli", "--data-dir", temporary, "insert",
                 '{"account":"crash-committed","amount":25}'],
                check=False, env=environment,
            )
            self.assertEqual(result.returncode, 137)

            recovered = LedgerDB(temporary)
            expected = recovered.filter_eq("account", "crash-committed", use_index=False)
            self.assertEqual(expected, [{"account": "crash-committed", "amount": 25}])
            self.assertEqual(recovered.filter_eq("account", "crash-committed"), expected)

            # The index must also remain correct across another restart.
            restarted = LedgerDB(temporary)
            self.assertEqual(restarted.filter_eq("account", "crash-committed"), expected)

    def test_query_results_include_wal_recovered_row_once(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            database = LedgerDB(temporary)
            database.insert({"group": 1, "amount": 10})
            database.insert({"group": 2, "amount": 5})
            database.insert({"group": 1, "amount": 20})
            environment = os.environ | {"LEDGERDB_CRASH_AFTER_WAL": "1"}
            result = subprocess.run(
                [sys.executable, "-m", "ledgerdb.cli", "--data-dir", temporary, "insert", '{"group":2,"amount":7}'],
                check=False, env=environment,
            )
            self.assertEqual(result.returncode, 137)
            recovered = LedgerDB(temporary)
            after = recovered.group_by("group", "amount")
            np.testing.assert_array_equal(after.keys, [1, 2])
            np.testing.assert_allclose(after.sums, [30, 12])
            np.testing.assert_array_equal(after.counts, [2, 2])
            self.assertEqual(recovered.prefix_sum("amount").range_avg(0, 4), 10.5)
