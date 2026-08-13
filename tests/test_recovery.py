"""End-to-end durability tests, including abrupt subprocess termination."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

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
