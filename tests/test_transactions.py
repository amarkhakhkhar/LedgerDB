"""Day 5 double-entry, idempotency, fraud-validator, and crash proofs."""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from ledgerdb import LedgerDB
from ledgerdb.transactions import suspicious_key_combinations


class TransactionTests(unittest.TestCase):
    def test_balanced_transaction_is_atomic_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            db = LedgerDB(temporary)
            first = db.post_transaction("PAY-1", "cash", "revenue", 100, transaction_key=10)
            second = db.post_transaction("PAY-1", "cash", "revenue", 100, transaction_key=10)
            self.assertEqual(first, second)
            self.assertEqual(db.ledger_balance(), (100, 100))
            self.assertEqual(len(db.ledger_entries()), 2)
            recovered = LedgerDB(temporary)
            self.assertEqual(recovered.ledger_balance(), (100, 100))
            self.assertEqual(len(recovered.ledger_entries()), 2)

    def test_real_process_kill_recovers_balanced_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            db = LedgerDB(temporary)
            db.post_transaction("OPEN-1", "cash", "capital", 50, transaction_key=1)
            env = os.environ | {"LEDGERDB_CRASH_DURING_TRANSACTION": "1"}
            result = subprocess.run(
                [sys.executable, "-m", "ledgerdb.cli", "--data-dir", temporary,
                 "transaction", "PAY-CRASH", "cash", "revenue", "125", "--transaction-key", "42"],
                check=False, env=env,
            )
            self.assertEqual(result.returncode, 137)
            recovered = LedgerDB(temporary)
            self.assertEqual(recovered.ledger_balance(), (175, 175))
            self.assertEqual(len(recovered.ledger_entries()), 4)
            self.assertEqual(recovered.post_transaction("PAY-CRASH", "cash", "revenue", 125, transaction_key=42), "tx-PAY-CRASH")
            self.assertEqual(recovered.ledger_balance(), (175, 175))
            self.assertEqual(len(recovered.ledger_entries()), 4)

    def test_validator_flags_three_and_four_key_combinations(self) -> None:
        self.assertEqual(suspicious_key_combinations([1, 2, 3, 4, 5], 6, 3), [(1, 2, 3)])
        self.assertEqual(suspicious_key_combinations([1, 2, 3, 4, 5], 10, 4), [(1, 2, 3, 4)])

    def test_transaction_validator_reads_persisted_keys(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            db = LedgerDB(temporary)
            db.post_transaction("T1", "a", "b", 10, transaction_key=1)
            db.post_transaction("T2", "a", "b", 20, transaction_key=2)
            db.post_transaction("T3", "a", "b", 30, transaction_key=3)
            self.assertEqual(db.validate_suspicious_transactions(6), [(1, 2, 3)])
