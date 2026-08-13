"""Standalone subprocess crash proof used by CI."""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ledgerdb import LedgerDB


def main() -> None:
    with tempfile.TemporaryDirectory() as temporary:
        db = LedgerDB(Path(temporary))
        db.post_transaction("BASE", "cash", "capital", 100)
        env = os.environ | {"LEDGERDB_CRASH_DURING_TRANSACTION": "1"}
        result = subprocess.run(
            [sys.executable, "-m", "ledgerdb.cli", "--data-dir", temporary,
             "transaction", "CRASH-1", "cash", "revenue", "75"],
            env=env, check=False,
        )
        assert result.returncode == 137, result.returncode
        recovered = LedgerDB(temporary)
        debit, credit = recovered.ledger_balance()
        assert debit == credit, (debit, credit)
        assert len(recovered.ledger_entries()) == 4
        recovered.post_transaction("CRASH-1", "cash", "revenue", 75)
        assert recovered.ledger_balance() == (175, 175)
        assert len(recovered.ledger_entries()) == 4
        print("transaction_crash=PASS debit=175 credit=175 idempotent_once=true")


if __name__ == "__main__":
    main()
