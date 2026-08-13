"""Minimal command-line interface used by container and recovery proofs."""

from __future__ import annotations

import argparse
import json
import os
import sys

from .engine import LedgerDB


def main() -> None:
    """Run an insert or inspect recovered rows in a mounted data directory."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default=os.environ.get("LEDGERDB_DATA_DIR", "/var/lib/ledgerdb"))
    commands = parser.add_subparsers(dest="command", required=True)
    insert = commands.add_parser("insert")
    insert.add_argument("row", help="JSON object to append")
    commands.add_parser("rows")
    transaction = commands.add_parser("transaction")
    transaction.add_argument("idempotency_key")
    transaction.add_argument("debit_account")
    transaction.add_argument("credit_account")
    transaction.add_argument("amount", type=int)
    transaction.add_argument("--transaction-key", type=int, default=0)
    arguments = parser.parse_args()
    database = LedgerDB(arguments.data_dir)
    if arguments.command == "insert":
        row = json.loads(arguments.row)
        print("ROW:", repr(row))
        if not isinstance(row, dict):
            raise SystemExit("row must be a JSON object")
        database.insert(row)
        print(json.dumps({"row_count": database.row_count}))
    elif arguments.command == "transaction":
        transaction_id = database.post_transaction(
            arguments.idempotency_key, arguments.debit_account, arguments.credit_account,
            arguments.amount, transaction_key=arguments.transaction_key,
        )
        print(json.dumps({"transaction_id": transaction_id, "balance": database.ledger_balance()}))
    else:
        print(json.dumps(database.rows(), sort_keys=True))


if __name__ == "__main__":
    main()
