"""Minimal command-line interface used by container and recovery proofs."""

from __future__ import annotations

import argparse
import json
import os

from .engine import LedgerDB


def main() -> None:
    """Run an insert or inspect recovered rows in a mounted data directory."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-dir", default=os.environ.get("LEDGERDB_DATA_DIR", "/var/lib/ledgerdb"))
    commands = parser.add_subparsers(dest="command", required=True)
    insert = commands.add_parser("insert")
    insert.add_argument("row", help="JSON object to append")
    commands.add_parser("rows")
    arguments = parser.parse_args()
    database = LedgerDB(arguments.data_dir)
    if arguments.command == "insert":
        row = json.loads(arguments.row)
        if not isinstance(row, dict):
            raise SystemExit("row must be a JSON object")
        database.insert(row)
        print(json.dumps({"row_count": database.row_count}))
    else:
        print(json.dumps(database.rows(), sort_keys=True))


if __name__ == "__main__":
    main()
