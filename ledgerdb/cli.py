"""Minimal command-line interface used by container and recovery proofs."""

from __future__ import annotations

import argparse
import json
import os
import sys

from .engine import LedgerDB
from .raft import RaftNode, parse_peers


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
    raft = commands.add_parser("raft-server")
    raft.add_argument("--node-id", default=os.environ.get("RAFT_NODE_ID", os.environ.get("HOSTNAME", "node-0")))
    raft.add_argument("--peers", default=os.environ.get("RAFT_PEERS", ""))
    raft.add_argument("--peer-service", default=os.environ.get("RAFT_PEER_SERVICE", ""))
    raft.add_argument("--replicas", type=int, default=int(os.environ.get("RAFT_REPLICAS", "3")))
    raft.add_argument("--port", type=int, default=int(os.environ.get("RAFT_PORT", "8000")))
    raft.add_argument("--host", default=os.environ.get("RAFT_HOST", "0.0.0.0"))
    raft.add_argument("--heartbeat-ms", type=int, default=int(os.environ.get("RAFT_HEARTBEAT_MS", "150")))
    raft.add_argument("--election-min-ms", type=int, default=int(os.environ.get("RAFT_ELECTION_MIN_MS", "600")))
    raft.add_argument("--election-max-ms", type=int, default=int(os.environ.get("RAFT_ELECTION_MAX_MS", "1000")))
    arguments = parser.parse_args()
    if arguments.command == "raft-server":
        peers = parse_peers(arguments.peers) if arguments.peers else None
        node = RaftNode(
            arguments.node_id, peers, os.path.join(arguments.data_dir, "raft"),
            host=arguments.host, port=arguments.port,
            heartbeat_interval=arguments.heartbeat_ms / 1000.0,
            peer_service=arguments.peer_service or None,
            replica_count=arguments.replicas,
            election_timeout=(arguments.election_min_ms / 1000.0, arguments.election_max_ms / 1000.0),
        )
        node.start()
        print(json.dumps({"raft": "started", "node_id": arguments.node_id, "port": arguments.port}), flush=True)
        try:
            while True:
                import time
                time.sleep(1)
        except KeyboardInterrupt:
            node.stop()
        return

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
