"""Day 8 proof: five SQL texts run through /query on a real three-node Raft cluster."""
from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from urllib.error import URLError
from urllib.request import Request, urlopen


def request(port: int, path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    method = "POST" if payload is not None else "GET"
    call = Request(f"http://127.0.0.1:{port}{path}", data=data, headers={"Content-Type": "application/json"}, method=method)
    with urlopen(call, timeout=2) as response:
        return json.loads(response.read())


def ports() -> list[int]:
    listeners = [socket.socket() for _ in range(3)]
    try:
        for listener in listeners:
            listener.bind(("127.0.0.1", 0))
        return [listener.getsockname()[1] for listener in listeners]
    finally:
        for listener in listeners:
            listener.close()


def main() -> None:
    root = tempfile.mkdtemp(prefix="ledgerdb-raft-sql-")
    node_ports, processes = ports(), []
    peers = ",".join(f"node-{i}=127.0.0.1:{port}" for i, port in enumerate(node_ports))
    try:
        for i, port in enumerate(node_ports):
            processes.append(subprocess.Popen([
                sys.executable, "-m", "ledgerdb.cli", "--data-dir", os.path.join(root, f"node-{i}"),
                "raft-server", "--node-id", f"node-{i}", "--peers", peers, "--port", str(port),
                "--heartbeat-ms", "60", "--election-min-ms", "450", "--election-max-ms", "750",
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        deadline = time.monotonic() + 6
        leader = None
        while time.monotonic() < deadline:
            try:
                leaders = [i for i, port in enumerate(node_ports) if request(port, "/status")["role"] == "leader"]
                if len(leaders) == 1:
                    leader = leaders[0]
                    break
            except (OSError, URLError):
                pass
            time.sleep(.05)
        if leader is None:
            raise RuntimeError("no leader elected")
        rows = [
            {"key": 1, "amount": 10, "category": 1}, {"key": 2, "amount": 20, "category": 1},
            {"key": 2, "amount": 30, "category": 2}, {"key": 3, "amount": 40, "category": 2},
        ]
        for row in rows:
            assert request(node_ports[leader], "/client-write", {"command": {"operation": "insert", "values": row}})["success"]
        queries = [
            "SELECT key, amount FROM ledger",
            "SELECT key FROM ledger WHERE key = 2",
            "SELECT key, amount FROM ledger WHERE amount BETWEEN 15 AND 35",
            "SELECT category, SUM(amount) AS total FROM ledger GROUP BY category",
            "SELECT a.key, a.amount, b.category FROM ledger a JOIN ledger b ON a.key = b.key WHERE a.amount BETWEEN 20 AND 20",
        ]
        expected_counts = [4, 2, 2, 2, 2]
        for number, (sql, expected) in enumerate(zip(queries, expected_counts, strict=True), 1):
            result = request(node_ports[leader], "/query", {"sql": sql})
            assert result["success"] and len(result["rows"]) == expected, result
            print(f"sql_query_{number}=PASS rows={len(result['rows'])} plan={','.join(result['plan'])}")
        print("sql_cluster=PASS queries=5 nodes=3")
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill(); process.wait(timeout=2)
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
