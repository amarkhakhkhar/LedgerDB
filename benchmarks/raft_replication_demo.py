"""Day 7 proof: kill a follower, let it lag, restart it, and prove convergence."""
from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from urllib.request import Request, urlopen
from urllib.error import URLError


def status(port: int) -> dict:
    with urlopen(f"http://127.0.0.1:{port}/status", timeout=2) as response:
        return json.loads(response.read())


def state(port: int) -> dict:
    with urlopen(f"http://127.0.0.1:{port}/state", timeout=2) as response:
        return json.loads(response.read())


def post(port: int, path: str, payload: dict) -> dict:
    request = Request(f"http://127.0.0.1:{port}{path}", data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(request, timeout=2) as response:
        return json.loads(response.read())


def available_ports(count: int) -> list[int]:
    """Allocate distinct ephemeral loopback ports for this isolated demo."""
    sockets = [socket.socket(socket.AF_INET, socket.SOCK_STREAM) for _ in range(count)]
    try:
        for listener in sockets:
            listener.bind(("127.0.0.1", 0))
        return [listener.getsockname()[1] for listener in sockets]
    finally:
        for listener in sockets:
            listener.close()


def main() -> None:
    root = tempfile.mkdtemp(prefix="ledgerdb-raft-replication-")
    ports = available_ports(3)
    peers = ",".join(f"node-{i}=127.0.0.1:{port}" for i, port in enumerate(ports))
    procs: dict[int, subprocess.Popen] = {}
    try:
        def start(i: int) -> subprocess.Popen:
            return subprocess.Popen([
                sys.executable, "-m", "ledgerdb.cli", "--data-dir", os.path.join(root, f"node-{i}"),
                "raft-server", "--node-id", f"node-{i}", "--peers", peers, "--port", str(ports[i]),
                "--heartbeat-ms", "80", "--election-min-ms", "700", "--election-max-ms", "1100",
            ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        for i in range(3):
            procs[i] = start(i)
        deadline = time.monotonic() + 6
        leader = None
        while time.monotonic() < deadline:
            leaders = []
            for i in range(3):
                if procs[i].poll() is not None:
                    continue
                try:
                    if status(ports[i])["role"] == "leader":
                        leaders.append(i)
                except (OSError, URLError):
                    pass
            if len(leaders) == 1:
                leader = leaders[0]
                break
            time.sleep(0.05)
        if leader is None:
            raise RuntimeError("no leader elected")

        for i in range(5):
            result = post(ports[leader], "/client-write", {"command": {"operation": "insert", "values": {"key": i, "value": i}}})
            assert result["success"], result
        follower = next(i for i in range(3) if i != leader)
        procs[follower].kill(); procs[follower].wait(timeout=2)
        offline_log_length = len(state(ports[leader])["log"])
        for i in range(5, 25):
            result = post(ports[leader], "/client-write", {"command": {"operation": "insert", "values": {"key": i, "value": i}}})
            assert result["success"], result
        leader_after = state(ports[leader])
        lag = len(leader_after["log"]) - offline_log_length
        start_time = time.monotonic()
        procs[follower] = start(follower)
        deadline = start_time + 15
        while time.monotonic() < deadline:
            try:
                follower_state = state(ports[follower])
                if follower_state["log_digest"] == leader_after["log_digest"] and follower_state["ledger_entries"] == leader_after["ledger_entries"]:
                    elapsed = time.monotonic() - start_time
                    print(f"replication=PASS leader=node-{leader} follower=node-{follower} lag_entries={lag} catchup_seconds={elapsed:.3f} final_log_length={len(leader_after['log'])}")
                    return
            except OSError:
                pass
            time.sleep(0.08)
        raise RuntimeError("follower failed to converge before timeout")
    finally:
        for proc in procs.values():
            if proc.poll() is None:
                proc.kill(); proc.wait(timeout=2)
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
