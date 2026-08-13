"""Record a real three-process Raft leader failure and re-election."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from urllib.request import urlopen


def status(port: int) -> dict:
    with urlopen(f"http://127.0.0.1:{port}/status", timeout=0.4) as response:
        return json.loads(response.read())


def leaders(ports: list[int]) -> list[dict]:
    found = []
    for port in ports:
        try:
            item = status(port)
        except OSError:
            continue
        if item["role"] == "leader":
            found.append(item)
    return found


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bound-seconds", type=float, default=3.0)
    args = parser.parse_args()
    root = tempfile.mkdtemp(prefix="ledgerdb-raft-demo-")
    ports = [19201, 19202, 19203]
    peers = ",".join(f"node-{i}=127.0.0.1:{port}" for i, port in enumerate(ports))
    procs = []
    try:
        for i, port in enumerate(ports):
            command = [sys.executable, "-m", "ledgerdb.cli", "--data-dir", os.path.join(root, f"node-{i}"),
                       "raft-server", "--node-id", f"node-{i}", "--peers", peers, "--port", str(port),
                       "--heartbeat-ms", "100", "--election-min-ms", "400", "--election-max-ms", "650"]
            procs.append(subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        deadline = time.monotonic() + 5
        old = None
        while time.monotonic() < deadline:
            found = leaders(ports)
            if len(found) == 1:
                old = found[0]
                break
            time.sleep(0.05)
        if old is None:
            raise SystemExit("no initial leader elected")
        old_id = old["node_id"]
        old_index = int(old_id.rsplit("-", 1)[1])
        print(f"initial_leader={old_id} term={old['term']}")
        procs[old_index].send_signal(signal.SIGTERM)
        procs[old_index].wait(timeout=2)
        start = time.monotonic()
        new = None
        while time.monotonic() - start < args.bound_seconds:
            found = leaders(ports)
            if len(found) == 1 and found[0]["node_id"] != old_id:
                new = found[0]
                break
            time.sleep(0.05)
        if new is None:
            raise SystemExit(f"leader re-election exceeded {args.bound_seconds}s")
        elapsed = time.monotonic() - start
        print(f"leader_re_election=PASS old={old_id} new={new['node_id']} elapsed_seconds={elapsed:.3f} bound_seconds={args.bound_seconds}")
    finally:
        for proc in procs:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=2)
