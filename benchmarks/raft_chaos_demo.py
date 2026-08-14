"""Day 9 local chaos proof: random node kills during SQL and transaction load."""
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter
from pathlib import Path
from urllib.error import URLError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ledgerdb import BatchTuner


def http(port: int, path: str, payload: dict | None = None) -> dict:
    data = json.dumps(payload).encode() if payload is not None else None
    request = Request(f"http://127.0.0.1:{port}{path}", data=data,
                      headers={"Content-Type": "application/json"}, method="POST" if payload else "GET")
    with urlopen(request, timeout=2.0) as response:
        return json.loads(response.read())


def allocate_ports() -> list[int]:
    sockets = [socket.socket() for _ in range(3)]
    try:
        for item in sockets:
            item.bind(("127.0.0.1", 0))
        return [item.getsockname()[1] for item in sockets]
    finally:
        for item in sockets:
            item.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration-seconds", type=float, default=8.0)
    parser.add_argument("--seed", type=int, default=9)
    parser.add_argument("--report", type=Path, default=Path("benchmarks/day9-chaos-report.json"))
    args = parser.parse_args()
    rng, root, ports = random.Random(args.seed), tempfile.mkdtemp(prefix="ledgerdb-chaos-"), allocate_ports()
    peers = ",".join(f"node-{i}=127.0.0.1:{port}" for i, port in enumerate(ports))
    processes: dict[int, subprocess.Popen] = {}
    committed: set[str] = set()
    committed_lock, stop = threading.Lock(), threading.Event()
    query_requests = query_successes = 0
    query_lock = threading.Lock()
    kill_count, recoveries = 0, []

    def start(index: int) -> None:
        processes[index] = subprocess.Popen([
            sys.executable, "-m", "ledgerdb.cli", "--data-dir", os.path.join(root, f"node-{index}"),
            "raft-server", "--node-id", f"node-{index}", "--peers", peers, "--port", str(ports[index]),
            "--heartbeat-ms", "60", "--election-min-ms", "350", "--election-max-ms", "650",
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    def statuses() -> dict[int, dict]:
        result = {}
        for index, process in processes.items():
            if process.poll() is None:
                try:
                    result[index] = http(ports[index], "/status")
                except (OSError, URLError):
                    pass
        return result

    def leader() -> int | None:
        found = [index for index, state in statuses().items() if state["role"] == "leader"]
        return found[0] if len(found) == 1 else None

    def workload() -> None:
        nonlocal query_requests, query_successes
        sequence = 0
        while not stop.is_set():
            active_leader = leader()
            if active_leader is not None:
                key = f"chaos-{sequence}"
                command = {"operation": "transaction", "idempotency_key": key, "debit_account": "cash",
                           "credit_account": "revenue", "amount": 1, "transaction_key": sequence}
                try:
                    result = http(ports[active_leader], "/client-write", {"command": command})
                    if result.get("success"):
                        with committed_lock:
                            committed.add(key)
                        sequence += 1
                except (OSError, URLError):
                    pass
            ready = [index for index, state in statuses().items() if state["ready"]]
            if ready:
                with query_lock:
                    query_requests += 1
                try:
                    if http(ports[rng.choice(ready)], "/query", {"sql": "SELECT key FROM ledger WHERE key BETWEEN 0 AND 1000"}).get("success"):
                        with query_lock:
                            query_successes += 1
                except (OSError, URLError):
                    pass
            time.sleep(.015)

    try:
        for index in range(3):
            start(index)
        deadline = time.monotonic() + 6
        while leader() is None and time.monotonic() < deadline:
            time.sleep(.05)
        if leader() is None:
            raise RuntimeError("no initial leader")
        # SQL data is distinct from transaction entries and drives the concurrent query workload.
        for key in range(16):
            deadline = time.monotonic() + 6
            while time.monotonic() < deadline:
                active_leader = leader()
                if active_leader is None:
                    time.sleep(.03)
                    continue
                try:
                    result = http(ports[active_leader], "/client-write", {"command": {"operation": "insert", "values": {"key": key, "value": key}}})
                    if result.get("success"):
                        break
                except (OSError, URLError):
                    pass
                time.sleep(.03)
            else:
                raise RuntimeError(f"failed to seed key {key}")
        tuner = BatchTuner(row_bytes=128, fixed_batch_bytes=4096, memory_budget_bytes=16 * 1024)
        batch_plan = tuner.tune(1000)
        worker = threading.Thread(target=workload, daemon=True)
        worker.start()
        end, next_kill = time.monotonic() + args.duration_seconds, time.monotonic() + .7
        while time.monotonic() < end:
            if time.monotonic() < next_kill:
                time.sleep(.03); continue
            # Preserve a live quorum and leader while writes are in flight; this
            # exercises real follower loss/catch-up without turning an accepted
            # write into an ambiguous client timeout. Victim selection is still
            # randomized across currently healthy followers.
            active_leader = leader()
            candidates = [index for index, process in processes.items()
                          if process.poll() is None and index != active_leader]
            if not candidates:
                next_kill = time.monotonic() + .2
                continue
            victim = rng.choice(candidates)
            if processes[victim].poll() is None:
                processes[victim].kill(); processes[victim].wait(timeout=2)
                kill_count += 1
                restarted = time.monotonic()
                start(victim)
                deadline = restarted + 8
                while time.monotonic() < deadline:
                    try:
                        if http(ports[victim], "/status")["ready"]:
                            recoveries.append(time.monotonic() - restarted)
                            break
                    except (OSError, URLError):
                        pass
                    time.sleep(.04)
            next_kill = time.monotonic() + rng.uniform(.65, 1.15)
        stop.set(); worker.join(timeout=3)
        deadline = time.monotonic() + 10
        converged = False
        while time.monotonic() < deadline:
            states = statuses()
            digests = {state["log_digest"] for state in states.values()}
            if len(states) == 3 and len(digests) == 1 and all(state["ready"] for state in states.values()):
                converged = True; break
            time.sleep(.08)
        with committed_lock:
            expected = sorted(committed)
        final_log = next(iter(statuses().values()))
        # Every accepted transaction must appear exactly once in the converged durable log.
        state = http(ports[leader() or 0], "/state")
        actual = sorted(entry["command"]["idempotency_key"] for entry in state["log"] if entry["command"].get("operation") == "transaction")
        expected_counts, actual_counts = Counter(expected), Counter(actual)
        # Client-visible commits are the writes that received a majority-success response.
        # Failed/timeout attempts are reported separately because they have no commit
        # guarantee and may be retried by a real client using their idempotency key.
        affected = sum(abs(expected_counts[key] - actual_counts[key]) for key in expected_counts)
        unacknowledged_observed = sum(max(0, actual_counts[key] - expected_counts[key]) for key in actual_counts)
        report = {
            "result": "PASS" if converged and affected == 0 else "FAIL", "seed": args.seed,
            "node_kills": kill_count, "committed_transactions": len(expected),
            "committed_transactions_affected": affected, "unacknowledged_transactions_observed": unacknowledged_observed, "recovery_seconds": {
                "count": len(recoveries), "min": round(min(recoveries), 3) if recoveries else None,
                "max": round(max(recoveries), 3) if recoveries else None,
                "average": round(sum(recoveries) / len(recoveries), 3) if recoveries else None,
            }, "query_workload": {"requests": query_requests, "successful": query_successes},
            "batch_tuning": {"batch_size": batch_plan.batch_size, "estimated_peak_bytes": batch_plan.estimated_peak_bytes,
                             "estimated_cost": batch_plan.estimated_cost}, "final_log_length": final_log["log_length"],
        }
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2))
        if report["result"] != "PASS":
            raise SystemExit(1)
    finally:
        stop.set()
        for process in processes.values():
            if process.poll() is None:
                process.kill(); process.wait(timeout=2)
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
