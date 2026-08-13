from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from urllib.request import urlopen


class RaftElectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = tempfile.mkdtemp(prefix="ledgerdb-raft-")
        self.procs: list[subprocess.Popen[str]] = []
        # unittest does not invoke tearDown when setUp raises. Register cleanup
        # first so a failed election setup cannot leak port-bound Raft children
        # into the next test run.
        self.addCleanup(self._cleanup)
        self.ports = self._available_ports(3)
        self.peers = ",".join(f"node-{i}=127.0.0.1:{port}" for i, port in enumerate(self.ports))
        package_root = os.path.dirname(os.path.dirname(__file__))
        for i, port in enumerate(self.ports):
            data = os.path.join(self.root, f"node-{i}")
            command = [
                sys.executable, "-m", "ledgerdb.cli", "--data-dir", data,
                "raft-server", "--node-id", f"node-{i}", "--peers", self.peers,
                "--port", str(port), "--heartbeat-ms", "100",
                # Windows subprocess startup and fsync can exceed the narrow
                # Day 6 window. This remains well inside the 2.5s re-election
                # bound while allowing a RequestVote round to finish.
                "--election-min-ms", "700", "--election-max-ms", "1100",
            ]
            self.procs.append(subprocess.Popen(command, cwd=package_root, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
        self._wait_for_leader(5.0)

    def _cleanup(self) -> None:
        for proc in self.procs:
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=2)
        shutil.rmtree(self.root, ignore_errors=True)

    def _status(self, port: int) -> dict:
        with urlopen(f"http://127.0.0.1:{port}/status", timeout=0.3) as response:
            return json.loads(response.read())

    @staticmethod
    def _available_ports(count: int) -> list[int]:
        """Reserve distinct ephemeral loopback ports for this test cluster."""
        sockets = [socket.socket(socket.AF_INET, socket.SOCK_STREAM) for _ in range(count)]
        try:
            for listener in sockets:
                listener.bind(("127.0.0.1", 0))
            return [listener.getsockname()[1] for listener in sockets]
        finally:
            for listener in sockets:
                listener.close()

    def _statuses(self) -> list[dict]:
        result = []
        for port in self.ports:
            try:
                result.append(self._status(port))
            except OSError:
                pass
        return result

    def _wait_for_leader(self, timeout: float) -> tuple[str, float]:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            statuses = self._statuses()
            leaders = [s for s in statuses if s["role"] == "leader"]
            if len(leaders) == 1:
                return leaders[0]["node_id"], time.monotonic()
            time.sleep(0.05)
        self.fail(f"no unique leader within {timeout}s: {self._statuses()}")

    def test_leader_is_re_elected_after_kill(self) -> None:
        old_leader, elected_at = self._wait_for_leader(3.0)
        leader_index = int(old_leader.rsplit("-", 1)[1])
        self.procs[leader_index].kill()
        self.procs[leader_index].wait(timeout=2)
        start = time.monotonic()
        new_leader, _ = self._wait_for_leader(3.0)
        elapsed = time.monotonic() - start
        self.assertNotEqual(old_leader, new_leader)
        self.assertLess(elapsed, 2.5)
        print(f"leader_election=PASS old={old_leader} new={new_leader} elapsed_seconds={elapsed:.3f}")
