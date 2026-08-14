from __future__ import annotations

import re
import subprocess
import sys
import unittest
from pathlib import Path


class RaftReplicationTests(unittest.TestCase):
    def test_follower_catches_up_after_failure(self) -> None:
        root = Path(__file__).resolve().parents[1]
        result = subprocess.run(
            [sys.executable, "benchmarks/raft_replication_demo.py"],
            cwd=root,
            text=True,
            capture_output=True,
            timeout=45,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertRegex(result.stdout, r"replication=PASS")
        self.assertRegex(result.stdout, r"lag_entries=20")
        self.assertRegex(result.stdout, r"final_log_length=25")
