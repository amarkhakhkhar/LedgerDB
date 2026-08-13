from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

from ledgerdb.raft import RaftNode


class RaftHealthTests(unittest.TestCase):
    def test_readiness_requires_a_fresh_caught_up_leader(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            node = RaftNode("node-1", {"node-0": "127.0.0.1:1", "node-1": "127.0.0.1:1", "node-2": "127.0.0.1:1"}, Path(temporary), host="127.0.0.1", port=0)
            node.start()
            try:
                port = node._server.server_address[1]  # test the HTTP probe Kubernetes calls
                node._append_entries({"term": 1, "leader_id": "node-0", "prev_log_index": 3, "leader_log_index": 3, "entries": []})
                self.assertFalse(node.status()["ready"])
                with self.assertRaises(HTTPError) as response:
                    urlopen(f"http://127.0.0.1:{port}/readyz")
                self.assertEqual(response.exception.code, 503)
                entries = [{"index": index, "term": 1, "command": {"operation": "insert", "values": {"key": index, "value": index}}} for index in range(1, 4)]
                node._append_entries({"term": 1, "leader_id": "node-0", "prev_log_index": 0, "leader_log_index": 3, "entries": entries})
                self.assertTrue(node.status()["ready"])
                self.assertTrue(json.loads(urlopen(f"http://127.0.0.1:{port}/readyz").read())["ready"])
            finally:
                node.stop()
