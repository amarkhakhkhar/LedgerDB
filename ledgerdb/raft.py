"""Minimal Raft leader-election implementation for a three-node LedgerDB cluster."""

from __future__ import annotations

import json
import os
import random
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen


@dataclass
class RaftState:
    current_term: int = 0
    voted_for: str | None = None
    role: str = "follower"
    leader_id: str | None = None
    votes_received: set[str] = field(default_factory=set)


class RaftNode:
    """Raft leader election with heartbeat-based failure detection.

    This Day 6 implementation intentionally focuses on the leader-election
    portion of Raft. It implements persistent term/vote state, RequestVote,
    AppendEntries heartbeats, majority voting, and automatic re-election.
    Log replication and commit-index advancement are Day 7 concerns.
    """

    def __init__(
        self,
        node_id: str,
        peers: dict[str, str],
        state_dir: str,
        *,
        host: str = "0.0.0.0",
        port: int = 8000,
        heartbeat_interval: float = 0.15,
        election_timeout: tuple[float, float] = (0.60, 1.00),
    ) -> None:
        if node_id not in peers:
            peers = {**peers, node_id: f"127.0.0.1:{port}"}
        self.node_id = node_id
        self.peers = peers
        self.host = host
        self.port = port
        self.heartbeat_interval = heartbeat_interval
        self.election_timeout = election_timeout
        self._state_path = os.path.join(state_dir, "raft-state.json")
        os.makedirs(state_dir, exist_ok=True)
        self._lock = threading.RLock()
        self._state = RaftState()
        self._server: ThreadingHTTPServer | None = None
        self._stop = threading.Event()
        self._election_deadline = 0.0
        self._load_state()
        self._reset_election_deadline()

    @property
    def majority(self) -> int:
        return len(self.peers) // 2 + 1

    def start(self) -> None:
        handler = self._handler_class()
        self._server = ThreadingHTTPServer((self.host, self.port), handler)
        self._server.daemon_threads = True
        threading.Thread(target=self._server.serve_forever, name=f"raft-http-{self.node_id}", daemon=True).start()
        threading.Thread(target=self._ticker, name=f"raft-ticker-{self.node_id}", daemon=True).start()

    def stop(self) -> None:
        self._stop.set()
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "node_id": self.node_id,
                "term": self._state.current_term,
                "role": self._state.role,
                "leader_id": self._state.leader_id,
                "votes": sorted(self._state.votes_received),
                "majority": self.majority,
                "heartbeat_interval": self.heartbeat_interval,
                "election_timeout": list(self.election_timeout),
            }

    def _handler_class(self):
        node = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                return

            def _json(self, status: int, payload: dict[str, Any]) -> None:
                body = json.dumps(payload, sort_keys=True).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _body(self) -> dict[str, Any]:
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length)
                return json.loads(raw or b"{}")

            def do_GET(self) -> None:  # noqa: N802
                if self.path == "/status":
                    self._json(200, node.status())
                else:
                    self._json(404, {"error": "not found"})

            def do_POST(self) -> None:  # noqa: N802
                try:
                    payload = self._body()
                    if self.path == "/request-vote":
                        self._json(200, node._request_vote(payload))
                    elif self.path == "/append-entries":
                        self._json(200, node._append_entries(payload))
                    else:
                        self._json(404, {"error": "not found"})
                except Exception as error:  # pragma: no cover - defensive HTTP boundary
                    self._json(500, {"error": str(error)})

        return Handler

    def _request_vote(self, request: dict[str, Any]) -> dict[str, Any]:
        candidate = str(request.get("candidate_id", ""))
        term = int(request.get("term", 0))
        with self._lock:
            if term < self._state.current_term:
                return {"term": self._state.current_term, "vote_granted": False}
            if term > self._state.current_term:
                self._become_follower(term, None)
            can_vote = self._state.voted_for in (None, candidate)
            if can_vote:
                self._state.voted_for = candidate
                self._state.leader_id = None
                self._reset_election_deadline()
                self._persist_state()
            return {"term": self._state.current_term, "vote_granted": can_vote}

    def _append_entries(self, request: dict[str, Any]) -> dict[str, Any]:
        leader = str(request.get("leader_id", ""))
        term = int(request.get("term", 0))
        with self._lock:
            if term < self._state.current_term:
                return {"term": self._state.current_term, "success": False}
            if term > self._state.current_term:
                self._become_follower(term, leader)
            elif self._state.role != "follower":
                self._state.role = "follower"
                self._state.leader_id = leader
                self._state.votes_received = set()
                self._reset_election_deadline()
                self._persist_state()
            else:
                self._state.leader_id = leader
                self._reset_election_deadline()
            return {"term": self._state.current_term, "success": True}

    def _ticker(self) -> None:
        while not self._stop.wait(0.05):
            now = time.monotonic()
            with self._lock:
                role = self._state.role
                deadline = self._election_deadline
            if role == "leader":
                self._send_heartbeats()
            elif now >= deadline:
                self._start_election()

    def _start_election(self) -> None:
        with self._lock:
            self._state.current_term += 1
            term = self._state.current_term
            self._state.role = "candidate"
            self._state.voted_for = self.node_id
            self._state.leader_id = None
            self._state.votes_received = {self.node_id}
            self._reset_election_deadline()
            self._persist_state()
            peers = [(peer_id, address) for peer_id, address in self.peers.items() if peer_id != self.node_id]

        if len(self._state.votes_received) >= self.majority:
            self._become_leader(term)
            return

        def ask(peer_id: str, address: str) -> None:
            response = self._post(address, "/request-vote", {"term": term, "candidate_id": self.node_id}, timeout=0.25)
            if response is None:
                return
            with self._lock:
                if self._state.current_term != term or self._state.role != "candidate":
                    return
                response_term = int(response.get("term", 0))
                if response_term > self._state.current_term:
                    self._become_follower(response_term, None)
                    return
                if response.get("vote_granted"):
                    self._state.votes_received.add(peer_id)
                    if len(self._state.votes_received) >= self.majority:
                        self._become_leader(term)

        for peer_id, address in peers:
            threading.Thread(target=ask, args=(peer_id, address), daemon=True).start()

    def _become_leader(self, term: int) -> None:
        with self._lock:
            if self._state.current_term != term:
                return
            self._state.role = "leader"
            self._state.leader_id = self.node_id
            self._persist_state()

    def _become_follower(self, term: int, leader_id: str | None) -> None:
        term_changed = term != self._state.current_term
        self._state.current_term = term
        self._state.role = "follower"
        if term_changed:
            self._state.voted_for = None
        self._state.leader_id = leader_id
        self._state.votes_received = set()
        self._reset_election_deadline()
        self._persist_state()

    def _send_heartbeats(self) -> None:
        with self._lock:
            if self._state.role != "leader":
                return
            term = self._state.current_term
            peers = [(peer_id, address) for peer_id, address in self.peers.items() if peer_id != self.node_id]
        for peer_id, address in peers:
            response = self._post(address, "/append-entries", {"term": term, "leader_id": self.node_id}, timeout=0.25)
            if response is not None:
                with self._lock:
                    response_term = int(response.get("term", 0))
                    if response_term > self._state.current_term:
                        self._become_follower(response_term, None)
        self._stop.wait(self.heartbeat_interval)

    def _reset_election_deadline(self) -> None:
        timeout = random.uniform(*self.election_timeout)
        self._election_deadline = time.monotonic() + timeout

    @staticmethod
    def _post(address: str, path: str, payload: dict[str, Any], timeout: float) -> dict[str, Any] | None:
        try:
            request = Request(
                f"http://{address}{path}",
                data=json.dumps(payload).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except (OSError, URLError, TimeoutError, json.JSONDecodeError):
            return None

    def _load_state(self) -> None:
        try:
            with open(self._state_path, "r", encoding="utf-8") as file:
                state = json.load(file)
            self._state.current_term = int(state.get("current_term", 0))
            self._state.voted_for = state.get("voted_for")
        except FileNotFoundError:
            return

    def _persist_state(self) -> None:
        temporary = f"{self._state_path}.tmp"
        with open(temporary, "w", encoding="utf-8") as file:
            json.dump({"current_term": self._state.current_term, "voted_for": self._state.voted_for}, file)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, self._state_path)


def parse_peers(raw: str) -> dict[str, str]:
    """Parse ``node-a=host:port,node-b=host:port`` into a peer map."""
    peers: dict[str, str] = {}
    for item in raw.split(","):
        if not item.strip():
            continue
        node, address = item.split("=", 1)
        peers[node.strip()] = address.strip()
    if not peers:
        raise ValueError("RAFT_PEERS must contain at least one node=host:port pair")
    return peers
