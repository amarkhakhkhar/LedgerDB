"""Raft leader election, WAL replication, follower catch-up, and lag metrics."""

from __future__ import annotations

import json
import os
import random
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

from .engine import LedgerDB


@dataclass
class RaftState:
    current_term: int = 0
    voted_for: str | None = None
    role: str = "follower"
    leader_id: str | None = None
    votes_received: set[str] = field(default_factory=set)


class RaftNode:
    """Three-node Raft layer with replicated application WAL entries.

    Day 7 adds the replication half of Raft. The leader owns a durable
    raft-log.jsonl, tracks match indexes for followers, and retries entries
    until followers acknowledge them. Followers persist an entry before
    acknowledging it and materialize the command into their LedgerDB.
    """

    def __init__(
        self,
        node_id: str,
        peers: dict[str, str] | None,
        state_dir: str,
        *,
        host: str = "0.0.0.0",
        port: int = 8000,
        heartbeat_interval: float = 0.15,
        election_timeout: tuple[float, float] = (0.60, 1.00),
        peer_service: str | None = None,
        replica_count: int = 3,
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.heartbeat_interval = heartbeat_interval
        self.election_timeout = election_timeout
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self._state_path = self.state_dir / "raft-state.json"
        self._log_path = self.state_dir / "raft-log.jsonl"
        self._lock = threading.RLock()
        self._state = RaftState()
        self._server: ThreadingHTTPServer | None = None
        self._stop = threading.Event()
        self._election_deadline = 0.0
        self._log: list[dict[str, Any]] = []
        self._applied_log_index = 0
        self._next_index: dict[str, int] = {}
        self._match_index: dict[str, int] = {}
        self._lag_windows: dict[str, deque[int]] = {}
        self._replication_locks: dict[str, threading.Lock] = {}
        # Each node receives independent jitter inside a stable slice of the
        # configured timeout window. Purely random timeouts can still collide
        # under synchronized process startup, repeatedly splitting a 3-node
        # vote. Slices retain jitter but guarantee a deterministic tie-break.
        self._election_rng = random.Random()
        self._db = LedgerDB(str(self.state_dir / "database"))
        self.peers = self._resolve_peers(peers, peer_service, replica_count)
        self._load_state()
        self._load_log()
        self._reset_election_deadline()
        self._reset_replication_state()

    def _resolve_peers(self, peers: dict[str, str] | None, service: str | None, replicas: int) -> dict[str, str]:
        if peers:
            return dict(peers)
        service = service or os.environ.get("RAFT_PEER_SERVICE")
        replicas = int(os.environ.get("RAFT_REPLICAS", replicas))
        if not service:
            raise ValueError("provide RAFT_PEERS or RAFT_PEER_SERVICE")
        if "." in service:
            headless = service.split(".", 1)[0]
            suffix = service.split(".", 1)[1]
            return {f"node-{i}": f"{headless}-{i}.{suffix}:8000" for i in range(replicas)}
        return {f"node-{i}": f"{service}-{i}.{service}:8000" for i in range(replicas)}

    @property
    def majority(self) -> int:
        return len(self.peers) // 2 + 1

    @property
    def log_length(self) -> int:
        with self._lock:
            return len(self._log)

    def start(self) -> None:
        handler = self._handler_class()
        ThreadingHTTPServer.allow_reuse_address = True
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
            last = len(self._log)
            replication = {}
            for peer_id in self.peers:
                if peer_id == self.node_id:
                    continue
                match = self._match_index.get(peer_id, 0)
                samples = list(self._lag_windows.get(peer_id, ()))
                replication[peer_id] = {
                    "match_index": match,
                    "lag": max(0, last - match),
                    "average_lag": round(sum(samples) / len(samples), 3) if samples else 0.0,
                    "max_lag": max(samples) if samples else 0,
                    "samples": len(samples),
                }
            return {
                "node_id": self.node_id,
                "term": self._state.current_term,
                "role": self._state.role,
                "leader_id": self._state.leader_id,
                "votes": sorted(self._state.votes_received),
                "majority": self.majority,
                "heartbeat_interval": self.heartbeat_interval,
                "election_timeout": list(self.election_timeout),
                "log_length": last,
                "log_digest": self._log_digest_unlocked(),
                "replication": replication,
            }

    def state_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "log": list(self._log),
                "log_digest": self._log_digest_unlocked(),
                "ledger_entries": self._db.rows(),
                "ledger_balance": self._db.ledger_balance(),
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
                return json.loads(self.rfile.read(length) or b"{}")

            def do_GET(self) -> None:  # noqa: N802
                if self.path == "/status":
                    self._json(200, node.status())
                elif self.path == "/state":
                    self._json(200, node.state_snapshot())
                else:
                    self._json(404, {"error": "not found"})

            def do_POST(self) -> None:  # noqa: N802
                try:
                    payload = self._body()
                    if self.path == "/request-vote":
                        self._json(200, node._request_vote(payload))
                    elif self.path == "/append-entries":
                        self._json(200, node._append_entries(payload))
                    elif self.path == "/client-write":
                        self._json(200, node._client_write(payload))
                    else:
                        self._json(404, {"error": "not found"})
                except Exception as error:
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
        entries = request.get("entries", [])
        prev_index = int(request.get("prev_log_index", 0))
        with self._lock:
            if term < self._state.current_term:
                return {"term": self._state.current_term, "success": False, "match_index": len(self._log)}
            if term > self._state.current_term:
                self._become_follower(term, leader)
            else:
                self._state.role = "follower"
                self._state.leader_id = leader
                self._reset_election_deadline()
            if prev_index > len(self._log):
                return {"term": self._state.current_term, "success": False, "match_index": len(self._log)}
            for entry in entries:
                index = int(entry["index"])
                if index <= len(self._log):
                    if self._log[index - 1] != entry:
                        return {"term": self._state.current_term, "success": False, "match_index": index - 1}
                    continue
                if len(self._log) + 1 != index:
                    return {"term": self._state.current_term, "success": False, "match_index": len(self._log)}
                self._append_log_entry(entry)
                self._apply_command(entry["command"])
                self._applied_log_index = index
                self._persist_state()
            return {"term": self._state.current_term, "success": True, "match_index": len(self._log)}

    def _client_write(self, request: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            if self._state.role != "leader":
                return {"success": False, "leader_id": self._state.leader_id, "term": self._state.current_term}
            command = request.get("command")
            if not isinstance(command, dict):
                return {"success": False, "error": "command must be an object"}
            index = len(self._log) + 1
            entry = {"index": index, "term": self._state.current_term, "command": command}
            self._append_log_entry(entry)
            self._apply_command(command)
            self._applied_log_index = index
            self._persist_state()
        self._replicate_until(index, timeout=3.0)
        with self._lock:
            acknowledged = sum(1 for peer in self.peers if peer != self.node_id and self._match_index.get(peer, 0) >= index) + 1
            return {"success": acknowledged >= self.majority, "index": index, "acknowledgements": acknowledged, "leader_id": self.node_id}

    def append_client_command(self, command: dict[str, Any], timeout: float = 3.0) -> dict[str, Any]:
        return self._client_write({"command": command, "timeout": timeout})

    def _ticker(self) -> None:
        while not self._stop.wait(0.03):
            with self._lock:
                role = self._state.role
                deadline = self._election_deadline
            if role == "leader":
                self._replicate_once()
            elif time.monotonic() >= deadline:
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
        def ask(peer_id: str, address: str) -> None:
            response = self._post(address, "/request-vote", {"term": term, "candidate_id": self.node_id}, timeout=0.25)
            if response is None:
                return
            with self._lock:
                if self._state.current_term != term or self._state.role != "candidate":
                    return
                if int(response.get("term", 0)) > self._state.current_term:
                    self._become_follower(int(response["term"]), None)
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
            self._reset_replication_state()
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

    def _replicate_once(self) -> None:
        with self._lock:
            if self._state.role != "leader":
                return
            peers = [(peer_id, address) for peer_id, address in self.peers.items() if peer_id != self.node_id]
        for peer_id, address in peers:
            self._replicate_peer(peer_id, address)

    def _replicate_until(self, target: int, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                acknowledgements = 1 + sum(
                    1 for peer in self.peers if peer != self.node_id and self._match_index.get(peer, 0) >= target
                )
                if acknowledgements >= self.majority:
                    return
            self._replicate_once()
            time.sleep(0.03)

    def _replicate_peer(self, peer_id: str, address: str) -> None:
        peer_lock = self._replication_locks.setdefault(peer_id, threading.Lock())
        if not peer_lock.acquire(blocking=False):
            return
        try:
            with self._lock:
                if self._state.role != "leader":
                    return
                next_index = self._next_index.get(peer_id, len(self._log) + 1)
                prev = next_index - 1
                entries = self._log[prev:]
                term = self._state.current_term
                payload = {"term": term, "leader_id": self.node_id, "prev_log_index": prev, "entries": entries}
                lag = len(self._log) - self._match_index.get(peer_id, 0)
                self._lag_windows.setdefault(peer_id, deque(maxlen=60)).append(max(0, lag))
            response = self._post(address, "/append-entries", payload, timeout=0.35)
            if response is None:
                return
            with self._lock:
                if int(response.get("term", 0)) > self._state.current_term:
                    self._become_follower(int(response["term"]), None)
                    return
                if response.get("success"):
                    match = int(response.get("match_index", 0))
                    self._match_index[peer_id] = match
                    self._next_index[peer_id] = match + 1
                else:
                    self._next_index[peer_id] = max(1, self._next_index.get(peer_id, 1) - 1)
        finally:
            peer_lock.release()

    def _reset_replication_state(self) -> None:
        next_index = len(self._log) + 1
        self._next_index = {peer: next_index for peer in self.peers if peer != self.node_id}
        self._match_index = {peer: 0 for peer in self.peers if peer != self.node_id}
        self._lag_windows = {peer: deque(maxlen=60) for peer in self.peers if peer != self.node_id}
        self._replication_locks = {peer: self._replication_locks.get(peer, threading.Lock()) for peer in self.peers if peer != self.node_id}

    def _append_log_entry(self, entry: dict[str, Any]) -> None:
        payload = json.dumps(entry, separators=(",", ":"), sort_keys=True) + "\n"
        with self._log_path.open("ab", buffering=0) as file:
            file.write(payload.encode())
            file.flush()
            os.fsync(file.fileno())
        self._log.append(entry)

    def _rewrite_log(self) -> None:
        temporary = self._log_path.with_suffix(".tmp")
        with temporary.open("wb") as file:
            for entry in self._log:
                file.write((json.dumps(entry, separators=(",", ":"), sort_keys=True) + "\n").encode())
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, self._log_path)

    def _load_log(self) -> None:
        if not self._log_path.exists():
            return
        with self._log_path.open("rb") as file:
            for raw in file:
                if not raw.endswith(b"\n"):
                    break
                entry = json.loads(raw)
                if int(entry["index"]) != len(self._log) + 1:
                    raise ValueError("non-contiguous Raft log")
                self._log.append(entry)
                if int(entry["index"]) > self._applied_log_index:
                    self._apply_command(entry["command"])
                    self._applied_log_index = int(entry["index"])
        self._persist_state()

    def _apply_command(self, command: dict[str, Any]) -> None:
        if command.get("operation") == "insert":
            self._db.insert(command["values"])
        elif command.get("operation") == "transaction":
            self._db.post_transaction(
                command["idempotency_key"], command["debit_account"], command["credit_account"],
                int(command["amount"]), transaction_key=int(command.get("transaction_key", 0)),
            )
        else:
            raise ValueError(f"unsupported replicated operation: {command.get('operation')!r}")

    def _log_digest_unlocked(self) -> str:
        import hashlib
        payload = "".join(json.dumps(entry, sort_keys=True, separators=(",", ":")) for entry in self._log).encode()
        return hashlib.sha256(payload).hexdigest()

    def _reset_election_deadline(self) -> None:
        minimum, maximum = self.election_timeout
        node_order = sorted(self.peers)
        try:
            rank = node_order.index(self.node_id)
        except ValueError:
            rank = 0
        width = (maximum - minimum) / max(1, len(node_order))
        lower = minimum + (rank * width)
        upper = lower + width
        self._election_deadline = time.monotonic() + self._election_rng.uniform(lower, upper)

    @staticmethod
    def _post(address: str, path: str, payload: dict[str, Any], timeout: float) -> dict[str, Any] | None:
        try:
            request = Request(f"http://{address}{path}", data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"}, method="POST")
            with urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode())
        except (OSError, URLError, TimeoutError, json.JSONDecodeError):
            return None

    def _load_state(self) -> None:
        try:
            state = json.loads(self._state_path.read_text())
            self._state.current_term = int(state.get("current_term", 0))
            self._state.voted_for = state.get("voted_for")
            self._applied_log_index = int(state.get("applied_log_index", 0))
        except FileNotFoundError:
            pass

    def _persist_state(self) -> None:
        temporary = self._state_path.with_suffix(".tmp")
        payload = json.dumps({
            "current_term": self._state.current_term,
            "voted_for": self._state.voted_for,
            "applied_log_index": self._applied_log_index,
        }, separators=(",", ":"), sort_keys=True).encode("utf-8")
        # Flush and fsync while the temporary file is writable. The former
        # implementation reopened it as ``rb`` and then called ``flush()``,
        # raising UnsupportedOperation inside the election ticker after nodes
        # voted for themselves. Atomic replacement preserves durable Raft term
        # state without killing the election loop.
        with temporary.open("wb") as file:
            file.write(payload)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, self._state_path)


def parse_peers(raw: str) -> dict[str, str]:
    peers: dict[str, str] = {}
    for item in raw.split(","):
        if not item.strip():
            continue
        node, address = item.split("=", 1)
        peers[node.strip()] = address.strip()
    if not peers:
        raise ValueError("RAFT_PEERS must contain at least one node=host:port pair")
    return peers
