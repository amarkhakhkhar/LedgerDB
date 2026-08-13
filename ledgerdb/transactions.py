"""Crash-safe double-entry ledger transactions and a small suspicious-key validator."""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from .errors import StorageCorruptionError
from .wal import WriteAheadLog


class LedgerTransactionStore:
    """Durable append-only store for balanced debit/credit transactions.

    The transaction WAL contains the complete pair as one durable record. The
    entries file is a derived materialization. Recovery replays any WAL
    transaction whose idempotency key is not already fully materialized.
    """

    def __init__(self, directory: Path) -> None:
        self._directory = directory
        self._directory.mkdir(parents=True, exist_ok=True)
        self._entries_path = directory / "entries.jsonl"
        self._wal = WriteAheadLog(directory / "transactions.wal")
        self._entries: list[dict[str, Any]] = []
        self._transaction_ids: set[str] = set()
        self._idempotency: dict[str, str] = {}
        self._load_entries()

    @property
    def entries(self) -> list[dict[str, Any]]:
        return [dict(entry) for entry in self._entries]

    @property
    def transaction_count(self) -> int:
        return len(self._transaction_ids)

    def has_idempotency_key(self, key: str) -> bool:
        return key in self._idempotency

    def transaction_for_key(self, key: str) -> str | None:
        return self._idempotency.get(key)

    def balance(self) -> tuple[int, int]:
        debits = sum(int(entry["amount"]) for entry in self._entries if entry["entry_type"] == "debit")
        credits = sum(int(entry["amount"]) for entry in self._entries if entry["entry_type"] == "credit")
        return debits, credits

    def apply_transaction(
        self,
        transaction_id: str,
        idempotency_key: str,
        debit: dict[str, Any],
        credit: dict[str, Any],
    ) -> bool:
        """Durably record a complete transaction and materialize both entries.

        Returns False when the idempotency key was already applied.
        """
        existing = self._idempotency.get(idempotency_key)
        if existing is not None:
            return False
        amount = int(debit["amount"])
        if amount <= 0 or amount != int(credit["amount"]):
            raise ValueError("debit and credit amounts must be equal and positive")
        record = {
            "operation": "transaction",
            "transaction_id": transaction_id,
            "idempotency_key": idempotency_key,
            "debit": dict(debit),
            "credit": dict(credit),
        }
        self._wal.append(record)
        self._apply_record(record, crash_after_first_entry=os.environ.get("LEDGERDB_CRASH_DURING_TRANSACTION") == "1")
        return True

    def recover(self) -> None:
        """Replay durable transaction records until the materialization is complete."""
        for record in self._wal.records():
            if record.get("operation") != "transaction":
                raise StorageCorruptionError("unsupported transaction WAL record")
            self._apply_record(record, crash_after_first_entry=False)

    def _apply_record(self, record: dict[str, Any], *, crash_after_first_entry: bool) -> None:
        transaction_id = str(record["transaction_id"])
        idempotency_key = str(record["idempotency_key"])
        debit = dict(record["debit"])
        credit = dict(record["credit"])
        if self._idempotency.get(idempotency_key) == transaction_id:
            return

        existing_ids = {entry["transaction_id"] for entry in self._entries if entry["transaction_id"] == transaction_id}
        if not existing_ids:
            self._append_entry(self._make_entry(transaction_id, idempotency_key, "debit", debit))
            if crash_after_first_entry:
                os._exit(137)
            self._append_entry(self._make_entry(transaction_id, idempotency_key, "credit", credit))
        else:
            # A crash may leave exactly one side materialized. Never duplicate it.
            types = {entry["entry_type"] for entry in self._entries if entry["transaction_id"] == transaction_id}
            if "debit" not in types:
                self._append_entry(self._make_entry(transaction_id, idempotency_key, "debit", debit))
            if "credit" not in types:
                self._append_entry(self._make_entry(transaction_id, idempotency_key, "credit", credit))
        self._idempotency[idempotency_key] = transaction_id
        self._transaction_ids.add(transaction_id)

    @staticmethod
    def _make_entry(transaction_id: str, idempotency_key: str, entry_type: str, entry: dict[str, Any]) -> dict[str, Any]:
        return {
            "transaction_id": transaction_id,
            "idempotency_key": idempotency_key,
            "entry_type": entry_type,
            "account": entry["account"],
            "amount": int(entry["amount"]),
            "transaction_key": int(entry.get("transaction_key", 0)),
        }

    def _append_entry(self, entry: dict[str, Any]) -> None:
        payload = json.dumps(entry, separators=(",", ":"), sort_keys=True).encode("utf-8") + b"\n"
        with self._entries_path.open("ab", buffering=0) as file:
            file.write(payload)
            file.flush()
            os.fsync(file.fileno())
        self._entries.append(entry)

    def _load_entries(self) -> None:
        if not self._entries_path.exists():
            return
        try:
            with self._entries_path.open("rb") as file:
                for raw_line in file:
                    if not raw_line.endswith(b"\n"):
                        break
                    entry = json.loads(raw_line)
                    if not isinstance(entry, dict):
                        raise ValueError("ledger entry must be an object")
                    self._entries.append(entry)
            # Only a complete debit/credit pair establishes the durable
            # idempotency marker. A crash may leave one side materialized;
            # recovery must still replay that transaction.
            by_transaction: dict[str, set[str]] = {}
            transaction_keys: dict[str, str] = {}
            for loaded in self._entries:
                txid = str(loaded["transaction_id"])
                by_transaction.setdefault(txid, set()).add(str(loaded["entry_type"]))
                transaction_keys[txid] = str(loaded["idempotency_key"])
            for txid, types in by_transaction.items():
                if {"debit", "credit"}.issubset(types):
                    self._transaction_ids.add(txid)
                    self._idempotency[transaction_keys[txid]] = txid
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
            raise StorageCorruptionError("ledger entry file is invalid") from error


def suspicious_key_combinations(keys: Iterable[int], target: int, arity: int = 3) -> list[tuple[int, ...]]:
    """Return unique 3Sum/4Sum-style combinations whose keys hit ``target``.

    The validator is intentionally small and deterministic: sorted keys are
    searched with hash complements, then duplicate combinations are removed.
    """
    values = sorted(set(int(key) for key in keys))
    if arity not in (3, 4):
        raise ValueError("arity must be 3 or 4")
    combinations: set[tuple[int, ...]] = set()

    def two_sum(start: int, needed: int) -> list[tuple[int, int]]:
        seen: set[int] = set()
        pairs: set[tuple[int, int]] = set()
        for value in values[start:]:
            complement = needed - value
            if complement in seen:
                pairs.add(tuple(sorted((complement, value))))
            seen.add(value)
        return sorted(pairs)

    if arity == 3:
        for index, first in enumerate(values):
            for second, third in two_sum(index + 1, target - first):
                if second > first:
                    combinations.add((first, second, third))
    else:
        for i, first in enumerate(values):
            for j in range(i + 1, len(values)):
                second = values[j]
                for third, fourth in two_sum(j + 1, target - first - second):
                    candidate = (first, second, third, fourth)
                    if len(set(candidate)) == arity:
                        combinations.add(candidate)
    return sorted(combinations)
