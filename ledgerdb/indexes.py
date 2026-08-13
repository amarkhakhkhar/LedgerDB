"""Persistent hash-based inverted indexes for equality predicates."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterable


class EqualityIndex:
    """Persistent ``value -> row ids`` index for one column.

    The index is derived from the WAL/column data. Appends are accumulated in
    memory and persisted in batches; a flush writes only new entries and fsyncs
    once. If the index is missing or incomplete after a crash, the database
    rebuilds it from the authoritative column data.
    """

    def __init__(self, directory: Path, column: str) -> None:
        self._directory = directory
        self._directory.mkdir(parents=True, exist_ok=True)
        self._path = self._directory / f"{column}.eqindex.jsonl"
        self._map: dict[str, list[int]] = {}
        self._pending: list[tuple[Any, int]] = []
        self._row_count = 0
        self._load()

    @staticmethod
    def _key(value: Any) -> str:
        return json.dumps(value, separators=(",", ":"), sort_keys=True)

    @property
    def row_count(self) -> int:
        return self._row_count

    def append(self, value: Any, row_id: int) -> None:
        self._map.setdefault(self._key(value), []).append(row_id)
        self._pending.append((value, row_id))
        self._row_count += 1

    def lookup(self, value: Any) -> list[int]:
        return list(self._map.get(self._key(value), ()))

    def clear(self) -> None:
        self._map.clear()
        self._pending.clear()
        self._row_count = 0

    def rebuild(self, values: Iterable[Any]) -> None:
        self.clear()
        for row_id, value in enumerate(values):
            self._map.setdefault(self._key(value), []).append(row_id)
            self._row_count += 1
        self._rewrite()

    def flush(self) -> None:
        """Persist only pending entries and fsync once."""
        if not self._pending:
            return
        with self._path.open("ab", buffering=0) as file:
            for value, row_id in self._pending:
                payload = json.dumps(
                    {"value": value, "row_id": row_id},
                    separators=(",", ":"),
                    sort_keys=True,
                ).encode("utf-8") + b"\n"
                file.write(payload)
            file.flush()
            os.fsync(file.fileno())
        self._pending.clear()

    def _rewrite(self) -> None:
        temporary = self._path.with_suffix(".tmp")
        with temporary.open("wb", buffering=0) as file:
            for key, row_ids in self._map.items():
                value = json.loads(key)
                for row_id in row_ids:
                    payload = json.dumps(
                        {"value": value, "row_id": row_id},
                        separators=(",", ":"),
                        sort_keys=True,
                    ).encode("utf-8") + b"\n"
                    file.write(payload)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, self._path)
        self._pending.clear()

    def _load(self) -> None:
        if not self._path.exists():
            return
        try:
            with self._path.open("rb") as file:
                for raw_line in file:
                    if not raw_line.endswith(b"\n"):
                        break
                    record = json.loads(raw_line)
                    if not isinstance(record, dict):
                        raise ValueError("index record must be an object")
                    value = record["value"]
                    row_id = int(record["row_id"])
                    self._map.setdefault(self._key(value), []).append(row_id)
                    self._row_count += 1
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            self.clear()
