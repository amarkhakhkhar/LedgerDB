"""Transactional facade coordinating WAL and disk-backed columns."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .columns import ColumnStore
from .wal import WriteAheadLog


class LedgerDB:
    """A small durable row-append database backed by a WAL and column files.

    Durability order is fixed: the append is fsynced to the WAL, then applied
    to column files. On startup, every WAL record beyond the column row count
    is replayed, providing idempotent crash recovery.
    """

    def __init__(self, data_directory: str | Path) -> None:
        root = Path(data_directory)
        root.mkdir(parents=True, exist_ok=True)
        self._columns = ColumnStore(root / "columns")
        self._wal = WriteAheadLog(root / "wal" / "ledger.wal")
        self._recover()

    def insert(self, values: Mapping[str, Any]) -> None:
        """Commit one schema-consistent row using WAL-before-data ordering."""
        row = dict(values)
        if not row:
            raise ValueError("rows cannot be empty")
        record = {"operation": "insert", "values": row}
        self._wal.append(record)
        # Test-only fault injection: models power loss precisely after the WAL
        # commit point and before any columnar write. It is intentionally an
        # environment switch so the proof exercises a separate OS process.
        if os.environ.get("LEDGERDB_CRASH_AFTER_WAL") == "1":
            os._exit(137)
        self._columns.append(row)

    def rows(self) -> list[dict[str, Any]]:
        """Return all durable, recovered rows in insertion order."""
        return self._columns.read_rows()

    @property
    def row_count(self) -> int:
        """Number of recovered, visible rows."""
        return self._columns.row_count

    def _recover(self) -> None:
        """Replay committed WAL records not yet reflected in column storage."""
        applied = self._columns.row_count
        for sequence, record in enumerate(self._wal.records()):
            if record.get("operation") != "insert" or not isinstance(record.get("values"), dict):
                raise ValueError("unsupported WAL record")
            if sequence >= applied:
                self._columns.append(record["values"])
