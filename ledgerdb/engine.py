"""Transactional facade coordinating WAL, column storage, and equality indexes."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np

from .analytics import GroupByResult, HashGroupBy, PrefixSumIndex
from .columns import ColumnStore
from .indexes import EqualityIndex
from .wal import WriteAheadLog


class LedgerDB:
    """Small durable row-append database with persistent equality indexes."""

    def __init__(self, data_directory: str | Path) -> None:
        root = Path(data_directory)
        root.mkdir(parents=True, exist_ok=True)
        self._columns = ColumnStore(root / "columns")
        self._wal = WriteAheadLog(root / "wal" / "ledger.wal")
        self._index_directory = root / "indexes"
        self._index_directory.mkdir(parents=True, exist_ok=True)
        self._indexes: dict[str, EqualityIndex] = {}
        self._recover()
        self._reconcile_indexes()

    def insert(self, values: Mapping[str, Any]) -> None:
        row = dict(values)
        if not row:
            raise ValueError("rows cannot be empty")
        record = {"operation": "insert", "values": row}
        self._wal.append(record)
        if os.environ.get("LEDGERDB_CRASH_AFTER_WAL") == "1":
            os._exit(137)
        row_id = self._columns.row_count
        self._columns.append(row)
        self._append_to_indexes(row, row_id)
        self._flush_indexes()

    def bulk_insert(self, rows: list[Mapping[str, Any]], *, flush_indexes: bool = True) -> None:
        """Durably append a batch; index flushing can be deferred for bulk setup."""
        normalized = [dict(row) for row in rows]
        if not normalized:
            return
        if any(not row for row in normalized):
            raise ValueError("rows cannot be empty")
        records = [{"operation": "insert", "values": row} for row in normalized]
        self._wal.append_many(records)
        start = self._columns.row_count
        self._columns.append_many(normalized)
        for offset, row in enumerate(normalized):
            self._append_to_indexes(row, start + offset)
        if flush_indexes:
            self._flush_indexes()

    def rows(self) -> list[dict[str, Any]]:
        return self._columns.read_rows()

    def filter_eq(self, column: str, value: Any, *, use_index: bool = True) -> list[dict[str, Any]]:
        """Return rows satisfying ``column == value`` using an optional index."""
        if column not in self._columns.columns:
            raise KeyError(f"unknown query column: {column!r}")
        if use_index:
            index = self._get_index(column)
            row_ids = index.lookup(value)
            return self._columns.read_rows_by_ids(row_ids)
        return [row for row in self.rows() if row[column] == value]

    def group_by(self, key_column: str, value_column: str) -> GroupByResult:
        rows = self.rows()
        try:
            keys = np.asarray([row[key_column] for row in rows], dtype=np.int64)
            values = np.asarray([row[value_column] for row in rows], dtype=np.float64)
        except KeyError as error:
            raise KeyError(f"unknown query column: {error.args[0]!r}") from error
        return HashGroupBy.aggregate(keys, values)

    def prefix_sum(self, column: str) -> PrefixSumIndex:
        try:
            values = np.asarray([row[column] for row in self.rows()], dtype=np.float64)
        except KeyError as error:
            raise KeyError(f"unknown query column: {error.args[0]!r}") from error
        return PrefixSumIndex(values)

    @property
    def row_count(self) -> int:
        return self._columns.row_count

    def close(self) -> None:
        self._flush_indexes()

    def _get_index(self, column: str) -> EqualityIndex:
        index = self._indexes.get(column)
        if index is None:
            index = EqualityIndex(self._index_directory, column)
            self._indexes[column] = index
        return index

    def _append_to_indexes(self, row: dict[str, Any], row_id: int) -> None:
        for column, value in row.items():
            self._get_index(column).append(value, row_id)

    def _flush_indexes(self) -> None:
        for index in self._indexes.values():
            index.flush()

    def _recover(self) -> None:
        applied = self._columns.row_count
        for sequence, record in enumerate(self._wal.records()):
            if record.get("operation") != "insert" or not isinstance(record.get("values"), dict):
                raise ValueError("unsupported WAL record")
            if sequence >= applied:
                self._columns.append(record["values"])

    def _reconcile_indexes(self) -> None:
        columns = self._columns.columns
        row_count = self._columns.row_count
        for column in columns:
            index = self._get_index(column)
            if index.row_count != row_count:
                values = self._columns.read_column(column, row_count)
                index.rebuild(values)
