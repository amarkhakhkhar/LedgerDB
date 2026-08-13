"""Transactional facade coordinating WAL, column storage, and equality indexes."""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np

from .concurrency import RWLock
from .analytics import GroupByResult, HashGroupBy, PrefixSumIndex
from .columns import ColumnStore
from .indexes import EqualityIndex, RangeIndex
from .transactions import LedgerTransactionStore, suspicious_key_combinations
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
        self._range_indexes: dict[str, RangeIndex] = {}
        self._lock = RWLock()
        self._transactions = LedgerTransactionStore(root / "ledger")
        self._recover()
        self._reconcile_indexes()
        self._transactions.recover()

    def insert(self, values: Mapping[str, Any]) -> None:
        """Commit one row atomically with respect to all readers."""
        with self._lock.write():
            self._insert_unlocked(dict(values))

    def bulk_insert(self, rows: list[Mapping[str, Any]], *, flush_indexes: bool = True) -> None:
        """Durably append a batch as one writer-critical section."""
        with self._lock.write():
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

    def post_transaction(
        self,
        idempotency_key: str,
        debit_account: str,
        credit_account: str,
        amount: int,
        *,
        transaction_key: int = 0,
    ) -> str:
        """Atomically post a balanced debit/credit pair.

        Retries with the same idempotency key are no-ops and return the
        original transaction id. The transaction WAL is the durability source
        of truth; recovery materializes any missing ledger side.
        """
        with self._lock.write():
            existing = self._transactions.transaction_for_key(idempotency_key)
            if existing is not None:
                return existing
            transaction_id = f"tx-{idempotency_key}"
            self._transactions.apply_transaction(
                transaction_id,
                idempotency_key,
                {"account": debit_account, "amount": amount, "transaction_key": transaction_key},
                {"account": credit_account, "amount": amount, "transaction_key": transaction_key},
            )
            return transaction_id

    def ledger_entries(self) -> list[dict[str, Any]]:
        with self._lock.read():
            return self._transactions.entries

    def ledger_balance(self) -> tuple[int, int]:
        with self._lock.read():
            return self._transactions.balance()

    def validate_suspicious_transactions(self, target: int, *, arity: int = 3) -> list[tuple[int, ...]]:
        with self._lock.read():
            keys = [entry["transaction_key"] for entry in self._transactions.entries]
            return suspicious_key_combinations(keys, target, arity)

    def rows(self) -> list[dict[str, Any]]:
        with self._lock.read():
            return self._columns.read_rows()

    def filter_eq(self, column: str, value: Any, *, use_index: bool = True) -> list[dict[str, Any]]:
        """Return rows satisfying ``column == value`` from one consistent state."""
        with self._lock.read():
            return self._filter_eq_unlocked(column, value, use_index=use_index)

    def filter_between(self, column: str, lower: Any, upper: Any) -> list[dict[str, Any]]:
        """Return an inclusive range using a sorted index and binary search."""
        with self._lock.read():
            if column not in self._columns.columns:
                raise KeyError(f"unknown query column: {column!r}")
            row_ids = self._get_range_index(column).lookup_between(lower, upper)
            # Index order is value order; SQL without ORDER BY retains durable row order.
            return self._columns.read_rows_by_ids(sorted(row_ids))

    def validate_filter_index(self, column: str, value: Any) -> bool:
        """Validate index results against a scan while holding one read lock."""
        with self._lock.read():
            indexed = self._filter_eq_unlocked(column, value, use_index=True)
            scanned = self._filter_eq_unlocked(column, value, use_index=False)
            return indexed == scanned

    def group_by(self, key_column: str, value_column: str) -> GroupByResult:
        with self._lock.read():
            rows = self._columns.read_rows()
            try:
                keys = np.asarray([row[key_column] for row in rows], dtype=np.int64)
                values = np.asarray([row[value_column] for row in rows], dtype=np.float64)
            except KeyError as error:
                raise KeyError(f"unknown query column: {error.args[0]!r}") from error
            return HashGroupBy.aggregate(keys, values)

    def sort_merge_join(
        self, other: "LedgerDB", left_column: str, right_column: str
    ) -> list[dict[str, Any]]:
        """Join two databases using a sort-merge join over a consistent read state.

        Result keys are prefixed with ``left.`` and ``right.`` to avoid collisions.
        Duplicate join keys produce all matching pairs. Locks are acquired in a
        deterministic object order so two concurrent cross-database joins cannot
        deadlock.
        """
        first, second = (self, other) if id(self) < id(other) else (other, self)
        with first._lock.read():
            with second._lock.read():
                left_rows = self._columns.read_rows()
                right_rows = other._columns.read_rows()
                if left_column not in self._columns.columns:
                    raise KeyError(f"unknown query column: {left_column!r}")
                if right_column not in other._columns.columns:
                    raise KeyError(f"unknown query column: {right_column!r}")
                left_sorted = sorted(left_rows, key=lambda row: row[left_column])
                right_sorted = sorted(right_rows, key=lambda row: row[right_column])
                result: list[dict[str, Any]] = []
                i = j = 0
                while i < len(left_sorted) and j < len(right_sorted):
                    left_key = left_sorted[i][left_column]
                    right_key = right_sorted[j][right_column]
                    if left_key < right_key:
                        i += 1
                    elif left_key > right_key:
                        j += 1
                    else:
                        i_end = i
                        while i_end < len(left_sorted) and left_sorted[i_end][left_column] == left_key:
                            i_end += 1
                        j_end = j
                        while j_end < len(right_sorted) and right_sorted[j_end][right_column] == right_key:
                            j_end += 1
                        for left_row in left_sorted[i:i_end]:
                            for right_row in right_sorted[j:j_end]:
                                merged = {f"left.{key}": value for key, value in left_row.items()}
                                merged.update({f"right.{key}": value for key, value in right_row.items()})
                                result.append(merged)
                        i, j = i_end, j_end
                return result

    def prefix_sum(self, column: str) -> PrefixSumIndex:
        with self._lock.read():
            try:
                values = np.asarray([row[column] for row in self._columns.read_rows()], dtype=np.float64)
            except KeyError as error:
                raise KeyError(f"unknown query column: {error.args[0]!r}") from error
            return PrefixSumIndex(values)

    @property
    def row_count(self) -> int:
        with self._lock.read():
            return self._columns.row_count

    def close(self) -> None:
        with self._lock.write():
            self._flush_indexes()

    def _insert_unlocked(self, row: dict[str, Any]) -> None:
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

    def _filter_eq_unlocked(self, column: str, value: Any, *, use_index: bool) -> list[dict[str, Any]]:
        if column not in self._columns.columns:
            raise KeyError(f"unknown query column: {column!r}")
        if use_index:
            row_ids = self._get_index(column).lookup(value)
            return self._columns.read_rows_by_ids(row_ids)
        return [row for row in self._columns.read_rows() if row[column] == value]

    def _get_index(self, column: str) -> EqualityIndex:
        index = self._indexes.get(column)
        if index is None:
            index = EqualityIndex(self._index_directory, column)
            self._indexes[column] = index
        return index

    def _get_range_index(self, column: str) -> RangeIndex:
        index = self._range_indexes.get(column)
        if index is None:
            index = RangeIndex(self._columns.read_column(column))
            self._range_indexes[column] = index
        return index

    def _append_to_indexes(self, row: dict[str, Any], row_id: int) -> None:
        for column, value in row.items():
            self._get_index(column).append(value, row_id)
            if column in self._range_indexes:
                self._range_indexes[column].append(value, row_id)

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
