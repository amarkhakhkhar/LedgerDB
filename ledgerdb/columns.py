"""Disk-backed append-only column files."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .errors import StorageCorruptionError


class ColumnStore:
    """One JSONL file per column with an fsynced row-count watermark.

    The watermark is the source of truth for visible rows. A process may die
    after appending a column value but before advancing it; recovery simply
    replays the durable WAL entry and makes the aligned row visible.
    """

    _METADATA_FILE = "metadata.json"

    def __init__(self, directory: Path) -> None:
        self._directory = directory
        self._directory.mkdir(parents=True, exist_ok=True)
        self._metadata_path = directory / self._METADATA_FILE
        if not self._metadata_path.exists():
            self._write_metadata({"columns": [], "row_count": 0})
        self._offsets: dict[str, list[int]] = {}
        self._load_offsets()

    @property
    def row_count(self) -> int:
        """Count of complete, visible rows."""
        return int(self._metadata()["row_count"])

    @property
    def columns(self) -> tuple[str, ...]:
        """Persisted schema, in insertion order."""
        return tuple(self._metadata()["columns"])

    def append(self, values: dict[str, Any]) -> None:
        """Append exactly one row and make it visible after all values are durable."""
        metadata = self._metadata()
        expected_columns = list(metadata["columns"])
        incoming_columns = list(values.keys())
        if not expected_columns:
            expected_columns = incoming_columns
            metadata["columns"] = expected_columns
        if set(incoming_columns) != set(expected_columns):
            raise ValueError(f"row schema {sorted(incoming_columns)!r} differs from {sorted(expected_columns)!r}")
        for column in expected_columns:
            self._append_value(column, values[column])
        metadata["row_count"] = int(metadata["row_count"]) + 1
        self._write_metadata(metadata)

    def append_many(self, rows: list[dict[str, Any]]) -> None:
        """Append a batch and fsync each column plus metadata once."""
        if not rows:
            return
        metadata = self._metadata()
        expected_columns = list(metadata["columns"])
        if not expected_columns:
            expected_columns = list(rows[0].keys())
            metadata["columns"] = expected_columns
        for row in rows:
            if set(row.keys()) != set(expected_columns):
                raise ValueError(f"row schema {sorted(row)!r} differs from {sorted(expected_columns)!r}")
        for column in expected_columns:
            path = self._directory / f"{column}.column.jsonl"
            offsets = self._offsets.setdefault(column, [])
            with path.open("ab", buffering=0) as file:
                for row in rows:
                    offsets.append(file.tell())
                    file.write(json.dumps(row[column], separators=(",", ":")).encode("utf-8") + b"\n")
                file.flush()
                os.fsync(file.fileno())
        metadata["row_count"] = int(metadata["row_count"]) + len(rows)
        self._write_metadata(metadata)

    def read_rows(self) -> list[dict[str, Any]]:
        """Read only complete rows guarded by the durable row-count watermark."""
        metadata = self._metadata()
        count = int(metadata["row_count"])
        columns = list(metadata["columns"])
        values_by_column = {column: self._read_values(column, count) for column in columns}
        return [{column: values_by_column[column][row] for column in columns} for row in range(count)]

    def read_column(self, column: str, count: int | None = None) -> list[Any]:
        """Read one persisted column up to the visible row count."""
        if column not in self.columns:
            raise KeyError(column)
        visible = self.row_count if count is None else count
        return self._read_values(column, visible)

    def read_rows_by_ids(self, row_ids: list[int]) -> list[dict[str, Any]]:
        """Read selected rows while preserving the requested row-id order."""
        if not row_ids:
            return []
        count = self.row_count
        if any(row_id < 0 or row_id >= count for row_id in row_ids):
            raise IndexError("row id lies outside the visible row range")
        return [
            {column: self._read_value_at(column, row_id) for column in self.columns}
            for row_id in row_ids
        ]

    def _append_value(self, column: str, value: Any) -> None:
        path = self._directory / f"{column}.column.jsonl"
        payload = json.dumps(value, separators=(",", ":")).encode("utf-8") + b"\n"
        offsets = self._offsets.setdefault(column, [])
        with path.open("ab", buffering=0) as file:
            offsets.append(file.tell())
            file.write(payload)
            file.flush()
            os.fsync(file.fileno())

    def _metadata(self) -> dict[str, Any]:
        try:
            return json.loads(self._metadata_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            raise StorageCorruptionError("column metadata is invalid") from error

    def _write_metadata(self, metadata: dict[str, Any]) -> None:
        temporary = self._metadata_path.with_suffix(".tmp")
        with temporary.open("w", encoding="utf-8") as file:
            json.dump(metadata, file, separators=(",", ":"), sort_keys=True)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, self._metadata_path)

    def _load_offsets(self) -> None:
        for column in self.columns:
            path = self._directory / f"{column}.column.jsonl"
            offsets: list[int] = []
            if path.exists():
                with path.open("rb") as file:
                    while True:
                        offset = file.tell()
                        raw_line = file.readline()
                        if not raw_line or not raw_line.endswith(b"\n"):
                            break
                        offsets.append(offset)
            self._offsets[column] = offsets

    def _read_value_at(self, column: str, row_id: int) -> Any:
        path = self._directory / f"{column}.column.jsonl"
        try:
            offset = self._offsets[column][row_id]
        except (KeyError, IndexError) as error:
            raise StorageCorruptionError(f"column {column} has no row {row_id}") from error
        with path.open("rb") as file:
            file.seek(offset)
            raw_line = file.readline()
        try:
            return json.loads(raw_line)
        except json.JSONDecodeError as error:
            raise StorageCorruptionError(f"invalid value in column {column}") from error

    def _read_values(self, column: str, count: int) -> list[Any]:
        path = self._directory / f"{column}.column.jsonl"
        if not path.exists() and count:
            raise StorageCorruptionError(f"column file is missing: {column}")
        values: list[Any] = []
        with path.open("rb") if path.exists() else open(os.devnull, "rb") as file:
            for raw_line in file:
                if len(values) == count:
                    break
                if not raw_line.endswith(b"\n"):
                    break
                try:
                    values.append(json.loads(raw_line))
                except json.JSONDecodeError as error:
                    raise StorageCorruptionError(f"invalid value in column {column}") from error
        if len(values) != count:
            raise StorageCorruptionError(f"column {column} has fewer values than the row watermark")
        return values
