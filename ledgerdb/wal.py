"""Durable JSON-lines write-ahead log."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from .errors import StorageCorruptionError


class WriteAheadLog:
    """Append-only log that durably records mutations before data-file writes."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.touch(exist_ok=True)

    def append(self, record: dict[str, Any]) -> None:
        """Append and fsync one complete mutation record.

        The newline is written with the JSON payload as one append. After
        ``fsync`` returns, recovery can replay this record even if the process
        dies before the matching columnar-file update.
        """
        payload = json.dumps(record, separators=(",", ":"), sort_keys=True).encode("utf-8") + b"\n"
        with self._path.open("ab", buffering=0) as file:
            file.write(payload)
            file.flush()
            os.fsync(file.fileno())

    def records(self) -> Iterator[dict[str, Any]]:
        """Yield valid committed records, ignoring an incomplete final crash tail."""
        with self._path.open("rb") as file:
            for line_number, raw_line in enumerate(file, start=1):
                if not raw_line.endswith(b"\n"):
                    break
                try:
                    record = json.loads(raw_line)
                except json.JSONDecodeError as error:
                    raise StorageCorruptionError(f"invalid WAL record at line {line_number}") from error
                if not isinstance(record, dict):
                    raise StorageCorruptionError(f"WAL record at line {line_number} must be an object")
                yield record
