"""Small reader-writer lock used to protect a LedgerDB commit boundary."""

from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Iterator


class RWLock:
    """Writer-preference reader-writer lock.

    Multiple readers may hold the lock simultaneously. Writers are exclusive and
    block new readers once a writer is waiting, preventing writer starvation.
    A writer may re-enter the write lock from the same thread.
    """

    def __init__(self) -> None:
        self._condition = threading.Condition(threading.RLock())
        self._readers = 0
        self._writer: int | None = None
        self._writer_depth = 0
        self._waiting_writers = 0
        self._reader_depth: dict[int, int] = {}

    @contextmanager
    def read(self) -> Iterator[None]:
        ident = threading.get_ident()
        with self._condition:
            # A writer already owns the lock. Allow that writer to call private
            # read helpers without deadlocking itself.
            if self._writer == ident:
                yield
                return
            while self._writer is not None or self._waiting_writers:
                self._condition.wait()
            self._readers += 1
            self._reader_depth[ident] = self._reader_depth.get(ident, 0) + 1
        try:
            yield
        finally:
            with self._condition:
                depth = self._reader_depth.get(ident, 0) - 1
                if depth <= 0:
                    self._reader_depth.pop(ident, None)
                else:
                    self._reader_depth[ident] = depth
                self._readers -= 1
                if self._readers == 0:
                    self._condition.notify_all()

    @contextmanager
    def write(self) -> Iterator[None]:
        ident = threading.get_ident()
        with self._condition:
            if self._writer == ident:
                self._writer_depth += 1
                try:
                    yield
                finally:
                    self._writer_depth -= 1
                return
            self._waiting_writers += 1
            try:
                while self._writer is not None or self._readers:
                    self._condition.wait()
                self._writer = ident
                self._writer_depth = 1
            finally:
                self._waiting_writers -= 1
        try:
            yield
        finally:
            with self._condition:
                self._writer_depth -= 1
                if self._writer_depth == 0:
                    self._writer = None
                    self._condition.notify_all()
