"""In-memory analytical indexes built from LedgerDB's recovered row snapshot."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
from numpy.typing import NDArray


@dataclass(frozen=True)
class GroupByResult:
    """Typed aggregate arrays emitted by :class:`HashGroupBy`.

    Arrays are ordered by ascending group key so repeated queries provide stable,
    presentation-ready output independent of hash-slot placement.
    """

    keys: NDArray[np.int64]
    sums: NDArray[np.float64]
    counts: NDArray[np.int64]
    avgs: NDArray[np.float64]


class HashGroupBy:
    """Open-addressed hash aggregation for signed integer keys and numeric values."""

    @staticmethod
    def aggregate(keys: NDArray[np.signedinteger[Any]], values: NDArray[np.number[Any]]) -> GroupByResult:
        """Compute ``SUM``, ``COUNT``, and ``AVG`` per key.

        The table keeps load factor below 0.5. A separate occupancy bitmap means
        every signed key, including ``-1``, remains valid; no sentinel is needed.
        """
        if keys.ndim != 1 or values.ndim != 1 or len(keys) != len(values):
            raise ValueError("keys and values must be equal-length one-dimensional arrays")
        if not np.issubdtype(keys.dtype, np.signedinteger) or not np.issubdtype(values.dtype, np.number):
            raise TypeError("keys must be signed integers and values must be numeric")
        capacity = 1
        while capacity < max(2, len(keys) * 2):
            capacity <<= 1
        mask = capacity - 1
        occupied = np.zeros(capacity, dtype=np.bool_)
        slot_keys = np.empty(capacity, dtype=np.int64)
        sums = np.zeros(capacity, dtype=np.float64)
        counts = np.zeros(capacity, dtype=np.int64)
        for raw_key, raw_value in zip(keys, values, strict=True):
            key = int(raw_key)
            # masking and generating unique key slots
            slot = (key * 11400714819323198485) & mask
            while occupied[slot] and slot_keys[slot] != key:
                slot = (slot + 1) & mask
            if not occupied[slot]:
                occupied[slot] = True
                slot_keys[slot] = key
            sums[slot] += float(raw_value)
            counts[slot] += 1
        slots = np.flatnonzero(occupied)
        slots = slots[np.argsort(slot_keys[slots], kind="stable")]
        output_sums = sums[slots].copy()
        output_counts = counts[slots].copy()
        return GroupByResult(slot_keys[slots].copy(), output_sums, output_counts, output_sums / output_counts)


class PrefixSumIndex:
    """Immutable O(1) range ``SUM`` and ``AVG`` index for a numeric array."""

    def __init__(self, values: NDArray[np.number[Any]]) -> None:
        """Precompute n + 1 float64 prefix totals in O(n) time."""
        if values.ndim != 1 or not np.issubdtype(values.dtype, np.number):
            raise TypeError("prefix sums require a one-dimensional numeric array")
        self._prefix = np.empty(len(values) + 1, dtype=np.float64)
        self._prefix[0] = 0.0
        np.cumsum(values, dtype=np.float64, out=self._prefix[1:])

    def range_sum(self, start: int, stop: int) -> float:
        """Return ``SUM(values[start:stop])`` in O(1)."""
        self._validate(start, stop)
        return float(self._prefix[stop] - self._prefix[start])

    def range_avg(self, start: int, stop: int) -> float:
        """Return ``AVG(values[start:stop])`` in O(1)."""
        self._validate(start, stop)
        if start == stop:
            raise ValueError("AVG is undefined for an empty range")
        return self.range_sum(start, stop) / (stop - start)

    def _validate(self, start: int, stop: int) -> None:
        if not isinstance(start, int) or not isinstance(stop, int):
            raise TypeError("range bounds must be integers")
        if start < 0 or stop > len(self._prefix) - 1:
            raise IndexError("range lies outside the indexed values")
        if start > stop:
            raise ValueError("range start must not exceed range stop")
