"""Memory-bounded batch tuning using binary search on the feasible answer."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BatchPlan:
    batch_size: int
    batch_count: int
    estimated_peak_bytes: int
    estimated_cost: float


class BatchTuner:
    """Choose the lowest-cost batch that fits the supplied memory budget.

    The cost model has a fixed cost per batch and a per-row cost. For this
    monotonic model, fewer batches are cheaper, so the optimum is the largest
    feasible chunk. ``tune`` finds it with binary search rather than trying
    every candidate size.
    """

    def __init__(self, *, row_bytes: int, memory_budget_bytes: int, fixed_batch_bytes: int = 4096,
                 batch_overhead_cost: float = 1.0, row_cost: float = 0.01) -> None:
        if row_bytes <= 0 or memory_budget_bytes <= 0 or fixed_batch_bytes < 0:
            raise ValueError("memory sizes must be positive (fixed overhead may be zero)")
        self.row_bytes = row_bytes
        self.memory_budget_bytes = memory_budget_bytes
        self.fixed_batch_bytes = fixed_batch_bytes
        self.batch_overhead_cost = batch_overhead_cost
        self.row_cost = row_cost

    def tune(self, total_rows: int, *, maximum_batch_size: int | None = None) -> BatchPlan:
        if total_rows <= 0:
            raise ValueError("total_rows must be positive")
        ceiling = min(total_rows, maximum_batch_size or total_rows)
        if self.estimated_bytes(1) > self.memory_budget_bytes:
            raise ValueError("memory budget cannot hold one row")
        low, high, answer = 1, ceiling, 1
        while low <= high:
            candidate = (low + high) // 2
            if self.estimated_bytes(candidate) <= self.memory_budget_bytes:
                answer = candidate
                low = candidate + 1
            else:
                high = candidate - 1
        batches = (total_rows + answer - 1) // answer
        return BatchPlan(answer, batches, self.estimated_bytes(answer),
                         batches * self.batch_overhead_cost + total_rows * self.row_cost)

    def estimated_bytes(self, batch_size: int) -> int:
        return self.fixed_batch_bytes + batch_size * self.row_bytes
