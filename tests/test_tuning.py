from __future__ import annotations

import unittest

from ledgerdb import BatchTuner


class BatchTunerTests(unittest.TestCase):
    def test_binary_search_picks_largest_memory_safe_batch(self) -> None:
        plan = BatchTuner(row_bytes=100, fixed_batch_bytes=1000, memory_budget_bytes=5500).tune(1000)
        self.assertEqual(plan.batch_size, 45)
        self.assertEqual(plan.estimated_peak_bytes, 5500)
        self.assertEqual(plan.batch_count, 23)

    def test_rejects_budget_that_cannot_hold_a_row(self) -> None:
        with self.assertRaises(ValueError):
            BatchTuner(row_bytes=100, memory_budget_bytes=99).tune(1)
