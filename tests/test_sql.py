from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ledgerdb import LedgerDB, QueryPlanner


class SQLPlannerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.database = LedgerDB(Path(self.temporary.name))
        self.database.bulk_insert([
            {"key": 1, "amount": 10, "category": 1},
            {"key": 2, "amount": 20, "category": 1},
            {"key": 2, "amount": 30, "category": 2},
            {"key": 3, "amount": 40, "category": 2},
        ])
        self.planner = QueryPlanner(self.database)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_select_where_between_uses_binary_range_index(self) -> None:
        sql = "SELECT key, amount FROM ledger WHERE amount BETWEEN 15 AND 35"
        self.assertIn("range_index_binary_search(amount)", self.planner.explain(sql))
        self.assertEqual(self.planner.execute(sql), [{"key": 2, "amount": 20}, {"key": 2, "amount": 30}])

    def test_select_where_group_and_join(self) -> None:
        self.assertEqual(self.planner.execute("SELECT key FROM ledger WHERE key = 2"), [{"key": 2}, {"key": 2}])
        self.assertEqual(
            self.planner.execute("SELECT category, SUM(amount) AS total FROM ledger GROUP BY category"),
            [{"category": 1, "total": 30.0}, {"category": 2, "total": 70.0}],
        )
        joined = self.planner.execute("SELECT a.key, a.amount, b.category FROM ledger a JOIN ledger b ON a.key = b.key WHERE a.amount BETWEEN 20 AND 20")
        self.assertEqual(joined, [{"a.key": 2, "a.amount": 20, "b.category": 1}, {"a.key": 2, "a.amount": 20, "b.category": 2}])
