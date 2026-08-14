"""Day 10 end-to-end benchmark suite for the integrated LedgerDB stack."""
from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ledgerdb import LedgerDB
from ledgerdb.sql import QueryPlanner

def timed(fn):
    start=time.perf_counter(); value=fn(); return value, time.perf_counter()-start

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--rows", type=int, default=100_000); args=ap.parse_args()
    root=Path(tempfile.mkdtemp(prefix="ledgerdb-day10-bench-"))
    try:
        db=LedgerDB(root)
        batch=[{"key":i%1000,"amount":i%100,"category":i%10} for i in range(args.rows)]
        _, insert_s=timed(lambda: db.bulk_insert(batch))
        scan, scan_s=timed(lambda: db.filter_eq("key",42,use_index=False))
        indexed, index_s=timed(lambda: db.filter_eq("key",42,use_index=True))
        planner=QueryPlanner(db)
        sql_result, sql_s=timed(lambda: planner.execute("SELECT category, SUM(amount) AS total FROM ledger GROUP BY category"))
        tx_id, tx_s=timed(lambda: db.post_transaction("day10-1","cash","revenue",100,transaction_key=10))
        retry_id, retry_s=timed(lambda: db.post_transaction("day10-1","cash","revenue",100,transaction_key=10))
        balance=db.ledger_balance()
        if scan != indexed: raise AssertionError("index result differs from scan")
        if tx_id != retry_id: raise AssertionError("idempotency retry created a different transaction")
        if balance[0] != balance[1]: raise AssertionError(f"unbalanced ledger: {balance}")
        report={"rows":args.rows,"insert_seconds":insert_s,"equality_scan_seconds":scan_s,"equality_index_seconds":index_s,"equality_speedup":scan_s/index_s if index_s else None,"group_by_seconds":sql_s,"group_count":len(sql_result),"transaction_seconds":tx_s,"retry_seconds":retry_s,"ledger_balance":{"debits":balance[0],"credits":balance[1]}}
        print(json.dumps(report,indent=2))
    finally:
        shutil.rmtree(root,ignore_errors=True)
if __name__ == "__main__": main()
