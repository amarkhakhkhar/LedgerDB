"""Reproducible LedgerDB analytics-kernel comparison against pandas."""
import argparse
import time
import sys
from pathlib import Path
import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ledgerdb.analytics import HashGroupBy, PrefixSumIndex

def elapsed(operation, repetitions=1):
    started = time.perf_counter()
    for _ in range(repetitions): operation()
    return (time.perf_counter() - started) / repetitions

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--groups", type=int, default=10_000)
    parser.add_argument("--range-repetitions", type=int, default=100_000)
    parser.add_argument("--seed", type=int, default=42)
    arguments = parser.parse_args()
    rng = np.random.default_rng(arguments.seed)
    keys = rng.integers(0, arguments.groups, arguments.rows, dtype=np.int64)
    values = rng.random(arguments.rows, dtype=np.float64)
    frame = pd.DataFrame({"key": keys, "value": values}, copy=False)
    index = PrefixSumIndex(values)
    print(f"rows={arguments.rows:,}, groups={arguments.groups:,}, seed={arguments.seed}")
    print("operation,ledgerdb_seconds,pandas_seconds")
    print(f"group_by,{elapsed(lambda: HashGroupBy.aggregate(keys, values)):.6f},{elapsed(lambda: frame.groupby('key', sort=True)['value'].agg(['sum','count','mean'])):.6f}")
    start, stop = 0, arguments.rows
    print(f"range_sum_avg_1m,{elapsed(lambda: (index.range_sum(start, stop), index.range_avg(start, stop)), arguments.range_repetitions):.9f},{elapsed(lambda: (frame['value'].iloc[start:stop].sum(), frame['value'].iloc[start:stop].mean()), arguments.range_repetitions):.9f}")

if __name__ == "__main__": main()
