# Multi-worker dedup check: N threads race on the same value and must
# produce exactly one duplicate-free claim on each backend.
from __future__ import annotations

import os
import sqlite3
import tempfile
import threading
import time
from typing import Callable

from dupfilter.filters.file import FileFilter
from dupfilter.filters.sql.pool import ConnectionPool, PooledSQLFilter
from dupfilter.filters.sql.sqlite import SQLiteFilter
from tests.helpers import quiet_logger


def run_workers(workers: int, per_worker: int,
                fn: Callable[[str], object]) -> None:
    barrier = threading.Barrier(workers)

    def worker() -> None:
        barrier.wait()
        for _ in range(per_worker):
            fn("shared")

    threads = [threading.Thread(target=worker) for _ in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def bench_file(workers: int, per_worker: int) -> tuple[float, int]:
    with tempfile.TemporaryDirectory() as directory:
        flt = FileFilter(path=directory, logger=quiet_logger())
        start = time.monotonic()
        run_workers(workers, per_worker, flt.exists_and_insert)
        elapsed = time.monotonic() - start
        dup_count = len(flt.dups)
        flt.close()
        return elapsed, dup_count


def bench_pooled_sqlite(workers: int, per_worker: int) -> tuple[float, int]:
    with tempfile.TemporaryDirectory() as directory:
        db = os.path.join(directory, "dup.db")
        pool = ConnectionPool(
            connect=lambda: sqlite3.connect(db, check_same_thread=False),
            size=4)
        flt = PooledSQLFilter(
            pool, "dup", filter_cls=SQLiteFilter, logger=quiet_logger())
        start = time.monotonic()
        run_workers(workers, per_worker, flt.exists_and_insert)
        elapsed = time.monotonic() - start
        conn = sqlite3.connect(db)
        dup_count = conn.execute("SELECT COUNT(*) FROM dup").fetchone()[0]
        conn.close()
        flt.close()
        return elapsed, dup_count


if __name__ == "__main__":
    WORKERS = 8
    PER_WORKER = 200
    print(f"{'backend':<16}{'workers':>8}{'per_worker':>11}"
          f"{'time(s)':>10}{'dups':>7}")
    for name, bench in (("file", bench_file),
                        ("pooled sqlite", bench_pooled_sqlite)):
        elapsed, dup_count = bench(WORKERS, PER_WORKER)
        print(f"{name:<16}{WORKERS:>8}{PER_WORKER:>11}"
              f"{elapsed:>10.4f}{dup_count:>7}")
