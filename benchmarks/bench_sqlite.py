import sqlite3
import timeit

from dupfilter.filters.memory import MemoryFilter
from dupfilter.filters.sql.sqlite import SQLiteFilter
from tests.helpers import quiet_logger


SIZES = [100, 1000, 10000]
ROUNDS = 3


def bench_memory(n):
    flt = MemoryFilter(logger=quiet_logger())
    flt.exists_and_insert_many([str(i) for i in range(n)])


def bench_sqlite_atomic(n):
    connection = sqlite3.connect(":memory:")
    flt = SQLiteFilter(
        connection, "dup", mode="atomic", logger=quiet_logger())
    flt.exists_and_insert_many([str(i) for i in range(n)])
    connection.close()


def bench_sqlite_batch(n):
    connection = sqlite3.connect(":memory:")
    flt = SQLiteFilter(
        connection, "dup", mode="batch", logger=quiet_logger())
    flt.exists_and_insert_many([str(i) for i in range(n)])
    connection.close()


def run():
    benches = {
        "memory": bench_memory,
        "sqlite atomic": bench_sqlite_atomic,
        "sqlite batch": bench_sqlite_batch,
    }
    print(f"{'backend':<16}{'n':>8}{'time(s)':>12}")
    for name, func in benches.items():
        for n in SIZES:
            elapsed = timeit.timeit(
                lambda: func(n), number=ROUNDS) / ROUNDS
            print(f"{name:<16}{n:>8}{elapsed:>12.4f}")


if __name__ == "__main__":
    run()
