import os
import timeit
import warnings

warnings.filterwarnings("ignore")

import redis  # noqa: E402

from dupfilter.filters.redis.bloomfilter import RedisBloomFilter  # noqa: E402
from dupfilter.filters.redis.setfilter import RedisSetFilter  # noqa: E402
from tests.helpers import quiet_logger  # noqa: E402


REDIS_URL = os.environ.get(
    "DUPFILTER_REDIS_URL", "redis://172.17.59.116:7379/0")
SIZES = [100, 1000, 10000]


def bench_set(server, n):
    flt = RedisSetFilter(server, "bench:set", logger=quiet_logger())
    flt.exists_and_insert_many([str(i) for i in range(n)])


def bench_bloom(server, n):
    flt = RedisBloomFilter(
        server, "bench:bf", expected_items=n * 2,
        false_positive_rate=0.01, logger=quiet_logger())
    flt.exists_and_insert_many([str(i) for i in range(n)])


def run():
    server = redis.Redis.from_url(REDIS_URL)
    server.flushdb()
    benches = {
        "redis set": bench_set,
        "redis bloom": bench_bloom,
    }
    print(f"redis: {REDIS_URL}")
    print(f"{'backend':<16}{'n':>8}{'time(s)':>12}")
    for name, func in benches.items():
        for n in SIZES:
            server.flushdb()
            elapsed = timeit.timeit(lambda: func(server, n), number=1)
            print(f"{name:<16}{n:>8}{elapsed:>12.4f}")
    server.flushdb()
    server.close()


if __name__ == "__main__":
    run()
