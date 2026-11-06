# Measure the false-positive rate of the bloom filter offline:
# offsets are deterministic, so we can count collisions without Redis.
from __future__ import annotations

from dupfilter.filters.redis.bloomfilter import RedisBloomFilter


class _NoopServer:
    def register_script(self, source):
        return lambda **kwargs: None


def measure(expected_items: int, target_fp: float,
            probes: int = 20000) -> float:
    flt = RedisBloomFilter(
        _NoopServer(), "bf", expected_items=expected_items,
        false_positive_rate=target_fp)
    used_offsets: set[str] = set()
    for i in range(expected_items):
        offsets = flt._get_keys_and_offsets([str(i)])[1][0].split(",")
        used_offsets.update(offsets)
    false_positives = 0
    for i in range(expected_items, expected_items + probes):
        offsets = flt._get_keys_and_offsets([str(i)])[1][0].split(",")
        if all(offset in used_offsets for offset in offsets):
            false_positives += 1
    return false_positives / probes


if __name__ == "__main__":
    print(f"{'expected_items':>14}{'target_fp':>10}{'measured_fp':>12}")
    for expected_items, target_fp in [(1000, 0.01), (10000, 0.01),
                                      (100000, 0.001)]:
        measured = measure(expected_items, target_fp)
        print(f"{expected_items:>14}{target_fp:>10.3f}{measured:>12.4f}")
