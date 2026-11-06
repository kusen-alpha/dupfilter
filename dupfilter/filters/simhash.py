# -*- coding:utf-8 -*-
from __future__ import annotations

import re
import threading
from collections import Counter
from typing import Any

from dupfilter.filters import Filter, decorate_warning, to_thread
from dupfilter.filters.bloom import _md5_int

_WORD_RE = re.compile(r"[A-Za-z0-9_]+|[\u4e00-\u9fff]+")
_CJK_RE = re.compile(r"[\u4e00-\u9fff]")
_FEATURE_BITS = 64


def tokenize(text: str) -> list[str]:
    """Split text into word tokens plus adjacent-pair shingles.

    Consecutive CJK characters are split into character bigrams so Chinese
    text without separators still produces comparable features.
    """
    parts = _WORD_RE.findall(text.lower())
    features: list[str] = []
    for part in parts:
        if _CJK_RE.search(part):
            if len(part) >= 2:
                features.extend(
                    part[i:i + 2] for i in range(len(part) - 1))
            else:
                features.append(part)
        else:
            features.append(part)
    if len(features) >= 2:
        features.extend(
            a + "\x00" + b for a, b in zip(features, features[1:]))
    return features


def simhash(text: str) -> int:
    """Return the 64-bit simhash fingerprint of ``text``."""
    weights = Counter(tokenize(text))
    if not weights:
        return 0
    vector = [0] * _FEATURE_BITS
    for token, weight in weights.items():
        h = _md5_int(token)
        for i in range(_FEATURE_BITS):
            vector[i] += weight if (h >> i) & 1 else -weight
    fingerprint = 0
    for i in range(_FEATURE_BITS):
        if vector[i] > 0:
            fingerprint |= 1 << i
    return fingerprint


def hamming_distance(a: int, b: int) -> int:
    return bin(a ^ b).count("1")


def similarity(a: int, b: int) -> float:
    """Similarity in [0, 1]: 1 - hamming distance / bits."""
    return 1 - hamming_distance(a, b) / _FEATURE_BITS


def is_similar(a: int, b: int, threshold: float = 0.9) -> bool:
    return similarity(a, b) >= threshold


class SimHashFilter(Filter):
    """Near-duplicate text dedup based on simhash fingerprints.

    Fingerprints are indexed into disjoint bit bands (LSH). Candidates
    sharing any band are verified with the exact hamming distance, so the
    result has no false positives; for ``threshold >= 0.89`` (at most 7 of
    64 bits differ) recall is also exact.
    """

    def __init__(self, threshold: float = 0.9, bands: int = 8,
                 *args: Any, **kwargs: Any) -> None:
        if not 0 <= threshold <= 1:
            raise ValueError('threshold must be in [0, 1]')
        if bands <= 0 or _FEATURE_BITS % bands:
            raise ValueError(f'bands must divide {_FEATURE_BITS}')
        self.threshold = threshold
        self.bands = bands
        self.band_size = _FEATURE_BITS // bands
        self.band_mask = (1 << self.band_size) - 1
        self._index: dict[int, set[int]] = {}
        self._fingerprints: set[int] = set()
        self._lock = threading.RLock()
        super().__init__(*args, **kwargs)

    def _band_keys(self, fingerprint: int) -> list[int]:
        return [
            band * (1 << 16) + ((fingerprint >> (band * self.band_size))
                                & self.band_mask)
            for band in range(self.bands)
        ]

    def add(self, text: str) -> int:
        """Index ``text`` and return its fingerprint."""
        fingerprint = simhash(text)
        if fingerprint in self._fingerprints:
            return fingerprint
        with self._lock:
            self._fingerprints.add(fingerprint)
            for key in self._band_keys(fingerprint):
                self._index.setdefault(key, set()).add(fingerprint)
        return fingerprint

    @decorate_warning
    def exists(self, value: Any) -> bool:
        """Exact-membership check of a raw string, not similarity."""
        return bool(simhash(str(value)) in self._fingerprints)

    def find_similar(self, text: str, threshold: float | None = None,
                     limit: int | None = None) -> list[tuple[int, float]]:
        """Return ``(fingerprint, similarity)`` pairs above the threshold,
        best match first. An identical previously added text is a valid
        duplicate, so its own fingerprint is included.
        """
        threshold = self.threshold if threshold is None else threshold
        fingerprint = simhash(text)
        candidates: set[int] = set()
        with self._lock:
            for key in self._band_keys(fingerprint):
                candidates.update(self._index.get(key, ()))
        result = []
        for other in candidates:
            score = similarity(fingerprint, other)
            if score >= threshold:
                result.append((other, score))
        result.sort(key=lambda item: item[1], reverse=True)
        if limit is not None:
            result = result[:limit]
        return result

    def exists_similar(self, text: str,
                       threshold: float | None = None) -> bool:
        return bool(self.find_similar(text, threshold, limit=1))

    def count(self) -> int:
        return len(self._fingerprints)

    def clear(self) -> None:
        with self._lock:
            self._index = {}
            self._fingerprints = set()


class AsyncSimHashFilter:
    """Async API over :class:`SimHashFilter`.

    Text hashing is CPU work, so each call runs in a worker thread via
    :func:`to_thread` and keeps the event loop responsive.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._flt: Any = SimHashFilter(*args, **kwargs)

    async def add(self, text: str) -> int:
        return int(await to_thread(self._flt.add, text))

    async def exists(self, value: Any, **kwargs: Any) -> Any:
        return await to_thread(self._flt.exists, value, **kwargs)

    async def find_similar(self, text: str, threshold: float | None = None,
                           limit: int | None = None) -> Any:
        return await to_thread(
            self._flt.find_similar, text, threshold, limit)

    async def exists_similar(self, text: str,
                             threshold: float | None = None) -> bool:
        return bool(await to_thread(
            self._flt.exists_similar, text, threshold))

    async def count(self) -> int:
        return int(self._flt.count())

    async def clear(self) -> None:
        self._flt.clear()

    async def close(self) -> None:
        self._flt.close()
