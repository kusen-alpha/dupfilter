# -*-coding:utf-8 -*-
from __future__ import annotations

import threading
from collections.abc import Iterable
from typing import Any

from dupfilter.filters import Filter, decorate_warning, to_thread
from dupfilter.filters.simhash import similarity

_BITS = 64


def _gray_pixels(source: Any, size: tuple[int, int]) -> list[int]:
    from PIL import Image
    resample = getattr(Image, "Resampling", Image).LANCZOS
    owns = not hasattr(source, "read")
    image = Image.open(source if not owns else str(source))
    try:
        return list(image.convert("L").resize(
            size, resample).getdata())
    finally:
        if owns:
            image.close()


def ahash(source: Any, hash_size: int = 8) -> int:
    """Average hash: compare pixels against the mean of an 8x8 thumbnail."""
    pixels = _gray_pixels(source, (hash_size, hash_size))
    average = sum(pixels) / len(pixels)
    fingerprint = 0
    for index, pixel in enumerate(pixels):
        if pixel >= average:
            fingerprint |= 1 << index
    return fingerprint


def dhash(source: Any, hash_size: int = 8) -> int:
    """Difference hash: compare horizontally adjacent pixels; robust to
    brightness changes."""
    pixels = _gray_pixels(source, (hash_size + 1, hash_size))
    fingerprint = 0
    index = 0
    for row in range(hash_size):
        for col in range(hash_size):
            left = pixels[row * (hash_size + 1) + col]
            right = pixels[row * (hash_size + 1) + col + 1]
            if left > right:
                fingerprint |= 1 << index
            index += 1
    return fingerprint


def image_hash(source: Any, method: str = "dhash",
               hash_size: int = 8) -> int:
    if method == "dhash":
        return dhash(source, hash_size)
    if method == "ahash":
        return ahash(source, hash_size)
    raise ValueError("method must be 'dhash' or 'ahash'")


class ImageHashFilter(Filter):
    """Near-duplicate image dedup based on perceptual hashes.

    Each image becomes a 64-bit fingerprint, indexed into 8 bit bands (LSH)
    with candidates verified by the exact hamming distance, so results have
    no false positives. ``dhash`` is the default method.
    """

    def __init__(self, method: str = "dhash", hash_size: int = 8,
                 threshold: float = 0.9, bands: int = 8,
                 *args: Any, **kwargs: Any) -> None:
        if method not in ("dhash", "ahash"):
            raise ValueError("method must be 'dhash' or 'ahash'")
        if not 0 <= threshold <= 1:
            raise ValueError('threshold must be in [0, 1]')
        if bands <= 0 or _BITS % bands:
            raise ValueError(f'bands must divide {_BITS}')
        self.method = method
        self.hash_size = hash_size
        self.threshold = threshold
        self.bands = bands
        self.band_size = _BITS // bands
        self.band_mask = (1 << self.band_size) - 1
        self._index: dict[int, set[int]] = {}
        self._fingerprints: set[int] = set()
        self._lock = threading.RLock()
        super().__init__(*args, **kwargs)

    def _compute(self, source: Any) -> int:
        return image_hash(source, self.method, self.hash_size)

    def _band_keys(self, fingerprint: int) -> list[int]:
        return [
            band * (1 << 16) + ((fingerprint >> (band * self.band_size))
                                & self.band_mask)
            for band in range(self.bands)
        ]

    def add(self, source: Any) -> int:
        """Index the perceptual hash of ``source`` (path or file-like)."""
        fingerprint = self._compute(source)
        return self.add_hash(fingerprint)

    def add_hash(self, fingerprint: int) -> int:
        if fingerprint in self._fingerprints:
            return fingerprint
        with self._lock:
            self._fingerprints.add(fingerprint)
            for key in self._band_keys(fingerprint):
                self._index.setdefault(key, set()).add(fingerprint)
        return fingerprint

    def find_similar(self, source: Any, threshold: float | None = None,
                     limit: int | None = None) -> list[tuple[int, float]]:
        """Return ``(fingerprint, similarity)`` pairs above the threshold,
        best match first."""
        threshold = self.threshold if threshold is None else threshold
        fingerprint = self._compute(source)
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

    def exists_similar(self, source: Any,
                       threshold: float | None = None) -> bool:
        return bool(self.find_similar(source, threshold, limit=1))

    @decorate_warning
    def exists(self, value: Any) -> bool:
        """Exact-membership check of an image hash, not similarity."""
        return self._compute(value) in self._fingerprints

    @decorate_warning
    def exists_many(self, values: Iterable[Any]) -> list[bool]:
        return [self.exists(value) for value in values]

    def count(self) -> int:
        return len(self._fingerprints)

    def clear(self) -> None:
        with self._lock:
            self._index = {}
            self._fingerprints = set()


class AsyncImageHashFilter:
    """Async API over :class:`ImageHashFilter`.

    Image decoding is CPU/IO work, so each call runs in a worker thread via
    :func:`to_thread` and keeps the event loop responsive.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._flt: Any = ImageHashFilter(*args, **kwargs)

    async def add(self, source: Any) -> int:
        return int(await to_thread(self._flt.add, source))

    async def add_hash(self, fingerprint: int) -> int:
        return int(self._flt.add_hash(fingerprint))

    async def find_similar(self, source: Any,
                           threshold: float | None = None,
                           limit: int | None = None) -> Any:
        return await to_thread(
            self._flt.find_similar, source, threshold, limit)

    async def exists_similar(self, source: Any,
                             threshold: float | None = None) -> bool:
        return bool(await to_thread(
            self._flt.exists_similar, source, threshold))

    async def exists(self, value: Any, **kwargs: Any) -> Any:
        return await to_thread(self._flt.exists, value, **kwargs)

    async def count(self) -> int:
        return int(self._flt.count())

    async def clear(self) -> None:
        self._flt.clear()

    async def close(self) -> None:
        self._flt.close()
