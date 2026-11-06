# -*-coding:utf-8 -*-
import asyncio
import io
import unittest

try:
    from PIL import Image
except ImportError:
    Image = None

from dupfilter.filters.image_hash import (
    AsyncImageHashFilter,
    ImageHashFilter,
    ahash,
    dhash,
    image_hash,
)

from .helpers import quiet_logger


def make_gradient(size=64, offset=0):
    image = Image.new("L", (size, size))
    pixels = image.load()
    for y in range(size):
        for x in range(size):
            pixels[x, y] = min(255, (x * 255) // size + offset)
    buffer = io.BytesIO()
    image.save(buffer, "PNG")
    buffer.seek(0)
    return buffer


def make_checker(size=64, cell=8):
    image = Image.new("L", (size, size))
    pixels = image.load()
    for y in range(size):
        for x in range(size):
            pixels[x, y] = 255 if ((x // cell) + (y // cell)) % 2 else 0
    buffer = io.BytesIO()
    image.save(buffer, "PNG")
    buffer.seek(0)
    return buffer


@unittest.skipUnless(Image, "Pillow not installed")
class ImageHashFunctionTests(unittest.TestCase):
    def test_hashes_are_stable(self):
        buffer = make_gradient()
        self.assertEqual(ahash(buffer), ahash(buffer))
        self.assertEqual(dhash(buffer), dhash(buffer))

    def test_methods_produce_int_fingerprint(self):
        buffer = make_gradient()
        for value in (ahash(buffer), dhash(buffer),
                      image_hash(buffer, "dhash")):
            self.assertIsInstance(value, int)
            self.assertGreaterEqual(value, 0)
            self.assertLess(value, 1 << 64)

    def test_invalid_method_rejected(self):
        with self.assertRaises(ValueError):
            image_hash(make_gradient(), method="nope")


@unittest.skipUnless(Image, "Pillow not installed")
class ImageHashFilterTests(unittest.TestCase):
    def setUp(self):
        self.flt = ImageHashFilter(logger=quiet_logger())
        self.gradient = make_gradient()
        self.flt.add(self.gradient)

    def test_identical_image_is_similar(self):
        self.assertTrue(self.flt.exists_similar(make_gradient()))

    def test_brightness_variant_is_similar(self):
        self.assertTrue(
            self.flt.exists_similar(make_gradient(offset=20), threshold=0.8))

    def test_different_pattern_is_not_similar(self):
        self.assertFalse(self.flt.exists_similar(make_checker()))

    def test_find_similar_returns_sorted(self):
        self.flt.add(make_checker())
        result = self.flt.find_similar(make_gradient(), threshold=0.5)
        scores = [score for _, score in result]
        self.assertEqual(scores, sorted(scores, reverse=True))

    def test_count_and_clear(self):
        self.assertEqual(self.flt.count(), 1)
        self.flt.clear()
        self.assertEqual(self.flt.count(), 0)
        self.assertFalse(self.flt.exists_similar(make_gradient()))

    def test_add_hash_precomputed(self):
        fingerprint = dhash(make_gradient())
        self.assertEqual(self.flt.add_hash(fingerprint), fingerprint)
        self.assertEqual(self.flt.count(), 1)


@unittest.skipUnless(Image, "Pillow not installed")
class AsyncImageHashFilterTests(unittest.TestCase):
    def test_async_add_and_similar(self):
        flt = AsyncImageHashFilter(logger=quiet_logger())
        buffer = make_gradient()

        async def run():
            await flt.add(buffer)
            return await flt.exists_similar(make_gradient())

        self.assertTrue(asyncio.run(run()))


if __name__ == "__main__":
    unittest.main()
