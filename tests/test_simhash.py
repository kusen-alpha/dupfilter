# -*-coding:utf-8 -*-
import asyncio
import unittest

from dupfilter.filters.simhash import (
    AsyncSimHashFilter,
    SimHashFilter,
    hamming_distance,
    is_similar,
    simhash,
    similarity,
    tokenize,
)

from .helpers import quiet_logger

_TEXT = "the quick brown fox jumps over the lazy dog"
_NEAR = "the quick brown fox jumps over the lazy cat"
_OTHER = "financial markets rallied today amid strong earnings reports"


class SimHashFunctionTests(unittest.TestCase):
    def test_tokenize_splits_words_and_bigrams(self):
        tokens = tokenize("hello world")
        self.assertIn("hello", tokens)
        self.assertIn("hello\x00world", tokens)

    def test_tokenize_cjk_bigrams(self):
        tokens = tokenize("北京天气")
        self.assertIn("北京", tokens)
        self.assertIn("京天", tokens)

    def test_identical_text_has_same_fingerprint(self):
        self.assertEqual(simhash(_TEXT), simhash(_TEXT))

    def test_similarity_scores(self):
        self.assertEqual(similarity(simhash(_TEXT), simhash(_TEXT)), 1.0)
        self.assertGreater(
            similarity(simhash(_TEXT), simhash(_NEAR)), 0.8)
        self.assertLess(
            similarity(simhash(_TEXT), simhash(_OTHER)), 0.8)

    def test_hamming_distance_and_is_similar(self):
        fp = simhash(_TEXT)
        self.assertEqual(hamming_distance(fp, fp), 0)
        self.assertTrue(is_similar(fp, fp))
        self.assertFalse(is_similar(simhash(_TEXT), simhash(_OTHER)))


class SimHashFilterTests(unittest.TestCase):
    def setUp(self):
        self.flt = SimHashFilter(logger=quiet_logger())
        self.flt.add(_TEXT)

    def test_identical_text_is_similar(self):
        self.assertTrue(self.flt.exists_similar(_TEXT))

    def test_near_duplicate_with_lower_threshold(self):
        self.assertTrue(self.flt.exists_similar(_NEAR, threshold=0.8))
        self.assertFalse(self.flt.exists_similar(_NEAR))

    def test_unrelated_text_is_not_similar(self):
        self.assertFalse(self.flt.exists_similar(_OTHER))

    def test_find_similar_sorted(self):
        self.flt.add(_NEAR)
        self.flt.add(_OTHER)
        result = self.flt.find_similar(_TEXT, threshold=0.8)
        self.assertGreaterEqual(len(result), 2)
        scores = [score for _, score in result]
        self.assertEqual(scores, sorted(scores, reverse=True))

    def test_count_and_clear(self):
        self.assertEqual(self.flt.count(), 1)
        self.flt.clear()
        self.assertEqual(self.flt.count(), 0)
        self.assertFalse(self.flt.exists_similar(_TEXT))


class AsyncSimHashFilterTests(unittest.TestCase):
    def test_async_add_and_similar(self):
        flt = AsyncSimHashFilter(logger=quiet_logger())
        asyncio.run(flt.add(_TEXT))
        self.assertTrue(asyncio.run(flt.exists_similar(_TEXT)))


if __name__ == "__main__":
    unittest.main()
