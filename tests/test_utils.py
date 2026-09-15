# -*- coding:utf-8 -*-
import unittest

from dupfilter import utils


class UtilsTests(unittest.TestCase):
    def test_md5_accepts_str_and_bytes(self):
        self.assertEqual(utils.md5("abc"), utils.md5(b"abc"))
        self.assertEqual(utils.md5("abc"), "900150983cd24fb0d6963f7d28e17f72")

    def test_hex_conversions(self):
        value = utils.md5("abc")
        self.assertEqual(utils.hex2b64(value),
                         "kAFQmDzST7DWlj99KOF/cg==")
        self.assertTrue(utils.hex2sfb(value))
