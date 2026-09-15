# -*- coding:utf-8 -*-
import os
import tempfile
import unittest

from dupfilter.filters.file_path import FilePathFilter

from .helpers import quiet_logger


class FilePathFilterTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.base = self.directory.name
        self.sub_dir = os.path.join(self.base, "sub")
        os.makedirs(self.sub_dir)
        with open(os.path.join(self.base, "a.txt"), "w") as handle:
            handle.write("x")
        with open(os.path.join(self.base, "empty.txt"), "w"):
            pass
        with open(os.path.join(self.sub_dir, "c.txt"), "w") as handle:
            handle.write("x")

    def tearDown(self):
        self.directory.cleanup()

    def test_exists_and_many(self):
        flt = FilePathFilter(self.base, logger=quiet_logger())
        self.assertTrue(flt.exists("a.txt"))
        self.assertFalse(flt.exists("empty.txt"))
        self.assertFalse(flt.exists("c.txt"))
        self.assertEqual(
            flt.exists_many(["a.txt", "missing.txt"]),
            [True, False])

    def test_keyword_arguments(self):
        flt = FilePathFilter(self.base, logger=quiet_logger())
        self.assertTrue(flt.exists(value="a.txt"))

    def test_deep_and_allow_empty(self):
        flt = FilePathFilter(
            self.base, allow_empty=True, deep=True, logger=quiet_logger())
        self.assertTrue(flt.exists("empty.txt"))
        self.assertTrue(flt.exists("c.txt"))

    def test_missing_directory_returns_list_for_batch(self):
        missing = os.path.join(
            tempfile.gettempdir(), "dupfilter-missing-dir")
        flt = FilePathFilter(missing, logger=quiet_logger())
        self.assertEqual(flt.exists_many(("a", "b")), [False, False])
        self.assertEqual(
            flt.exists_many(iter(["a", "b"])), [False, False])

    def test_raise_on_error(self):
        missing = os.path.join(
            tempfile.gettempdir(), "dupfilter-missing-dir")
        flt = FilePathFilter(
            missing, raise_on_error=True, logger=quiet_logger())
        with self.assertRaises(OSError):
            flt.exists("a")

    def test_symlink_loop_is_not_followed(self):
        try:
            os.symlink(self.base, os.path.join(self.base, "loop"))
        except (OSError, NotImplementedError):
            self.skipTest("symlink not available")
        flt = FilePathFilter(
            self.base, allow_empty=True, deep=True, logger=quiet_logger())
        self.assertFalse(flt.exists("not-exist"))
