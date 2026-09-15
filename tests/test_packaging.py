# -*- coding:utf-8 -*-
import os
import unittest


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class PackagingTests(unittest.TestCase):
    def test_setup_cfg_is_ascii_safe(self):
        with open(os.path.join(ROOT, "setup.cfg"), "rb") as handle:
            handle.read().decode("ascii")

    def test_no_runtime_dependency_on_cachetools(self):
        with open(os.path.join(ROOT, "setup.cfg"), encoding="utf-8") as handle:
            self.assertNotIn("cachetools", handle.read())
        for base, _, files in os.walk(os.path.join(ROOT, "dupfilter")):
            for name in files:
                if not name.endswith(".py"):
                    continue
                path = os.path.join(base, name)
                with open(path, encoding="utf-8") as handle:
                    self.assertNotIn(
                        "cachetools", handle.read(), msg=path)

    def test_setup_py_does_not_declare_missing_module(self):
        with open(os.path.join(ROOT, "setup.py"), encoding="utf-8") as handle:
            self.assertNotIn("py_modules", handle.read())
