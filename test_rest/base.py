from massive import RESTClient

import os
import re
import unittest
from typing import Set
import pook  # type: ignore

# mocks are stored in file tree
mocks = []
dirname = os.path.dirname(__file__)
mockdir = os.path.join(dirname, "mocks")
for dname, _, files in os.walk(mockdir):
    for fname in files:
        if fname.endswith(".json"):
            abspath = os.path.join(dname, fname)
            with open(abspath, "r") as f:
                urllpath = abspath.replace(mockdir, "").replace("\\", "/")
                urllpath = re.sub(".json$", "", urllpath)
                urllpath = re.sub("/index$", "", urllpath)
                # Windows will be sad. We support dev on Windows.
                if "?" in urllpath:
                    raise Exception(f"use & instead of ? in path ${urllpath}")
                urllpath = urllpath.replace("&", "?", 1)
                if ":" in urllpath:
                    raise Exception(f"use ; instead of : in path ${urllpath}")
                urllpath = urllpath.replace(";", ":", 1)
                # print(abspath, urllpath)
                mocks.append((urllpath, f.read()))


unittest.util._MAX_LENGTH = 30000  # type: ignore


class BaseTest(unittest.TestCase):
    # mocks that answer more than once, for tests that repeat a request;
    # others answer once, which the pagination tests rely on
    persistent_mocks: Set[str] = set()

    @classmethod
    def setUpClass(cls):
        cls.maxDiff = None
        cls.c = RESTClient("", verbose=True)
        pook.on()
        for m in mocks:
            url = cls.c.BASE + m[0]
            # print("register", url)
            pook.get(
                url,
                reply=200,
                response_body=m[1],
                persist=m[0] in cls.persistent_mocks,
            )

    @classmethod
    def tearDownClass(cls):
        # stop pook and drop the mocks, so they don't leak into later classes
        pook.off()
