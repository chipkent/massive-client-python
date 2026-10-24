import unittest


class ExportsTest(unittest.TestCase):
    def test_star_import(self):
        ns = {}
        exec("from massive import *", ns)
        for name in ("RESTClient", "WebSocketClient", "AuthError", "BadResponse"):
            self.assertIn(name, ns)
