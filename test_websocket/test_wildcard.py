import unittest
from massive import WebSocketClient


class WildcardTest(unittest.TestCase):
    def client(self, *subs):
        c = WebSocketClient(api_key="", subscriptions=list(subs))
        c.subs = set(subs)  # as if connect() had sent them
        return c

    def test_subscribe_wildcard_keeps_other_channels(self):
        c = self.client("A.MSFT", "AM.AAPL")
        c.subscribe("A.*")
        self.assertEqual(c.scheduled_subs, {"A.*", "AM.AAPL"})

    def test_subscribe_wildcard_twice_keeps_it(self):
        c = self.client("A.*")
        c.subscribe("A.*")
        self.assertEqual(c.scheduled_subs, {"A.*"})

    def test_unsubscribe_wildcard_keeps_other_channels(self):
        c = self.client("A.*", "A.MSFT", "AM.AAPL")
        c.unsubscribe("A.*")
        self.assertEqual(c.scheduled_subs, {"AM.AAPL"})


if __name__ == "__main__":
    unittest.main()
