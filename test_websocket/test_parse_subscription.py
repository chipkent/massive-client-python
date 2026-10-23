import unittest

from massive import WebSocketClient


class ParseSubscriptionTest(unittest.TestCase):
    def test_valid_subscription_is_split(self):
        self.assertEqual(WebSocketClient._parse_subscription(" T.AAPL "), ["T", "AAPL"])

    def test_invalid_subscription_logs_the_topic(self):
        with self.assertLogs("WebSocketClient", level="WARNING") as logs:
            result = WebSocketClient._parse_subscription("AAPL")
        self.assertEqual(result, [None, None])
        self.assertEqual(
            logs.output, ["WARNING:WebSocketClient:invalid subscription: AAPL"]
        )
