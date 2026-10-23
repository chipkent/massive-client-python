import unittest

import pook  # type: ignore

from massive import RESTClient
from massive.exceptions import BadResponse


class BadResponseTest(unittest.TestCase):
    def setUp(self):
        self.c = RESTClient("key", retries=0)
        self.url = self.c.BASE + "/v1/marketstatus/now"

    def get_error(self, status: int, body: str) -> BadResponse:
        with pook.use():
            pook.get(self.url, reply=status, response_body=body)
            with self.assertRaises(BadResponse) as ctx:
                self.c.get_market_status()
        return ctx.exception

    def test_forbidden_carries_status_and_body(self):
        body = '{"status":"NOT_AUTHORIZED","message":"not entitled"}'
        exc = self.get_error(403, body)
        self.assertEqual(exc.status, 403)
        self.assertEqual(exc.body, body)
        self.assertEqual(str(exc), body)
        self.assertEqual(exc.args, (body,))

    def test_rate_limited_carries_status_and_body(self):
        body = '{"status":"ERROR","error":"exceeded the maximum requests"}'
        exc = self.get_error(429, body)
        self.assertEqual(exc.status, 429)
        self.assertEqual(exc.body, body)
        self.assertEqual(str(exc), body)

    def test_server_error_carries_status_and_body(self):
        body = "internal error"
        exc = self.get_error(500, body)
        self.assertEqual(exc.status, 500)
        self.assertEqual(exc.body, body)
        self.assertEqual(str(exc), body)

    def test_message_only_construction_is_unchanged(self):
        exc = BadResponse("boom")
        self.assertEqual(str(exc), "boom")
        self.assertEqual(exc.args, ("boom",))
        self.assertIsNone(exc.status)
        self.assertEqual(exc.body, "boom")
