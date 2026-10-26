from massive.exceptions import BadResponse
from base import BaseTest

import json
import pook  # type: ignore


class BadResponseTest(BaseTest):
    def test_status_is_set(self):
        cases = [
            (403, '{"status":"NOT_AUTHORIZED","message":"not entitled"}'),
            (404, '{"status":"NOT_FOUND","message":"not found"}'),
        ]
        for status, body in cases:
            with self.subTest(status=status):
                ticker = f"ERR{status}"  # not covered by the fixture mocks
                pook.get(
                    self.c.BASE + "/v3/reference/tickers/" + ticker,
                    reply=status,
                    response_body=body,
                )
                with self.assertRaises(BadResponse) as ctx:
                    self.c.get_ticker_details(ticker)
                self.assertEqual(ctx.exception.status, status)
                self.assertEqual(ctx.exception.args, (body,))
                self.assertEqual(str(ctx.exception), body)

    def test_invalid_json_raises(self):
        pook.get(self.c.BASE + "/v3/reference/tickers/BADJSON", response_body="<html>")
        with self.assertRaises(BadResponse) as ctx:
            self.c.get_ticker_details("BADJSON")
        self.assertEqual(ctx.exception.status, 200)
        self.assertEqual(str(ctx.exception), "Invalid JSON in response: <html>")
        self.assertIsInstance(ctx.exception.__cause__, json.JSONDecodeError)

    def test_invalid_json_first_page_raises(self):
        pook.get(self.c.BASE + "/v3/trades/BADJSON", response_body="<html>")
        with self.assertRaises(BadResponse) as ctx:
            list(self.c.list_trades("BADJSON"))
        self.assertEqual(ctx.exception.status, 200)

    def test_invalid_json_later_page_raises(self):
        first = {
            "results": [{"price": 1.0}],
            "next_url": self.c.BASE + "/v3/trades/BADJSON2?cursor=2",
        }
        pook.get(
            self.c.BASE + "/v3/trades/BADJSON2?cursor=2",
            response_body="<html>",
        )
        pook.get(self.c.BASE + "/v3/trades/BADJSON2", response_json=first)
        prices = []
        with self.assertRaises(BadResponse) as ctx:
            for t in self.c.list_trades("BADJSON2"):
                prices.append(t.price)
        self.assertEqual(prices, [1.0])
        self.assertEqual(ctx.exception.status, 200)

    def test_status_defaults_to_none(self):
        for args in [(), ("boom",), ("boom", 1)]:
            with self.subTest(args=args):
                exc = BadResponse(*args)
                self.assertEqual(exc.args, args)
                self.assertIsNone(exc.status)

    def test_status_does_not_change_message(self):
        exc = BadResponse("boom", status=500)
        self.assertEqual(exc.args, ("boom",))
        self.assertEqual(str(exc), "boom")
        self.assertEqual(exc.status, 500)
