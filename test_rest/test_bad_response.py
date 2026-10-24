from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest import mock
import threading
import unittest

from urllib3.exceptions import MaxRetryError
from urllib3.util.retry import Retry

from massive import RESTClient
from massive.exceptions import BadResponse
from base import BaseTest

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


class StatusServer(HTTPServer):
    """Answers every GET with a 429 and counts the requests.

    pook replaces urllib3's urlopen, which is where retries happen, so the
    raise_on_status tests need a real server.
    """

    def __init__(self):
        self.requests = 0
        super().__init__(("127.0.0.1", 0), StatusHandler)


class StatusHandler(BaseHTTPRequestHandler):
    server: StatusServer

    def do_GET(self):
        self.server.requests += 1
        self.send_response(429)
        self.send_header("Content-Length", "4")
        self.end_headers()
        self.wfile.write(b"slow")

    def log_message(self, format, *args):
        pass


class RaiseOnStatusTest(unittest.TestCase):
    def setUp(self):
        # BaseTest leaves pook on for the rest of the run
        if pook.isactive():
            pook.disable()
            self.addCleanup(pook.activate)
        self.server = StatusServer()
        threading.Thread(
            target=self.server.serve_forever,
            kwargs={"poll_interval": 0.01},
            daemon=True,
        ).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def get_error(self, error, **kwargs):
        base = f"http://127.0.0.1:{self.server.server_port}"
        c = RESTClient("key", base=base, retries=2, **kwargs)
        with mock.patch.object(Retry, "sleep"):  # skip the backoff
            with self.assertRaises(error) as ctx:
                c.get_market_status()
        self.assertEqual(self.server.requests, 3)  # first attempt + 2 retries
        return ctx.exception

    def test_default_raises_max_retry_error(self):
        self.get_error(MaxRetryError)

    def test_false_raises_bad_response(self):
        exc = self.get_error(BadResponse, raise_on_status=False)
        self.assertEqual(exc.status, 429)
        self.assertEqual(str(exc), "slow")

    def test_reaches_vx_client(self):
        c = RESTClient("key", raise_on_status=False)
        for pool in (c.client, c.vx.client):
            self.assertFalse(pool.connection_pool_kw["retries"].raise_on_status)
