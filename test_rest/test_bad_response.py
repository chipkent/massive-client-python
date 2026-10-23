import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, List, Tuple, Type
from unittest import mock

import pook  # type: ignore
from urllib3.exceptions import MaxRetryError
from urllib3.util.retry import Retry

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

    def test_error_carries_status(self):
        cases = [
            (403, '{"status":"NOT_AUTHORIZED","message":"not entitled"}'),
            (429, '{"status":"ERROR","error":"exceeded the maximum requests"}'),
            (500, "internal error"),
        ]
        for status, body in cases:
            with self.subTest(status=status):
                exc = self.get_error(status, body)
                self.assertEqual(exc.status, status)
                self.assertEqual(str(exc), body)
                self.assertEqual(exc.args, (body,))

    def test_construction_without_status_is_unchanged(self):
        cases = [
            ((), ""),
            (("boom",), "boom"),
            (("boom", 1), "('boom', 1)"),
        ]
        for args, message in cases:
            with self.subTest(args=args):
                exc = BadResponse(*args)
                self.assertEqual(exc.args, args)
                self.assertEqual(str(exc), message)
                self.assertIsNone(exc.status)

    def test_status_does_not_change_args(self):
        exc = BadResponse("boom", status=500)
        self.assertEqual(exc.args, ("boom",))
        self.assertEqual(str(exc), "boom")
        self.assertEqual(exc.status, 500)


class StatusServer(HTTPServer):
    """Local server that answers every GET with a fixed status and body.

    pook replaces urllib3's urlopen, which is also where urllib3 retries, so
    the retry tests need a real server to see each attempt.
    """

    def __init__(self, status: int, body: bytes):
        self.status = status
        self.body = body
        self.paths: List[str] = []
        super().__init__(("127.0.0.1", 0), StatusHandler)


class StatusHandler(BaseHTTPRequestHandler):
    server: StatusServer

    def do_GET(self):
        self.server.paths.append(self.path)
        self.send_response(self.server.status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(self.server.body)))
        self.end_headers()
        self.wfile.write(self.server.body)

    def log_message(self, format, *args):
        pass


class RetriedStatusTest(unittest.TestCase):
    """What happens when a retried status (429, 5xx, ...) is still returned
    after the last retry, with and without raise_on_status."""

    cases = [
        (429, '{"status":"ERROR","error":"exceeded the maximum requests"}'),
        (503, "service unavailable"),
    ]

    def setUp(self):
        # BaseTest turns pook on for the rest of the run; these tests need the
        # real urllib3 request path
        if pook.isactive():
            pook.disable()
            self.addCleanup(pook.activate)

    def get_error(
        self, status: int, body: str, error: Type[Exception], **kwargs: Any
    ) -> Tuple[Any, List[str]]:
        server = StatusServer(status, body.encode())
        thread = threading.Thread(
            target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
        )
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)

        c = RESTClient("key", base=f"http://127.0.0.1:{server.server_port}", **kwargs)
        # skip the backoff between attempts so the test stays fast
        with mock.patch.object(Retry, "sleep"):
            with self.assertRaises(error) as ctx:
                c.get_market_status()
        return ctx.exception, server.paths

    def test_default_raises_max_retry_error(self):
        for status, body in self.cases:
            with self.subTest(status=status):
                _, paths = self.get_error(status, body, MaxRetryError)
                # the first attempt plus the default three retries
                self.assertEqual(paths, ["/v1/marketstatus/now"] * 4)

    def test_raise_on_status_false_raises_bad_response(self):
        for retries in (0, 3):
            for status, body in self.cases:
                with self.subTest(retries=retries, status=status):
                    exc, paths = self.get_error(
                        status,
                        body,
                        BadResponse,
                        retries=retries,
                        raise_on_status=False,
                    )
                    self.assertEqual(exc.status, status)
                    self.assertEqual(str(exc), body)
                    self.assertEqual(paths, ["/v1/marketstatus/now"] * (retries + 1))

    def test_raise_on_status_reaches_both_clients(self):
        for value in (True, False):
            with self.subTest(raise_on_status=value):
                c = RESTClient("key", raise_on_status=value)
                for pool in (c.client, c.vx.client):
                    retry = pool.connection_pool_kw["retries"]
                    self.assertIs(retry.raise_on_status, value)
