import asyncio
import socket
import ssl
import time
import unittest
from typing import Any, List, Optional
from unittest import mock

from fake_server import FakeServer
from websockets.asyncio.server import ServerConnection
from websockets.datastructures import Headers
from websockets.exceptions import (
    ConnectionClosedError,
    InvalidMessage,
    InvalidStatus,
    InvalidURI,
)
from websockets.http11 import Request, Response

from massive import WebSocketClient
from massive.exceptions import AuthError

# small waits keep the tests fast; the default is 15 s
BACKOFF = 0.01
MAX_BACKOFF = 0.05


class TimedServer(FakeServer):
    """A FakeServer that records when each handshake was attempted."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.attempts: List[float] = []
        # if set, answer handshakes with this HTTP status
        self.reject_status: Optional[int] = None

    def _process_request(
        self, websocket: ServerConnection, request: Request
    ) -> Optional[Response]:
        self.attempts.append(time.monotonic())
        if self.reject_status is not None:
            return websocket.respond(self.reject_status, "rejected\n")
        return super()._process_request(websocket, request)


async def ignore(msgs: Any) -> None:
    pass


class ReconnectTest(unittest.IsolatedAsyncioTestCase):
    """How the client behaves when its connection ends."""

    async def asyncSetUp(self) -> None:
        self.server = TimedServer()
        await self.server.start()
        self.addAsyncCleanup(self.server.stop)

    def client(self, **kwargs) -> WebSocketClient:
        kwargs.setdefault("api_key", "key")
        kwargs.setdefault("reconnect_backoff", BACKOFF)
        kwargs.setdefault("max_reconnect_backoff", MAX_BACKOFF)
        return WebSocketClient(feed=self.server.feed, secure=False, **kwargs)

    def start(self, c: WebSocketClient, processor=ignore) -> "asyncio.Task[None]":
        task = asyncio.create_task(c.connect(processor))

        async def cancel():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        self.addAsyncCleanup(cancel)
        return task

    async def test_keeps_changes_scheduled_during_an_outage(self) -> None:
        c = self.client(subscriptions=["T.KEEP", "T.OLD", "Q.OLD"])
        task = self.start(c)
        await self.server.wait_for_subscriptions({"T.KEEP", "T.OLD", "Q.OLD"})

        self.server.reject_handshakes = True
        # scheduled but not sent yet when the connection drops
        c.subscribe("T.NEW")
        c.unsubscribe("T.OLD")
        await self.server.drop_connections()
        # scheduled while the client cannot reconnect
        await asyncio.sleep(0.1)
        c.subscribe("Q.NEW")
        c.unsubscribe("Q.OLD")
        self.server.reject_handshakes = False

        await self.server.wait_for_connections(2)
        await self.server.wait_for_subscriptions({"T.KEEP", "T.NEW", "Q.NEW"})
        topics: List[str] = []
        for m in self.server.connection.inbox[1:]:
            self.assertEqual(m["action"], "subscribe")
            topics += m["params"].split(",")
        self.assertCountEqual(topics, ["T.KEEP", "T.NEW", "Q.NEW"])
        self.assertEqual(c.scheduled_subs, {"T.KEEP", "T.NEW", "Q.NEW"})

        await c.close()
        await asyncio.wait_for(task, 5)

    async def test_reconnects_after_a_clean_close_until_closed(self) -> None:
        c = self.client(subscriptions=["T.AAPL"])
        task = self.start(c)
        await self.server.wait_for_subscriptions({"T.AAPL"})

        # e.g. the server closes cleanly after a slow_consumer status
        await self.server.close_connections(code=1000)
        await self.server.wait_for_connections(2)
        await self.server.wait_for_subscriptions({"T.AAPL"})
        self.assertFalse(task.done())

        await c.close()
        await asyncio.wait_for(task, 5)
        self.assertEqual(len(self.server.connections), 2)

    async def test_waits_between_attempts(self) -> None:
        c = self.client(reconnect_backoff=0.05, max_reconnect_backoff=0.2)
        delays: List[float] = []
        delay = c._reconnect_delay

        def record(attempt: int) -> float:
            delays.append(delay(attempt))
            return delays[-1]

        with mock.patch.object(c, "_reconnect_delay", side_effect=record):
            task = self.start(c)
            await self.server.wait_for_connections(1)
            self.server.reject_handshakes = True
            await self.server.drop_connections()
            # 0.05, 0.1, 0.2, then the cap
            await self.server.wait_for(lambda: len(self.server.attempts) == 6)

            # close() during the wait stops connect() at once
            start = time.monotonic()
            await c.close()
            await asyncio.wait_for(task, 5)
            self.assertLess(time.monotonic() - start, 0.5)

        self.assertEqual(len(self.server.attempts), 6)
        # one delay before each refused attempt, maybe one more being waited
        self.assertIn(len(delays), (5, 6))
        for d in delays:
            self.assertGreaterEqual(d, 0.05)
            self.assertLessEqual(d, 0.2)
        self.assertGreaterEqual(delays[1], 0.1)
        # at the cap, jitter cannot lengthen the wait
        self.assertEqual(delays[2:], [0.2] * (len(delays) - 2))
        # each refused attempt waited for its delay before the next one
        gaps = [
            b - a for a, b in zip(self.server.attempts[1:], self.server.attempts[2:])
        ]
        # allow for the timer resolution, about 15.6 ms on Windows
        for gap, d in zip(gaps, delays[1:]):
            self.assertGreaterEqual(gap, d - 0.03)

    async def test_transient_handshake_errors_are_retried(self) -> None:
        for code in (429, 500, 503):
            self.server.reject_status = code
            n = len(self.server.attempts)
            c = self.client()
            with self.assertLogs("WebSocketClient", level="WARNING") as logs:
                task = self.start(c)
                await self.server.wait_for(lambda: len(self.server.attempts) >= n + 3)
            self.assertIn(
                f"connect failed; reconnecting in 0.0 s: server rejected WebSocket "
                f"connection: HTTP {code}",
                logs.output[0],
            )
            self.server.reject_status = None
            await self.server.wait_for(
                lambda: bool(self.server.open_connections), timeout=5
            )
            await c.close()
            await asyncio.wait_for(task, 5)

    async def test_refused_connections_are_retried(self) -> None:
        port = self.server.port
        await self.server.stop()
        c = WebSocketClient(
            api_key="key", feed=f"127.0.0.1:{port}", secure=False, reconnect_backoff=0
        )
        with self.assertLogs("WebSocketClient", level="WARNING") as logs:
            task = self.start(c)
            # Windows retries the SYN before it reports a refusal, so there an
            # attempt can take about 2 s
            await self.server.wait_for(lambda: len(logs.records) >= 2, timeout=15)
        self.assertFalse(task.done())
        self.assertIn("connect failed; reconnecting in", logs.output[0])

    async def test_fatal_handshake_errors_raise_at_once(self) -> None:
        for code in (400, 401, 403, 404):
            self.server.reject_status = code
            n = len(self.server.attempts)
            with self.assertRaises(InvalidStatus) as e:
                await asyncio.wait_for(self.client().connect(ignore), 5)
            self.assertEqual(e.exception.response.status_code, code)
            self.assertEqual(len(self.server.attempts), n + 1)

    async def test_invalid_arguments_raise_at_once(self) -> None:
        # rejected when building the connection
        with self.assertRaises(ValueError):
            await self.client(max_reconnects=None).connect(ignore, compression="x")
        # rejected by the first attempt
        with self.assertRaises(TypeError):
            await self.client(max_reconnects=None).connect(ignore, bogus=1)
        c = WebSocketClient(api_key="key", feed="127.0.0.1:port", secure=False)
        with self.assertRaises(ValueError):
            await asyncio.wait_for(c.connect(ignore), 5)
        self.assertEqual(self.server.attempts, [])


class TransientTest(unittest.TestCase):
    """Which failed attempts to connect are retried."""

    def test_classification(self) -> None:
        def status(code: int) -> InvalidStatus:
            return InvalidStatus(Response(code, "", Headers(), b""))

        eof = InvalidMessage("did not receive a valid HTTP response")
        eof.__cause__ = EOFError("connection closed while reading HTTP status line")
        transient = [
            status(429),
            status(500),
            status(503),
            eof,
            ConnectionRefusedError(),
            OSError("unreachable"),
            socket.gaierror(),
            TimeoutError(),
            asyncio.TimeoutError(),
            ConnectionClosedError(None, None),
        ]
        fatal = [
            status(400),
            status(401),
            status(403),
            status(404),
            InvalidMessage("bad response"),
            InvalidURI("ws://x", "bad"),
            ssl.SSLCertVerificationError(),
            TypeError(),
            ValueError(),
            AuthError("bad key"),
        ]
        for e in transient:
            self.assertTrue(WebSocketClient._is_transient(e), repr(e))
        for e in fatal:
            self.assertFalse(WebSocketClient._is_transient(e), repr(e))


class ReconnectDelayTest(unittest.TestCase):
    """The wait before each consecutive reconnect attempt."""

    def delays(self, c: WebSocketClient, n: int) -> List[float]:
        return [c._reconnect_delay(i) for i in range(n)]

    def test_defaults(self) -> None:
        c = WebSocketClient(api_key="key")
        self.assertEqual(c.reconnect_backoff, 15.0)
        self.assertEqual(c.max_reconnect_backoff, 120.0)
        with mock.patch("random.uniform", side_effect=lambda a, b: a):
            self.assertEqual(self.delays(c, 6), [15, 30, 60, 120, 120, 120])
        with mock.patch("random.uniform", side_effect=lambda a, b: b):
            self.assertEqual(self.delays(c, 6), [18, 36, 72, 120, 120, 120])

    def test_jitter_is_clamped_to_the_cap(self) -> None:
        c = WebSocketClient(
            api_key="key", reconnect_backoff=10, max_reconnect_backoff=100
        )
        with mock.patch("random.uniform", side_effect=lambda a, b: a):
            self.assertEqual(self.delays(c, 6), [10, 20, 40, 80, 100, 100])
        with mock.patch("random.uniform", side_effect=lambda a, b: b):
            self.assertEqual(self.delays(c, 6), [12, 24, 48, 96, 100, 100])

    def test_every_delay_is_within_bounds(self) -> None:
        c = WebSocketClient(api_key="key")
        for attempt in list(range(20)) * 50 + [10**6]:
            d = c._reconnect_delay(attempt)
            self.assertGreaterEqual(d, 15.0)
            self.assertLessEqual(d, 120.0)

    def test_equal_bounds(self) -> None:
        c = WebSocketClient(api_key="key", reconnect_backoff=0, max_reconnect_backoff=0)
        self.assertEqual(self.delays(c, 3), [0, 0, 0])

    def test_invalid_bounds(self) -> None:
        with self.assertRaises(ValueError):
            WebSocketClient(api_key="key", reconnect_backoff=-1)
        with self.assertRaises(ValueError):
            WebSocketClient(
                api_key="key", reconnect_backoff=30, max_reconnect_backoff=20
            )


if __name__ == "__main__":
    unittest.main()
