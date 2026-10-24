import asyncio
import socket
import ssl
import threading
import time
import unittest
from typing import Any, Generator, List, Optional
from unittest import mock

from fake_server import FakeServer
from websockets.asyncio.server import ServerConnection
from websockets.datastructures import Headers
from websockets.exceptions import (
    InvalidMessage,
    InvalidStatus,
    InvalidURI,
)
from websockets.http11 import Request, Response

import massive.websocket as ws_module
from massive import WebSocketClient
from massive.exceptions import AuthError

# small waits keep the tests fast; the default is 15 s
BACKOFF = 0.01
MAX_BACKOFF = 0.05


class TimedServer(FakeServer):
    """A FakeServer that records when each handshake was attempted."""

    def __init__(self, **kwargs: Any) -> None:
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


class FakeConnect:
    """
    Stands in for websockets' connect(): each attempt waits for `opened` and
    then returns `conn`, even if it was cancelled meanwhile, like a handshake
    that completes as it is cancelled.
    """

    def __init__(self) -> None:
        self.conn = mock.AsyncMock()
        self.attempts = 0
        self.started = asyncio.Event()
        self.opened = asyncio.Event()

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        fake = self

        class Attempt:
            def __await__(self) -> Generator[Any, None, Any]:
                return fake._attempt().__await__()

        return Attempt()

    async def _attempt(self) -> Any:
        self.attempts += 1
        self.started.set()
        try:
            await self.opened.wait()
        except asyncio.CancelledError:
            pass
        return self.conn


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
        c = self.client(
            reconnect_backoff=0.05, max_reconnect_backoff=0.2, max_reconnects=None
        )
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

            # close() during the wait stops connect()
            await c.close()
            await asyncio.wait_for(task, 5)

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
        for code in (408, 429, 500, 503):
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
        # connect() decides which errors to retry
        with self.assertRaises(TypeError):
            await self.client().connect(ignore, process_exception=lambda e: None)
        c = WebSocketClient(api_key="key", feed="127.0.0.1:port", secure=False)
        with self.assertRaises(ValueError):
            await asyncio.wait_for(c.connect(ignore), 5)
        self.assertEqual(self.server.attempts, [])

    async def test_outages_with_successes_between_do_not_raise(self) -> None:
        c = self.client(subscriptions=["T.AAPL"], max_reconnects=5)
        task = self.start(c)
        for i in range(1, 8):
            await self.server.wait_for_connections(i)
            await self.server.wait_for_subscriptions({"T.AAPL"})
            await self.server.drop_connections()
        await self.server.wait_for_connections(8)
        self.assertFalse(task.done())

        await c.close()
        await asyncio.wait_for(task, 5)

    async def test_consecutive_failures_raise(self) -> None:
        c = self.client(max_reconnects=2)
        task = self.start(c)
        await self.server.wait_for_connections(1)
        self.server.reject_handshakes = True
        await self.server.drop_connections()

        # the drop and two refused attempts
        with self.assertRaises(InvalidStatus):
            await asyncio.wait_for(task, 5)
        self.assertEqual(len(self.server.attempts), 3)

    async def test_unlimited_reconnects(self) -> None:
        c = self.client(max_reconnects=None, max_reconnect_backoff=BACKOFF)
        task = self.start(c)
        self.server.reject_handshakes = True
        await self.server.wait_for(lambda: len(self.server.attempts) >= 20)
        self.assertFalse(task.done())
        self.server.reject_handshakes = False
        await self.server.wait_for_connections(1)

        await c.close()
        await asyncio.wait_for(task, 5)

    async def request_close_from_a_thread(self, c: WebSocketClient):
        thread = threading.Thread(target=c.request_close)
        thread.start()
        await asyncio.get_running_loop().run_in_executor(None, thread.join)

    async def test_request_close_from_another_thread(self) -> None:
        c = self.client(subscriptions=["T.AAPL"])
        task = self.start(c)
        await self.server.wait_for_subscriptions({"T.AAPL"})
        # reconnects after a clean close, until request_close()
        await self.server.close_connections(code=1000)
        await self.server.wait_for_connections(2)
        await self.server.wait_for_subscriptions({"T.AAPL"})

        await self.request_close_from_a_thread(c)
        await asyncio.wait_for(task, 2)
        await self.server.wait_for(lambda: not self.server.open_connections)
        self.assertEqual(len(self.server.connections), 2)

    async def test_request_close_while_waiting_to_reconnect(self) -> None:
        c = self.client(reconnect_backoff=60, max_reconnect_backoff=60)
        task = self.start(c)
        await self.server.wait_for_connections(1)
        await self.server.drop_connections()

        await self.request_close_from_a_thread(c)
        await asyncio.wait_for(task, 2)
        self.assertEqual(len(self.server.attempts), 1)

    async def test_request_close_while_connecting(self) -> None:
        # a server that accepts TCP connections but never answers the handshake
        accepted = asyncio.Event()

        async def stall(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            accepted.set()
            await reader.read()
            writer.close()

        server = await asyncio.start_server(stall, "127.0.0.1", 0)
        self.addAsyncCleanup(server.wait_closed)
        self.addCleanup(server.close)
        port = server.sockets[0].getsockname()[1]
        c = WebSocketClient(api_key="key", feed=f"127.0.0.1:{port}", secure=False)
        task = self.start(c)
        await asyncio.wait_for(accepted.wait(), 5)

        await self.request_close_from_a_thread(c)
        await asyncio.wait_for(task, 2)

    async def test_cancel_while_connecting_closes_the_socket(self) -> None:
        accepted = asyncio.Event()
        disconnected = asyncio.Event()

        async def stall(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
            accepted.set()
            await reader.read()
            disconnected.set()
            writer.close()

        server = await asyncio.start_server(stall, "127.0.0.1", 0)
        self.addAsyncCleanup(server.wait_closed)
        self.addCleanup(server.close)
        port = server.sockets[0].getsockname()[1]
        c = WebSocketClient(api_key="key", feed=f"127.0.0.1:{port}", secure=False)
        task = asyncio.create_task(c.connect(ignore))
        await asyncio.wait_for(accepted.wait(), 5)

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await asyncio.wait_for(disconnected.wait(), 2)

    async def test_cancel_closes_a_connection_opened_meanwhile(self) -> None:
        fake = FakeConnect()
        with mock.patch.object(ws_module, "connect", fake):
            task = asyncio.create_task(self.client().connect(ignore))
            await asyncio.wait_for(fake.started.wait(), 5)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        self.assertEqual(fake.attempts, 1)
        fake.conn.close.assert_awaited_once()

    async def test_no_attempt_after_request_close(self) -> None:
        fake = FakeConnect()
        with mock.patch.object(ws_module, "connect", fake):
            c = self.client()
            task = asyncio.create_task(c.connect(ignore))
            await asyncio.wait_for(fake.started.wait(), 5)
            c.request_close()
            # the handshake completes as the close is requested
            fake.opened.set()
            await asyncio.wait_for(task, 5)
            # give a stray attempt every chance to start
            for _ in range(20):
                await asyncio.sleep(0)
        self.assertEqual(fake.attempts, 1)
        fake.conn.close.assert_awaited_once()
        fake.conn.recv.assert_not_awaited()

    async def test_request_close_before_connect(self) -> None:
        c = self.client()
        c.request_close()
        await asyncio.wait_for(c.connect(ignore), 2)
        self.assertEqual(self.server.attempts, [])

        # the request applied to that connect() only
        task = self.start(c)
        await self.server.wait_for_connections(1)
        c.request_close()
        await asyncio.wait_for(task, 2)

    async def test_close_request_stays_pending_if_connect_raises(self) -> None:
        c = self.client()
        c.request_close()
        with self.assertRaises(ValueError):
            await c.connect(ignore, compression="x")
        # the next connect() returns because of it, which consumes it
        await asyncio.wait_for(c.connect(ignore), 2)
        self.assertEqual(self.server.attempts, [])
        task = self.start(c)
        await self.server.wait_for_connections(1)
        c.request_close()
        await asyncio.wait_for(task, 2)


class TransientTest(unittest.TestCase):
    """Which failed attempts to connect are retried."""

    def test_classification(self) -> None:
        def status(code: int) -> InvalidStatus:
            return InvalidStatus(Response(code, "", Headers(), b""))

        def invalid(cause: Optional[BaseException]) -> InvalidMessage:
            # how websockets 15 reports a handshake without a valid response
            e = InvalidMessage("did not receive a valid HTTP response")
            e.__cause__ = cause
            return e

        transient = [
            status(408),
            status(429),
            status(500),
            status(503),
            invalid(EOFError("connection closed while reading HTTP status line")),
            invalid(ConnectionResetError()),
            # how websockets 14 reports a connection closed before a response
            EOFError("line without CRLF"),
            ConnectionRefusedError(),
            OSError("unreachable"),
            socket.gaierror(),
            TimeoutError(),
            asyncio.TimeoutError(),
        ]
        fatal = [
            status(400),
            status(401),
            status(403),
            status(404),
            invalid(ValueError("invalid HTTP status line")),
            invalid(None),
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
        self.assertIsNone(c.max_reconnects)
        with mock.patch.object(ws_module.random, "uniform", side_effect=lambda a, b: a):
            self.assertEqual(self.delays(c, 6), [15, 30, 60, 120, 120, 120])
        with mock.patch.object(ws_module.random, "uniform", side_effect=lambda a, b: b):
            self.assertEqual(self.delays(c, 6), [18, 36, 72, 120, 120, 120])

    def test_jitter_is_clamped_to_the_cap(self) -> None:
        c = WebSocketClient(
            api_key="key", reconnect_backoff=10, max_reconnect_backoff=100
        )
        with mock.patch.object(ws_module.random, "uniform", side_effect=lambda a, b: a):
            self.assertEqual(self.delays(c, 6), [10, 20, 40, 80, 100, 100])
        with mock.patch.object(ws_module.random, "uniform", side_effect=lambda a, b: b):
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
