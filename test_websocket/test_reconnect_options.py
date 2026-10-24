from massive import WebSocketClient
from base_ws import BaseTest
from mock_server import inbox, port
from unittest import mock
from websockets.exceptions import (
    ConnectionClosedError,
    ConnectionClosedOK,
    InvalidStatus,
)
import asyncio


class RecordSleeps:
    # asyncio, except that sleep() records the delay and returns at once
    def __init__(self):
        self.delays = []

    def __getattr__(self, name):
        return getattr(asyncio, name)

    async def sleep(self, delay):
        self.delays.append(delay)


async def ignore(msgs):
    pass


class ReconnectOptionsTest(BaseTest):
    def client(self, *subscriptions, **kwargs):
        return WebSocketClient(
            api_key=kwargs.pop("api_key", ""),
            feed=kwargs.pop("feed", f"localhost:{port}"),
            secure=False,
            subscriptions=list(subscriptions),
            **kwargs,
        )

    async def drop_until_raised(self, c, exc=ConnectionClosedError):
        # DROP.X makes the server drop every connection right away
        sleeps = RecordSleeps()
        with mock.patch("massive.websocket.asyncio", sleeps), mock.patch(
            "massive.websocket.random"
        ) as rnd:
            rnd.uniform.side_effect = lambda a, b: a
            with self.assertRaises(exc):
                await asyncio.wait_for(c.connect(ignore), 5)
        return sleeps.delays, rnd.uniform.call_args_list

    async def test_reconnects_at_once_by_default(self):
        c = self.client("DROP.X", max_reconnects=2)
        delays, _ = await self.drop_until_raised(c)
        self.assertEqual(delays, [])
        self.assertEqual(len(inbox), 3)

    async def test_backoff_doubles_up_to_the_cap(self):
        c = self.client(
            "DROP.X", max_reconnects=4, reconnect_backoff=1, max_reconnect_backoff=4
        )
        delays, uniform = await self.drop_until_raised(c)
        at_cap = mock.call(4 / 1.2, 4)
        self.assertEqual(
            uniform, [mock.call(1, 1.2), mock.call(2, 2.4), at_cap, at_cap]
        )
        self.assertEqual(delays, [1, 2, 4 / 1.2, 4 / 1.2])

    async def test_consecutive_failures_are_limited(self):
        c = self.client("DROP.X", max_reconnects=None, max_consecutive_failures=2)
        await self.drop_until_raised(c)
        self.assertEqual(len(inbox), 3)

    async def test_stable_connection_resets_failures(self):
        c = self.client(
            "DROP.X", max_reconnects=3, max_consecutive_failures=0, stable_uptime=0
        )
        await self.drop_until_raised(c)
        self.assertEqual(len(inbox), 4)

    async def test_reconnect_on_close(self):
        # SLOW.X makes the server close every connection normally
        c = self.client("SLOW.X", max_reconnects=2, reconnect_on_close=True)
        with self.assertLogs("WebSocketClient", level="WARNING"):
            await self.drop_until_raised(c, ConnectionClosedOK)
        self.assertEqual(len(inbox), 3)

    async def test_close_still_returns_with_reconnect_on_close(self):
        c = self.client("T.AAPL", reconnect_on_close=True)
        task = asyncio.create_task(c.connect(ignore))
        await self.until(lambda: inbox and len(inbox[0]) == 2)
        await c.close()
        await asyncio.wait_for(task, 5)
        self.assertEqual(len(inbox), 1)

    async def test_raise_fatal_errors(self):
        c = self.client(feed=f"localhost:{port}/404", raise_fatal_errors=True)
        with self.assertRaises(InvalidStatus):
            await asyncio.wait_for(c.connect(ignore), 5)

    async def test_handshake_failures_count(self):
        c = self.client(feed=f"localhost:{port}/404", max_consecutive_failures=0)
        with self.assertRaises(InvalidStatus):
            await asyncio.wait_for(c.connect(ignore), 5)

    async def test_process_exception_is_passed_to_websockets(self):
        c = self.client(feed=f"localhost:{port}/404")
        with self.assertRaises(InvalidStatus):
            await asyncio.wait_for(
                c.connect(ignore, process_exception=lambda exc: exc), 5
            )

    async def test_max_connections_reached(self):
        processor = mock.AsyncMock()
        c = self.client(api_key="MAX-CONNECTIONS", raw=True, max_reconnects=0)
        self.assertFalse(c.max_connections_reached)
        with self.assertLogs("WebSocketClient", level="WARNING"):
            with self.assertRaises(ConnectionClosedError):
                await asyncio.wait_for(c.connect(processor), 5)
        self.assertTrue(c.max_connections_reached)
        processor.assert_not_called()
