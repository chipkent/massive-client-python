from massive import WebSocketClient
from base_ws import BaseTest
from mock_server import inbox, port
import asyncio
import threading
from unittest import mock


class StopInSleep:
    # asyncio, except that sleep() awaits stop() and returns at once
    def __init__(self, stop):
        self.stop = stop

    def __getattr__(self, name):
        return getattr(asyncio, name)

    async def sleep(self, delay):
        await self.stop()


class RequestCloseTest(BaseTest):
    def client(self, *subscriptions, **kwargs):
        return WebSocketClient(
            api_key="",
            feed=f"localhost:{port}",
            secure=False,
            subscriptions=list(subscriptions),
            **kwargs,
        )

    async def test_stops_run_from_another_thread(self):
        c = self.client("T.AAPL", reconnect_on_close=True)
        thread = threading.Thread(target=c.run, args=(lambda msgs: None,))
        thread.start()
        await self.until(lambda: inbox and len(inbox[0]) == 2)
        c.request_close()
        await asyncio.to_thread(thread.join, 5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(inbox), 1)

    async def test_stops_reconnecting(self):
        # DROP.X makes the server drop the connection
        c = self.client("DROP.X", max_reconnects=None)

        async def processor(msgs):
            c.request_close()

        await asyncio.wait_for(c.connect(processor), 5)
        self.assertEqual(len(inbox), 1)

    async def test_before_connect(self):
        c = self.client("T.AAPL")
        c.request_close()
        await asyncio.wait_for(c.connect(lambda msgs: asyncio.sleep(0)), 5)
        self.assertEqual(inbox, [])
        # the request is used up
        task = asyncio.create_task(c.connect(lambda msgs: asyncio.sleep(0)))
        await self.until(lambda: inbox and len(inbox[0]) == 2)
        await c.close()
        await task

    async def test_close_before_connect_is_unchanged(self):
        c = self.client("T.AAPL")
        await c.close()  # no connection yet: only logs a warning
        task = asyncio.create_task(c.connect(lambda msgs: asyncio.sleep(0)))
        await self.until(lambda: inbox and len(inbox[0]) == 2)
        await c.close()
        await task

    async def stop_during_wait(self, stop):
        # DROP.X makes the server drop the connection; stop() runs during
        # the wait before reconnecting
        c = self.client("DROP.X", max_reconnects=None, reconnect_backoff=1)
        with mock.patch("massive.websocket.asyncio", StopInSleep(lambda: stop(c))):
            await asyncio.wait_for(c.connect(lambda msgs: asyncio.sleep(0)), 5)
        self.assertEqual(len(inbox), 1)

    async def test_stops_a_wait_before_reconnecting(self):
        await self.stop_during_wait(lambda c: asyncio.to_thread(c.request_close))

    async def test_close_stops_a_wait_before_reconnecting(self):
        await self.stop_during_wait(lambda c: c.close())
