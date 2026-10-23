import asyncio
import unittest
from typing import Any, List

from fake_server import FakeServer

from massive import WebSocketClient


async def ignore(msgs: Any) -> None:
    pass


class ReconnectTest(unittest.IsolatedAsyncioTestCase):
    """How the client behaves when its connection ends."""

    async def asyncSetUp(self) -> None:
        self.server = await FakeServer().start()
        self.addAsyncCleanup(self.server.stop)

    def client(self, **kwargs) -> WebSocketClient:
        kwargs.setdefault("api_key", "key")
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

        await self.server.wait_for_connections(2, timeout=15)
        await self.server.wait_for_subscriptions({"T.KEEP", "T.NEW", "Q.NEW"})
        topics: List[str] = []
        for m in self.server.connection.inbox[1:]:
            self.assertEqual(m["action"], "subscribe")
            topics += m["params"].split(",")
        self.assertCountEqual(topics, ["T.KEEP", "T.NEW", "Q.NEW"])
        self.assertEqual(c.scheduled_subs, {"T.KEEP", "T.NEW", "Q.NEW"})

        await c.close()
        await asyncio.wait_for(task, 5)


if __name__ == "__main__":
    unittest.main()
