import asyncio
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Set

from fake_server import FakeServer

from massive import WebSocketClient


async def ignore(msgs):
    pass


class SubscriptionsTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.server = await FakeServer().start()
        self.addAsyncCleanup(self.server.stop)

    def client(self, **kwargs) -> WebSocketClient:
        kwargs.setdefault("api_key", "key")
        return WebSocketClient(feed=self.server.feed, secure=False, **kwargs)

    async def start(self, c: WebSocketClient) -> "asyncio.Task[None]":
        task = asyncio.create_task(c.connect(ignore))

        async def stop():
            await c.close()
            await asyncio.wait_for(task, 5)

        self.addAsyncCleanup(stop)
        await self.server.wait_for_connections(1)
        return task


class ThreadSafetyTest(SubscriptionsTest):
    threads = 16
    calls = 10_000
    topics_per_thread = 50

    async def test_concurrent_subscribe_unsubscribe(self) -> None:
        c = self.client(subscriptions=["T.INITIAL"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.INITIAL"})
        barrier = threading.Barrier(self.threads, timeout=10)
        # the last call for each topic decides whether it is subscribed
        last: Dict[str, bool] = {}
        last_lock = threading.Lock()

        def run(i: int):
            mine: Dict[str, bool] = {}
            barrier.wait()
            for j in range(self.calls):
                topic = f"T.S{i}X{j % self.topics_per_thread}"
                if j % 3 == 0:
                    c.unsubscribe(topic)
                    mine[topic] = False
                else:
                    c.subscribe(topic)
                    mine[topic] = True
            with last_lock:
                last.update(mine)

        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(self.threads) as pool:
            await asyncio.gather(
                *(loop.run_in_executor(pool, run, i) for i in range(self.threads))
            )

        desired: Set[str] = {"T.INITIAL"} | {t for t, on in last.items() if on}
        self.assertEqual(c.scheduled_subs, desired)
        await self.server.wait_for_subscriptions(desired, timeout=10)
        await self.server.wait_for(lambda: c.subs == desired)
        self.assertEqual(len(self.server.open_connections), 1)


if __name__ == "__main__":
    unittest.main()
