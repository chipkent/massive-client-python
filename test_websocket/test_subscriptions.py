import asyncio
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Set

from fake_server import FakeConnection, FakeServer

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
            if not task.done():
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


class WakeUpTest(SubscriptionsTest):
    # generous for slow CI machines, yet far below recv_interval, so a missed
    # wake-up fails the test deterministically
    max_latency = 0.5
    recv_interval = 30.0

    def client(self, **kwargs) -> WebSocketClient:
        c = super().client(**kwargs)
        c._recv_interval = self.recv_interval
        return c

    async def measure(self, change) -> float:
        """Seconds from calling ``change()`` to the server receiving a frame."""
        received: List[float] = []

        async def on_message(conn: FakeConnection, msg: Any):
            if isinstance(msg, dict) and msg.get("action") != "auth":
                received.append(time.monotonic())

        self.server.on_message = on_message
        c = self.client(subscriptions=["T.AAPL"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.AAPL"})
        received.clear()
        # let the loop settle into waiting for a message
        await asyncio.sleep(0.2)
        start = time.monotonic()
        await change(c)
        await self.server.wait_for(lambda: bool(received))
        return received[0] - start

    async def test_subscribe_is_sent_promptly(self) -> None:
        async def change(c: WebSocketClient):
            c.subscribe("T.MSFT")

        self.assertLess(await self.measure(change), self.max_latency)
        await self.server.wait_for_subscriptions({"T.AAPL", "T.MSFT"})

    async def test_unsubscribe_from_another_thread_is_sent_promptly(self) -> None:
        async def change(c: WebSocketClient):
            await asyncio.to_thread(c.unsubscribe, "T.AAPL")

        self.assertLess(await self.measure(change), self.max_latency)
        await self.server.wait_for_subscriptions(set())

    async def test_unsubscribe_all_is_sent_promptly(self) -> None:
        async def change(c: WebSocketClient):
            c.unsubscribe_all()

        self.assertLess(await self.measure(change), self.max_latency)
        await self.server.wait_for_subscriptions(set())

    async def test_subscribe_before_connect_and_after_its_loop_closed(self) -> None:
        c = self.client()
        c.subscribe("T.AAPL")  # connect() has not created its loop yet

        async def processor(msgs):
            await c.close()

        connected = asyncio.create_task(
            asyncio.to_thread(asyncio.run, c.connect(processor))
        )
        await self.server.wait_for_subscriptions({"T.AAPL"})
        await self.server.send_data(
            [{"ev": "T", "sym": "AAPL", "x": 1, "p": 1, "s": 1, "t": 1}]
        )
        await asyncio.wait_for(connected, 5)
        c.subscribe("T.MSFT")  # connect()'s loop is closed
        self.assertEqual(c.scheduled_subs, {"T.AAPL", "T.MSFT"})

    async def test_wake_up_while_recv_times_out(self) -> None:
        # a wake-up that lands while the recv() timeout is expiring must not
        # try to reschedule it
        errors: List[Dict[str, Any]] = []
        loop = asyncio.get_running_loop()
        handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda loop, context: errors.append(context))
        self.addCleanup(loop.set_exception_handler, handler)

        c = self.client(subscriptions=["T.AAPL"])
        c._recv_interval = 0.0005
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.AAPL"})
        stop = threading.Event()

        def hammer():
            while not stop.is_set():
                c.subscribe("T.X")
                c.unsubscribe("T.X")

        thread = threading.Thread(target=hammer)
        thread.start()
        try:
            await asyncio.sleep(1)
        finally:
            stop.set()
            thread.join()
        await self.server.wait_for_subscriptions({"T.AAPL"})
        self.assertEqual(errors, [])

    async def test_messages_are_not_lost_when_woken(self) -> None:
        received: List[str] = []

        async def processor(msgs):
            received.extend(m.symbol for m in msgs)

        c = self.client(subscriptions=["T.AAPL"])
        task = asyncio.create_task(c.connect(processor))
        await self.server.wait_for_subscriptions({"T.AAPL"})
        for i in range(200):
            await self.server.send_data(
                [{"ev": "T", "sym": f"S{i}", "x": 1, "p": 1, "s": 1, "t": 1}]
            )
            if i % 2:
                c.subscribe(f"T.S{i}")
            else:
                c.unsubscribe(f"T.S{i - 1}")
        await self.server.wait_for(lambda: len(received) == 200)
        self.assertEqual(received, [f"S{i}" for i in range(200)])
        await c.close()
        await asyncio.wait_for(task, 5)


class WildcardTest(SubscriptionsTest):
    async def test_wildcard_subscribe_keeps_other_channels(self) -> None:
        # the collapse (on by default) matches the channel exactly
        c = self.client()
        await self.start(c)
        c.subscribe("A.X")
        await self.server.wait_for_subscriptions({"A.X"})
        c.subscribe("AM.*")
        await self.server.wait_for_subscriptions({"A.X", "AM.*"})
        self.assertEqual(c.scheduled_subs, {"A.X", "AM.*"})

    async def test_wildcard_subscribe_without_collapse(self) -> None:
        c = self.client(subscriptions=["T.AAPL", "TX.AAPL"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.AAPL", "TX.AAPL"})
        c.subscribe("T.*", collapse=False)
        await self.server.wait_for_subscriptions({"T.AAPL", "TX.AAPL", "T.*"})

    async def test_wildcard_unsubscribe_without_collapse(self) -> None:
        c = self.client(subscriptions=["T.AAPL", "T.*", "TX.*"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.AAPL", "T.*", "TX.*"})
        c.unsubscribe("T.*", collapse=False)
        await self.server.wait_for_subscriptions({"T.AAPL", "TX.*"})
        self.assertEqual(c.scheduled_subs, {"T.AAPL", "TX.*"})

    async def test_collapse_on_subscribe(self) -> None:
        # by default; pending topics collapse too, not only subscribed ones
        c = self.client(subscriptions=["T.AAPL", "TX.AAPL", "A.X"])
        c.subscribe("T.MSFT")
        c.subscribe("T.*")
        self.assertEqual(c.scheduled_subs, {"T.*", "TX.AAPL", "A.X"})
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.*", "TX.AAPL", "A.X"})
        c.subscribe("A.*")
        await self.server.wait_for_subscriptions({"T.*", "TX.AAPL", "A.*"})

    async def test_collapse_on_unsubscribe(self) -> None:
        c = self.client(subscriptions=["T.AAPL", "T.*", "TX.AAPL", "AM.X"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.AAPL", "T.*", "TX.AAPL", "AM.X"})
        c.unsubscribe("T.*", "A.*")  # collapses by default
        await self.server.wait_for_subscriptions({"TX.AAPL", "AM.X"})


if __name__ == "__main__":
    unittest.main()
