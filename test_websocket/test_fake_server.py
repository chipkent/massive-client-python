import asyncio
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

from fake_server import FakeConnection, FakeServer

from massive import WebSocketClient
from massive.exceptions import AuthError
from massive.websocket import EquityTrade


def trade(sym: str, price: float = 1.0):
    return {"ev": "T", "sym": sym, "x": 4, "p": price, "s": 100, "t": 1, "q": 1}


class FakeServerTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.server = await FakeServer().start()
        self.addAsyncCleanup(self.server.stop)

    def client(self, **kwargs) -> WebSocketClient:
        kwargs.setdefault("api_key", "key")
        return WebSocketClient(feed=self.server.feed, secure=False, **kwargs)

    async def test_connect_auth_subscribe_and_receive(self) -> None:
        c = self.client(subscriptions=["T.AAPL"])
        received: List[EquityTrade] = []

        async def processor(msgs):
            received.extend(msgs)

        task = asyncio.create_task(c.connect(processor))
        await self.server.wait_for_subscriptions({"T.AAPL"})
        conn = self.server.connection
        self.assertEqual(conn.path, "/stocks")
        self.assertEqual(conn.api_key, "key")
        self.assertEqual(
            conn.inbox,
            [
                {"action": "auth", "params": "key"},
                {"action": "subscribe", "params": "T.AAPL"},
            ],
        )

        await self.server.send_data([trade("AAPL", 2.5)])
        await self.server.wait_for(lambda: len(received) == 1)
        self.assertEqual(received[0].symbol, "AAPL")
        self.assertEqual(received[0].price, 2.5)

        c.subscribe("T.MSFT")
        c.unsubscribe("T.AAPL")
        await self.server.wait_for_subscriptions({"T.MSFT"})
        self.assertEqual(c.subs, {"T.MSFT"})

        await c.close()
        await asyncio.wait_for(task, 5)
        await self.server.wait_for(lambda: not conn.open)

    async def test_auth_failed_raises(self) -> None:
        self.server.reject_keys = {"bad"}
        c = self.client(api_key="bad")
        with self.assertRaises(AuthError):
            await asyncio.wait_for(c.connect(lambda msgs: asyncio.sleep(0)), 5)
        self.assertEqual(self.server.inbox, [{"action": "auth", "params": "bad"}])

    async def test_resubscribes_after_reconnect(self) -> None:
        c = self.client(subscriptions=["T.AAPL", "Q.AAPL"])
        task = asyncio.create_task(c.connect(lambda msgs: asyncio.sleep(0)))
        await self.server.wait_for_subscriptions({"T.AAPL", "Q.AAPL"})

        # an abnormal close makes the client reconnect and subscribe again
        await self.server.close_connections(code=1011)
        await self.server.wait_for_connections(2)
        await self.server.wait_for_subscriptions({"T.AAPL", "Q.AAPL"})
        self.assertEqual(len(self.server.open_connections), 1)

        await c.close()
        await asyncio.wait_for(task, 5)

    async def test_scripted_auth_reply_and_messages(self) -> None:
        # the fake server can replace its replies, e.g. to emulate other statuses
        replies: List[str] = []

        async def on_message(conn: FakeConnection, msg):
            if msg.get("action") == "subscribe":
                await conn.send([trade(t.split(".", 1)[1]) for t in conn.subscriptions])

        self.server.on_message = on_message
        self.server.ack_subscriptions = False
        self.server.script_auth(
            {"ev": "status", "status": "auth_success", "message": "scripted"}
        )
        c = self.client(subscriptions=["T.AMZN"])

        async def processor(msgs):
            replies.extend(m.symbol for m in msgs)
            await c.close()

        await asyncio.wait_for(c.connect(processor), 5)
        self.assertEqual(replies, ["AMZN"])
        self.assertTrue(self.server.connection.authenticated)

    async def test_rejected_handshake_is_retried(self) -> None:
        self.server.reject_handshakes = True
        c = self.client(subscriptions=["T.AAPL"])
        task = asyncio.create_task(c.connect(lambda msgs: asyncio.sleep(0)))
        await asyncio.sleep(0.2)
        self.assertEqual(self.server.connections, [])
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task


class ConcurrentClientsTest(unittest.IsolatedAsyncioTestCase):
    """Independent clients, each with its own thread and event loop."""

    threads = 16

    async def test_clients_in_parallel_threads(self) -> None:
        async def on_subscribe(conn: FakeConnection, msg):
            if msg.get("action") == "subscribe":
                await conn.send(
                    [trade(t.split(".", 1)[1]) for t in msg["params"].split(",")]
                )

        server = await FakeServer(on_message=on_subscribe).start()
        self.addAsyncCleanup(server.stop)
        barrier = threading.Barrier(self.threads, timeout=10)
        received: Dict[int, List[str]] = {}

        def run(i: int):
            c = WebSocketClient(
                api_key=f"key{i}",
                feed=server.feed,
                secure=False,
                subscriptions=[f"T.S{i}"],
            )

            async def processor(msgs):
                received[i] = [m.symbol for m in msgs]
                await c.close()

            barrier.wait()
            asyncio.run(asyncio.wait_for(c.connect(processor), 10))

        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(self.threads) as pool:
            await asyncio.gather(
                *(loop.run_in_executor(pool, run, i) for i in range(self.threads))
            )

        self.assertEqual(received, {i: [f"S{i}"] for i in range(self.threads)})
        self.assertEqual(len(server.connections), self.threads)
        by_key = {c.api_key: c for c in server.connections}
        for i in range(self.threads):
            self.assertEqual(
                by_key[f"key{i}"].inbox[1], {"action": "subscribe", "params": f"T.S{i}"}
            )


if __name__ == "__main__":
    unittest.main()
