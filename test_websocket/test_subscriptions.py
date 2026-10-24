import asyncio
import json
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Set, Tuple
from unittest import mock

from fake_server import FakeServer

from massive import WebSocketClient
from massive import websocket as websocket_module


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
        """Run ``c.connect()`` until the test ends; wait for it to connect."""
        task = asyncio.create_task(c.connect(ignore))

        async def stop():
            if not task.done():
                await c.close()
            await asyncio.wait_for(task, 5)

        self.addAsyncCleanup(stop)
        await self.server.wait_for_connections(len(self.server.connections) + 1)
        return task


class ThreadSafetyTest(SubscriptionsTest):
    threads = 16
    calls = 10_000
    topics_per_thread = 50

    async def test_concurrent_subscribe_unsubscribe(self) -> None:
        # without acknowledgements, so the client does not have tens of
        # thousands of them to read before it can close
        self.server.ack_subscriptions = False
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
        self.assertEqual(len(self.server.open_connections), 1)


class CompatibilityTest(SubscriptionsTest):
    async def test_changing_the_attributes_directly(self) -> None:
        # as before, changing scheduled_subs and setting schedule_resub works
        c = self.client(subscriptions=["T.A"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.A"})
        c.scheduled_subs.add("T.B")
        c.schedule_resub = True
        await self.server.wait_for_subscriptions({"T.A", "T.B"})
        self.assertIsInstance(c.scheduled_subs, set)
        self.assertIsInstance(c.subs, set)

    async def test_subscribe_after_changing_the_attributes_directly(self) -> None:
        c = self.client(subscriptions=["T.A"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.A"})
        c.scheduled_subs.add("T.B")
        c.schedule_resub = True
        c.subscribe("T.C")  # schedule_resub is already set
        await self.server.wait_for_subscriptions({"T.A", "T.B", "T.C"})
        c.subscribe("T.D")
        await self.server.wait_for_subscriptions({"T.A", "T.B", "T.C", "T.D"})


def recv_timeout(seconds: float):
    """
    Patch the client's receive loop to wait up to ``seconds`` for a message,
    instead of one second, by giving its module an ``asyncio`` whose
    ``wait_for()`` uses that timeout.
    """

    class Asyncio:
        def __getattr__(self, name: str) -> Any:
            return getattr(asyncio, name)

        @staticmethod
        def wait_for(aw: Any, timeout: float) -> Any:
            return asyncio.wait_for(aw, seconds)

    return mock.patch.object(websocket_module, "asyncio", Asyncio())


class WakeUpTest(SubscriptionsTest):
    """
    With a 30 second receive timeout, the receive loop sees a change only
    after the next message: changes reach the server promptly only if they
    are sent without waiting for it.
    """

    latency = 0.5

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        patcher = recv_timeout(30.0)
        patcher.start()
        self.addCleanup(patcher.stop)

    async def test_subscribe_is_sent_promptly(self) -> None:
        c = self.client(subscriptions=["T.AAPL"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.AAPL"})
        c.subscribe("T.MSFT")
        await self.server.wait_for_subscriptions({"T.AAPL", "T.MSFT"}, self.latency)

    async def test_unsubscribe_from_another_thread_is_sent_promptly(self) -> None:
        c = self.client(subscriptions=["T.AAPL", "T.MSFT"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.AAPL", "T.MSFT"})
        await asyncio.to_thread(c.unsubscribe, "T.AAPL")
        await self.server.wait_for_subscriptions({"T.MSFT"}, self.latency)

    async def test_unsubscribe_all_is_sent_promptly(self) -> None:
        c = self.client(subscriptions=["T.AAPL"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.AAPL"})
        await asyncio.to_thread(c.unsubscribe_all)
        await self.server.wait_for_subscriptions(set(), self.latency)

    async def test_changes_between_connect_calls(self) -> None:
        c = self.client()
        c.subscribe("T.AAPL")  # before connect() runs

        async def processor(msgs):
            await c.close()

        # connect() in a thread with its own event loop, closed afterwards
        connected = asyncio.create_task(
            asyncio.to_thread(asyncio.run, c.connect(processor))
        )
        await self.server.wait_for_subscriptions({"T.AAPL"})
        await self.server.send_data(
            [{"ev": "T", "sym": "AAPL", "x": 1, "p": 1, "s": 1, "t": 1}]
        )
        await asyncio.wait_for(connected, 5)

        # connect()'s loop is closed: scheduling still works and does not raise
        c.subscribe("T.MSFT")
        c.unsubscribe("T.AAPL")
        self.assertEqual(c.scheduled_subs, {"T.MSFT"})

    async def test_frequent_receive_timeouts(self) -> None:
        errors: List[Dict[str, Any]] = []
        loop = asyncio.get_running_loop()
        handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda loop, context: errors.append(context))
        self.addCleanup(loop.set_exception_handler, handler)

        c = self.client(subscriptions=["T.AAPL"])
        with recv_timeout(0.0005):
            await self.start(c)
            await self.server.wait_for_subscriptions({"T.AAPL"})

            def hammer():
                for _ in range(20_000):
                    c.subscribe("T.X")
                    c.unsubscribe("T.X")

            await asyncio.to_thread(hammer)
            await self.server.wait_for_subscriptions({"T.AAPL"})
        self.assertEqual(errors, [])

    async def test_changes_while_receiving_data(self) -> None:
        # subscription frames go out while the receive loop is in recv() and
        # the server is streaming data to it
        errors: List[Dict[str, Any]] = []
        loop = asyncio.get_running_loop()
        handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda loop, context: errors.append(context))
        self.addCleanup(loop.set_exception_handler, handler)

        n = 300
        received: List[str] = []
        all_received = asyncio.Event()

        async def processor(msgs):
            received.extend(m.symbol for m in msgs)
            if len(received) >= n:
                all_received.set()

        c = self.client(subscriptions=["T.AAPL"])
        task = asyncio.create_task(c.connect(processor))
        await self.server.wait_for_subscriptions({"T.AAPL"})
        subscribed = {"T.AAPL"}
        for i in range(n):
            await self.server.send_data(
                [{"ev": "T", "sym": f"S{i}", "x": 1, "p": 1, "s": 1, "t": 1}]
            )
            if i % 30 == 29:
                # a burst of data is on its way to the client: change the
                # subscriptions, alternately from this thread and another
                topic = f"Q.S{i}"
                if i % 60 == 29:
                    c.subscribe(topic)
                else:
                    await asyncio.to_thread(c.subscribe, topic)
                subscribed.add(topic)
                await self.server.wait_for_subscriptions(subscribed, self.latency)
        await asyncio.wait_for(all_received.wait(), 5)

        self.assertEqual(received, [f"S{i}" for i in range(n)])
        subscribes = [m for m in self.server.inbox if m.get("action") == "subscribe"]
        self.assertEqual(len(subscribes), 1 + n // 30)
        self.assertEqual(errors, [])
        await c.close()
        await asyncio.wait_for(task, 5)

    async def test_cancel_while_connected(self) -> None:
        received = asyncio.Event()

        async def processor(msgs):
            received.set()

        c = self.client(subscriptions=["T.AAPL"])
        task = asyncio.create_task(c.connect(processor))
        await self.server.wait_for_subscriptions({"T.AAPL"})
        ws = c.websocket
        assert ws is not None
        self.addAsyncCleanup(ws.close)
        # Cancel while the client waits for a message, not while one arrives:
        # on Python 3.10 and 3.11, asyncio.wait_for() ignores a cancellation
        # that coincides with the completion of what it waits for
        # (https://bugs.python.org/issue42130). The acknowledgements are read
        # before this message, so none is in flight once it is processed.
        await self.server.send_data(
            [{"ev": "T", "sym": "AAPL", "x": 1, "p": 1, "s": 1, "t": 1}]
        )
        await asyncio.wait_for(received.wait(), 5)

        task.cancel()
        done, _ = await asyncio.wait({task}, timeout=2)
        self.assertIn(task, done, "connect() did not finish after cancel()")
        self.assertTrue(task.cancelled())
        c.subscribe("T.MSFT")  # does not raise after connect() ended


class SenderErrorTest(SubscriptionsTest):
    async def test_send_error_is_raised_by_connect(self) -> None:
        class Json:
            loads = staticmethod(json.loads)

            @staticmethod
            def dumps(obj: Any) -> str:
                if obj.get("params") == "T.BAD":
                    raise TypeError("cannot encode")
                return json.dumps(obj)

        c = self.client(subscriptions=["T.A"], custom_json=Json)
        task = asyncio.create_task(c.connect(ignore))
        await self.server.wait_for_subscriptions({"T.A"})
        ws = c.websocket
        assert ws is not None
        self.addAsyncCleanup(ws.close)
        c.subscribe("T.BAD")
        with self.assertRaises(TypeError):
            await asyncio.wait_for(task, 5)


class WildcardTest(SubscriptionsTest):
    async def test_subscribe_collapses_only_the_same_channel(self) -> None:
        c = self.client(subscriptions=["A.X", "T.AAPL", "TX.AAPL"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"A.X", "T.AAPL", "TX.AAPL"})
        c.subscribe("AM.*")  # "A" is a prefix of "AM", not the same channel
        await self.server.wait_for_subscriptions({"A.X", "T.AAPL", "TX.AAPL", "AM.*"})
        c.subscribe("T.*")
        await self.server.wait_for_subscriptions({"A.X", "TX.AAPL", "AM.*", "T.*"})

    async def test_collapse_leaves_topics_not_yet_subscribed(self) -> None:
        # as before: only subscribed topics collapse
        c = self.client(subscriptions=["T.AAPL"])
        c.subscribe("T.*")
        self.assertEqual(c.scheduled_subs, {"T.AAPL", "T.*"})

    async def test_unsubscribe_collapses_only_the_same_channel(self) -> None:
        # T.AAPL after T.* (subscribing to T.AAPL first would collapse it)
        c = self.client(subscriptions=["T.*", "T.AAPL", "TX.AAPL", "AM.X"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.AAPL", "T.*", "TX.AAPL", "AM.X"})
        c.unsubscribe("T.*", "A.*")
        await self.server.wait_for_subscriptions({"TX.AAPL", "AM.X"})

    async def test_subscribing_to_a_subscribed_wildcard_keeps_it(self) -> None:
        c = self.client(subscriptions=["T.*"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.*"})
        c.subscribe("T.*", "Q.Z")
        await self.server.wait_for_subscriptions({"T.*", "Q.Z"})

    async def test_topics_are_kept_as_given(self) -> None:
        # as before: the constructor does not collapse, and nothing is stripped
        c = self.client(subscriptions=["T.AAPL", "T.*", " Q.X "])
        self.assertEqual(c.scheduled_subs, {"T.AAPL", "T.*", " Q.X "})
        c.unsubscribe(" Q.X ")
        self.assertEqual(c.scheduled_subs, {"T.AAPL", "T.*"})


class FramesTest(SubscriptionsTest):
    """Tests that check the exact subscribe and unsubscribe frames sent."""

    def frames_after(self, n: int) -> List[Tuple[str, Set[str]]]:
        """The frames the server received after its first ``n`` messages."""
        return [
            (m["action"], set(m["params"].split(",")))
            for m in self.server.connection.inbox[n:]
        ]

    async def wait_for_frame(self, action: str, topics: Set[str]):
        await self.server.wait_for_message(
            lambda m: m.get("action") == action
            and set(m["params"].split(",")) == topics
        )


class SetSubscriptionsTest(FramesTest):
    async def test_one_frame_per_action(self) -> None:
        c = self.client(subscriptions=["T.A", "T.B", "T.C", "Q.*"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.A", "T.B", "T.C", "Q.*"})
        n = len(self.server.connection.inbox)
        c.set_subscriptions(["T.B", "T.C", "T.D", "T.E"])
        self.assertEqual(c.scheduled_subs, {"T.B", "T.C", "T.D", "T.E"})
        await self.wait_for_frame("subscribe", {"T.D", "T.E"})
        self.assertEqual(
            self.frames_after(n),
            [("unsubscribe", {"T.A", "Q.*"}), ("subscribe", {"T.D", "T.E"})],
        )
        await self.server.wait_for_subscriptions({"T.B", "T.C", "T.D", "T.E"})

    async def test_subscribe_and_unsubscribe_send_additions_first(self) -> None:
        # as before
        c = self.client(subscriptions=["T.A"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.A"})
        n = len(self.server.connection.inbox)
        c.subscribe("T.B")
        c.unsubscribe("T.A")
        await self.wait_for_frame("unsubscribe", {"T.A"})
        self.assertEqual(
            self.frames_after(n), [("subscribe", {"T.B"}), ("unsubscribe", {"T.A"})]
        )

    async def test_combined_update_sends_removals_first(self) -> None:
        c = self.client(subscriptions=["T.A", "T.B"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.A", "T.B"})
        n = len(self.server.connection.inbox)
        c.subscribe("T.C")
        c.set_subscriptions(["T.B", "T.C", "T.D"])
        await self.wait_for_frame("subscribe", {"T.C", "T.D"})
        self.assertEqual(
            self.frames_after(n),
            [("unsubscribe", {"T.A"}), ("subscribe", {"T.C", "T.D"})],
        )

    async def test_unchanged_set_sends_nothing(self) -> None:
        c = self.client(subscriptions=["T.A"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.A"})
        n = len(self.server.connection.inbox)
        c.set_subscriptions({"T.A"})
        # a change of its own, to show nothing was sent before it
        c.subscribe("T.Z")
        await self.wait_for_frame("subscribe", {"T.Z"})
        self.assertEqual(self.frames_after(n), [("subscribe", {"T.Z"})])

    async def test_wildcards_do_not_collapse(self) -> None:
        c = self.client(subscriptions=["T.A"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.A"})
        c.set_subscriptions(["T.A", "T.*"])
        await self.server.wait_for_subscriptions({"T.A", "T.*"})

    async def test_before_connect(self) -> None:
        c = self.client(subscriptions=["T.A"])
        c.set_subscriptions(["T.B", "T.*"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.B", "T.*"})
        self.assertEqual(self.frames_after(1), [("subscribe", {"T.B", "T.*"})])

    async def test_malformed_topics_are_skipped(self) -> None:
        c = self.client(subscriptions=["T.Z"])
        with mock.patch.object(websocket_module.logger, "warning") as warning:
            c.set_subscriptions(["T.A", "bad", "T.B"])
        self.assertEqual(c.scheduled_subs, {"T.A", "T.B"})
        self.assertEqual([call.args[1] for call in warning.call_args_list], ["bad"])


class ResubscribeTest(FramesTest):
    async def connected(self, *topics: str) -> WebSocketClient:
        """A client subscribed to ``topics``, acknowledged by the server."""
        c = self.client(subscriptions=list(topics))
        await self.start(c)
        await self.server.wait_for_subscriptions(set(topics))
        return c

    async def test_sends_an_acknowledged_topic_again(self) -> None:
        c = await self.connected("T.A", "T.B")
        n = len(self.server.connection.inbox)
        c.resubscribe("T.A")
        await self.wait_for_frame("subscribe", {"T.A"})
        # a change of its own, to show nothing else was sent in between
        c.subscribe("T.Z")
        await self.wait_for_frame("subscribe", {"T.Z"})
        self.assertEqual(
            self.frames_after(n), [("subscribe", {"T.A"}), ("subscribe", {"T.Z"})]
        )

    async def test_joins_the_subscribe_message_for_new_topics(self) -> None:
        c = await self.connected("T.A", "T.B")
        n = len(self.server.connection.inbox)
        c.set_subscriptions(["T.A", "T.B", "T.C"])
        c.resubscribe("T.A")
        await self.wait_for_frame("subscribe", {"T.A", "T.C"})
        self.assertEqual(self.frames_after(n), [("subscribe", {"T.A", "T.C"})])

    async def test_not_sent_if_unsubscribed_first(self) -> None:
        c = await self.connected("T.A", "T.B")
        n = len(self.server.connection.inbox)
        c.resubscribe("T.A")
        c.unsubscribe("T.A")
        await self.wait_for_frame("unsubscribe", {"T.A"})
        c.subscribe("T.Z")
        await self.wait_for_frame("subscribe", {"T.Z"})
        self.assertEqual(
            self.frames_after(n), [("unsubscribe", {"T.A"}), ("subscribe", {"T.Z"})]
        )

    async def test_topic_must_be_subscribed(self) -> None:
        c = await self.connected("T.A")
        n = len(self.server.connection.inbox)
        with self.assertRaises(ValueError):
            c.resubscribe("T.A", "T.B")
        c.subscribe("T.Z")
        await self.wait_for_frame("subscribe", {"T.Z"})
        self.assertEqual(self.frames_after(n), [("subscribe", {"T.Z"})])

    async def test_malformed_topics_are_skipped(self) -> None:
        c = await self.connected("T.A", "T.B")
        n = len(self.server.connection.inbox)
        with mock.patch.object(websocket_module.logger, "warning") as warning:
            c.resubscribe("bad", "T.A")
        self.assertEqual([call.args[1] for call in warning.call_args_list], ["bad"])
        await self.wait_for_frame("subscribe", {"T.A"})
        self.assertEqual(self.frames_after(n), [("subscribe", {"T.A"})])


class ReconnectTest(SubscriptionsTest):
    async def test_changes_during_an_outage_are_kept(self) -> None:
        c = self.client(subscriptions=["T.A", "T.B"])
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.A", "T.B"})

        # drop the connection, and hold the next one before authentication,
        # so the client is between connections while the changes are made
        self.server.script_auth()
        await self.server.close_connections(code=1011)
        await self.server.wait_for(
            lambda: len(self.server.connections) == 2
            and self.server.connection.api_key is not None
        )
        c.subscribe("T.C")
        c.unsubscribe("T.A")
        await self.server.connection.close(code=1011)

        await self.server.wait_for_connections(2)
        self.assertEqual(len(self.server.connections), 3)
        await self.server.wait_for_subscriptions({"T.B", "T.C"})
        self.assertEqual(c.scheduled_subs, {"T.B", "T.C"})

    async def test_connect_again_after_close(self) -> None:
        c = self.client(subscriptions=["T.A"])
        task = await self.start(c)
        await self.server.wait_for_subscriptions({"T.A"})
        await c.close()
        await asyncio.wait_for(task, 5)

        c.subscribe("T.B")
        await self.start(c)
        await self.server.wait_for_subscriptions({"T.A", "T.B"})

    async def test_connect_again_after_cancel(self) -> None:
        received = asyncio.Event()

        async def processor(msgs):
            received.set()

        c = self.client(subscriptions=["T.A"])
        task = asyncio.create_task(c.connect(processor))
        await self.server.wait_for_subscriptions({"T.A"})
        ws = c.websocket
        assert ws is not None
        self.addAsyncCleanup(ws.close)
        # cancel once idle; see WakeUpTest.test_cancel_while_connected
        await self.server.send_data(
            [{"ev": "T", "sym": "A", "x": 1, "p": 1, "s": 1, "t": 1}]
        )
        await asyncio.wait_for(received.wait(), 5)
        task.cancel()
        done, _ = await asyncio.wait({task}, timeout=2)
        self.assertIn(task, done)

        await self.start(c)
        await self.server.wait_for_subscriptions({"T.A"})
        c.subscribe("T.B")
        await self.server.wait_for_subscriptions({"T.A", "T.B"})


if __name__ == "__main__":
    unittest.main()
