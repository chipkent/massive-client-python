from massive import WebSocketClient
from base_ws import BaseTest
from mock_server import inbox, port
import asyncio


def sent(conn):
    # the subscribe and unsubscribe messages one connection received
    return [
        (m["action"], set(m["params"].split(",")))
        for m in conn
        if m.get("action") in ("subscribe", "unsubscribe")
    ]


class SetSubscriptionsTest(BaseTest):
    async def connected(self, *subscriptions):
        c = WebSocketClient(
            api_key="",
            feed=f"localhost:{port}",
            secure=False,
            subscriptions=list(subscriptions),
        )
        task = asyncio.create_task(c.connect(lambda msgs: asyncio.sleep(0)))
        await self.until(lambda: inbox and sent(inbox[0]))
        return c, task

    async def test_set_subscriptions_sends_removals_first(self):
        c, task = await self.connected("T.AAPL", "T.MSFT")
        c.set_subscriptions(["T.AAPL", "T.NVDA"])
        await self.until(lambda: len(sent(inbox[0])) == 3)
        self.assertEqual(
            sent(inbox[0])[1:],
            [("unsubscribe", {"T.MSFT"}), ("subscribe", {"T.NVDA"})],
        )
        await c.close()
        await task

    async def test_resubscribe_sends_again(self):
        c, task = await self.connected("T.AAPL")
        c.resubscribe("T.AAPL")
        await self.until(lambda: len(sent(inbox[0])) == 2)
        self.assertEqual(sent(inbox[0])[1], ("subscribe", {"T.AAPL"}))
        await c.close()
        await task

    async def test_resubscribe_unknown_topic_sends_nothing(self):
        c, task = await self.connected("T.AAPL")
        with self.assertRaises(ValueError):
            c.resubscribe("T.AAPL", "T.MSFT")
        c.subscribe("T.NVDA")
        await self.until(lambda: len(sent(inbox[0])) == 2)
        self.assertEqual(sent(inbox[0])[1], ("subscribe", {"T.NVDA"}))
        await c.close()
        await task
