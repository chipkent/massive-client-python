from massive import WebSocketClient
from base_ws import BaseTest
from mock_server import inbox, port
from unittest import mock
import asyncio


class NoRecvTimeout:
    # asyncio, except that connect() waits in recv() until a message arrives
    def __getattr__(self, name):
        return getattr(asyncio, name)

    @staticmethod
    def wait_for(aw, timeout):
        return asyncio.wait_for(aw, None)


def sent(conn, action):
    # the topics in each `action` message one connection received
    return [set(m["params"].split(",")) for m in conn if m.get("action") == action]


class SenderTest(BaseTest):
    async def connected(self, *subscriptions):
        # a client whose receive loop has handled every message so far
        c = WebSocketClient(
            api_key="",
            feed=f"localhost:{port}",
            secure=False,
            subscriptions=list(subscriptions),
        )
        received = []

        async def processor(msgs):
            received.append(msgs)

        task = asyncio.create_task(c.connect(processor))
        # each subscribed topic gets two trade messages
        await self.until(lambda: len(received) == 2 * len(subscriptions))
        return c, task

    async def test_change_is_sent_while_waiting_for_a_message(self):
        with mock.patch("massive.websocket.asyncio", NoRecvTimeout()):
            c, task = await self.connected("T.AAPL")
            await asyncio.to_thread(c.subscribe, "T.MSFT")
            await self.until(lambda: len(sent(inbox[0], "subscribe")) == 2)
            await asyncio.to_thread(c.unsubscribe, "T.AAPL")
            await self.until(lambda: sent(inbox[0], "unsubscribe"))
            self.assertEqual(sent(inbox[0], "subscribe"), [{"T.AAPL"}, {"T.MSFT"}])
            self.assertEqual(sent(inbox[0], "unsubscribe"), [{"T.AAPL"}])
            await c.close()
            await task

    async def test_scheduled_subs_can_still_be_changed_directly(self):
        c, task = await self.connected("T.AAPL")
        c.scheduled_subs.add("T.MSFT")
        c.schedule_resub = True
        await self.until(lambda: len(sent(inbox[0], "subscribe")) == 2)
        self.assertEqual(sent(inbox[0], "subscribe")[1], {"T.MSFT"})
        await c.close()
        await task
