from massive import WebSocketClient
from base_ws import BaseTest
from mock_server import inbox, port
import asyncio


def sent(conn, action):
    # the topics in each `action` message one connection received
    return [set(m["params"].split(",")) for m in conn if m.get("action") == action]


class ReconnectTest(BaseTest):
    async def test_changes_made_before_a_drop_are_kept(self):
        c = WebSocketClient(
            api_key="",
            feed=f"localhost:{port}",
            secure=False,
            subscriptions=["T.AAPL", "T.MSFT", "DROP.X"],
        )

        async def processor(msgs):
            if len(inbox) == 1:
                # DROP.X makes the server drop the connection; change the
                # subscriptions before the client sends anything else
                await c.websocket.wait_closed()
                c.unsubscribe("T.MSFT", "DROP.X")
                c.subscribe("T.NVDA")

        task = asyncio.create_task(c.connect(processor))
        await self.until(lambda: len(inbox) == 2 and sent(inbox[1], "subscribe"))
        self.assertEqual(sent(inbox[1], "subscribe"), [{"T.AAPL", "T.NVDA"}])
        await c.close()
        await task

    async def test_connect_again_subscribes_again(self):
        # SLOW.X makes the server close the connection, so connect() returns
        c = WebSocketClient(
            api_key="", feed=f"localhost:{port}", secure=False, subscriptions=["SLOW.X"]
        )
        with self.assertLogs("WebSocketClient", level="WARNING"):
            for _ in range(2):
                await asyncio.wait_for(c.connect(lambda msgs: asyncio.sleep(0)), 5)
        self.assertEqual(
            [sent(conn, "subscribe") for conn in inbox], [[{"SLOW.X"}], [{"SLOW.X"}]]
        )
