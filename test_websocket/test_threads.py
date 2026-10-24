from massive import WebSocketClient
from base_ws import BaseTest
from mock_server import inbox, port
import asyncio


def streamed(conn):
    # the topics one connection was asked to stream
    topics = set()
    for m in conn:
        if m.get("action") == "subscribe":
            topics |= set(m["params"].split(","))
        elif m.get("action") == "unsubscribe":
            topics -= set(m["params"].split(","))
    return topics


class ThreadTest(BaseTest):
    async def test_change_made_while_sending_is_sent(self):
        c = WebSocketClient(
            api_key="", feed=f"localhost:{port}", secure=False, subscriptions=["T.AAPL"]
        )
        send = c._subscribe
        changed = False

        async def send_and_change(topics):
            nonlocal changed
            await send(topics)
            if not changed:
                changed = True
                # another thread's change, made while the loop is sending
                c.subscribe("T.LATE")

        c._subscribe = send_and_change
        task = asyncio.create_task(c.connect(lambda msgs: asyncio.sleep(0)))
        await self.until(lambda: inbox and "T.LATE" in streamed(inbox[0]))
        await c.close()
        await task
