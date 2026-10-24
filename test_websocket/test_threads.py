from massive import WebSocketClient
from base_ws import BaseTest
from mock_server import inbox, port
import asyncio
import random
import threading
import time


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
    async def test_changes_from_many_threads_all_reach_the_server(self):
        c = WebSocketClient(api_key="", feed=f"localhost:{port}", secure=False)
        task = asyncio.create_task(c.connect(lambda msgs: asyncio.sleep(0)))
        await self.until(lambda: inbox)

        def churn(seed):
            rng = random.Random(seed)
            for _ in range(250):
                topic = f"T.S{rng.randrange(20)}"
                if rng.random() < 0.5:
                    c.subscribe(topic)
                else:
                    c.unsubscribe(topic)
                time.sleep(0.001)  # let the event loop run in between

        threads = [threading.Thread(target=churn, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        await asyncio.to_thread(lambda: [t.join() for t in threads])

        await self.until(lambda: streamed(inbox[0]) == c.scheduled_subs == c.subs)
        await c.close()
        await task
