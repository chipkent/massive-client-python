import unittest
import asyncio
from mock_server import run_mock_server, inbox, port
from typing import List
from massive.websocket import WebSocketMessage
from websockets.asyncio.client import connect

unittest.util._MAX_LENGTH = 30000  # type: ignore


# https://docs.python.org/3/library/unittest.html#unittest.IsolatedAsyncioTestCase
class BaseTest(unittest.IsolatedAsyncioTestCase):
    expected: List[WebSocketMessage] = []
    count = 0

    def expectProcessor(self, msg):
        self.assertEqual(msg, self.expected[self.count])
        self.count += 1

    def expectResponse(self, msg):
        self.expected.append(msg)

    async def asyncSetUp(self):
        self.maxDiff = None
        loop = asyncio.get_event_loop()
        self.task = loop.create_task(run_mock_server())
        inbox.clear()  # entries left by earlier tests
        # wait until the mock server is accepting connections
        for _ in range(100):
            try:
                async with connect(f"ws://localhost:{port}"):
                    break
            except OSError:
                await asyncio.sleep(0.05)
        else:
            self.fail("mock server did not start")
        await self.until(lambda: inbox)  # the connection above
        inbox.clear()

    async def asyncTearDown(self):
        # wait for the server to shut down so the next test can bind the port
        self.task.cancel()
        try:
            await self.task
        except asyncio.CancelledError:
            pass

    async def until(self, condition):
        # wait for condition() to hold, checking every 10 ms for up to 5 s
        for _ in range(500):
            if condition():
                return
            await asyncio.sleep(0.01)
        self.fail("timed out")
