import asyncio
import unittest
from typing import Any

from fake_server import FakeConnection, FakeServer
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed


class FakeServerTest(unittest.IsolatedAsyncioTestCase):
    """The fake server's own behaviour, where tests rely on it."""

    async def hook_closes(self, close: str) -> None:
        async def on_message(conn: FakeConnection, msg: Any):
            if isinstance(msg, dict) and msg.get("action") == "subscribe":
                if close == "drop":
                    await conn.drop()
                else:
                    await conn.close(1011)

        server = await FakeServer(on_message=on_message).start()
        async with connect(f"ws://{server.feed}/stocks") as ws:
            await ws.recv()  # connected
            await ws.send('{"action": "subscribe", "params": "T.A"}')
            with self.assertRaises(ConnectionClosed):
                while True:
                    await asyncio.wait_for(ws.recv(), 5)
        await server.wait_for(lambda: not server.connection.open, 5)
        await asyncio.wait_for(server.stop(), 5)

    async def test_hook_can_close_the_connection(self) -> None:
        await self.hook_closes("close")

    async def test_hook_can_drop_the_connection(self) -> None:
        await self.hook_closes("drop")


if __name__ == "__main__":
    unittest.main()
