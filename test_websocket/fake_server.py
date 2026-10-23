"""
A scriptable fake Massive WebSocket server for tests.

Unlike ``mock_server``, which listens on a fixed port and keeps module-level
state, each ``FakeServer`` listens on a free port and records everything per
instance, so tests can run several servers and assert on exactly what a client
sent. Run it inside the test's event loop::

    async with FakeServer() as server:
        c = WebSocketClient(api_key="key", feed=server.feed, secure=False)
        c.subscribe("T.AAPL")
        task = asyncio.create_task(c.connect(processor))
        await server.wait_for_subscriptions({"T.AAPL"})
        await server.send_data([{"ev": "T", "sym": "AAPL", "p": 1.0}])
        await server.close_connections(code=1011)  # simulate an outage

By default the server speaks the Massive protocol: it sends ``connected``,
answers ``auth`` with ``auth_success`` (or ``auth_failed`` for a key in
``reject_keys``), and acknowledges each subscribed or unsubscribed topic with a
``success`` status. Scripted replies and hooks override that behaviour.
"""

import asyncio
import json
from http import HTTPStatus
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
)

from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Request, Response


def status(status: str, message: str) -> Dict[str, Any]:
    """Build a Massive status message, e.g. ``status("max_connections", "...")``."""
    return {"ev": "status", "status": status, "message": message}


class AuthReply:
    """
    A scripted reply to one ``auth`` message.

    :param frames: The messages to send, as a JSON array.
    :param close_code: If set, close the connection with this code afterwards.
    :param close_reason: The reason sent with the close frame.
    """

    def __init__(
        self,
        frames: List[Dict[str, Any]],
        close_code: Optional[int] = None,
        close_reason: str = "",
    ):
        self.frames = frames
        self.close_code = close_code
        self.close_reason = close_reason


class FakeConnection:
    """One client connection accepted by a ``FakeServer``."""

    def __init__(self, server: "FakeServer", websocket: ServerConnection, index: int):
        self.server = server
        self.websocket = websocket
        self.index = index
        self.path = websocket.request.path if websocket.request else ""
        self.api_key: Optional[str] = None
        self.authenticated = False
        # every message the client sent, decoded from JSON
        self.inbox: List[Any] = []
        # topics the client is subscribed to on this connection
        self.subscriptions: Set[str] = set()
        self.closed = asyncio.Event()

    @property
    def open(self) -> bool:
        return not self.closed.is_set()

    async def send(self, frames: List[Dict[str, Any]]):
        """Send a JSON array of messages to this client."""
        await self.websocket.send(json.dumps(frames))

    async def send_raw(self, message: str):
        """Send a message as-is."""
        await self.websocket.send(message)

    async def close(self, code: int = 1000, reason: str = ""):
        """Close with a close frame; 1000 is a clean close, e.g. 1011 is not."""
        await self.websocket.close(code, reason)
        await self.closed.wait()

    async def drop(self):
        """Drop the TCP connection without a close frame."""
        transport = self.websocket.transport
        if transport is not None:
            transport.abort()
        await self.closed.wait()


class FakeServer:
    """
    A fake Massive WebSocket server listening on ``host`` and a free port.

    :param host: The interface to listen on.
    :param valid_keys: If set, only these API keys authenticate.
    :param reject_keys: API keys answered with ``auth_failed``.
    :param ack_subscriptions: Whether to acknowledge subscribe/unsubscribe.
    :param reject_topics: Topics answered with an ``error`` status instead of
        being subscribed.
    :param on_message: Called with every decoded client message after it has
        been recorded and handled; use it to script replies.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        valid_keys: Optional[Iterable[str]] = None,
        reject_keys: Iterable[str] = (),
        ack_subscriptions: bool = True,
        reject_topics: Iterable[str] = (),
        on_message: Optional[Callable[[FakeConnection, Any], Awaitable[None]]] = None,
    ):
        self.host = host
        self.port = 0
        self.valid_keys = set(valid_keys) if valid_keys is not None else None
        self.reject_keys = set(reject_keys)
        self.ack_subscriptions = ack_subscriptions
        self.reject_topics = set(reject_topics)
        self.on_message = on_message
        # while True, new connections get an HTTP 503 instead of a handshake
        self.reject_handshakes = False
        self.connections: List[FakeConnection] = []
        self._auth_replies: List[AuthReply] = []
        self._server: Optional[Server] = None

    # lifecycle

    async def start(self) -> "FakeServer":
        self._server = await serve(
            self._handler,
            self.host,
            self.port,
            process_request=self._process_request,
        )
        self.port = next(iter(self._server.sockets)).getsockname()[1]
        return self

    async def stop(self):
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def __aenter__(self) -> "FakeServer":
        return await self.start()

    async def __aexit__(self, *exc):
        await self.stop()

    @property
    def feed(self) -> str:
        """The ``feed`` to pass to ``WebSocketClient`` (with ``secure=False``)."""
        return f"{self.host}:{self.port}"

    # scripting

    def script_auth(
        self,
        *frames: Dict[str, Any],
        close_code: Optional[int] = None,
        close_reason: str = "",
    ):
        """
        Queue the reply to the next ``auth`` message, replacing the default.

        Replies are used in order, one per connection, e.g.
        ``script_auth(status("max_connections", "..."), close_code=1008)``.
        """
        self._auth_replies.append(AuthReply(list(frames), close_code, close_reason))

    @property
    def connection(self) -> FakeConnection:
        """The most recent connection."""
        if not self.connections:
            raise AssertionError("no client has connected")
        return self.connections[-1]

    @property
    def open_connections(self) -> List[FakeConnection]:
        return [c for c in self.connections if c.open]

    @property
    def inbox(self) -> List[Any]:
        """Every message received, from all connections, in order per connection."""
        return [m for c in self.connections for m in c.inbox]

    @property
    def subscriptions(self) -> Set[str]:
        """The topics subscribed on the most recent connection."""
        return set(self.connections[-1].subscriptions) if self.connections else set()

    async def send(self, frames: List[Dict[str, Any]]):
        """Send messages to every open, authenticated connection."""
        for c in self.open_connections:
            if c.authenticated:
                await c.send(frames)

    async def send_status(self, status_: str, message: str):
        await self.send([status(status_, message)])

    async def send_data(self, events: List[Dict[str, Any]]):
        """Send data events, e.g. ``[{"ev": "T", "sym": "AAPL", ...}]``."""
        await self.send(events)

    async def close_connections(self, code: int = 1000, reason: str = ""):
        """Close every open connection with ``code``."""
        await asyncio.gather(*(c.close(code, reason) for c in self.open_connections))

    async def drop_connections(self):
        """Drop every open connection without a close frame."""
        await asyncio.gather(*(c.drop() for c in self.open_connections))

    # waiting

    async def wait_for(self, predicate: Callable[[], bool], timeout: float = 5):
        """Wait until ``predicate()`` is true, polling every 10 ms."""

        async def wait():
            while not predicate():
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait(), timeout)

    async def wait_for_connections(self, n: int, timeout: float = 5):
        """Wait until ``n`` clients have authenticated, in total."""
        await self.wait_for(
            lambda: sum(c.authenticated for c in self.connections) >= n, timeout
        )

    async def wait_for_subscriptions(self, topics: Set[str], timeout: float = 5):
        """Wait until the most recent connection is subscribed to exactly ``topics``."""
        await self.wait_for(
            lambda: bool(self.connections) and self.subscriptions == set(topics),
            timeout,
        )

    async def wait_for_message(
        self, predicate: Callable[[Any], bool], timeout: float = 5
    ) -> Any:
        """Wait for a client message matching ``predicate`` and return it."""
        found: List[Any] = []

        def check() -> bool:
            found[:] = [m for m in self.inbox if predicate(m)]
            return bool(found)

        await self.wait_for(check, timeout)
        return found[0]

    # protocol

    def _process_request(
        self, websocket: ServerConnection, request: Request
    ) -> Optional[Response]:
        if self.reject_handshakes:
            return websocket.respond(HTTPStatus.SERVICE_UNAVAILABLE, "unavailable\n")
        return None

    async def _handler(self, websocket: ServerConnection):
        conn = FakeConnection(self, websocket, len(self.connections))
        self.connections.append(conn)
        try:
            await conn.send([status("connected", "Connected Successfully")])
            async for raw in websocket:
                try:
                    msg = json.loads(raw)
                except ValueError:
                    msg = raw
                conn.inbox.append(msg)
                if isinstance(msg, dict):
                    await self._handle(conn, msg)
                if self.on_message is not None:
                    await self.on_message(conn, msg)
        except ConnectionClosed:
            pass
        finally:
            conn.closed.set()

    async def _handle(self, conn: FakeConnection, msg: Dict[str, Any]):
        action = msg.get("action")
        params = msg.get("params")
        if action == "auth":
            await self._auth(conn, params)
        elif action in ("subscribe", "unsubscribe") and isinstance(params, str):
            topics = [p.strip() for p in params.split(",") if p.strip()]
            await self._subscription(conn, action, topics)

    async def _auth(self, conn: FakeConnection, key: Optional[str]):
        conn.api_key = key
        if self._auth_replies:
            reply = self._auth_replies.pop(0)
            conn.authenticated = any(
                f.get("status") == "auth_success" for f in reply.frames
            )
            if reply.frames:
                await conn.send(reply.frames)
            if reply.close_code is not None:
                await conn.websocket.close(reply.close_code, reply.close_reason)
            return
        if key in self.reject_keys or (
            self.valid_keys is not None and key not in self.valid_keys
        ):
            await conn.send([status("auth_failed", "authentication failed")])
            await conn.websocket.close(1008, "authentication failed")
            return
        conn.authenticated = True
        await conn.send([status("auth_success", "authenticated")])

    async def _subscription(self, conn: FakeConnection, action: str, topics: List[str]):
        replies = []
        for t in topics:
            if action == "subscribe" and t in self.reject_topics:
                replies.append(status("error", f"subscription rejected: {t}"))
                continue
            if action == "subscribe":
                conn.subscriptions.add(t)
                ack = f"subscribed to: {t}"
            else:
                conn.subscriptions.discard(t)
                ack = f"unsubscribed to: {t}"
            if self.ack_subscriptions:
                replies.append(status("success", ack))
        if replies:
            await conn.send(replies)
