"""
A scriptable fake Massive WebSocket server for tests.

Unlike ``mock_server``, which listens on a fixed port and keeps module-level
state, each ``FakeServer`` listens on a free port and records everything per
instance, so tests can assert on exactly what a client sent. Run it inside the
test's event loop::

    server = await FakeServer().start()
    c = WebSocketClient(api_key="key", feed=server.feed, secure=False)
    c.subscribe("T.AAPL")
    task = asyncio.create_task(c.connect(processor))
    await server.wait_for_subscriptions({"T.AAPL"})
    await server.send_data([{"ev": "T", "sym": "AAPL", "p": 1.0}])
    await server.close_connections(code=1011)  # simulate an outage
    ...
    await server.stop()

By default the server speaks the Massive protocol: it sends ``connected``,
answers ``auth`` with ``auth_success``, and acknowledges each subscribed or
unsubscribed topic with a ``success`` status. Scripted replies and hooks
override that behaviour.
"""

import asyncio
import json
import threading
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed
from websockets.http11 import Request, Response


def status(kind: str, message: str) -> Dict[str, Any]:
    """Build a Massive status message, e.g. ``status("max_connections", "...")``."""
    return {"ev": "status", "status": kind, "message": message}


@dataclass
class AuthReply:
    """
    A scripted reply to one ``auth`` message.

    :param frames: The messages to send, as a JSON array.
    :param close_code: If set, close the connection with this code afterwards.
    :param close_reason: The reason sent with the close frame.
    """

    frames: List[Dict[str, Any]]
    close_code: Optional[int] = None
    close_reason: str = ""


class FakeConnection:
    """
    One client connection accepted by a ``FakeServer``.

    Like the server, it belongs to the server's event loop: read its attributes
    and call its methods only from that loop.
    """

    def __init__(self, websocket: ServerConnection):
        self.websocket = websocket
        self.api_key: Optional[str] = None
        self.authenticated = False
        # every message the client sent, decoded from JSON
        self.inbox: List[Any] = []
        # topics the client is subscribed to on this connection
        self.subscriptions: Set[str] = set()
        self.closed = asyncio.Event()
        # the task running the server's handler for this connection
        self.handler: Optional["asyncio.Task[Any]"] = None

    @property
    def open(self) -> bool:
        return not self.closed.is_set()

    async def send(self, frames: List[Dict[str, Any]]):
        """Send a JSON array of messages to this client."""
        await self.websocket.send(json.dumps(frames))

    async def close(self, code: int = 1000, reason: str = ""):
        """Close with a close frame; 1000 is a clean close, e.g. 1011 is not."""
        await self.websocket.close(code, reason)
        await self._wait_closed()

    async def drop(self):
        """Drop the TCP connection without a close frame."""
        transport = self.websocket.transport
        if transport is not None:
            transport.abort()
        await self._wait_closed()

    async def _wait_closed(self):
        # The handler sets closed when it returns. Called from the handler
        # itself, e.g. from an on_message hook, waiting would never end: the
        # handler returns once the hook does, as the connection is closed.
        if asyncio.current_task() is not self.handler:
            await self.closed.wait()


class FakeServer:
    """
    A fake Massive WebSocket server listening on ``host`` and a free port.

    Threading model: the server and all its state (``connections``, each
    connection's ``inbox`` and ``subscriptions``, the scripted replies) live on
    the event loop that called ``start()``, and are only read or changed there.
    Clients under test may run on other threads with their own event loops; they
    talk to the server only over the socket. The state accessors raise
    ``RuntimeError`` when called from another thread, and return copies, so a
    test never holds a collection the server is changing.

    :param host: The interface to listen on.
    :param ack_subscriptions: Whether to acknowledge subscribe/unsubscribe.
    :param on_message: Called on the server's loop with every decoded client
        message after it has been recorded and handled; use it to script replies.
        It may close or drop the connection.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        ack_subscriptions: bool = True,
        on_message: Optional[Callable[[FakeConnection, Any], Awaitable[None]]] = None,
    ):
        self.host = host
        self.port = 0
        self.ack_subscriptions = ack_subscriptions
        self.on_message = on_message
        # while True, new connections get an HTTP 503 instead of a handshake
        self.reject_handshakes = False
        self._connections: List[FakeConnection] = []
        self._auth_replies: List[AuthReply] = []
        self._server: Optional[Server] = None
        self._thread: Optional[int] = None
        self._changed: Optional[asyncio.Condition] = None

    # lifecycle

    async def start(self) -> "FakeServer":
        self._thread = threading.get_ident()
        self._changed = asyncio.Condition()
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

    @property
    def feed(self) -> str:
        """The ``feed`` to pass to ``WebSocketClient`` (with ``secure=False``)."""
        return f"{self.host}:{self.port}"

    def _check_thread(self):
        if self._thread is not None and threading.get_ident() != self._thread:
            raise RuntimeError(
                "FakeServer state must be used from the thread running its event loop"
            )

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
        ``script_auth(status("max_connections", "..."), close_code=1008)``. A
        reply without ``auth_success`` leaves the connection unauthenticated,
        and one without frames sends nothing.
        """
        self._check_thread()
        self._auth_replies.append(AuthReply(list(frames), close_code, close_reason))

    @property
    def connections(self) -> List[FakeConnection]:
        """Every connection accepted so far, oldest first."""
        self._check_thread()
        return list(self._connections)

    @property
    def connection(self) -> FakeConnection:
        """The most recent connection."""
        self._check_thread()
        if not self._connections:
            raise AssertionError("no client has connected")
        return self._connections[-1]

    @property
    def open_connections(self) -> List[FakeConnection]:
        self._check_thread()
        return [c for c in self._connections if c.open]

    @property
    def inbox(self) -> List[Any]:
        """Every message received, from all connections, in order per connection."""
        self._check_thread()
        return [m for c in self._connections for m in c.inbox]

    @property
    def subscriptions(self) -> Set[str]:
        """The topics subscribed on the most recent connection."""
        self._check_thread()
        return set(self._connections[-1].subscriptions) if self._connections else set()

    async def send_data(self, events: List[Dict[str, Any]]):
        """Send data events, e.g. ``[{"ev": "T", "sym": "AAPL", ...}]``, to every
        open, authenticated connection."""
        for c in self.open_connections:
            if c.authenticated:
                await c.send(events)

    async def close_connections(self, code: int = 1000, reason: str = ""):
        """Close every open connection with ``code``."""
        await asyncio.gather(*(c.close(code, reason) for c in self.open_connections))

    async def drop_connections(self):
        """Drop every open connection without a close frame."""
        await asyncio.gather(*(c.drop() for c in self.open_connections))

    # waiting

    async def wait_for(
        self,
        predicate: Callable[[], bool],
        timeout: float = 5,
        description: str = "condition",
    ):
        """
        Wait until ``predicate()`` is true.

        The predicate is checked now and after every change to the server's
        state, so it must depend only on that state. On timeout this fails with
        an ``AssertionError`` naming ``description`` and the server's state.
        """
        self._check_thread()
        if self._changed is None:
            raise RuntimeError("FakeServer is not started")
        changed = self._changed

        async def wait():
            async with changed:
                await changed.wait_for(predicate)

        try:
            await asyncio.wait_for(wait(), timeout)
        except asyncio.TimeoutError:
            raise AssertionError(
                f"timed out after {timeout}s waiting for {description}; {self._state()}"
            ) from None

    async def wait_for_connections(self, n: int, timeout: float = 5):
        """Wait until ``n`` clients have authenticated, in total."""
        await self.wait_for(
            lambda: sum(c.authenticated for c in self._connections) >= n,
            timeout,
            f"{n} authenticated connections",
        )

    async def wait_for_subscriptions(self, topics: Set[str], timeout: float = 5):
        """Wait until the most recent connection is subscribed to exactly ``topics``."""
        await self.wait_for(
            lambda: bool(self._connections) and self.subscriptions == set(topics),
            timeout,
            f"subscriptions {sorted(topics)}",
        )

    async def wait_for_message(
        self, predicate: Callable[[Any], bool], timeout: float = 5
    ) -> Any:
        """Wait for a client message matching ``predicate`` and return it."""
        found: List[Any] = []

        def check() -> bool:
            found[:] = [m for m in self.inbox if predicate(m)]
            return bool(found)

        await self.wait_for(check, timeout, "a matching client message")
        return found[0]

    def _state(self) -> str:
        return (
            f"connections={len(self._connections)} "
            f"open={len(self.open_connections)} "
            f"subscriptions={sorted(self.subscriptions)} "
            f"last_messages={self.inbox[-5:]}"
        )

    async def _notify(self):
        if self._changed is not None:
            async with self._changed:
                self._changed.notify_all()

    # protocol

    def _process_request(
        self, websocket: ServerConnection, request: Request
    ) -> Optional[Response]:
        if self.reject_handshakes:
            return websocket.respond(HTTPStatus.SERVICE_UNAVAILABLE, "unavailable\n")
        return None

    async def _handler(self, websocket: ServerConnection):
        conn = FakeConnection(websocket)
        conn.handler = asyncio.current_task()
        self._connections.append(conn)
        await self._notify()
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
                await self._notify()
        except ConnectionClosed:
            pass
        finally:
            conn.closed.set()
            await self._notify()

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
        conn.authenticated = True
        await conn.send([status("auth_success", "authenticated")])

    async def _subscription(self, conn: FakeConnection, action: str, topics: List[str]):
        replies = []
        for t in topics:
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
