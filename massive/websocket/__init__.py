import os
from collections import Counter
from enum import Enum
from typing import Optional, Union, List, Set, Callable, Awaitable, Any
import logging
import json
import asyncio
import random
import ssl
import threading
import time
from .models import *
from websockets.asyncio.client import connect, ClientConnection
from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedOK,
    ConnectionClosedError,
    InvalidMessage,
    InvalidStatus,
)
from ..certs import ca_bundle_path
from ..logging import get_logger
import logging
from ..exceptions import AuthError

env_key = "MASSIVE_API_KEY"
logger = get_logger("WebSocketClient")

# Status messages that mean things are working. Any other status (e.g. "error",
# "max_connections", "slow_consumer", "force_disconnect") is logged as a warning.
OK_STATUSES = {"connected", "auth_success", "success"}


class WebSocketClient:
    def __init__(
        self,
        api_key: Optional[str] = os.getenv(env_key),
        feed: Union[str, Feed] = Feed.RealTime,
        market: Union[str, Market] = Market.Stocks,
        raw: bool = False,
        verbose: bool = False,
        subscriptions: Optional[List[str]] = None,
        max_reconnects: Optional[int] = None,
        secure: bool = True,
        custom_json: Optional[Any] = None,
        ca_certs: Optional[str] = None,
        reconnect_backoff: float = 15.0,
        max_reconnect_backoff: float = 120.0,
        **kwargs,
    ):
        """
        Initialize a Massive WebSocketClient.

        :param api_key: Your API key.
        :param feed: The feed to subscribe to.
        :param raw: Whether to pass raw Union[str, bytes] to user callback.
        :param verbose: Whether to log client and server status messages.
        :param subscriptions: List of subscription parameters.
        :param max_reconnects: How many consecutive reconnect attempts may fail (the connection is refused, or ends before or after authenticating) before .connect raises the last error. The count starts over whenever a connection authenticates. None, the default, retries forever. Only transient failures are retried and the wait between attempts grows, while other errors (e.g. HTTP 4xx) are raised at once, so with the default .run is safe in a long-lived process.
        :param custom_json: Optional module exposing loads/dumps functions (similar to Python's json module) to be used for JSON conversions.
        :param ca_certs: Path to a CA bundle to verify TLS with. Defaults to the SSL_CERT_FILE or REQUESTS_CA_BUNDLE environment variable, then certifi.
        :param reconnect_backoff: Seconds to wait before reconnecting after the connection ends or an attempt to connect fails. The wait doubles with each consecutive failure and gets up to 20% random jitter, but never leaves [reconnect_backoff, max_reconnect_backoff]. Defaults to 15, the minimum Massive asks clients to wait; 0 disables the wait and is meant only for tests.
        :param max_reconnect_backoff: The longest wait between reconnect attempts, jitter included.
        :raises ValueError: If reconnect_backoff is negative or greater than max_reconnect_backoff.
        :return: A client.
        """
        if reconnect_backoff < 0 or max_reconnect_backoff < reconnect_backoff:
            raise ValueError(
                "need 0 <= reconnect_backoff <= max_reconnect_backoff, got "
                f"{reconnect_backoff} and {max_reconnect_backoff}"
            )
        if api_key is None:
            raise AuthError(
                f"Must specify env var {env_key} or pass api_key in constructor"
            )
        self.api_key = api_key
        self.feed = feed
        if isinstance(market, str):
            self.market = Market(market)  # converts str input to enum
        else:
            self.market = market

        self.market_value = self.market.value
        self.raw = raw
        if verbose:
            logger.setLevel(logging.DEBUG)
        self.websocket_cfg = kwargs
        if isinstance(feed, Enum):
            feed = feed.value
        self.url = f"ws{'s' if secure else ''}://{feed}/{self.market_value}"
        self.subscribed = False
        self.subs: Set[str] = set()
        self.max_reconnects = max_reconnects
        self.websocket: Optional[ClientConnection] = None
        if subscriptions is None:
            subscriptions = []
        self.scheduled_subs: Set[str] = set(subscriptions)
        self.schedule_resub = True
        self._server_errors: Counter = Counter()
        self._server_errors_since = 0.0
        self.ca_certs = ca_certs
        if custom_json:
            self.json = custom_json
        else:
            self.json = json
        # set while connect() runs; close() and request_close() stop it
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._close_requested = threading.Event()
        self._close_event: Optional[asyncio.Event] = None
        self._close_task: Optional[asyncio.Future] = None
        self.reconnect_backoff = reconnect_backoff
        self.max_reconnect_backoff = max_reconnect_backoff

    # https://websockets.readthedocs.io/en/stable/reference/client.html#opening-a-connection
    async def connect(
        self,
        processor: Union[
            Callable[[List[WebSocketMessage]], Awaitable],
            Callable[[Union[str, bytes]], Awaitable],
        ],
        close_timeout: int = 1,
        **kwargs,
    ):
        """
        Connect to websocket server and run `processor(msg)` on every new `msg`.

        When the connection ends, whether the server closed it cleanly or not,
        or an attempt to connect fails for a reason that may pass (the server
        is unreachable, times out, or answers with HTTP 5xx or 429), the
        client waits (see `reconnect_backoff`), reconnects, and subscribes
        again to every scheduled subscription. Other errors, e.g. HTTP 4xx or
        a TLS certificate that cannot be verified, are raised at once. It
        returns once `close()` or `request_close()` is called.

        :param processor: The callback to process messages.
        :param close_timeout: How long to wait for handshake when calling .close.
        :raises AuthError: If invalid API key is supplied.
        :raises Exception: The last error, if more than `max_reconnects`
            consecutive attempts fail; ConnectionClosedError if the server
            closed the last connection, even cleanly.
        """
        self._loop = asyncio.get_running_loop()
        self._close_event = asyncio.Event()
        try:
            await self._connect(processor, close_timeout, **kwargs)
            # it returned because close was requested, which is now done
            self._close_requested.clear()
        finally:
            self._loop = None
            self._close_event = None
            closing, self._close_task = self._close_task, None
            if closing is not None:
                await asyncio.gather(closing, return_exceptions=True)

    async def _connect(
        self,
        processor: Union[
            Callable[[List[WebSocketMessage]], Awaitable],
            Callable[[Union[str, bytes]], Awaitable],
        ],
        close_timeout: int,
        **kwargs,
    ):
        # consecutive failed attempts, which set the backoff and are limited
        # by max_reconnects
        failures = 0
        logger.debug("connect: %s", self.url)
        ssl_context = self._ssl_context()

        # invalid arguments raise here; each attempt awaits it again
        connector = connect(
            self.url, close_timeout=close_timeout, ssl=ssl_context, **kwargs
        )
        last_exc: Optional[Exception] = None
        delay = 0.0
        while not self._close_requested.is_set():
            if failures > 0 and await self._wait_for_close(delay):
                return
            try:
                s = await self._open(connector)
            except Exception as e:
                if not self._is_transient(e):
                    raise
                last_exc = e
                failures += 1
                if self._too_many_failures(failures):
                    break
                delay = self._reconnect_delay(failures - 1)
                logger.warning("connect failed; reconnecting in %.1f s: %s", delay, e)
                continue
            if s is None:
                return
            self.websocket = s
            try:
                msg = await s.recv()
                logger.debug("connected: %s", msg)
                self._log_server_statuses(self.json.loads(msg))
                logger.debug("authing...")
                await s.send(
                    self.json.dumps({"action": "auth", "params": self.api_key})
                )
                auth_msg = await s.recv()
                auth_msg_parsed = self.json.loads(auth_msg)
                logger.debug("authed: %s", auth_msg)
                if auth_msg_parsed[0]["status"] == "auth_failed":
                    raise AuthError(auth_msg_parsed[0]["message"])
                failures = 0
                # e.g. max_connections, which the server sends before closing
                self._log_server_statuses(auth_msg_parsed)
                self._flush_server_errors(force=True)
                while True:
                    if self.schedule_resub:
                        logger.debug(
                            "reconciling: %s %s", self.subs, self.scheduled_subs
                        )
                        new_subs = self.scheduled_subs.difference(self.subs)
                        await self._subscribe(new_subs)
                        old_subs = self.subs.difference(self.scheduled_subs)
                        await self._unsubscribe(old_subs)
                        self.subs = self.scheduled_subs
                        self.subs = set(self.scheduled_subs)
                        self.schedule_resub = False

                    try:
                        cmsg: Union[List[WebSocketMessage], Union[str, bytes]] = (
                            await asyncio.wait_for(s.recv(), timeout=1)
                        )
                    except asyncio.TimeoutError:
                        self._flush_server_errors()
                        continue

                    if not self.raw:
                        # we know cmsg is Data
                        msgJson = self.json.loads(cmsg)  # type: ignore
                        for m in msgJson:
                            if m["ev"] == "status":
                                logger.debug("status: %s", m["message"])
                        self._log_server_statuses(msgJson)
                        cmsg = parse(msgJson, logger, self.market)
                    self._flush_server_errors()

                    if len(cmsg) > 0:
                        await processor(cmsg)  # type: ignore
            except ConnectionClosed as e:
                ok = isinstance(e, ConnectionClosedOK)
                logger.debug("connection closed (%s): %s", "OK" if ok else "ERR", e)
                # the server sends a status (e.g. slow_consumer) before closing
                self._flush_server_errors(force=True)
                if self._close_requested.is_set():
                    return
                # the server can close cleanly too, e.g. after max_connections
                last_exc = e
                failures += 1
                if self._too_many_failures(failures):
                    break
                delay = self._reconnect_delay(failures - 1)
                logger.warning(
                    "connection closed; reconnecting in %.1f s: %s", delay, e
                )
            finally:
                self.websocket = None
                # scheduled_subs is the desired set, including changes not sent
                # yet: keep it, and subscribe all of it on the next connection
                self.subs = set()
                self.schedule_resub = True
                await s.close()

        if last_exc is not None and self._too_many_failures(failures):
            if isinstance(last_exc, ConnectionClosedOK):
                # a clean close is still a failure once connect() gives up
                raise ConnectionClosedError(
                    last_exc.rcvd, last_exc.sent, last_exc.rcvd_then_sent
                ) from last_exc
            raise last_exc

    def _too_many_failures(self, failures: int) -> bool:
        return self.max_reconnects is not None and failures > self.max_reconnects

    @staticmethod
    def _is_transient(exc: Exception) -> bool:
        """Whether a failed attempt to connect is worth retrying."""
        if isinstance(exc, InvalidStatus):
            status = exc.response.status_code
            return status >= 500 or status == 429
        if isinstance(exc, InvalidMessage):
            # e.g. the server closed the connection during the handshake
            return isinstance(exc.__cause__, EOFError)
        if isinstance(exc, ssl.SSLCertVerificationError):
            return False
        # e.g. refused, unreachable, or timed out
        return isinstance(exc, (OSError, asyncio.TimeoutError, ConnectionClosed))

    def _reconnect_delay(self, attempt: int) -> float:
        """
        The wait before reconnect attempt `attempt`, counting from 0.

        The base doubles from `reconnect_backoff` up to `max_reconnect_backoff`;
        jitter only lengthens it, by up to 20%, and is clamped to the cap.
        """
        # bound the exponent so a long outage cannot overflow the float
        base = min(
            self.reconnect_backoff * 2.0 ** min(attempt, 1000),
            self.max_reconnect_backoff,
        )
        return random.uniform(base, min(1.2 * base, self.max_reconnect_backoff))

    async def _open(
        self, connector: Awaitable[ClientConnection]
    ) -> Optional[ClientConnection]:
        """Open a connection, or return None if close is requested first."""
        assert self._close_event is not None
        attempt = asyncio.ensure_future(connector)
        closing = asyncio.ensure_future(self._close_event.wait())
        try:
            await asyncio.wait({attempt, closing}, return_when=asyncio.FIRST_COMPLETED)
        except BaseException:
            attempt.cancel()
            closing.cancel()
            raise
        stop = closing.done() or self._close_requested.is_set()
        closing.cancel()
        if not stop:
            return attempt.result()
        # close was requested: abandon the attempt, or close what it opened
        attempt.cancel()
        await asyncio.wait({attempt})
        if not attempt.cancelled() and attempt.exception() is None:
            await attempt.result().close()
        return None

    async def _wait_for_close(self, timeout: float) -> bool:
        """Wait up to `timeout` seconds; return whether close was requested."""
        assert self._close_event is not None
        try:
            await asyncio.wait_for(self._close_event.wait(), timeout)
        except asyncio.TimeoutError:
            pass
        return self._close_requested.is_set()

    def _ssl_context(self) -> Optional[ssl.SSLContext]:
        # darwin needs some extra <3
        if not self.url.startswith("wss://"):
            return None
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.load_verify_locations(self.ca_certs or ca_bundle_path(logger))
        return context

    def run(
        self,
        handle_msg: Union[
            Callable[[List[WebSocketMessage]], None],
            Callable[[Union[str, bytes]], None],
        ],
        close_timeout: int = 1,
        **kwargs,
    ):
        """
        Connect to websocket server and run `processor(msg)` on every new `msg`. Synchronous version of `.connect`.

        :param processor: The callback to process messages.
        :param close_timeout: How long to wait for handshake when calling .close.
        :raises AuthError: If invalid API key is supplied.
        """

        async def handle_msg_wrapper(msgs):
            handle_msg(msgs)

        asyncio.run(self.connect(handle_msg_wrapper, close_timeout, **kwargs))

    def _log_server_statuses(self, msgs: List[Any]):
        # status messages are dropped by parse(), so problems (e.g. subscriptions
        # the server rejected, or why it disconnected) are logged here. The server
        # can send one error per channel, so identical ones are counted and logged
        # once a second.
        for m in msgs:
            if m.get("ev") != "status" or m.get("status") in OK_STATUSES:
                continue
            if not self._server_errors:
                self._server_errors_since = time.monotonic()
            self._server_errors[(m.get("status"), m.get("message"))] += 1

    def _flush_server_errors(self, force: bool = False):
        if not self._server_errors:
            return
        if not force and time.monotonic() - self._server_errors_since < 1:
            return
        for (status, message), count in self._server_errors.items():
            logger.warning(
                "server %s: %s%s", status, message, f" (x{count})" if count > 1 else ""
            )
        self._server_errors = Counter()

    async def _subscribe(self, topics: Union[List[str], Set[str]]):
        if self.websocket is None or len(topics) == 0:
            return
        subs = ",".join(topics)
        logger.debug("subbing: %s", subs)
        await self.websocket.send(
            self.json.dumps({"action": "subscribe", "params": subs})
        )

    async def _unsubscribe(self, topics: Union[List[str], Set[str]]):
        if self.websocket is None or len(topics) == 0:
            return
        subs = ",".join(topics)
        logger.debug("unsubbing: %s", subs)
        await self.websocket.send(
            self.json.dumps({"action": "unsubscribe", "params": subs})
        )

    @staticmethod
    def _parse_subscription(s: str):
        s = s.strip()
        split = s.split(".", 1)  # Split at the first period
        if len(split) != 2:
            logger.warning("invalid subscription:", s)
            return [None, None]

        return split

    def subscribe(self, *subscriptions: str):
        """
        Subscribe to given subscriptions.

        :param subscriptions: Subscriptions (args)
        """
        for s in subscriptions:
            topic, sym = self._parse_subscription(s)
            if topic == None:
                continue
            logger.debug("sub desired: %s", s)
            self.scheduled_subs.add(s)
            # If user subs to X.*, remove other X.\w+
            if sym == "*":
                for t in list(self.subs):
                    if t.startswith(topic):
                        self.scheduled_subs.discard(t)

        self.schedule_resub = True

    def unsubscribe(self, *subscriptions: str):
        """
        Unsubscribe from given subscriptions.

        :param subscriptions: Subscriptions (args)
        """
        for s in subscriptions:
            topic, sym = self._parse_subscription(s)
            if topic == None:
                continue
            logger.debug("sub undesired: %s", s)
            self.scheduled_subs.discard(s)

            # If user unsubs to X.*, remove other X.\w+
            if sym == "*":
                for t in list(self.subs):
                    if t.startswith(topic):
                        self.scheduled_subs.discard(t)

        self.schedule_resub = True

    def unsubscribe_all(self):
        """
        Unsubscribe from all subscriptions.
        """
        self.scheduled_subs = set()
        self.schedule_resub = True

    def request_close(self):
        """
        Close the websocket connection and make `connect()` return.

        Unlike `close()`, this can be called from any thread, and it does not
        wait: `connect()` returns soon after, including while it is
        connecting or waiting to reconnect. If no `connect()` is running, the
        next one returns without connecting; the request stays pending until a
        `connect()` returns because of it.
        """
        logger.debug("close requested")
        self._close_requested.set()
        loop = self._loop
        if loop is None:
            return
        try:
            loop.call_soon_threadsafe(self._on_close_requested)
        except RuntimeError:
            # the loop is closed, so connect() has returned
            pass

    def _on_close_requested(self):
        # runs on the event loop of connect(); the request may be stale, e.g.
        # queued for an earlier connect() on the same loop
        if not self._close_requested.is_set():
            return
        if self._close_event is not None:
            self._close_event.set()
        if self.websocket is not None:
            self._close_task = asyncio.ensure_future(self.websocket.close())

    async def close(self):
        """
        Close the websocket connection and stop `connect()` from reconnecting.
        """
        logger.debug("closing")
        if self._loop is not None:
            self._close_requested.set()
            if self._close_event is not None:
                self._close_event.set()
        self._flush_server_errors(force=True)

        if self.websocket:
            await self.websocket.close()
            self.websocket = None
        elif self._loop is None:
            # (while connect() runs, it may be between connections)
            logger.warning("no websocket open to close")
