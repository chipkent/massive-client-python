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
from websockets.asyncio.client import connect, ClientConnection, process_exception
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError
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
        max_reconnects: Optional[int] = 5,
        secure: bool = True,
        custom_json: Optional[Any] = None,
        ca_certs: Optional[str] = None,
        reconnect_backoff: float = 0.0,
        max_reconnect_backoff: float = 120.0,
        reconnect_on_close: bool = False,
        max_consecutive_failures: Optional[int] = None,
        stable_uptime: float = 60.0,
        raise_fatal_errors: bool = False,
        **kwargs,
    ):
        """
        Initialize a Massive WebSocketClient.

        :param api_key: Your API key.
        :param feed: The feed to subscribe to.
        :param raw: Whether to pass raw Union[str, bytes] to user callback.
        :param verbose: Whether to log client and server status messages.
        :param subscriptions: List of subscription parameters.
        :param max_reconnects: How many times to reconnect on network outage before ending .connect event loop.
        :param custom_json: Optional module exposing loads/dumps functions (similar to Python's json module) to be used for JSON conversions.
        :param ca_certs: Path to a CA bundle to verify TLS with. Defaults to the SSL_CERT_FILE or REQUESTS_CA_BUNDLE environment variable, then certifi.
        :param reconnect_backoff: Seconds to wait before reconnecting after an open connection closes. The wait doubles with each consecutive failure, up to max_reconnect_backoff, with random jitter. 0 (the default) reconnects at once.
        :param max_reconnect_backoff: The longest wait before reconnecting, in seconds.
        :param reconnect_on_close: Also reconnect when the server closes the connection normally (e.g. code 1000), counting toward max_reconnects. By default, .connect returns then.
        :param max_consecutive_failures: How many failures in a row .connect allows before it raises the last error. A failure is a connection attempt that fails, or a connection that closes before it has been open for stable_uptime seconds. None (the default) allows any number. max_reconnects applies too.
        :param stable_uptime: Seconds a connection must stay open to reset the failure count and the backoff.
        :param raise_fatal_errors: Raise connection errors that retrying will not fix, such as an HTTP 404 during the handshake, instead of retrying them forever. Uses websockets' default process_exception.
        :raises ValueError: If reconnect_backoff is negative or greater than max_reconnect_backoff.
        :return: A client.
        """
        if not 0 <= reconnect_backoff <= max_reconnect_backoff:
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
        self.reconnect_backoff = reconnect_backoff
        self.max_reconnect_backoff = max_reconnect_backoff
        self.reconnect_on_close = reconnect_on_close
        self.max_consecutive_failures = max_consecutive_failures
        self.stable_uptime = stable_uptime
        self.raise_fatal_errors = raise_fatal_errors
        # whether the server sent a max_connections status during the last
        # handshake: the account has too many connections open
        self.max_connections_reached = False
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
        # set by request_close(), and by close() while connect() runs;
        # connect() returns instead of reconnecting
        self._close_requested = threading.Event()
        # the event loop connect() runs on, while it runs
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._closing: Optional[asyncio.Future] = None

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

        :param processor: The callback to process messages.
        :param close_timeout: How long to wait for handshake when calling .close.
        :param kwargs: Passed to websockets' connect(), e.g. process_exception.
        :raises AuthError: If invalid API key is supplied.
        """
        self._loop = asyncio.get_running_loop()
        try:
            await self._connect(processor, close_timeout, **kwargs)
        finally:
            self._loop = None
            self._close_requested.clear()

    async def _connect(
        self,
        processor: Union[
            Callable[[List[WebSocketMessage]], Awaitable],
            Callable[[Union[str, bytes]], Awaitable],
        ],
        close_timeout: int,
        **kwargs,
    ):
        if self._close_requested.is_set():
            return
        # a new connection has no subscriptions, whatever an earlier call left
        self.subs = set()
        self.schedule_resub = True
        reconnects = 0
        # failed attempts and short-lived connections in a row
        failures = 0
        logger.debug("connect: %s", self.url)
        ssl_context = self._ssl_context()

        retry_or_raise = kwargs.pop(
            "process_exception",
            process_exception if self.raise_fatal_errors else lambda exc: None,
        )

        def on_connect_error(exc: Exception) -> Optional[Exception]:
            nonlocal failures
            failures += 1
            if self._too_many(failures, self.max_consecutive_failures):
                return exc
            return retry_or_raise(exc)

        last_exc: Optional[Exception] = None
        async for s in connect(
            self.url,
            close_timeout=close_timeout,
            ssl=ssl_context,
            process_exception=on_connect_error,
            **kwargs,
        ):
            if self._close_requested.is_set():
                await s.close()  # requested while connecting
                return
            self.websocket = s
            connected_at = None
            try:
                msg = await s.recv()
                logger.debug("connected: %s", msg)
                msg_parsed = self.json.loads(msg)
                self._log_server_statuses(msg_parsed)
                logger.debug("authing...")
                await s.send(
                    self.json.dumps({"action": "auth", "params": self.api_key})
                )
                auth_msg = await s.recv()
                auth_msg_parsed = self.json.loads(auth_msg)
                logger.debug("authed: %s", auth_msg)
                if auth_msg_parsed[0]["status"] == "auth_failed":
                    raise AuthError(auth_msg_parsed[0]["message"])
                self.max_connections_reached = any(
                    m.get("status") == "max_connections"
                    for m in msg_parsed + auth_msg_parsed
                )
                connected_at = time.monotonic()
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
            except ConnectionClosedOK as e:
                logger.debug("connection closed (OK): %s", e)
                # the server sends a status (e.g. slow_consumer) before closing
                self._flush_server_errors(force=True)
                # rcvd_then_sent: the server closed it, rather than close()
                if not (self.reconnect_on_close and e.rcvd_then_sent):
                    return
                last_exc = e
            except ConnectionClosedError as e:
                last_exc = e
                logger.debug("connection closed (ERR): %s", e)
                self._flush_server_errors(force=True)
            if self._close_requested.is_set():
                return
            reconnects += 1
            # keep scheduled_subs, which may hold changes not sent yet,
            # and subscribe to all of it on the next connection
            self.subs = set()
            self.schedule_resub = True
            if (
                connected_at is not None
                and time.monotonic() - connected_at >= self.stable_uptime
            ):
                failures = 0
            else:
                failures += 1
            if self._too_many(reconnects, self.max_reconnects) or self._too_many(
                failures, self.max_consecutive_failures
            ):
                break
            if self.reconnect_backoff > 0:
                delay = self._reconnect_delay(failures)
                logger.debug("reconnecting in %.1f s", delay)
                await asyncio.sleep(delay)
                if self._close_requested.is_set():
                    return  # requested during the wait

        if last_exc is not None:
            raise last_exc

    @staticmethod
    def _too_many(count: int, limit: Optional[int]) -> bool:
        return limit is not None and count > limit

    def _reconnect_delay(self, failures: int) -> float:
        # doubles with each failure in a row, up to the cap; jitter adds up
        # to 20%, and at the cap draws from [max(base, cap / 1.2), cap] instead
        base, cap = self.reconnect_backoff, self.max_reconnect_backoff
        delay = min(base * 2.0 ** min(max(failures - 1, 0), 64), cap)
        if delay < cap:
            return random.uniform(delay, min(delay * 1.2, cap))
        return random.uniform(max(base, cap / 1.2), cap)

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

    async def close(self):
        """
        Close the websocket connection. If ``connect()`` is running, it
        returns instead of reconnecting, also during a wait before
        reconnecting.
        """
        logger.debug("closing")
        if self._loop is not None:
            self._close_requested.set()  # connect() clears it when it returns
        self._flush_server_errors(force=True)

        if self.websocket:
            await self.websocket.close()
            self.websocket = None
        else:
            logger.warning("no websocket open to close")

    def request_close(self):
        """
        Close the websocket connection and make ``connect()`` return instead
        of reconnecting. Unlike ``close()``, this is safe to call from any
        thread, e.g. to stop ``run()``, and it does not wait.

        An attempt to connect, or a wait before reconnecting, is not
        interrupted: ``connect()`` returns once it ends. If ``connect()`` is
        not running, the next call returns at once.
        """
        logger.debug("close requested")
        self._close_requested.set()
        loop = self._loop
        if loop is None:
            return
        try:
            loop.call_soon_threadsafe(self._close_soon)
        except RuntimeError:
            pass  # the loop is closed, so connect() has returned

    def _close_soon(self):
        # on connect()'s loop; recv() then raises and connect() returns
        if self.websocket is not None:
            self._closing = asyncio.ensure_future(self.websocket.close())
