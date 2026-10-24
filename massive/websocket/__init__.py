import os
from collections import Counter
from enum import Enum
from typing import (
    Optional,
    Union,
    List,
    Set,
    Callable,
    Awaitable,
    Any,
    Iterable,
    Tuple,
)
import logging
import json
import asyncio
import random
import ssl
import sys
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
        :param max_reconnects: How many consecutive reconnect attempts may fail
            (the connection is refused, or ends before or after authenticating)
            before .connect raises the last error. The count starts over whenever
            a connection authenticates. None, the default, retries forever. Only
            transient failures are retried and the wait between attempts grows,
            while other errors (e.g. HTTP 4xx) are raised at once, so with the
            default .run is safe in a long-lived process.
        :param custom_json: Optional module exposing loads/dumps functions (similar to Python's json module) to be used for JSON conversions.
        :param ca_certs: Path to a CA bundle to verify TLS with. Defaults to the SSL_CERT_FILE or REQUESTS_CA_BUNDLE environment variable, then certifi.
        :param reconnect_backoff: Seconds to wait before reconnecting after the
            connection ends or an attempt to connect fails. The wait doubles with
            each consecutive failure and gets up to 20% random jitter, but never
            leaves [reconnect_backoff, max_reconnect_backoff]. Defaults to 15, the
            minimum Massive asks clients to wait; 0 disables the wait and is meant
            only for tests.
        :param max_reconnect_backoff: The longest wait between reconnect
            attempts, jitter included.
        :raises ValueError: If reconnect_backoff is negative or greater than
            max_reconnect_backoff.
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
        # guards scheduled_subs and schedule_resub, which subscribe() and
        # friends change on the caller's thread while connect() reads them on
        # the event loop's thread
        self._subs_lock = threading.Lock()
        # topics to subscribe to again even if already subscribed
        self._resend: Set[str] = set()
        # wake-up of connect() when subscriptions change: the loop it runs
        # on, whether a wake-up is queued there, whether one arrived since the
        # last reconciliation, and how to interrupt the recv() in progress
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._wake_pending = False
        self._woken = False
        self._recv_timeout: Optional[Any] = None  # asyncio.Timeout
        self._recv_woken: Optional["asyncio.Future[None]"] = None  # Python 3.10
        # how long connect() waits for a message before doing housekeeping
        self._recv_interval = 1.0
        self._server_errors: Counter = Counter()
        self._server_errors_since = 0.0
        self.ca_certs = ca_certs
        if custom_json:
            self.json = custom_json
        else:
            self.json = json
        # close() and request_close() stop connect()
        self._close_requested = threading.Event()
        self._close_event: Optional[asyncio.Event] = None
        self._close_task: Optional[asyncio.Future] = None
        # whether the server said max_connections during the last handshake
        self.max_connections_reached = False
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
        or an attempt to connect fails for a reason that may pass, the client
        waits (see `reconnect_backoff`), reconnects, and subscribes again to
        every scheduled subscription. Such reasons are network errors (refused,
        unreachable, timed out, a DNS lookup failure, or a TLS error other
        than certificate verification), the connection closing during the
        handshake, and HTTP 408, 429 or 5xx. Other errors, e.g. any other HTTP
        4xx or a TLS certificate that cannot be verified, are raised at once.
        It returns once `close()` or `request_close()` is called.

        If the server sends a `max_connections` status during the handshake
        (the account has too many connections open), `max_connections_reached`
        is set until a later handshake succeeds, and in raw mode the message is
        also passed to `processor`. The server then closes the connection, and
        the client reconnects after the backoff.

        :param processor: The callback to process messages.
        :param close_timeout: How long to wait for handshake when calling .close.
        :raises AuthError: If invalid API key is supplied.
        :raises Exception: The last error, if more than `max_reconnects`
            consecutive attempts fail; ConnectionClosedError if the server
            closed the last connection, even cleanly.
        """
        self._woken = False
        with self._subs_lock:
            self._wake_pending = False
        self._loop = asyncio.get_running_loop()
        self._close_event = asyncio.Event()
        try:
            await self._connect(processor, close_timeout, **kwargs)
            # it returned because close was requested, which is now done
            self._close_requested.clear()
        finally:
            # subscription changes made from now on must not wake this loop
            self._loop = None
            with self._subs_lock:
                self._wake_pending = False
            self._woken = False
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
                self.max_connections_reached = False
                for frame, parsed in ((msg, msg_parsed), (auth_msg, auth_msg_parsed)):
                    if any(m.get("status") == "max_connections" for m in parsed):
                        self.max_connections_reached = True
                        if self.raw:
                            await processor(frame)  # type: ignore
                if not self.max_connections_reached:
                    failures = 0
                # e.g. max_connections, which the server sends before closing
                self._log_server_statuses(auth_msg_parsed)
                self._flush_server_errors(force=True)
                while True:
                    await self._reconcile()

                    cmsg: Optional[Union[List[WebSocketMessage], Union[str, bytes]]] = (
                        await self._recv(s, timeout=self._recv_interval)
                    )
                    if cmsg is None:
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
                with self._subs_lock:
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
            return status >= 500 or status in (408, 429)
        if isinstance(exc, InvalidMessage):
            # e.g. the server closed the connection during the handshake
            return isinstance(exc.__cause__, EOFError)
        if isinstance(exc, ssl.SSLCertVerificationError):
            return False
        # e.g. refused, unreachable, timed out, DNS, or another TLS error
        return isinstance(exc, (OSError, asyncio.TimeoutError))

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
            # e.g. connect() was cancelled: don't leave a connection open
            closing.cancel()
            await asyncio.shield(self._abandon(attempt))
            raise
        stop = closing.done() or self._close_requested.is_set()
        closing.cancel()
        if not stop:
            return attempt.result()
        await self._abandon(attempt)
        return None

    @staticmethod
    async def _abandon(
        attempt: "asyncio.Future[ClientConnection]", timeout: float = 1.0
    ):
        """Cancel an attempt to connect, or close the connection it opened."""
        attempt.cancel()
        await asyncio.wait({attempt}, timeout=timeout)
        if not attempt.done():
            # it has not taken the cancellation yet (e.g. a pending connect on
            # Windows): don't wait for it, but close whatever it opens
            attempt.add_done_callback(WebSocketClient._close_opened)
            return
        if WebSocketClient._opened(attempt):
            await attempt.result().close()

    # tasks closing connections that abandoned attempts opened late
    _late_closes: Set["asyncio.Future[None]"] = set()

    @staticmethod
    def _opened(attempt: "asyncio.Future[ClientConnection]") -> bool:
        return not attempt.cancelled() and attempt.exception() is None

    @staticmethod
    def _close_opened(attempt: "asyncio.Future[ClientConnection]"):
        if WebSocketClient._opened(attempt):
            task = asyncio.ensure_future(attempt.result().close())
            WebSocketClient._late_closes.add(task)
            task.add_done_callback(WebSocketClient._late_closes.discard)

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

    async def _recv(
        self, s: ClientConnection, timeout: float
    ) -> Optional[Union[str, bytes]]:
        """
        Receive the next message, or return None after ``timeout`` seconds or
        as soon as subscriptions change, whichever comes first.

        recv() is cancellation-safe: a message it has not returned yet is
        returned by the next call, so interrupting it loses nothing.
        """
        if self._woken:
            return None
        if sys.version_info >= (3, 11):
            # _wake_up() moves the deadline to now; no extra task per message
            try:
                async with asyncio.timeout(timeout) as self._recv_timeout:
                    return await s.recv()
            except TimeoutError:
                return None
            finally:
                self._recv_timeout = None
        woken: "asyncio.Future[None]" = asyncio.get_running_loop().create_future()
        self._recv_woken = woken
        recv = asyncio.ensure_future(s.recv())
        # asyncio.wait() takes futures of one type
        waiters: Set["asyncio.Future[Any]"] = {recv, woken}
        try:
            await asyncio.wait(
                waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED
            )
        finally:
            self._recv_woken = None
            if not recv.done():
                recv.cancel()
                # recv() must not be called again until this one has finished
                await asyncio.wait({recv})
        if recv.cancelled():
            return None
        return recv.result()

    def _notify(self):
        """
        Wake ``connect()`` so it applies scheduled changes now rather than
        after its ``recv`` timeout. Safe to call from any thread.
        """
        loop = self._loop
        if loop is None:
            return
        with self._subs_lock:
            # one queued wake-up covers every change made before it runs
            if self._wake_pending:
                return
            self._wake_pending = True
        try:
            loop.call_soon_threadsafe(self._wake_up)
        except RuntimeError:
            # the loop is closed: connect() is no longer running
            with self._subs_lock:
                self._wake_pending = False

    def _wake_up(self):
        # runs on the loop, before it reads the changes it is woken for
        with self._subs_lock:
            self._wake_pending = False
        self._woken = True
        timeout = self._recv_timeout
        # an expiring timeout has already cancelled recv(), and rescheduling
        # it would raise
        if timeout is not None and not timeout.expired():
            timeout.reschedule(asyncio.get_running_loop().time())
        if self._recv_woken is not None and not self._recv_woken.done():
            self._recv_woken.set_result(None)

    async def _reconcile(self):
        """
        Send the frames that move the server from ``subs`` to ``scheduled_subs``.

        Runs on the event loop's thread. The scheduled set is copied under the
        lock, so ``subscribe()`` and ``unsubscribe()`` may run concurrently on
        other threads; anything they schedule meanwhile is applied on the next
        call.
        """
        # clear before reading, so a change made after the copy wakes the loop
        self._woken = False
        with self._subs_lock:
            if not self.schedule_resub:
                return
            desired = set(self.scheduled_subs)
            resend = self._resend
            self._resend = set()
            self.schedule_resub = False
        logger.debug("reconciling: %s %s", self.subs, desired)
        try:
            # unsubscribe first, so a client at the server's subscription limit
            # can swap topics
            await self._unsubscribe(self.subs.difference(desired))
            await self._subscribe(
                desired.difference(self.subs) | resend.intersection(desired)
            )
        except BaseException:
            # e.g. the connection closed: try again on the next connection
            with self._subs_lock:
                self._resend |= resend
                self.schedule_resub = True
            raise
        self.subs = desired

    @staticmethod
    def _parse_subscription(s: str):
        s = s.strip()
        split = s.split(".", 1)  # Split at the first period
        if len(split) != 2:
            logger.warning("invalid subscription: %s", s)
            return [None, None]

        return split

    def _parse_subscriptions(
        self, subscriptions: Iterable[str]
    ) -> List[Tuple[str, str, str]]:
        """
        Return ``(subscription, channel, symbol)`` for each well-formed
        subscription, skipping (and logging) the rest. Parsing happens before
        taking the lock, so nothing is logged while it is held.
        """
        parsed = []
        for s in subscriptions:
            topic, sym = self._parse_subscription(s)
            if topic is not None:
                parsed.append((s, topic, sym))
        return parsed

    def _collapse(self, channel: str, keep: Optional[str] = None):
        """
        Unschedule every topic on ``channel`` (e.g. ``T``) except ``keep``.

        Matches the channel exactly, so collapsing ``A`` leaves ``AM.AAPL``,
        and covers topics that are scheduled but not yet subscribed. Call with
        the lock held.
        """
        for t in list(self.scheduled_subs):
            if t != keep and t.split(".", 1)[0].strip() == channel:
                self.scheduled_subs.discard(t)

    def subscribe(self, *subscriptions: str, collapse: bool = True):
        """
        Subscribe to given subscriptions.

        Safe to call from any thread, before or while ``connect()`` runs.

        :param subscriptions: Subscriptions (args)
        :param collapse: If true (the default), subscribing to a wildcard such
            as ``T.*`` also unsubscribes the other topics on that channel
            (``T.AAPL``, but not ``TX.AAPL``). If false, they stay subscribed.
        """
        parsed = self._parse_subscriptions(subscriptions)
        with self._subs_lock:
            for s, topic, sym in parsed:
                self.scheduled_subs.add(s)
                if collapse and sym == "*":
                    self._collapse(topic, keep=s)

            self.schedule_resub = True
        self._notify()
        for s, _, _ in parsed:
            logger.debug("sub desired: %s", s)

    def unsubscribe(self, *subscriptions: str, collapse: bool = True):
        """
        Unsubscribe from given subscriptions.

        Safe to call from any thread, before or while ``connect()`` runs.

        :param subscriptions: Subscriptions (args)
        :param collapse: If true (the default), unsubscribing from a wildcard
            such as ``T.*`` also unsubscribes every other topic on that channel
            (``T.AAPL``, but not ``TX.AAPL``). If false, only the wildcard is
            removed.
        """
        parsed = self._parse_subscriptions(subscriptions)
        with self._subs_lock:
            for s, topic, sym in parsed:
                self.scheduled_subs.discard(s)
                if collapse and sym == "*":
                    self._collapse(topic)

            self.schedule_resub = True
        self._notify()
        for s, _, _ in parsed:
            logger.debug("sub undesired: %s", s)

    def set_subscriptions(self, desired: Iterable[str], *, resend: Iterable[str] = ()):
        """
        Replace the subscriptions with ``desired``, in one step.

        Safe to call from any thread, before or while ``connect()`` runs. The
        client sends at most one unsubscribe message, for the topics no longer
        desired, followed by at most one subscribe message, for the new topics
        plus those in ``resend``. Topics in both the old and the new set are
        left alone unless they are in ``resend``.

        Use ``resend`` to subscribe again to topics the client already
        considers subscribed, e.g. ones the server never acknowledged.
        Wildcards such as ``T.*`` are ordinary topics here: nothing collapses.

        :param desired: Every topic to be subscribed to afterwards, e.g.
            ``["T.AAPL", "Q.*"]``. Malformed topics are logged and skipped.
        :param resend: Topics of ``desired`` to send in the subscribe message
            even if already subscribed. Each call replaces the resends of
            earlier calls that have not been sent yet.
        :raises ValueError: If a topic in ``resend`` is not in ``desired``; the
            subscriptions are then left unchanged.
        """
        topics = {s for s, _, _ in self._parse_subscriptions(desired)}
        again = set(resend)
        unknown = again.difference(topics)
        if unknown:
            raise ValueError(f"resend topics not in desired: {sorted(unknown)}")
        with self._subs_lock:
            self.scheduled_subs = topics
            self._resend = again
            self.schedule_resub = True
        self._notify()
        logger.debug("subs desired: %s (resend %s)", topics, again)

    def unsubscribe_all(self):
        """
        Unsubscribe from all subscriptions.

        Safe to call from any thread, before or while ``connect()`` runs.
        """
        with self._subs_lock:
            self.scheduled_subs = set()
            self.schedule_resub = True
        self._notify()

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
