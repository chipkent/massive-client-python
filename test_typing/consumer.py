"""A small consumer of the public API, checked with ``mypy --strict``.

Run it from this directory (``make static`` does) so that ``massive`` is
resolved as an installed package, which only type-checks when the package
ships ``py.typed``.
"""

from typing import List, Optional

from massive import RESTClient, WebSocketClient
from massive.rest.models import LastTrade, PreviousCloseAgg, Ticker
from massive.websocket.models import CryptoQuote, IndexValue, WebSocketMessage


def ticker_names(client: RESTClient) -> List[str]:
    names: List[str] = []
    for t in client.list_tickers(cusip="037833100", limit=1):
        if isinstance(t, Ticker) and t.name is not None:
            names.append(t.name)
    return names


def previous_close_ms(aggs: List[PreviousCloseAgg]) -> Optional[int]:
    return aggs[0].timestamp if aggs else None


def next_sequence(trade: LastTrade) -> Optional[int]:
    return None if trade.sequence_number is None else trade.sequence_number + 1


def spread(q: CryptoQuote) -> Optional[float]:
    if q.ask_price is None or q.bid_price is None:
        return None
    return q.ask_price - q.bid_price


def index_time_ms(v: IndexValue) -> Optional[int]:
    return v.timestamp


def handle(msgs: List[WebSocketMessage]) -> None:
    for m in msgs:
        if isinstance(m, CryptoQuote):
            spread(m)
        elif isinstance(m, IndexValue):
            index_time_ms(m)


def make_ws_client() -> WebSocketClient:
    return WebSocketClient(api_key="key", subscriptions=["XQ.BTC-USD"])


def build_models() -> List[object]:
    quote = CryptoQuote(pair="BTC-USD", bid_price=33052.79, ask_size=0.601)
    ts: Optional[int] = IndexValue(ticker="I:SPX", timestamp=1678220098130).timestamp
    trade = LastTrade(ticker="AAPL", sequence_number=3135876)
    return [quote, ts, trade]
