"""Typical user code, checked with ``mypy --strict`` but never run.

``make static`` checks this file from inside ``test_rest/``, so ``massive``
resolves to the installed package, as it does for a user. The check fails if:

- the package stops shipping ``py.typed`` or re-exporting a public name;
- a default call stops returning the model type, so using the result
  needs an ``isinstance`` check;
- ``raw=True`` stops returning ``HTTPResponse``;
- a model constructor stops accepting its fields as keywords;
- a corrected hint regresses (``assert_type`` checks the exact type);
- code written for earlier releases, which narrows each result with
  ``isinstance`` and passes an ``int`` CUSIP, stops type-checking.

The file is named so that unittest discovery does not import it.
"""

import sys
from typing import Iterator, List, Optional, Union

from urllib3 import HTTPResponse

if sys.version_info >= (3, 11):
    from typing import assert_type
else:
    from typing_extensions import assert_type

from massive import AuthError, BadResponse, RESTClient, WebSocketClient
from massive.rest.models import (
    Agg,
    IPOListing,
    LastTrade,
    PreviousCloseAgg,
    ShortInterest,
    ShortVolume,
    Ticker,
)
from massive.rest.models.economy import TreasuryYield
from massive.websocket.models import CryptoQuote, IndexValue, WebSocketMessage


def ticker_symbols(client: RESTClient) -> List[str]:
    # list_* yields models; no isinstance check is needed to read them.
    return [t.ticker for t in client.list_tickers(market="stocks") if t.ticker]


def apple_tickers(client: RESTClient) -> List[Ticker]:
    # A CUSIP is a string; an int is still accepted for backward compatibility.
    return list(client.list_tickers(cusip="037833100")) + list(
        client.list_tickers(cusip=37833100)
    )


def first_close(client: RESTClient) -> Optional[float]:
    # get_* list endpoints can be indexed directly.
    bars = client.get_aggs("AAPL", 1, "day", "2024-01-02", "2024-01-31")
    assert_type(bars, List[Agg])
    return bars[0].close if bars else None


def previous_close(client: RESTClient) -> Optional[float]:
    aggs = client.get_previous_close_agg("AAPL")
    assert_type(aggs, List[PreviousCloseAgg])
    return aggs[0].close if aggs else None


def last_price(client: RESTClient) -> Optional[float]:
    trade = client.get_last_trade("AAPL")
    assert_type(trade, LastTrade)
    return trade.price


def paginated(client: RESTClient) -> None:
    assert_type(
        client.list_aggs("AAPL", 1, "day", "2024-01-02", "2024-01-31"), Iterator[Agg]
    )
    assert_type(client.list_treasury_yields(), Iterator[TreasuryYield])
    assert_type(client.list_short_interest(ticker="AAPL"), Iterator[ShortInterest])
    assert_type(client.list_short_volume(ticker="AAPL"), Iterator[ShortVolume])
    assert_type(client.vx.list_ipos(), Iterator[IPOListing])


def raw_status(client: RESTClient) -> int:
    resp = client.get_aggs("AAPL", 1, "day", "2024-01-02", "2024-01-31", raw=True)
    assert_type(resp, HTTPResponse)
    assert_type(client.list_tickers(raw=True), HTTPResponse)
    assert_type(client.get_last_trade("AAPL", raw=True), HTTPResponse)
    return resp.status


def raw_flag(client: RESTClient, raw: bool) -> None:
    # A non-literal flag gets the full union, as before.
    assert_type(client.get_last_trade("AAPL", raw=raw), Union[LastTrade, HTTPResponse])
    assert_type(client.list_tickers(raw=raw), Union[Iterator[Ticker], HTTPResponse])


def ticker_symbols_or_none(client: RESTClient) -> Optional[List[str]]:
    try:
        return ticker_symbols(client)
    except (AuthError, BadResponse):
        return None


def spread(q: CryptoQuote) -> Optional[float]:
    assert_type(q.bid_price, Optional[float])
    assert_type(q.ask_price, Optional[float])
    assert_type(q.ask_size, Optional[float])
    if q.ask_price is None or q.bid_price is None:
        return None
    return q.ask_price - q.bid_price


def index_time_ms(v: IndexValue) -> Optional[int]:
    assert_type(v.timestamp, Optional[int])
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
    # Model constructors accept their fields as keywords.
    quote = CryptoQuote(pair="BTC-USD", bid_price=33052.79, ask_size=0.601)
    value = IndexValue(ticker="I:SPX", value=3988.5, timestamp=1678220098130)
    trade = LastTrade(ticker="AAPL", sequence_number=3135876)
    return [quote, value, trade]


def written_for_earlier_releases(client: RESTClient, raw: bool) -> None:
    # Code written when every REST method returned Union[Model, HTTPResponse]
    # narrows the result itself. It must keep type-checking.
    trade = client.get_last_trade("AAPL")
    if isinstance(trade, LastTrade):
        print(trade.price)
    for t in client.list_tickers(cusip=37833100):
        if isinstance(t, Ticker):
            print(t.ticker)
    resp = client.get_last_trade("AAPL", raw=True)
    if isinstance(resp, HTTPResponse):
        print(resp.status)
    either: Union[LastTrade, HTTPResponse] = client.get_last_trade("AAPL", raw=raw)
    tickers: Union[Iterator[Ticker], HTTPResponse] = client.list_tickers()
    print(either, tickers)
