from typing import List, Optional
from ...modelclass import modelclass


@modelclass
class Agg:
    "Contains aggregate data for a given ticker symbol over a given date range in a custom time window size."

    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None
    vwap: Optional[float] = None
    timestamp: Optional[int] = None
    transactions: Optional[int] = None
    otc: Optional[bool] = None

    @staticmethod
    def from_dict(d):
        return Agg(
            d.get("o", None),
            d.get("h", None),
            d.get("l", None),
            d.get("c", None),
            d.get("v", None),
            d.get("vw", None),
            d.get("t", None),
            d.get("n", None),
            d.get("otc", None),
        )


@modelclass
class GroupedDailyAgg:
    "Contains daily open, high, low, and close (OHLC) data for a given date."

    ticker: Optional[str] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    volume: Optional[float] = None
    vwap: Optional[float] = None
    timestamp: Optional[int] = None
    transactions: Optional[int] = None
    otc: Optional[bool] = None

    @staticmethod
    def from_dict(d):
        return GroupedDailyAgg(
            d.get("T", None),
            d.get("o", None),
            d.get("h", None),
            d.get("l", None),
            d.get("c", None),
            d.get("v", None),
            d.get("vw", None),
            d.get("t", None),
            d.get("n", None),
            d.get("otc", None),
        )


@modelclass
class DailyOpenCloseAgg:
    "Contains data for open, close and afterhours prices of a ticker symbol on a specified date."

    after_hours: Optional[float] = None
    close: Optional[float] = None
    from_: Optional[str] = None
    high: Optional[float] = None
    low: Optional[float] = None
    open: Optional[float] = None
    pre_market: Optional[float] = None
    status: Optional[str] = None
    symbol: Optional[str] = None
    volume: Optional[float] = None
    otc: Optional[bool] = None

    @staticmethod
    def from_dict(d):
        return DailyOpenCloseAgg(
            d.get("afterHours", None),
            d.get("close", None),
            d.get("from", None),
            d.get("high", None),
            d.get("low", None),
            d.get("open", None),
            d.get("preMarket", None),
            d.get("status", None),
            d.get("symbol", None),
            d.get("volume", None),
            d.get("otc", None),
        )


@modelclass
class DailyOpenCloseCryptoTrade:
    "Contains data for one of the opening or closing trades of a crypto pair on a specified date."

    conditions: Optional[List[int]] = None
    id: Optional[str] = None
    price: Optional[float] = None
    size: Optional[float] = None
    timestamp: Optional[int] = None
    exchange: Optional[int] = None

    @staticmethod
    def from_dict(d):
        return DailyOpenCloseCryptoTrade(
            conditions=d.get("c", None),
            id=d.get("i", None),
            price=d.get("p", None),
            size=d.get("s", None),
            timestamp=d.get("t", None),
            exchange=d.get("x", None),
        )


@modelclass
class DailyOpenCloseCrypto:
    "Contains data for open and close prices, and the opening and closing trades, of a crypto pair on a specified date."

    symbol: Optional[str] = None
    is_utc: Optional[bool] = None
    day: Optional[str] = None
    open: Optional[float] = None
    close: Optional[float] = None
    open_trades: Optional[List[DailyOpenCloseCryptoTrade]] = None
    closing_trades: Optional[List[DailyOpenCloseCryptoTrade]] = None

    @staticmethod
    def from_dict(d):
        return DailyOpenCloseCrypto(
            symbol=d.get("symbol", None),
            is_utc=d.get("isUTC", None),
            day=d.get("day", None),
            open=d.get("open", None),
            close=d.get("close", None),
            open_trades=(
                None
                if "openTrades" not in d
                else [DailyOpenCloseCryptoTrade.from_dict(t) for t in d["openTrades"]]
            ),
            closing_trades=(
                None
                if "closingTrades" not in d
                else [
                    DailyOpenCloseCryptoTrade.from_dict(t) for t in d["closingTrades"]
                ]
            ),
        )


@modelclass
class PreviousCloseAgg:
    "Contains data for the previous day's open, high, low, and close (OHLC) of the specified stock ticker."

    ticker: Optional[str] = None
    close: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    open: Optional[float] = None
    timestamp: Optional[int] = None
    volume: Optional[float] = None
    vwap: Optional[float] = None

    @staticmethod
    def from_dict(d):
        return PreviousCloseAgg(
            d.get("T", None),
            d.get("c", None),
            d.get("h", None),
            d.get("l", None),
            d.get("o", None),
            d.get("t", None),
            d.get("v", None),
            d.get("vw", None),
        )
