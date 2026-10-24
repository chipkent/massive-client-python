"""Guard the names that ``from <package> import *`` exports.

Code that does ``from massive.rest.models import *`` (or the same for any
other package below) depends on every name it got before. This test fails if
a package stops exporting a name it exported before type-checking support was
added (commit e462088, just after v2.9.0). Adding names is fine.

The snapshots are the complete star-export of each package at that commit,
including incidental names such as ``Optional`` or ``logging``: removing one
of those could still break a caller that relied on it. To remove a name on
purpose, delete it from the snapshot in the same change.
"""

import importlib
import unittest

# Names that ``from <package> import *`` exported at commit e462088.
BASELINE_EXPORTS = {
    "massive": """
        AuthError BadResponse RESTClient WebSocketClient certs exceptions logging
        modelclass rest version websocket
    """.split(),
    "massive.rest": """
        AggsClient Any BASE BenzingaClient ConditionsClient ContractsClient Dict
        DividendsClient ENV_KEY EconomyClient EtfGlobalClient ExchangesClient
        FinancialsClient FuturesClient IndicatorsClient MarketsClient Optional
        QuotesClient RESTClient SnapshotClient SplitsClient SummariesClient
        TickersClient TmxClient TradesClient VXClient aggs base benzinga
        ca_bundle_path economy etf_global financials futures indicators logger
        models os quotes reference snapshot summaries tmx trades vX
    """.split(),
    "massive.rest.models": """
        Agg Any AssetClass BalanceSheet Branding CashFlowStatement CompanyAddress
        ComprehensiveIncome Condition Consolidated CryptoTrade DailyOpenCloseAgg
        DataPoint DataType DayOptionContractSnapshot Dict Direction Disclosure
        DisclosureTaxonomy Dividend DividendType EMAIndicatorResults Enum Exchange
        ExchangeType Filing13F Filing8K FilingFootnote FilingForm3 FilingForm4
        FilingIndex FilingSection FinancialBalanceSheet FinancialCashFlowStatement
        FinancialFloat FinancialIncomeStatement FinancialRatio Financials ForexQuote
        Frequency FuturesAgg FuturesContract FuturesExchange FuturesMarketStatus
        FuturesProduct FuturesQuote FuturesSchedule FuturesSnapshot
        FuturesSnapshotDetails FuturesSnapshotMinute FuturesSnapshotQuote
        FuturesSnapshotSession FuturesSnapshotTrade FuturesTrade Greeks
        GroupedDailyAgg IPOListing IncomeStatement IndicatorUnderlying
        IndicatorValue IndicesSession IndicesSnapshot Insight LastForexQuote
        LastQuote LastQuoteOptionContractSnapshot LastTrade
        LastTradeOptionContractSnapshot LaunchPadOptions List Locale
        MACDIndicatorResults MACDIndicatorValue Market MarketCenter MarketCurrencies
        MarketExchanges MarketHoliday MarketIndices MarketStatus MinuteSnapshot
        OptionContractSnapshot OptionDetails Optional Options OptionsContract Order
        OrderBookQuote Precision PreviousCloseAgg Publisher Quote
        RSIIndicatorResults RealTimeCurrencyConversion RelatedCompany RiskFactor
        RiskFactorTaxonomy SIP SMAIndicatorResults SeriesType Session ShortInterest
        ShortVolume SingleIndicatorResults SipMapping SnapshotMarketType
        SnapshotTickerFullBook Sort Split StockDividend StockFinancial StockSplit
        SummaryResult Ticker TickerChange TickerChangeEvent TickerChangeResults
        TickerDetails TickerNews TickerSnapshot TickerTypes Timeframe Trade
        Underlying UnderlyingAsset Union UniversalSnapshot UniversalSnapshotDetails
        UniversalSnapshotLastMinute UniversalSnapshotLastQuote
        UniversalSnapshotLastTrade UniversalSnapshotSession
        UniversalSnapshotUnderlyingAsset UpdateRules aggs benzinga common conditions
        contracts dataclass dividends economy etf_global exchanges financials
        futures indicators markets modelclass quotes request snapshot splits
        summaries tickers tmx trades
    """.split(),
    "massive.websocket": """
        Any AuthError Awaitable Callable ClientConnection ConnectionClosedError
        ConnectionClosedOK Counter CryptoQuote CryptoTrade CurrencyAgg Dict Enum
        EquityAgg EquityQuote EquityTrade EventType FairMarketValue Feed ForexQuote
        FromDictProtocol FuturesAgg FuturesQuote FuturesTrade Imbalance IndexValue
        LaunchpadValue Level2Book LimitUpLimitDown List MARKET_EVENT_MAP Market
        NewType OK_STATUSES Optional Protocol Set Type Union WebSocketClient
        WebSocketMessage asyncio ca_bundle_path cast common connect env_key
        get_logger json logger logging modelclass models os parse parse_single ssl
        time
    """.split(),
    "massive.websocket.models": """
        Any CryptoQuote CryptoTrade CurrencyAgg Dict Enum EquityAgg EquityQuote
        EquityTrade EventType FairMarketValue Feed ForexQuote FromDictProtocol
        FuturesAgg FuturesQuote FuturesTrade Imbalance IndexValue LaunchpadValue
        Level2Book LimitUpLimitDown List MARKET_EVENT_MAP Market NewType Optional
        Protocol Type Union WebSocketMessage cast common logging modelclass models
        parse parse_single
    """.split(),
}


def star_exports(module_name):
    """Return the names ``from module_name import *`` binds."""
    module = importlib.import_module(module_name)
    names = getattr(module, "__all__", None)
    if names is None:
        names = [n for n in vars(module) if not n.startswith("_")]
    return set(names)


class StarExportsTest(unittest.TestCase):
    def test_no_name_removed(self):
        for package, names in BASELINE_EXPORTS.items():
            with self.subTest(package=package):
                missing = sorted(set(names) - star_exports(package))
                self.assertEqual(missing, [], f"no longer exported by {package}")


if __name__ == "__main__":
    unittest.main()
