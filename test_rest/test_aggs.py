from base import BaseTest
from massive.rest.models import (
    Agg,
    GroupedDailyAgg,
    DailyOpenCloseAgg,
    DailyOpenCloseCrypto,
    DailyOpenCloseCryptoTrade,
    PreviousCloseAgg,
)


class AggsTest(BaseTest):
    def test_list_aggs(self):
        aggs = [
            a for a in self.c.list_aggs("AAPL", 1, "day", "2005-04-02", "2005-04-04")
        ]
        expected = [
            Agg(
                open=1.5032,
                high=1.5064,
                low=1.4489,
                close=1.4604,
                volume=642646396.0,
                vwap=1.469,
                timestamp=1112331600000,
                transactions=82132,
            ),
            Agg(
                open=1.4639,
                high=1.4754,
                low=1.4343,
                close=1.4675,
                volume=578172308.0,
                vwap=1.4589,
                timestamp=1112587200000,
                transactions=65543,
            ),
        ]
        self.assertEqual(aggs, expected)

    def test_get_aggs(self):
        aggs = self.c.get_aggs("AAPL", 1, "day", "2005-04-01", "2005-04-04")
        expected = [
            Agg(
                open=1.5032,
                high=1.5064,
                low=1.4489,
                close=1.4604,
                volume=642646396.0,
                vwap=1.469,
                timestamp=1112331600000,
                transactions=82132,
            ),
            Agg(
                open=1.4639,
                high=1.4754,
                low=1.4343,
                close=1.4675,
                volume=578172308.0,
                vwap=1.4589,
                timestamp=1112587200000,
                transactions=65543,
            ),
        ]
        self.assertEqual(aggs, expected)

    def test_get_grouped_daily_aggs(self):
        aggs = self.c.get_grouped_daily_aggs("2005-04-04", True)
        expected = [
            GroupedDailyAgg(
                ticker="GIK",
                open=9.99,
                high=10.02,
                low=9.9,
                close=10.02,
                volume=895345,
                vwap=9.9979,
                timestamp=1602705600000,
                transactions=96,
            )
        ]
        self.assertEqual(aggs, expected)

    def test_get_daily_open_close_agg(self):
        aggs = self.c.get_daily_open_close_agg("AAPL", "2005-04-01", True)
        expected = DailyOpenCloseAgg(
            after_hours=123,
            close=123,
            from_="2021-04-01",
            high=124.18,
            low=122.49,
            open=123.66,
            pre_market=123.45,
            status="OK",
            symbol="AAPL",
            volume=75089134,
        )

        self.assertEqual(aggs, expected)

    def test_get_daily_open_close_crypto(self):
        aggs = self.c.get_daily_open_close_crypto(
            "BTC", "USD", "2020-10-09", adjusted=True
        )
        expected = DailyOpenCloseCrypto(
            symbol="BTC-USD",
            is_utc=True,
            day="2020-10-09T00:00:00.000Z",
            open=10932.44,
            close=11050.64,
            open_trades=[
                DailyOpenCloseCryptoTrade(
                    conditions=[2],
                    id="511235746",
                    price=10932.44,
                    size=0.002,
                    timestamp=1602201600056,
                    exchange=1,
                ),
                DailyOpenCloseCryptoTrade(
                    conditions=[2],
                    id="511235751",
                    price=10923.76,
                    size=0.02,
                    timestamp=1602201600141,
                    exchange=4,
                ),
            ],
            closing_trades=[
                DailyOpenCloseCryptoTrade(
                    conditions=[2],
                    id="973323250",
                    price=11050.64,
                    size=0.006128,
                    timestamp=1602287999795,
                    exchange=4,
                ),
                DailyOpenCloseCryptoTrade(
                    conditions=[1],
                    id="105717893",
                    price=11049.4,
                    size=0.014,
                    timestamp=1602287999659,
                    exchange=17,
                ),
            ],
        )

        self.assertEqual(aggs, expected)

    def test_get_previous_close_agg(self):
        aggs = self.c.get_previous_close_agg("AAPL")
        self.assertIsInstance(aggs, list)
        expected = [
            PreviousCloseAgg(
                ticker="AAPL",
                close=156.8,
                high=162.34,
                low=156.72,
                open=162.25,
                timestamp=1651003200000,
                volume=95595226.0,
                vwap=158.6074,
            )
        ]
        self.assertEqual(aggs, expected)
