from massive.rest.models.request import RequestOptionBuilder
from base import BaseTest

import pook  # type: ignore


class RequestOptionsTest(BaseTest):
    """`options` sets headers; it must not leak into the query string."""

    def options(self):
        return RequestOptionBuilder(edge_id="test-edge", edge_ip_address="1.2.3.4")

    def assert_no_options_in_query(self, mock):
        self.assertEqual(len(mock.matches), 1)
        self.assertNotIn("options", mock.matches[0].query or {})

    def test_get_with_options(self):
        mock = pook.get(
            self.c.BASE + "/v3/reference/tickers/OPTS",
            reply=200,
            response_body='{"results": {"ticker": "OPTS"}}',
        )
        details = self.c.get_ticker_details("OPTS", options=self.options())
        self.assertEqual(details.ticker, "OPTS")
        self.assert_no_options_in_query(mock)

    def test_paginate_with_options(self):
        mock = pook.get(
            self.c.BASE + "/v3/trades/OPTS",
            reply=200,
            response_body='{"results": []}',
        )
        trades = list(self.c.list_trades("OPTS", options=self.options()))
        self.assertEqual(trades, [])
        self.assert_no_options_in_query(mock)
