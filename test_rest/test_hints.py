import typing
from typing import Optional
from unittest import mock

from massive.rest.models import LastTrade, PreviousCloseAgg
from base import BaseTest


def hint_admits(cls: type, field: str, value: object) -> bool:
    "Whether the runtime type of value is one of the types the field's hint allows."
    if value is None:
        return True  # every model field is Optional
    hint = typing.get_type_hints(cls)[field]
    allowed = tuple(
        typing.get_origin(a) or a for a in typing.get_args(hint) if a is not type(None)
    )
    if float in allowed:
        allowed += (int,)  # an int is a valid float (PEP 484 numeric tower)
    return isinstance(value, allowed)


class HintsTest(BaseTest):
    def test_previous_close_agg(self):
        aggs = self.c.get_previous_close_agg("AAPL")
        self.assertEqual(
            typing.get_type_hints(PreviousCloseAgg)["timestamp"], Optional[int]
        )
        for agg in aggs:
            for field, value in vars(agg).items():
                self.assertTrue(hint_admits(PreviousCloseAgg, field, value), field)

    def test_last_trade(self):
        trade = self.c.get_last_trade("AAPL")
        self.assertEqual(
            typing.get_type_hints(LastTrade)["sequence_number"], Optional[int]
        )
        for field, value in vars(trade).items():
            self.assertTrue(hint_admits(LastTrade, field, value), field)

    def test_list_tickers_cusip(self):
        # CUSIPs are 9-character alphanumeric strings; leading zeros matter
        with mock.patch.object(self.c, "_paginate") as paginate:
            self.c.list_tickers(cusip="037833100")
        self.assertEqual(paginate.call_args.kwargs["params"]["cusip"], "037833100")
