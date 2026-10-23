import logging
import typing
import unittest

from massive.websocket.models import (
    CryptoQuote,
    IndexValue,
    Market,
    parse,
)

logger = logging.getLogger(__name__)


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


class ModelsTest(unittest.TestCase):
    def test_crypto_quote(self):
        # sample payload from https://massive.com/docs/websocket/crypto/quotes
        msg = {
            "ev": "XQ",
            "pair": "BTC-USD",
            "bp": 33052.79,
            "bs": 0.48,
            "ap": 33073.19,
            "as": 0.601,
            "t": 1610462411115,
            "x": 1,
            "r": 1610462411128,
        }
        parsed = parse([msg], logger, Market.Crypto)
        expected = CryptoQuote(
            event_type="XQ",
            pair="BTC-USD",
            bid_price=33052.79,
            bid_size=0.48,
            ask_price=33073.19,
            ask_size=0.601,
            timestamp=1610462411115,
            exchange_id=1,
            received_timestamp=1610462411128,
        )
        self.assertEqual(parsed, [expected])
        for field, value in vars(parsed[0]).items():
            self.assertTrue(hint_admits(CryptoQuote, field, value), field)

    def test_index_value(self):
        # sample payload from https://massive.com/docs/websocket/indices/value
        msg = {"ev": "V", "val": 3988.5, "T": "I:SPX", "t": 1678220098130}
        parsed = parse([msg], logger, Market.Indices)
        expected = IndexValue(
            event_type="V",
            value=3988.5,
            ticker="I:SPX",
            timestamp=1678220098130,
        )
        self.assertEqual(parsed, [expected])
        for field, value in vars(parsed[0]).items():
            self.assertTrue(hint_admits(IndexValue, field, value), field)


if __name__ == "__main__":
    unittest.main()
