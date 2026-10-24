import types

from base import BaseTest


class PaginationTest(BaseTest):
    def test_list_methods_return_iterators(self):
        # The iterators are lazy, so no request is sent.
        for result in (
            self.c.list_treasury_yields(),
            self.c.list_short_interest(),
            self.c.list_short_volume(),
            self.c.vx.list_ipos(),
        ):
            self.assertIsInstance(result, types.GeneratorType)
