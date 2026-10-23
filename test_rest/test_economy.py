from base import BaseTest
from massive.rest.models.economy import FedFundingConditions


class EconomyTest(BaseTest):
    def test_list_funding_conditions(self):
        conditions = [
            c for c in self.c.list_funding_conditions(date_gte="2025-06-01", limit=1)
        ]
        expected = [
            FedFundingConditions(
                date="2025-06-01",
                effective_fed_funds_rate=4.33,
                fed_funds_target_lower=4.25,
                fed_funds_target_upper=4.5,
                interest_on_reserve_balances=4.4,
            ),
            FedFundingConditions(
                date="2025-06-02",
                effective_fed_funds_rate=4.33,
                effective_fed_funds_volume=98,
                fed_funds_target_lower=4.25,
                fed_funds_target_upper=4.5,
                fed_overnight_repo_treasury_amount=0,
                fed_overnight_reverse_repo_treasury_amount=214.5,
                financial_commercial_paper_90d_rate=4.35,
                interest_on_reserve_balances=4.4,
                nonfinancial_commercial_paper_90d_rate=4.34,
                obfr_25th_percentile=4.3,
                obfr_75th_percentile=4.33,
                obfr_volume=305,
                overnight_bank_funding_rate=4.32,
                secured_overnight_financing_rate=4.31,
                sofr_25th_percentile=4.28,
                sofr_75th_percentile=4.32,
                sofr_volume=2214,
                tgcr_25th_percentile=4.27,
                tgcr_75th_percentile=4.31,
                tri_party_general_collateral_rate=4.3,
                tri_party_general_collateral_volume=1876,
            ),
        ]
        self.assertEqual(conditions, expected)

    def test_list_funding_conditions_query_params(self):
        params = self.c._get_params(
            self.c.list_funding_conditions,
            {
                "params": None,
                "date": "2025-06-02",
                "date_gt": "2025-06-01",
                "date_gte": "2025-06-01",
                "date_lt": "2025-06-03",
                "date_lte": "2025-06-03",
                "limit": 10,
                "sort": "date.desc",
            },
        )
        self.assertEqual(
            set(params),
            {"date", "date.gt", "date.gte", "date.lt", "date.lte", "limit", "sort"},
        )
