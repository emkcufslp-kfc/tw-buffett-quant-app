import unittest
from unittest.mock import patch

import pandas as pd

from data_validation import validate_financial_data
from portfolio_engine import build_portfolio
from screening import scan_universe
from strict_mode_engine import evaluate_stock_strict_mode


class BusinessRuleTests(unittest.TestCase):
    def test_validate_financial_data_accepts_annual_history(self):
        df = pd.DataFrame(
            [
                {"date": "2020-12-31", "ROE": 12, "OperatingCashFlow": 100, "FCF": 80},
                {"date": "2021-12-31", "ROE": 13, "OperatingCashFlow": 110, "FCF": 85},
                {"date": "2022-12-31", "ROE": 14, "OperatingCashFlow": 120, "FCF": 90},
                {"date": "2023-12-31", "ROE": 15, "OperatingCashFlow": 130, "FCF": 95},
            ]
        )

        self.assertTrue(validate_financial_data(df))

    def test_build_portfolio_reallocates_to_open_sector_capacity(self):
        portfolio = build_portfolio(
            ["A", "B", "C", "D", "E"],
            {
                "A": "Tech",
                "B": "Tech",
                "C": "Tech",
                "D": "Finance",
                "E": "Energy",
            },
            max_stock_weight=0.30,
            max_sector_weight=0.40,
        )

        self.assertAlmostEqual(sum(portfolio.values()), 1.0, places=6)
        self.assertLessEqual(portfolio["A"] + portfolio["B"] + portfolio["C"], 0.40 + 1e-9)
        self.assertLessEqual(max(portfolio.values()), 0.30 + 1e-9)

    def test_scan_universe_blocks_selection_when_regime_is_bearish(self):
        mock_financials = pd.DataFrame(
            [
                {"date": "2020-12-31", "ROE": 18, "OperatingCashFlow": 100, "FCF": 80},
                {"date": "2021-12-31", "ROE": 19, "OperatingCashFlow": 110, "FCF": 90},
                {"date": "2022-12-31", "ROE": 20, "OperatingCashFlow": 120, "FCF": 100},
                {"date": "2023-12-31", "ROE": 21, "OperatingCashFlow": 130, "FCF": 110},
            ]
        )
        mock_daily = pd.DataFrame([{"stock_id": "2330", "PE": 10.0, "PB": 2.0, "Yield": 5.0}]).set_index("stock_id")
        mock_price = pd.DataFrame({"Close": [100 + i for i in range(260)]})
        mock_benchmark = pd.DataFrame({"Close": [90 + i * 0.1 for i in range(260)]})
        mock_valuation = pd.DataFrame(
            {"date": [f"20{i}-12-31" for i in range(5)], "PE": [14, 15, 16, 17, 18], "PB": [1.8, 1.9, 2.0, 2.1, 2.2]}
        )
        mock_revenue = pd.DataFrame(
            {
                "stock_id": ["2330"] * 3,
                "period": pd.to_datetime(["2023-10-01", "2023-11-01", "2023-12-01"]),
                "monthly_revenue": [100, 110, 120],
                "revenue_yoy": [10, 12, 14],
            }
        )

        with patch("screening.get_stock_universe", return_value=pd.DataFrame([{"stock_id": "2330", "industry_category": "Semiconductor", "stock_name": "TSMC"}])), \
             patch("screening.market_regime", return_value=False), \
             patch("screening.get_taiex_history", return_value=mock_benchmark), \
             patch("screening.fetch_monthly_revenue_history", return_value=mock_revenue), \
             patch("screening.get_financials", return_value=mock_financials), \
             patch("screening.get_historical_valuation", return_value=mock_valuation), \
             patch("screening.get_price_history", return_value=mock_price), \
             patch("screening.CacheManager.save", return_value=None):
            results = scan_universe(cache={}, daily_stats_df=mock_daily)

        self.assertEqual(results["selected"], [])
        self.assertEqual(results["regime_blocked_count"], 1)
        self.assertFalse(results["diagnostics"].iloc[0]["selected"])

    def test_evaluate_stock_strict_mode_respects_custom_score_thresholds(self):
        financials_df = pd.DataFrame(
            [
                {"date": "2020-12-31", "ROE": 12, "OperatingCashFlow": 100, "FCF": 80},
                {"date": "2021-12-31", "ROE": 13, "OperatingCashFlow": 110, "FCF": 90},
                {"date": "2022-12-31", "ROE": 14, "OperatingCashFlow": 120, "FCF": 100},
                {"date": "2023-12-31", "ROE": 15, "OperatingCashFlow": 130, "FCF": 110},
            ]
        )
        daily_stats_df = pd.DataFrame([{"stock_id": "2330", "PE": 10.0, "PB": 1.5, "Yield": 6.5}]).set_index("stock_id")
        valuation_df = pd.DataFrame(
            {"date": [f"20{i}-12-31" for i in range(5)], "PE": [15, 16, 17, 18, 19], "PB": [1.5, 1.6, 1.7, 1.8, 1.9]}
        )
        price_df = pd.DataFrame({"Close": [100 + i for i in range(260)]})
        benchmark_df = pd.DataFrame({"Close": [100 + i * 0.2 for i in range(260)]})
        revenue_metrics = {"avg_3m_revenue_yoy": 12.0, "latest_revenue_month": "2023-12"}
        custom_criteria = {
            "quality_compounder_min_roe": 20.0,
            "quality_investable_min_roe": 10.0,
            "quality_compounder_max_decline": -0.5,
            "quality_investable_max_decline": -1.0,
            "turnaround_min_revenue_yoy": 25.0,
            "value_trap_revenue_yoy": 0.0,
            "valuation_min_history": 5,
            "valuation_deep_value_std": 2.0,
            "valuation_cheap_std": 1.0,
            "valuation_expensive_std": 1.0,
            "peg_min_growth": 0.0,
            "peg_undervalued_max": 0.9,
            "peg_fair_value_max": 1.5,
            "peg_overvalued_min": 2.0,
            "yield_floor_min": 6.0,
            "yield_neutral_min": 4.0,
            "momentum_ma_window": 200,
            "momentum_lookback_days": 21,
            "action_strong_buy_min": 120,
            "action_accumulate_min": 100,
            "action_hold_min": 60,
            "action_trim_min": 30,
        }

        evaluation = evaluate_stock_strict_mode(
            ticker="2330",
            sector="Semiconductor",
            financials_df=financials_df,
            valuation_df=valuation_df,
            daily_stats_df=daily_stats_df,
            revenue_metrics=revenue_metrics,
            price_df=price_df,
            benchmark_df=benchmark_df,
            criteria=custom_criteria,
        )

        self.assertEqual(evaluation["action_plan"], "HOLD")
        self.assertFalse(evaluation["selected"])


if __name__ == "__main__":
    unittest.main()
