import pandas as pd

from factor_engine import quality_filter
from portfolio_engine import build_portfolio


def test_normalization():
    print("Mock testing quality and portfolio logic...")

    mock_financials = pd.DataFrame(
        [
            {"date": "2019-12-31", "ROE": 16, "OperatingCashFlow": 100, "FCF": 80},
            {"date": "2020-12-31", "ROE": 17, "OperatingCashFlow": 110, "FCF": 90},
            {"date": "2021-12-31", "ROE": 18, "OperatingCashFlow": 120, "FCF": 100},
            {"date": "2022-12-31", "ROE": 19, "OperatingCashFlow": 130, "FCF": 110},
            {"date": "2023-12-31", "ROE": 20, "OperatingCashFlow": 140, "FCF": 120},
        ]
    )

    quality_pass, quality_msg = quality_filter(
        mock_financials,
        roe_avg_tgt=15,
        roe_min_tgt=10,
        roe_min_count=2,
        fcf_consecutive=5,
    )
    print(f"Quality filter result: {quality_pass} ({quality_msg})")

    portfolio = build_portfolio(
        ["2330", "2317", "2454", "2382", "2881"],
        {
            "2330": "Technology",
            "2317": "Technology",
            "2454": "Technology",
            "2382": "Technology",
            "2881": "Financial Services",
        },
    )
    print(f"Portfolio output: {portfolio}")

    max_weight = max(portfolio.values()) if portfolio else 0
    if quality_pass and max_weight <= 0.10:
        print("\nALL MOCK TESTS PASSED!")
    else:
        print("\nMOCK TESTS FAILED.")


if __name__ == "__main__":
    test_normalization()
