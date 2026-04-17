import os

from dotenv import load_dotenv

from data_loader import fetch_twse_daily_stats, get_stock_universe


def test():
    load_dotenv()
    token = os.getenv("FINMIND_API_KEY") or os.getenv("FINMIND_TOKEN")
    if token:
        print("Found a configured FinMind token/secret in the environment.")
    else:
        print("No FinMind token configured. This app currently relies on TWSE OpenAPI and yfinance.")

    try:
        universe = get_stock_universe()
        daily_stats = fetch_twse_daily_stats()
        print(f"Success! Loaded {len(universe)} stocks in the universe.")
        print(f"TWSE daily stats rows: {len(daily_stats)}")
        print("\nFirst 5 universe rows:")
        print(universe.head())
    except Exception as e:
        print(f"Data source smoke test failed: {e}")


if __name__ == "__main__":
    test()
