import os
from dotenv import load_dotenv
from data_loader import SafeDataLoader, get_stock_universe

def test():
    load_dotenv()
    token = os.getenv("FINMIND_TOKEN")
    if not token:
        print("❌ No FINMIND_TOKEN found in .env")
        return

    print(f"Testing FinMind API with token: {token[:10]}...")
    try:
        api = SafeDataLoader(token)
        universe = get_stock_universe(api)
        print(f"Success! Loaded {len(universe)} stocks in the universe.")
        print("\nFirst 5 stocks:")
        print(universe.head())
    except Exception as e:
        print(f"API Test Failed: {e}")

if __name__ == "__main__":
    test()
