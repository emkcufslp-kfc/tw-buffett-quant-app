import os
from data_loader import get_financials_yf, SafeDataLoader, get_financials
from dotenv import load_dotenv

def test_yf_fallback():
    print("Test 1: Core YFinance Extraction...")
    try:
        df, source = get_financials_yf("2330")
        print(f"Success! Fetched {len(df)} lines from {source}.")
        print("Last 3 entries:")
        print(df[['date', 'ROE', 'FCF']].tail(3))
    except Exception as e:
        print(f"Failed YFinance Extraction: {e}")

    print("\nTest 2: Universal Fallback Driver...")
    # Using a fake API key to force fallback
    api = SafeDataLoader("FAIL_ME")
    try:
        df, source = get_financials(api, "2330")
        print(f"Success! Universal driver fell back to: {source}")
    except Exception as e:
        print(f"Universal Fallback Failed: {e}")

if __name__ == "__main__":
    test_yf_fallback()
