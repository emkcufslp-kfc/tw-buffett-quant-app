from data_loader import get_financials, get_historical_valuation, get_industry_info


def test_yf_fallback():
    print("Test 1: Core yfinance financial extraction...")
    try:
        df = get_financials("2330")
        print(f"Success! Fetched {len(df)} financial rows.")
        print(df[["date", "ROE", "FCF"]].tail(3))
    except Exception as e:
        print(f"Failed yfinance extraction: {e}")

    print("\nTest 2: Valuation and sector metadata...")
    try:
        df = get_financials("2330")
        val_history = get_historical_valuation("2330", df)
        sector, name = get_industry_info("2330")
        print(f"Sector: {sector}")
        print(f"Name: {name}")
        print(f"Historical valuation rows: {len(val_history)}")
    except Exception as e:
        print(f"Metadata/valuation test failed: {e}")


if __name__ == "__main__":
    test_yf_fallback()
