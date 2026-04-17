import os

from dotenv import load_dotenv

from data_loader import get_financials, get_historical_valuation, get_industry_info


def verify_fix(ticker):
    load_dotenv()
    token = os.getenv("FINMIND_API_KEY") or os.getenv("FINMIND_TOKEN")
    print(f"Environment token configured: {bool(token)}")
    try:
        print(f"--- Verifying fix for {ticker} ---")
        df = get_financials(ticker)
        val_history = get_historical_valuation(ticker, df)
        sector, name = get_industry_info(ticker)
        print(f"Success! Managed to fetch financials for {ticker}.")
        print(f"Company: {name}")
        print(f"Sector: {sector}")
        print(f"Fields found: {list(df.columns)}")
        print(f"Last ROE: {df['ROE'].iloc[-1]}, Last FCF: {df['FCF'].iloc[-1]}")
        print(f"Historical valuation rows: {len(val_history)}")
    except Exception as e:
        print(f"Verification failed: {e}")


if __name__ == "__main__":
    verify_fix("5481")
