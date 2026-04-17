import os
from dotenv import load_dotenv
from data_loader import SafeDataLoader, get_financials

def verify_fix(ticker):
    load_dotenv()
    token = os.getenv("FINMIND_TOKEN")
    if not token:
        print("No token found")
        return

    api = SafeDataLoader(token)
    try:
        print(f"--- Verifying fix for {ticker} ---")
        df = get_financials(api, ticker)
        print(f"✅ Success! Managed to fetch and merge financials for {ticker}.")
        print(f"Fields found: {list(df.columns)}")
        print(f"Last ROE: {df['ROE'].iloc[-1]}, Last FCF: {df['FCF'].iloc[-1]}")
    except Exception as e:
        print(f"❌ Verification Failed: {e}")

if __name__ == "__main__":
    verify_fix("5481")
