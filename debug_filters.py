from data_loader import fetch_twse_daily_stats, get_financials, get_historical_valuation, get_industry_info
from factor_engine import quality_filter
from valuation_engine import valuation_filter


tickers = ["2330", "2317", "2454", "2382", "2881"]
daily_stats = fetch_twse_daily_stats()

print(f"{'Ticker':<8} | {'ROE Avg':<8} | {'Quality':<25} | {'Valuation':<25} | {'Sector'}")
print("-" * 110)

for ticker in tickers:
    try:
        df = get_financials(ticker)
        roe_avg = df["ROE"].mean()
        q_pass, q_msg = quality_filter(df, roe_avg_tgt=7, roe_min_tgt=5, roe_min_count=2, fcf_consecutive=5)
        val_hist = get_historical_valuation(ticker, df)
        sector, _ = get_industry_info(ticker)
        v_pass, v_msg = valuation_filter(ticker, val_hist, daily_stats, sector)

        print(
            f"{ticker:<8} | {roe_avg:<8.2f} | {str((q_pass, q_msg)):<25} | "
            f"{str((v_pass, v_msg)):<25} | {sector}"
        )
    except Exception as e:
        print(f"{ticker}: Error {e}")
