from data_loader import get_financials, get_historical_valuation
from factor_engine import quality_filter
from valuation_engine import valuation_filter
import yfinance as yf

tickers = ['2330', '2317', '2454', '2382', '2881']

print(f"{'Ticker':<8} | {'ROE Avg':<8} | {'Quality':<8} | {'Valuation':<10} | {'PE/PB Now':<10} | {'PE/PB Target'}")
print("-" * 80)

for t in tickers:
    try:
        df = get_financials(t)
        roe_avg = df['ROE'].mean()
        q = quality_filter(df, roe_avg_threshold=7, roe_min_threshold=5)
        
        val_hist = get_historical_valuation(t, df)
        v = valuation_filter(t, val_hist)
        
        info = yf.Ticker(t+".TW").info
        is_fin = info.get('sector') == "Financial Services"
        
        now = info.get('trailingPE') if not is_fin else info.get('priceToBook')
        target = "N/A"
        if not val_hist.empty:
            target = val_hist['PE'].median() if not is_fin else (val_hist['PB'].mean() - val_hist['PB'].std())
            
        print(f"{t:<8} | {roe_avg:<8.2f} | {str(q):<8} | {str(v):<10} | {now:<10.2f} | {target:.2f}")
    except Exception as e:
        print(f"{t}: Error {e}")
