from data_loader import get_stock_universe, get_financials
from data_validation import validate_financial_data
from factor_engine import quality_filter
from valuation_engine import valuation_filter
from regime_filter import market_regime
from strategy import entry_rule
from portfolio_engine import build_portfolio

def run():
    print("Initializing Quantitative Check (Pure YFinance)...")
    
    # 1. Get Universe
    universe_df = get_stock_universe()
    tickers = universe_df['stock_id'].tolist()
    sector_map = dict(zip(universe_df['stock_id'], universe_df['industry_category']))
    
    # 2. Check Regime
    regime = market_regime()
    print(f"Market Regime Bullish: {regime}")
    
    selected = []
    
    print(f"Scanning {len(tickers)} stocks...")
    for i, ticker in enumerate(tickers):
        try:
            df = get_financials(ticker)
            if validate_financial_data(df):
                q = quality_filter(df)
                v = valuation_filter(ticker)
                
                if entry_rule(q, v, regime):
                    print(f"💎 Stock Qualified: {ticker}")
                    selected.append(ticker)
        except Exception as e:
            # Silent skip for batch processing
            continue
            
    # 3. Build Portfolio
    if not selected:
        print("No stocks met the criteria.")
    else:
        portfolio = build_portfolio(selected, sector_map)
        print("\n--- Qualified Portfolio ---")
        for stock, weight in portfolio.items():
            print(f"{stock}: {weight:.2%}")

if __name__ == "__main__":
    run()