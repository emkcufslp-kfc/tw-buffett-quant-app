from data_loader import *
from data_validation import *
from factor_engine import *
from valuation_engine import *
from regime_filter import *
from portfolio_engine import *
from backtest_engine import *

def run():
    api = DataLoader()  # Assume API key set
    universe, uni_source = get_stock_universe(api)
    print(f"Using Universe Source: {uni_source}")
    regime = market_regime()
    selected = []
    sector_map = {}
    for _, row in universe.iterrows():
        ticker = row['stock_id']
        sector_map[ticker] = row['industry_category']
        df, source = get_financials(api, ticker)
        if not validate_financial_data(df):
            continue
        q = quality_filter(df)
        v = valuation_filter(api, ticker)
        if q and v and regime:
            selected.append(ticker)
    portfolio = build_portfolio(selected, sector_map)
    print("Portfolio:")
    for k, v in portfolio.items():
        print(k, v)

if __name__ == "__main__":
    run()