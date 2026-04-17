import yfinance as yf

def valuation_filter(ticker):
    """
    Simplified valuation filter using yfinance 'info' metrics.
    No longer depends on FinMind PBR/PE database.
    """
    ticker_tw = ticker + ".TW"
    t = yf.Ticker(ticker_tw)
    info = t.info
    
    # Extract trailing P/E and Dividend Yield
    # Default high PE and low yield if missing to be conservative
    current_pe = info.get('trailingPE', 999)
    div_yield = info.get('dividendYield', 0)
    
    # Valuation Anchor: 
    # Current P/E < 20 (General Value Threshold) AND Dividend Yield > 4%
    # Note: div_yield from yfinance is decimal (e.g. 0.04)
    return current_pe < 25 and div_yield >= 0.03