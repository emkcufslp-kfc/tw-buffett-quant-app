import yfinance as yf

def market_regime():
    data = yf.download("^TWII", start="2006-01-01", progress=False)
    # Handle yfinance MultiIndex columns
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)
    
    data['MA200'] = data['Close'].rolling(200).mean()
    result = data['Close'].iloc[-1] > data['MA200'].iloc[-1]
    
    # Ensure we return a single boolean, even if it's a pandas Series
    return bool(result.iloc[0]) if hasattr(result, 'iloc') else bool(result)