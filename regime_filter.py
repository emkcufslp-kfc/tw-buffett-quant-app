import yfinance as yf

def market_regime():
    data = yf.download("^TWII", start="2006-01-01", progress=False)
    data['MA200'] = data['Close'].rolling(200).mean()
    return data['Close'].iloc[-1] > data['MA200'].iloc[-1]