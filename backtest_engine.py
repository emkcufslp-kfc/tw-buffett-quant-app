import pandas as pd

def compute_returns(price_data):
    returns = price_data.pct_change().dropna()
    return returns

def portfolio_return(weights, returns):
    w = pd.Series(weights)
    aligned = returns[w.index]
    portfolio = aligned.mul(w, axis=1).sum(axis=1)
    return portfolio