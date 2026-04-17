import numpy as np
from FinMind.data import DataLoader

def valuation_filter(api, ticker):
    pepb = api.taiwan_stock_per_pbr(stock_id=ticker)
    div = api.taiwan_stock_dividend(stock_id=ticker)
    price = api.taiwan_stock_price(stock_id=ticker)
    if len(pepb) < 10:
        return False
    pe = pepb['PE_ratio'].dropna()
    pe_median = pe.tail(10).median()
    current_pe = pe.iloc[-1]
    current_price = price['close'].iloc[-1]
    dividend = div['cash_dividend'].tail(3).mean()
    dividend_yield = dividend / current_price
    return current_pe < pe_median and dividend_yield > 0.04