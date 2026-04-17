import yfinance as yf
import pandas as pd
import numpy as np

def valuation_filter(ticker, val_history_df):
    """
    Modular Valuation Engine:
    - Tech/Manufacturing: Current P/E < 10yr Median P/E
    - Financials: Current P/B < (10yr Mean - 1 StdDev)
    """
    if val_history_df.empty:
        return False
        
    ticker_tw = ticker + ".TW"
    t = yf.Ticker(ticker_tw)
    info = t.info
    
    sector = info.get('sector', 'Unknown')
    is_financial = sector == "Financial Services"
    
    if is_financial:
        # --- Financials Module: P/B Mean - 1SD ---
        pb_series = val_history_df['PB'].dropna()
        if len(pb_series) < 5:
            return False
            
        pb_mean = pb_series.mean()
        pb_std = pb_series.std()
        pb_threshold = pb_mean - pb_std
        
        current_pb = info.get('priceToBook', 999)
        return current_pb < pb_threshold
    else:
        # --- Tech/Mfg Module: P/E Median Comparison ---
        pe_series = val_history_df['PE'].dropna()
        if len(pe_series) < 5:
            return False
            
        pe_median = pe_series.median()
        current_pe = info.get('trailingPE', 999)
        
        # P/E must be below historical median AND not extremely high (e.g. < 40)
        return current_pe < pe_median and current_pe < 40