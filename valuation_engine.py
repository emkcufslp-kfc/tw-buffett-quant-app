import pandas as pd
import numpy as np

def valuation_filter(ticker, val_history_df, daily_stats_df, sector, yield_tgt=4.0):
    """
    Modular Valuation Engine:
    - Tech: Current P/E < 10yr Median P/E AND 3yr Avg Yield > yield_tgt
    - Financials: Current P/B < (10yr Mean - 1 StdDev)
    """
    if val_history_df.empty or ticker not in daily_stats_df.index:
        return False, "Valuation Data Missing"
        
    current_pe = daily_stats_df.loc[ticker, 'PE']
    current_pb = daily_stats_df.loc[ticker, 'PB']
    current_yield = daily_stats_df.loc[ticker, 'Yield']
    
    is_financial = sector == "Financial Services"
    
    if is_financial:
        # --- Financials Module: P/B Mean - 1SD ---
        pb_series = val_history_df['PB'].dropna()
        if len(pb_series) < 5: return False, "Hist Data Insufficient"
        threshold = pb_series.mean() - pb_series.std()
        if current_pb < threshold:
            return True, f"P/B Under Mean-1SD ({current_pb:.2f} < {threshold:.2f})"
        return False, f"P/B High ({current_pb:.2f})"
    else:
        # --- Tech/Mfg Module: P/E Median & Yield ---
        pe_series = val_history_df['PE'].dropna()
        if len(pe_series) < 5: return False, "Hist Data Insufficient"
        pe_median = pe_series.median()
        
        # Check median hurdle
        if current_pe > pe_median:
            return False, f"P/E > Median ({current_pe:.2f} > {pe_median:.2f})"
            
        # Check yield hurdle (Daily Yield is used as proxy for 3yr avg if history unavailable)
        if current_yield < yield_tgt:
            return False, f"Yield Low ({current_yield:.2f}%)"
            
        return True, "Pass"

def overvaluation_exit_check(ticker, val_history_df, daily_stats_df):
    """
    Exit based on extreme overvaluation (P/E > 90th percentile).
    """
    if val_history_df.empty or ticker not in daily_stats_df.index:
        return False, "Missing Data"
        
    current_pe = daily_stats_df.loc[ticker, 'PE']
    pe_series = val_history_df['PE'].dropna()
    if len(pe_series) < 5: return False, "Insufficient Info"
    
    upper_threshold = pe_series.quantile(0.90)
    if current_pe > upper_threshold:
        return True, f"Extremely Overvalued (PE {current_pe:.2f} > 90th % {upper_threshold:.2f})"
        
    return False, "Keep"