import numpy as np

def quality_filter(df, roe_avg_threshold=15, roe_min_threshold=10):
    """
    Modular Quality Filter with dynamic thresholds.
    Ensures 10-year ROE consistency and positive cash flows.
    """
    if df.empty or len(df) < 5:
        return False
        
    # Standardize ROE logic
    roe = df['ROE']
    
    # Condition 1: 10-year Average ROE > Threshold
    cond1 = roe.mean() > roe_avg_threshold
    
    # Condition 2: No more than 2 years below ROE Min Threshold
    cond2 = (roe < roe_min_threshold).sum() <= 2
    
    # Condition 3: Mandatory positive Operating Cash Flow for all years
    ocf = df['OperatingCashFlow']
    cond3 = (ocf > 0).all()
    
    # Condition 4: Mandatory positive Free Cash Flow for all years
    fcf = df['FCF']
    cond4 = (fcf > 0).all()
    
    return cond1 and cond2 and cond3 and cond4