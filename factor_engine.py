import numpy as np

def quality_filter(df, roe_avg_tgt=15, roe_min_tgt=10, roe_min_count=2, fcf_consecutive=10):
    """
    Hardened Quality Filter:
    - 過去 10 年平均 ROE > roe_avg_tgt
    - ROE 低於 roe_min_tgt 次數 <= roe_min_count
    - 連續 fcf_consecutive 年 OCF > 0 且 FCF > 0
    """
    if df.empty or len(df) < 5:
        return False, "Data Insufficient"
        
    # 1. ROE Avg
    if df['ROE'].mean() < roe_avg_tgt:
        return False, "Avg ROE Low"
        
    # 2. ROE Stability
    if (df['ROE'] < roe_min_tgt).sum() > roe_min_count:
        return False, "ROE Stability Fail"
        
    # 3. Cash Flow Consistency (OCF & FCF)
    # Check last N years
    lookback = min(len(df), fcf_consecutive)
    recent_df = df.head(lookback)
    
    if not (recent_df['OperatingCashFlow'] > 0).all():
        return False, "OCF Negative"
        
    if not (recent_df['FCF'] > 0).all():
        return False, "FCF Negative"
        
    return True, "Pass"

def fundamental_exit_check(df, roe_exit_threshold=10, fcf_exit_true=True):
    """
    Rational Exit Rule for Fundamentals:
    - ROE < 10% (last 2 periods)
    - Free Cash Flow turns negative
    """
    if df.empty or len(df) < 2:
        return False, "Keep"
        
    # Check ROE collapse
    if (df['ROE'].iloc[-2:] < roe_exit_threshold).all():
        return True, "ROE Collapse"
        
    # Check FCF turn negative
    if fcf_exit_true and df['FCF'].iloc[-1] < 0:
        return True, "FCF Turned Negative"
        
    return False, "Keep"