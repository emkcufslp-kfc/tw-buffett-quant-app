import pandas as pd

def validate_financial_data(df):
    if df is None:
        return False
    if len(df) < 10:
        return False
    if df.isna().sum().sum() > 0.3 * df.size:
        return False
    return True