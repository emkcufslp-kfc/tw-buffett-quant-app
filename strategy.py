import numpy as np

def entry_rule(quality, valuation, regime):
    return quality and valuation and regime

def exit_rule(df, pe_series):
    roe = df['ROE'].tail(2)
    cond1 = (roe < 10).all()
    fcf = df['FCF'].iloc[-1]
    cond2 = fcf < 0
    pe90 = np.percentile(pe_series.dropna(), 90)
    cond3 = pe_series.iloc[-1] > pe90
    return cond1 or cond2 or cond3