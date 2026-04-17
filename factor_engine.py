import numpy as np

def quality_filter(df):
    df = df.tail(10)
    roe = df['ROE']
    cond1 = roe.mean() > 15
    cond2 = (roe < 10).sum() <= 2
    ocf = df['OperatingCashFlow']
    fcf = df['FCF']
    cond3 = (ocf > 0).all()
    cond4 = (fcf > 0).all()
    return cond1 and cond2 and cond3 and cond4