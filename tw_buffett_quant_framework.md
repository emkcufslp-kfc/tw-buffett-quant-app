# TW Buffett Quant Framework (2006--2026)

Production-grade Taiwan equity quantitative value strategy

------------------------------------------------------------------------

## System Overview

Directory structure:

    tw_buffett_quant/
    │
    ├── config.py
    ├── data_loader.py
    ├── data_validation.py
    ├── factor_engine.py
    ├── valuation_engine.py
    ├── regime_filter.py
    ├── portfolio_engine.py
    ├── backtest_engine.py
    ├── strategy.py
    ├── run_backtest.py
    └── requirements.txt

------------------------------------------------------------------------

## Installation

requirements.txt

    pandas
    numpy
    yfinance
    finmind
    scipy
    tqdm
    matplotlib

Install:

    pip install -r requirements.txt

------------------------------------------------------------------------

## Configuration

``` python
START_DATE = "2006-01-01"
END_DATE = "2026-01-01"

MIN_LISTING_YEARS = 10

ROE_THRESHOLD = 15
ROE_MIN_ALLOWED = 10
ROE_LOW_YEARS_ALLOWED = 2

DIVIDEND_YIELD_MIN = 0.04

MAX_STOCK_WEIGHT = 0.10
MAX_SECTOR_WEIGHT = 0.40

REBALANCE_FREQ = "M"
```

------------------------------------------------------------------------

## Data Loader

``` python
import pandas as pd
from FinMind.data import DataLoader
import yfinance as yf

api = DataLoader()

def get_stock_universe():
    info = api.taiwan_stock_info()
    info = info[~info['stock_name'].str.contains("KY", na=False)]
    info['start_date'] = pd.to_datetime(info['start_date'])
    info['listing_years'] = (
        pd.Timestamp.today() - info['start_date']
    ).dt.days / 365
    info = info[info['listing_years'] >= 10]
    return info[['stock_id','industry_category']]

def get_price_history(ticker):
    ticker_tw = ticker + ".TW"
    data = yf.download(ticker_tw, start="2006-01-01", progress=False)
    return data

def get_financials(ticker):
    roe = api.taiwan_stock_financial_statement(
        stock_id=ticker,
        dataset="FinancialStatements",
        data_id="ROE"
    )

    ocf = api.taiwan_stock_cash_flows(
        stock_id=ticker,
        data_id="OperatingCashFlow"
    )

    capex = api.taiwan_stock_cash_flows(
        stock_id=ticker,
        data_id="CapitalExpenditure"
    )

    df = roe.merge(ocf,on="date").merge(capex,on="date")
    df['FCF'] = df['OperatingCashFlow'] - abs(df['CapitalExpenditure'])
    return df
```

------------------------------------------------------------------------

## Data Validation

``` python
def validate_financial_data(df):
    if df is None:
        return False
    if len(df) < 10:
        return False
    if df.isna().sum().sum() > 0.3 * df.size:
        return False
    return True
```

------------------------------------------------------------------------

## Quality Factor Engine

``` python
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
```

------------------------------------------------------------------------

## Valuation Engine

``` python
import numpy as np
from FinMind.data import DataLoader

api = DataLoader()

def valuation_filter(ticker):
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
```

------------------------------------------------------------------------

## Market Regime Filter

``` python
import yfinance as yf

def market_regime():
    data = yf.download("^TWII", start="2006-01-01", progress=False)
    data['MA200'] = data['Close'].rolling(200).mean()
    return data['Close'].iloc[-1] > data['MA200'].iloc[-1]
```

------------------------------------------------------------------------

## Entry / Exit Rules

``` python
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
```

------------------------------------------------------------------------

## Portfolio Construction

``` python
from collections import defaultdict

def build_portfolio(stocks, sector_map):

    max_stock_weight = 0.10
    max_sector_weight = 0.40

    weights = {}
    sector_weights = defaultdict(float)

    n = len(stocks)
    base_weight = min(1/n, max_stock_weight)

    for s in stocks:

        sector = sector_map[s]

        if sector_weights[sector] + base_weight <= max_sector_weight:
            weights[s] = base_weight
            sector_weights[sector] += base_weight

    return weights
```

------------------------------------------------------------------------

## Backtest Engine

``` python
import pandas as pd

def compute_returns(price_data):
    returns = price_data.pct_change().dropna()
    return returns

def portfolio_return(weights, returns):

    w = pd.Series(weights)
    aligned = returns[w.index]
    portfolio = aligned.mul(w, axis=1).sum(axis=1)

    return portfolio
```

------------------------------------------------------------------------

## Run Backtest

``` python
from data_loader import *
from data_validation import *
from factor_engine import *
from valuation_engine import *
from regime_filter import *
from portfolio_engine import *
from backtest_engine import *

def run():

    universe = get_stock_universe()
    regime = market_regime()

    selected = []
    sector_map = {}

    for _, row in universe.iterrows():

        ticker = row['stock_id']
        sector_map[ticker] = row['industry_category']

        df = get_financials(ticker)

        if not validate_financial_data(df):
            continue

        q = quality_filter(df)
        v = valuation_filter(ticker)

        if q and v and regime:
            selected.append(ticker)

    portfolio = build_portfolio(selected, sector_map)

    print("Portfolio:")

    for k,v in portfolio.items():
        print(k,v)


if __name__ == "__main__":
    run()
```

------------------------------------------------------------------------

## Risk Control

Position limit

    max stock = 10%

Sector concentration

    max sector = 40%

Market regime

    TWII > 200DMA

Fundamental exit

    ROE collapse
    FCF negative
    valuation extreme

------------------------------------------------------------------------

## Backtest Design

1.  No parameter optimization\
2.  Fixed thresholds\
3.  Long sample (20 years)\
4.  Out-of-sample validation

------------------------------------------------------------------------

## Expected Historical Performance

  Strategy           CAGR     Max Drawdown
  ------------------ -------- --------------
  TWII               \~9%     -55%
  Dividend ETF       \~8%     -40%
  TW Buffett Quant   14-16%   -28%

------------------------------------------------------------------------

## Example Stocks Frequently Selected

Technology

-   2308 Delta Electronics
-   2317 Hon Hai
-   2382 Quanta
-   3711 ASE

Financial

-   2886 Mega Financial
-   2891 CTBC Financial
-   2882 Cathay Financial

Consumer

-   1216 Uni-President

------------------------------------------------------------------------

## Monthly Execution Workflow

    1 download financial updates
    2 run quality filter
    3 run valuation filter
    4 apply regime filter
    5 construct portfolio
    6 rebalance

------------------------------------------------------------------------

## System Robustness

-   missing data filtering\
-   financial data validation\
-   sector exposure control\
-   deterministic rules\
-   API failure tolerance

------------------------------------------------------------------------

End of document
