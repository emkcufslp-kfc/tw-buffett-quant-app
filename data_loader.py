import pandas as pd
from FinMind.data import DataLoader
import yfinance as yf

def get_stock_universe(api):
    try:
        info = api.taiwan_stock_info()
    except Exception as exc:
        raise RuntimeError(
            "Failed to load FinMind stock universe. "
            "Please verify your FinMind API token and network access."
        ) from exc

    if info is None or info.empty:
        raise RuntimeError(
            "FinMind taiwan_stock_info returned no data. "
            "Please verify your FinMind API token and try again."
        )

    if 'stock_name' in info.columns:
        info = info[~info['stock_name'].str.contains("KY", na=False)]

    date_column = None
    for candidate in ['start_date', 'listing_date', 'issue_date', '上市日期', '上市日']:
        if candidate in info.columns:
            date_column = candidate
            break

    if date_column is not None:
        info['start_date'] = pd.to_datetime(info[date_column], errors='coerce')
        if info['start_date'].notna().any():
            info['listing_years'] = (pd.Timestamp.today() - info['start_date']).dt.days / 365
            info = info[info['listing_years'] >= 10]
    else:
        # No date column found, keep the universe but continue without the 10-year filter
        info = info.copy()

    expected_columns = {'stock_id', 'industry_category'}
    missing = expected_columns - set(info.columns)
    if missing:
        raise RuntimeError(
            "FinMind response is missing expected columns: "
            f"{', '.join(sorted(missing))}. "
            f"Available columns: {', '.join(info.columns)}"
        )

    return info[['stock_id', 'industry_category']].drop_duplicates()

def get_price_history(ticker):
    ticker_tw = ticker + ".TW"
    data = yf.download(ticker_tw, start="2006-01-01", progress=False)
    return data

def get_financials(api, ticker):
    roe = api.taiwan_stock_financial_statement(stock_id=ticker, dataset="FinancialStatements", data_id="ROE")
    ocf = api.taiwan_stock_cash_flows(stock_id=ticker, data_id="OperatingCashFlow")
    capex = api.taiwan_stock_cash_flows(stock_id=ticker, data_id="CapitalExpenditure")
    df = roe.merge(ocf, on="date").merge(capex, on="date")
    df['FCF'] = df['OperatingCashFlow'] - abs(df['CapitalExpenditure'])
    return df