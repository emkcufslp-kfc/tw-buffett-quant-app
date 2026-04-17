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

def _pivot_financial_df(df):
    if df is None or df.empty:
        return pd.DataFrame()

    if 'date' in df.columns and 'value' in df.columns:
        pivot_column = 'type' if 'type' in df.columns else 'origin_name' if 'origin_name' in df.columns else None
        if pivot_column is not None:
            df = df.pivot(index='date', columns=pivot_column, values='value')
            df.columns = [col if isinstance(col, str) else str(col) for col in df.columns]
            df = df.reset_index()
            return df

    return df


def get_financials(api, ticker):
    try:
        roe_df = api.taiwan_stock_financial_statement(stock_id=ticker)
        cash_df = api.taiwan_stock_cash_flows_statement(stock_id=ticker)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to load financials for {ticker}. "
            "Please verify the FinMind API token and ticker value."
        ) from exc

    roe_df = _pivot_financial_df(roe_df)
    cash_df = _pivot_financial_df(cash_df)

    if roe_df.empty or cash_df.empty:
        raise RuntimeError(
            f"No financial or cash flow data returned for {ticker}."
        )

    if 'date' not in roe_df.columns or 'date' not in cash_df.columns:
        raise RuntimeError(
            f"Unexpected financial data format for {ticker}."
        )

    merged = pd.merge(roe_df, cash_df, on='date', how='inner')
    if merged.empty:
        raise RuntimeError(
            f"No merged financial data available for {ticker}."
        )

    if 'OperatingCashFlow' not in merged.columns or 'CapitalExpenditure' not in merged.columns:
        raise RuntimeError(
            f"Missing cash flow fields for {ticker}."
        )

    merged['FCF'] = merged['OperatingCashFlow'] - merged['CapitalExpenditure'].abs()
    return merged