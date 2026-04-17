import os
import pickle
import time
import urllib3

import pandas as pd
import requests
import yfinance as yf
urllib3.disable_warnings()

CACHE_FILE = "data_cache.pkl"
CACHE_EXPIRY_DAYS = 7
UNIVERSE_CACHE_FILE = "universe_cache.pkl"
UNIVERSE_CACHE_EXPIRY_DAYS = 30
DEFAULT_TOP_N_PER_SECTOR = 100

C_ROE = 'ROE'
C_OCF = 'OperatingCashFlow'
C_CAPEX = 'CapitalExpenditure'
C_FCF = 'FCF'
C_NET_INCOME = 'NetIncome'

TWSE_LISTED_INFO_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"

STOCK_ID_COLUMNS = ["公司代號", "股票代號", "證券代號", "stock_id"]
STOCK_NAME_COLUMNS = ["公司名稱", "股票名稱", "證券名稱", "stock_name"]
INDUSTRY_COLUMNS = ["產業別", "industry_category", "industry"]
LISTING_DATE_COLUMNS = ["上市日期", "成立日期", "start_date"]


def _pick_first_available(df, candidates, default=None):
    for column in candidates:
        if column in df.columns:
            return df[column]
    if default is None:
        return pd.Series(index=df.index, dtype="object")
    return pd.Series([default] * len(df), index=df.index)


def _safe_market_cap(ticker):
    ticker_tw = ticker + ".TW"
    try:
        yf_ticker = yf.Ticker(ticker_tw)
        fast_info = getattr(yf_ticker, "fast_info", None)
        if fast_info:
            market_cap = fast_info.get("market_cap")
            if market_cap:
                return float(market_cap)
        info = yf_ticker.info
        market_cap = info.get("marketCap")
        if market_cap:
            return float(market_cap)
    except Exception:
        return None
    return None


def _load_cached_universe():
    if os.path.exists(UNIVERSE_CACHE_FILE):
        with open(UNIVERSE_CACHE_FILE, "rb") as f:
            cache = pickle.load(f)
        ts = cache.get("timestamp", 0)
        if (time.time() - ts) < (UNIVERSE_CACHE_EXPIRY_DAYS * 86400):
            return cache.get("data")
    return None


def _save_cached_universe(df):
    payload = {"timestamp": time.time(), "data": df}
    with open(UNIVERSE_CACHE_FILE, "wb") as f:
        pickle.dump(payload, f)


def _fetch_twse_stock_info():
    response = requests.get(TWSE_LISTED_INFO_URL, verify=False, timeout=20)
    response.raise_for_status()
    raw = response.json()
    df = pd.DataFrame(raw)
    if df.empty:
        return pd.DataFrame(columns=["stock_id", "stock_name", "industry_category", "listing_date"])

    stock_id = _pick_first_available(df, STOCK_ID_COLUMNS, "")
    stock_name = _pick_first_available(df, STOCK_NAME_COLUMNS, "")
    industry = _pick_first_available(df, INDUSTRY_COLUMNS, "Unknown")
    listing_date = _pick_first_available(df, LISTING_DATE_COLUMNS)

    result = pd.DataFrame(
        {
            "stock_id": stock_id.astype(str).str.strip(),
            "stock_name": stock_name.astype(str).str.strip(),
            "industry_category": industry.astype(str).str.strip().replace("", "Unknown"),
            "listing_date": pd.to_datetime(listing_date, errors="coerce"),
        }
    )

    result = result[result["stock_id"].str.fullmatch(r"\d{4}", na=False)]
    result = result[~result["stock_name"].str.contains("KY", case=False, na=False)]
    return result.reset_index(drop=True)

class CacheManager:
    @staticmethod
    def load():
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, 'rb') as f:
                cache = pickle.load(f)
                ts = cache.get('timestamp', 0)
                if (time.time() - ts) < (CACHE_EXPIRY_DAYS * 86400):
                    return cache.get('data', {})
        return {}

    @staticmethod
    def save(data):
        cache = {
            'timestamp': time.time(),
            'data': data
        }
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(cache, f)

def fetch_twse_daily_stats():
    """
    Fetches Daily P/E, P/B and Yield for the whole market from TWSE OpenAPI.
    """
    url = "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL"
    try:
        r = requests.get(url, verify=False, timeout=10)
        data = r.json()
        df = pd.DataFrame(data)
        # Standardize columns: PEratio, PBratio, DividendYield, Code
        df = df.rename(columns={'Code': 'stock_id', 'PEratio': 'PE', 'PBratio': 'PB', 'DividendYield': 'Yield'})
        df['PE'] = pd.to_numeric(df['PE'], errors='coerce')
        df['PB'] = pd.to_numeric(df['PB'], errors='coerce')
        df['Yield'] = pd.to_numeric(df['Yield'], errors='coerce')
        return df[['stock_id', 'PE', 'PB', 'Yield']].set_index('stock_id')
    except Exception as e:
        print(f"TWSE OpenAPI Error: {e}")
        return pd.DataFrame()

def get_stock_universe(top_n_per_sector=DEFAULT_TOP_N_PER_SECTOR, min_listing_years=10, force_refresh=False):
    if not force_refresh:
        cached = _load_cached_universe()
        if cached is not None:
            return (
                cached.sort_values(["industry_category", "market_cap"], ascending=[True, False])
                .groupby("industry_category", dropna=False)
                .head(top_n_per_sector)
                .reset_index(drop=True)
            )

    info = _fetch_twse_stock_info()
    if info.empty:
        return pd.DataFrame(columns=["stock_id", "stock_name", "industry_category", "listing_date", "market_cap"])

    listing_age_years = (
        (pd.Timestamp.today().normalize() - info["listing_date"]).dt.days / 365.25
    )
    info["listing_age_years"] = listing_age_years
    info = info[info["listing_age_years"] >= min_listing_years].copy()

    market_caps = []
    for ticker in info["stock_id"]:
        market_caps.append(_safe_market_cap(ticker))
    info["market_cap"] = market_caps
    info["market_cap"] = pd.to_numeric(info["market_cap"], errors="coerce").fillna(0.0)

    ranked = info.sort_values(
        ["industry_category", "market_cap", "stock_id"],
        ascending=[True, False, True],
    ).reset_index(drop=True)
    _save_cached_universe(ranked)

    return ranked.groupby("industry_category", dropna=False).head(top_n_per_sector).reset_index(drop=True)

def get_financials(ticker):
    """
    Fetch annual financials from Yahoo Finance.
    """
    ticker_tw = ticker + ".TW"
    t = yf.Ticker(ticker_tw)
    
    income = t.financials.T
    cashflow = t.cashflow.T
    balance = t.balance_sheet.T
    
    if income.empty or cashflow.empty or balance.empty:
        return pd.DataFrame()

    df = pd.DataFrame(index=income.index)
    df.index.name = "date"
    
    df[C_NET_INCOME] = income.get('Net Income', 0)
    df[C_OCF] = cashflow.get('Operating Cash Flow', 0)
    df[C_CAPEX] = cashflow.get('Capital Expenditure', 0)
    
    equity = balance.get('Stockholders Equity', 0)
    if isinstance(equity, pd.Series):
        df = df.join(equity.to_frame('Equity'), how='left')
        df[C_ROE] = (df[C_NET_INCOME] / df['Equity'] * 100).fillna(0)
    else:
        df[C_ROE] = 0
        
    df = df.reset_index()
    df = df.sort_values("date").reset_index(drop=True)
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    df[C_FCF] = df[C_OCF] - df[C_CAPEX].abs()
    
    return df

def get_historical_valuation(ticker, financials_df):
    """
    Get 10-year Median PE/PB for self-comparison.
    Uses Yahoo Finance historical prices for back-calculation.
    """
    ticker_tw = ticker + ".TW"
    t = yf.Ticker(ticker_tw)
    shares = t.info.get('sharesOutstanding')
    if not shares: return pd.DataFrame()

    val_data = []
    for _, row in financials_df.iterrows():
        try:
            date_str = row['date']
            hist = t.history(start=date_str, periods=5)
            if hist.empty: continue
            price = hist['Close'].iloc[0]
            mkt_cap = price * shares
            pe = mkt_cap / row[C_NET_INCOME] if row[C_NET_INCOME] > 0 else None
            pb = mkt_cap / row['Equity'] if row['Equity'] > 0 else None
            val_data.append({'date': date_str, 'PE': pe, 'PB': pb})
        except: continue
    return pd.DataFrame(val_data)

def get_industry_info(ticker):
    """
    Fetch industry/sector info from YFinance.
    """
    info = yf.Ticker(ticker + ".TW").info
    return info.get('sector', 'Unknown'), info.get('longName', ticker)
