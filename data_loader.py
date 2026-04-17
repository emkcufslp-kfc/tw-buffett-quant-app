import os
import pickle
import re
import time
try:
    import urllib3
except Exception:
    urllib3 = None

import pandas as pd
import requests
import yfinance as yf

if urllib3 is not None and hasattr(urllib3, "disable_warnings"):
    urllib3.disable_warnings()

CACHE_FILE = "data_cache.pkl"
CACHE_EXPIRY_DAYS = 7
UNIVERSE_CACHE_FILE = "universe_cache.pkl"
UNIVERSE_CACHE_EXPIRY_DAYS = 30
MONTHLY_REVENUE_CACHE_FILE = "monthly_revenue_cache.pkl"
MONTHLY_REVENUE_CACHE_EXPIRY_HOURS = 24
DEFAULT_TOP_N_PER_SECTOR = 100

C_ROE = "ROE"
C_OCF = "OperatingCashFlow"
C_CAPEX = "CapitalExpenditure"
C_FCF = "FCF"
C_NET_INCOME = "NetIncome"

TWSE_LISTED_INFO_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
MOPS_MONTHLY_REVENUE_URL_TEMPLATE = "https://mops.twse.com.tw/nas/t21/sii/t21sc03_{roc_year}_{month}_0.html"

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


def _normalize_industry_name(value):
    text = str(value).strip()
    if not text:
        return "未知"
    text = re.sub(r"^\s*\d+\s*[.\-、．]+\s*", "", text)
    return text or "未知"


def _safe_market_cap(ticker):
    ticker_tw = ticker + ".TW"
    try:
        ticker_obj = yf.Ticker(ticker_tw)
        fast_info = getattr(ticker_obj, "fast_info", None)
        if fast_info:
            market_cap = fast_info.get("market_cap")
            if market_cap:
                return float(market_cap)
        info = ticker_obj.info
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


def _load_monthly_revenue_cache():
    if os.path.exists(MONTHLY_REVENUE_CACHE_FILE):
        with open(MONTHLY_REVENUE_CACHE_FILE, "rb") as f:
            cache = pickle.load(f)
        ts = cache.get("timestamp", 0)
        if (time.time() - ts) < (MONTHLY_REVENUE_CACHE_EXPIRY_HOURS * 3600):
            return cache.get("data")
    return None


def _save_cached_universe(df):
    payload = {"timestamp": time.time(), "data": df}
    with open(UNIVERSE_CACHE_FILE, "wb") as f:
        pickle.dump(payload, f)


def _save_monthly_revenue_cache(df):
    payload = {"timestamp": time.time(), "data": df}
    with open(MONTHLY_REVENUE_CACHE_FILE, "wb") as f:
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
    industry = _pick_first_available(df, INDUSTRY_COLUMNS, "未知")
    listing_date = _pick_first_available(df, LISTING_DATE_COLUMNS)

    result = pd.DataFrame(
        {
            "stock_id": stock_id.astype(str).str.strip(),
            "stock_name": stock_name.astype(str).str.strip(),
            "industry_category": industry.map(_normalize_industry_name),
            "listing_date": pd.to_datetime(listing_date, errors="coerce"),
        }
    )
    result = result[result["stock_id"].str.fullmatch(r"\d{4}", na=False)]
    result = result[~result["stock_name"].str.contains("KY", case=False, na=False)]
    return result.reset_index(drop=True)


def _parse_monthly_revenue_table(df, year, month):
    if df.empty:
        return pd.DataFrame()

    columns = [str(col).strip() for col in df.columns]
    df.columns = columns

    stock_id_col = next((col for col in columns if "公司代號" in col or "股票代號" in col), None)
    revenue_col = next((col for col in columns if "當月營收" in col), None)
    yoy_col = next((col for col in columns if "去年同月增減" in col), None)
    stock_name_col = next((col for col in columns if "公司名稱" in col or "公司簡稱" in col), None)

    if not stock_id_col or not revenue_col:
        return pd.DataFrame()

    parsed = pd.DataFrame(
        {
            "stock_id": df[stock_id_col].astype(str).str.strip(),
            "stock_name": df[stock_name_col].astype(str).str.strip() if stock_name_col else "",
            "monthly_revenue": pd.to_numeric(
                df[revenue_col].astype(str).str.replace(",", "", regex=False),
                errors="coerce",
            ),
            "revenue_yoy": pd.to_numeric(
                df[yoy_col].astype(str).str.replace("%", "", regex=False) if yoy_col else None,
                errors="coerce",
            ),
            "year": year,
            "month": month,
        }
    )

    parsed = parsed[parsed["stock_id"].str.fullmatch(r"\d{4}", na=False)]
    parsed["period"] = pd.to_datetime(
        parsed["year"].astype(str) + "-" + parsed["month"].astype(str).str.zfill(2) + "-01",
        errors="coerce",
    )
    return parsed.dropna(subset=["period"]).reset_index(drop=True)


def fetch_monthly_revenue_history(months=15, force_refresh=False):
    if not force_refresh:
        cached = _load_monthly_revenue_cache()
        if cached is not None:
            return cached

    today = pd.Timestamp.today().normalize()
    month_starts = pd.date_range(end=today, periods=months, freq="MS")
    frames = []

    for month_start in month_starts:
        roc_year = month_start.year - 1911
        month = month_start.month
        url = MOPS_MONTHLY_REVENUE_URL_TEMPLATE.format(roc_year=roc_year, month=month)
        try:
            response = requests.get(url, verify=False, timeout=20)
            response.encoding = "big5"
            tables = pd.read_html(response.text)
            for table in tables:
                parsed = _parse_monthly_revenue_table(table, month_start.year, month)
                if not parsed.empty:
                    frames.append(parsed)
                    break
        except Exception:
            continue

    if not frames:
        return pd.DataFrame(columns=["stock_id", "stock_name", "monthly_revenue", "revenue_yoy", "year", "month", "period"])

    history = pd.concat(frames, ignore_index=True)
    history = history.sort_values(["stock_id", "period"]).drop_duplicates(["stock_id", "period"], keep="last").reset_index(drop=True)
    _save_monthly_revenue_cache(history)
    return history


def get_latest_monthly_revenue_metrics(ticker, revenue_history_df):
    if revenue_history_df.empty:
        return {}

    stock_df = revenue_history_df[revenue_history_df["stock_id"] == ticker].sort_values("period").tail(12).copy()
    if stock_df.empty:
        return {}

    recent_three = stock_df.tail(3)
    latest_row = stock_df.iloc[-1]
    latest_yoy = pd.to_numeric(latest_row.get("revenue_yoy"), errors="coerce")
    avg_3m_yoy = pd.to_numeric(recent_three["revenue_yoy"], errors="coerce").mean()

    return {
        "latest_revenue_month": latest_row["period"].strftime("%Y-%m"),
        "latest_revenue": _safe_number(latest_row.get("monthly_revenue")),
        "latest_revenue_yoy": _safe_number(latest_yoy),
        "avg_3m_revenue_yoy": _safe_number(avg_3m_yoy),
    }


def _safe_number(value):
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


class CacheManager:
    @staticmethod
    def load():
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "rb") as f:
                cache = pickle.load(f)
                ts = cache.get("timestamp", 0)
                if (time.time() - ts) < (CACHE_EXPIRY_DAYS * 86400):
                    return cache.get("data", {})
        return {}

    @staticmethod
    def save(data):
        cache = {"timestamp": time.time(), "data": data}
        with open(CACHE_FILE, "wb") as f:
            pickle.dump(cache, f)


def fetch_twse_daily_stats():
    url = "https://openapi.twse.com.tw/v1/exchangeReport/BWIBBU_ALL"
    try:
        response = requests.get(url, verify=False, timeout=10)
        df = pd.DataFrame(response.json())
        df = df.rename(
            columns={
                "Code": "stock_id",
                "PEratio": "PE",
                "PBratio": "PB",
                "DividendYield": "Yield",
            }
        )
        df["PE"] = pd.to_numeric(df["PE"], errors="coerce")
        df["PB"] = pd.to_numeric(df["PB"], errors="coerce")
        df["Yield"] = pd.to_numeric(df["Yield"], errors="coerce")
        return df[["stock_id", "PE", "PB", "Yield"]].set_index("stock_id")
    except Exception as exc:
        print(f"TWSE OpenAPI Error: {exc}")
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
        return pd.DataFrame(
            columns=["stock_id", "stock_name", "industry_category", "listing_date", "listing_age_years", "market_cap"]
        )

    info["listing_age_years"] = (
        (pd.Timestamp.today().normalize() - info["listing_date"]).dt.days / 365.25
    )
    info = info[info["listing_age_years"] >= min_listing_years].copy()

    market_cap_series = pd.Series(
        [_safe_market_cap(ticker) for ticker in info["stock_id"]],
        index=info.index,
        dtype="float64",
    )
    info["market_cap"] = market_cap_series.fillna(0.0)

    ranked = info.sort_values(
        ["industry_category", "market_cap", "stock_id"],
        ascending=[True, False, True],
    ).reset_index(drop=True)
    _save_cached_universe(ranked)

    return ranked.groupby("industry_category", dropna=False).head(top_n_per_sector).reset_index(drop=True)


def get_universe_reference(top_n_per_sector=DEFAULT_TOP_N_PER_SECTOR, min_listing_years=10, force_refresh=False):
    universe_df = get_stock_universe(
        top_n_per_sector=top_n_per_sector,
        min_listing_years=min_listing_years,
        force_refresh=force_refresh,
    ).copy()
    if universe_df.empty:
        return universe_df

    universe_df["sector_rank"] = (
        universe_df.groupby("industry_category", dropna=False)["market_cap"]
        .rank(method="first", ascending=False)
        .astype(int)
    )
    universe_df["market_cap_billion_twd"] = (universe_df["market_cap"] / 1_000_000_000).round(2)
    universe_df["listing_date"] = pd.to_datetime(universe_df["listing_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    universe_df["listing_age_years"] = universe_df["listing_age_years"].round(1)

    return universe_df[
        [
            "industry_category",
            "sector_rank",
            "stock_id",
            "stock_name",
            "market_cap_billion_twd",
            "listing_date",
            "listing_age_years",
        ]
    ].rename(
        columns={
            "industry_category": "產業別",
            "sector_rank": "產業排名",
            "stock_id": "股票代號",
            "stock_name": "股票名稱",
            "market_cap_billion_twd": "市值(十億台幣)",
            "listing_date": "上市日期",
            "listing_age_years": "上市年數",
        }
    )


def get_financials(ticker):
    ticker_obj = yf.Ticker(ticker + ".TW")
    income = ticker_obj.financials.T
    cashflow = ticker_obj.cashflow.T
    balance = ticker_obj.balance_sheet.T

    if income.empty or cashflow.empty or balance.empty:
        return pd.DataFrame()

    df = pd.DataFrame(index=income.index)
    df.index.name = "date"
    df[C_NET_INCOME] = income.get("Net Income", 0)
    df[C_OCF] = cashflow.get("Operating Cash Flow", 0)
    df[C_CAPEX] = cashflow.get("Capital Expenditure", 0)

    equity = balance.get("Stockholders Equity", 0)
    if isinstance(equity, pd.Series):
        df = df.join(equity.to_frame("Equity"), how="left")
        df[C_ROE] = (df[C_NET_INCOME] / df["Equity"] * 100).fillna(0)
    else:
        df[C_ROE] = 0

    df = df.reset_index()
    df = df.sort_values("date").reset_index(drop=True)
    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df[C_FCF] = df[C_OCF] - df[C_CAPEX].abs()
    return df


def get_historical_valuation(ticker, financials_df):
    ticker_obj = yf.Ticker(ticker + ".TW")
    shares = ticker_obj.info.get("sharesOutstanding")
    if not shares:
        return pd.DataFrame()

    valuation_rows = []
    for _, row in financials_df.iterrows():
        try:
            hist = ticker_obj.history(start=row["date"], periods=5)
            if hist.empty:
                continue
            price = hist["Close"].iloc[0]
            market_cap = price * shares
            pe = market_cap / row[C_NET_INCOME] if row[C_NET_INCOME] > 0 else None
            pb = market_cap / row["Equity"] if row["Equity"] > 0 else None
            valuation_rows.append({"date": row["date"], "PE": pe, "PB": pb})
        except Exception:
            continue
    return pd.DataFrame(valuation_rows)


def get_price_history(ticker, period="1y", interval="1d"):
    try:
        data = yf.download(f"{ticker}.TW", period=period, interval=interval, progress=False, auto_adjust=False)
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        return data.reset_index()
    except Exception:
        return pd.DataFrame()


def get_taiex_history(period="1y", interval="1d"):
    try:
        data = yf.download("^TWII", period=period, interval=interval, progress=False, auto_adjust=False)
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.get_level_values(0)
        return data.reset_index()
    except Exception:
        return pd.DataFrame()


def get_industry_info(ticker):
    info = yf.Ticker(ticker + ".TW").info
    return info.get("sector", "Unknown"), info.get("longName", ticker)
