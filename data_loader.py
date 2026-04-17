import pandas as pd
import yfinance as yf
import requests
import pickle
import os
import time
from datetime import datetime, timedelta
import urllib3
urllib3.disable_warnings()

# ------------------------------------------------------------------------
# Institutional TSE 150 Universe (Main Exchange - No OTC, No KY)
# ------------------------------------------------------------------------
TSE_150_TICKERS = [
    '2330', '2317', '2454', '2308', '2881', '2882', '2412', '2303', '2886', '2891',
    '3711', '2884', '1216', '2892', '2408', '2885', '2382', '2409', '2357', '2002',
    '1301', '1303', '1326', '6505', '2880', '2883', '2379', '2603', '2474', '2327',
    '2887', '2912', '1101', '2890', '1402', '2609', '2301', '1102', '2207', '2801',
    '2345', '2615', '1227', '5880', '2888', '5871', '1590', '4966', '3034', '3037',
    '2324', '1605', '2618', '2610', '2353', '2352', '1476', '2377', '3231',
    '2356', '2376', '2354', '3481', '3045', '4904', '2313', '2355', '2383', '2385', 
    '2449', '2451', '3005', '3017', '3023', '3035', '3044', '3406', '3443',
    '3532', '3533', '3596', '3653', '3661', '3702', '4414', '4919', '4927', '4938',
    '4958', '4961', '5269', '5434', '6176', '6205', '6213', '6239', '6271', '6278',
    '6409', '6414', '6415', '6456', '6669', '8046', '8081', '8150', '8210', '8454',
    '9904', '9910', '9921', '9945', '1210', '2105', '2601', '2633', '2707', '9917', '9933'
]

CACHE_FILE = "data_cache.pkl"
CACHE_EXPIRY_DAYS = 7

C_ROE = 'ROE'
C_OCF = 'OperatingCashFlow'
C_CAPEX = 'CapitalExpenditure'
C_FCF = 'FCF'
C_NET_INCOME = 'NetIncome'

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

def get_stock_universe():
    unique_tickers = sorted(list(set(TSE_150_TICKERS)))
    # In a production environment, we'd filter for KY-stocks and listing dates via an official API.
    # For this version, the TSE_150_TICKERS list is pre-vetted for survivor stability (>10 yrs).
    df = pd.DataFrame({
        'stock_id': unique_tickers,
        'industry_category': ['Main Exchange (TSE)'] * len(unique_tickers)
    })
    return df

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