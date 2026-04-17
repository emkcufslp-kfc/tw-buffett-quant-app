import pandas as pd
import yfinance as yf
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# ------------------------------------------------------------------------
# TSE 150 Universe (Taiwan 50 + MidCap 100) - Main Exchange (No OTC)
# Compiled from FTSE TWSE Ticker List
# ------------------------------------------------------------------------
TSE_150_TICKERS = [
    '2330', '2317', '2454', '2308', '2881', '2882', '2412', '2303', '2886', '2891',
    '3711', '2884', '1216', '2892', '2408', '2885', '2382', '2409', '2357', '2002',
    '1301', '1303', '1326', '6505', '2880', '2883', '2379', '2603', '2474', '2327',
    '2887', '2912', '1101', '2890', '1402', '2609', '2301', '1102', '2207', '2801',
    '2345', '2615', '1227', '5880', '2888', '5871', '1590', '4966', '3034', '3037',
    '2324', '1605', '2618', '2610', '2353', '2352', '1476', '2377', '2301', '3231',
    '2356', '2376', '2354', '2409', '3481', '3045', '4904', '2408', '2603', '2609',
    '2615', '2327', '2379', '2345', '2313', '2355', '2383', '2385', '2449', '2451',
    '2474', '3005', '3017', '3023', '3034', '3035', '3037', '3044', '3406', '3443',
    '3532', '3533', '3596', '3653', '3661', '3702', '4414', '4919', '4927', '4938',
    '4958', '4961', '5269', '5434', '6176', '6205', '6213', '6239', '6271', '6278',
    '6409', '6414', '6415', '6456', '6669', '8046', '8081', '8150', '8210', '8454',
    '9904', '9910', '9921', '9945', '1102', '1101', '1210', '1301', '1303', '1326',
    '1402', '1101', '2105', '2002', '2601', '2603', '2609', '2610', '2618', '2633',
    '2707', '2912', '9917', '9921', '9933'
]

# Standardize names for strategy logic
C_ROE = 'ROE'
C_OCF = 'OperatingCashFlow'
C_CAPEX = 'CapitalExpenditure'
C_FCF = 'FCF'
C_NET_INCOME = 'NetIncome'

def get_stock_universe():
    """
    Returns the fixed TSE 150 universe. No longer relies on FinMind.
    """
    # Deduplicate
    unique_tickers = sorted(list(set(TSE_150_TICKERS)))
    df = pd.DataFrame({
        'stock_id': unique_tickers,
        'industry_category': ['Main Exchange (TSE)'] * len(unique_tickers)
    })
    return df

def get_financials(ticker):
    """
    Pure yfinance implementation for fundamental extraction.
    """
    ticker_tw = ticker + ".TW"
    t = yf.Ticker(ticker_tw)
    
    # 1. Fetch Statements (Annual)
    income = t.financials.T
    cashflow = t.cashflow.T
    balance = t.balance_sheet.T
    
    if income.empty or cashflow.empty or balance.empty:
        raise RuntimeError(f"YFinance returned incomplete data for {ticker}")

    # 2. Extract and Align Keys
    # Note: Column names in yfinance are Case-Sensitive
    df = pd.DataFrame(index=income.index)
    df.index.name = "date"
    
    # Financial fields mapping
    df[C_NET_INCOME] = income.get('Net Income', 0)
    df[C_OCF] = cashflow.get('Operating Cash Flow', 0)
    df[C_CAPEX] = cashflow.get('Capital Expenditure', 0)
    
    # Equity for ROE calculation
    equity = balance.get('Stockholders Equity', 0)
    if isinstance(equity, pd.Series):
        df = df.join(equity.to_frame('Equity'), how='left')
        df[C_ROE] = (df[C_NET_INCOME] / df['Equity'] * 100).fillna(0)
    else:
        df[C_ROE] = 0
        
    df = df.reset_index()
    # Format date for consistency
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    
    # Calculate FCF
    df[C_FCF] = df[C_OCF] - df[C_CAPEX].abs()
    
    return df

def get_price_history(ticker):
    ticker_tw = ticker + ".TW"
    data = yf.download(ticker_tw, start="2006-01-01", progress=False)
    # Ensure MultiIndex columns are handled if yfinance returns them
    if isinstance(data.columns, pd.MultiIndex):
        data.columns = data.columns.get_level_values(0)
    return data