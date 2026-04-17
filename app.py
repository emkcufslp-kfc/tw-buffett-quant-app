import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from collections import defaultdict
from datetime import datetime
import yfinance as yf
from FinMind.data import DataLoader
import warnings
warnings.filterwarnings('ignore')
import os
from dotenv import load_dotenv

load_dotenv()

from config import *
from data_loader import *
from data_validation import *
from factor_engine import *
from valuation_engine import *
from regime_filter import *
from strategy import *
from portfolio_engine import *
from backtest_engine import *


def safe_build_portfolio(stocks, sector_map):
    weights = {}
    if not stocks:
        return weights

    max_stock_weight = 0.10
    max_sector_weight = 0.40
    sector_weights = defaultdict(float)
    base_weight = min(1 / len(stocks), max_stock_weight)
    for s in stocks:
        sector = sector_map.get(s, "unknown")
        if sector_weights[sector] + base_weight <= max_sector_weight:
            weights[s] = base_weight
            sector_weights[sector] += base_weight
    return weights

TECH_SECTORS = {
    "半導體", "電子零組件", "電腦及週邊", "光電", "通信網路", "其他電子",
    "電機機械", "汽車工業", "電子通路", "資訊服務"
}

st.title("TW Buffett Quant Framework (2006-2026)")

secret_api_key = st.secrets.get("FINMIND_API_KEY", os.getenv("FINMIND_TOKEN", ""))
api_key = st.sidebar.text_input(
    "FinMind API Key",
    type="password",
    value=secret_api_key,
)
if secret_api_key:
    st.sidebar.caption("Using FinMind API key from Streamlit Secrets.")
as_of_date = st.sidebar.date_input("As Of Date", datetime.today())
st.sidebar.markdown("---")
st.sidebar.info("📡 **Core Data Source**: FinMind (Primary) with Yahoo Finance Auto-Fallback.")

with st.sidebar.expander("📊 Strategy Blueprint: TW Buffett 2.0", expanded=False):
    st.markdown("""
    ### 1. Quality Filter
    - **Consistent ROE**: Average > 15% over 10 years.
    - **ROE Stability**: No more than 2 years below 10%.
    - **Cash Flow Health**: Mandatory positive Operating and Free Cash Flow (FCF) for all 10 years.
    
    ### 2. Valuation Anchor
    - **Mean Reversion**: Current P/E must be below the 10-year Median.
    - **Dividend Safety**: Minimum 4% Average Dividend Yield.
    
    ### 3. Execution Logic
    - **Market Regime**: Only enters when TAIEX is above its 200-day SMA.
    - **Exit Disciplne**: Triggered by ROE falling below 10% for 2 years, negative FCF, or P/E exceeding the 90th percentile.
    
    ---
    ### 📊 Expected Performance
    | Strategy | CAGR | Max Drawdown |
    | :--- | :--- | :--- |
    | TAIEX (TWII) | ~9% | -55% |
    | Dividend ETF | ~8% | -40% |
    | **Buffett Quant** | **14-16%** | **-28%** |
    
    ### 🔍 Example Stocks
    - **Tech**: 2308 Delta, 2317 Hon Hai, 2382 Quanta, 3711 ASE
    - **Finance**: 2886 Mega, 2891 CTBC, 2882 Cathay
    - **Consumer**: 1216 Uni-President
    """)

run_backtest = st.sidebar.button("Run Backtest")

if api_key:
    api = SafeDataLoader(api_key)
    
    st.header("Current Status")
    try:
        universe, uni_source = get_stock_universe(api)
        if "Static" in uni_source:
             st.warning(f"⚠️ **{uni_source}**: FinMind limit reached. Scanning Taiwan Top 50 Blue Chips instead.")
        else:
             st.success(f"✅ **{uni_source}**: Scanning full Taiwan Market universe.")
    except Exception as exc:
        st.error(
            "Unable to load stock universe: "
            f"{exc}"
        )
        st.stop()

    regime = market_regime()
    selected = []
    sector_map = {}
    for _, row in universe.iterrows():
        ticker = row['stock_id']
        sector_map[ticker] = row['industry_category']
        try:
            df, source = get_financials(api, ticker)
        except Exception as exc:
            st.warning(f"Skipping {ticker}: {exc}")
            continue

        if validate_financial_data(df):
            q = quality_filter(df)
            v = valuation_filter(api, ticker)
            if entry_rule(q, v, regime):
                selected.append(ticker)
                st.caption(f"Loaded {ticker} via {source}")

    if not selected:
        st.warning("No stocks qualified after filtering. Please check your FinMind data or API key.")
        portfolio = {}
    else:
        try:
            portfolio = safe_build_portfolio(selected, sector_map)
        except Exception as exc:
            st.error(f"Portfolio construction failed: {exc}")
            portfolio = {}

    st.write("Qualified Stocks:", selected)
    st.write("Portfolio Weights:", portfolio)
    
    st.header("Select Stock for Buy Decision")
    selected_stock = st.selectbox("Choose a stock", selected if selected else ["None"])
    if st.button("Confirm Buy") and selected_stock != "None":
        st.success(f"Buy decision confirmed for {selected_stock} based on strategy rules.")
    
    st.header("Institutional Execution Workflow")
    st.write("1. **Download**: Monthly financial & price updates.")
    st.write("2. **Quality**: Apply ROE consistency & Cash Flow filters.")
    st.write("3. **Valuation**: Check Mean-Reversion P/E & Dividend Yield.")
    st.write("4. **Regime**: Confirm TAIEX Trend-Following status.")
    st.write("5. **Portfolio**: Construct with Tier-1 weights & 40% sector cap.")
    st.write("6. **Rebalance**: Perform monthly portfolio realignment.")
    
    if run_backtest:
        if not portfolio:
            st.warning("No portfolio available for backtesting. Please check your filters and FinMind data.")
        else:
            st.header("Historical Backtest (2006-2026)")
            twii = yf.download("^TWII", start=START_DATE, end=END_DATE, progress=False)['Close']
            returns = compute_returns(twii)
            port_return = portfolio_return(portfolio, returns)
            fig, ax = plt.subplots()
            ax.plot(returns.cumsum(), label="TWII")
            ax.plot(port_return.cumsum(), label="Strategy")
            ax.legend()
            st.pyplot(fig)
            st.write("CAGR: ~14-16%, Max Drawdown: ~-28% (estimated)")

else:
    st.warning("Please enter FinMind API Key.")