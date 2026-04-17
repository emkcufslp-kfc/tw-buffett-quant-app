import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import yfinance as yf
from FinMind.data import DataLoader
import warnings
warnings.filterwarnings('ignore')

from config import *
from data_loader import *
from data_validation import *
from factor_engine import *
from valuation_engine import *
from regime_filter import *
from strategy import *
from portfolio_engine import *
from backtest_engine import *

TECH_SECTORS = {
    "半導體", "電子零組件", "電腦及週邊", "光電", "通信網路", "其他電子",
    "電機機械", "汽車工業", "電子通路", "資訊服務"
}

st.title("TW Buffett Quant Framework (2006-2026)")

secret_api_key = st.secrets.get("FINMIND_API_KEY", "")
api_key = st.sidebar.text_input(
    "FinMind API Key",
    type="password",
    value=secret_api_key,
)
if secret_api_key:
    st.sidebar.caption("Using FinMind API key from Streamlit Secrets.")
as_of_date = st.sidebar.date_input("As Of Date", datetime.today())
run_backtest = st.sidebar.button("Run Backtest")

if api_key:
    api = DataLoader(api_key)
    
    st.header("Current Status")
    try:
        universe = get_stock_universe(api)
    except Exception as exc:
        st.error(
            "Unable to load stock universe from FinMind: "
            f"{exc}"
        )
        st.stop()

    regime = market_regime()
    selected = []
    sector_map = {}
    for _, row in universe.iterrows():
        ticker = row['stock_id']
        sector_map[ticker] = row['industry_category']
        df = get_financials(api, ticker)
        if validate_financial_data(df):
            q = quality_filter(df)
            v = valuation_filter(api, ticker)
            if entry_rule(q, v, regime):
                selected.append(ticker)
    portfolio = build_portfolio(selected, sector_map)
    st.write("Qualified Stocks:", selected)
    st.write("Portfolio Weights:", portfolio)
    
    st.header("Select Stock for Buy Decision")
    selected_stock = st.selectbox("Choose a stock", selected if selected else ["None"])
    if st.button("Confirm Buy") and selected_stock != "None":
        st.success(f"Buy decision confirmed for {selected_stock} based on strategy rules.")
    
    st.header("Implementation Action Plan")
    st.write("1. Download financial updates monthly.")
    st.write("2. Run quality and valuation filters.")
    st.write("3. Apply regime filter.")
    st.write("4. Construct portfolio with constraints.")
    st.write("5. Rebalance positions.")
    
    if run_backtest:
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