import streamlit as st
import pandas as pd
from datetime import datetime
from data_loader import get_stock_universe, get_financials, get_price_history
from data_validation import validate_financial_data
from factor_engine import quality_filter
from valuation_engine import valuation_filter
from regime_filter import market_regime
from strategy import entry_rule
from portfolio_engine import build_portfolio

# Page Config
st.set_page_config(page_title="TW Buffett 2.0 L", layout="wide")

# Sidebar
st.sidebar.title("💎 TW Buffett 2.0 L")
st.sidebar.caption("Institutional Pure YFinance Edition")

as_of_date = st.sidebar.date_input("As Of Date", datetime.today())
st.sidebar.info("📡 **Data Source**: Unified Yahoo Finance (No API Key Required).")

with st.sidebar.expander("📊 Strategy Blueprint: TW Buffett 2.0", expanded=False):
    st.markdown("""
    ### 1. Quality Filter
    - **Consistent ROE**: Average > 15% over 10 years.
    - **ROE Stability**: No more than 2 years below 10%.
    - **Cash Flow Health**: Mandatory positive Operating and Free Cash Flow (FCF) for all 10 years.
    
    ### 2. Valuation Anchor
    - **P/E Hurdle**: Current P/E must be below 25.
    - **Dividend Yield**: Minimum 3% trailing yield.
    
    ### 3. Execution Logic
    - **Market Regime**: Only enters when TAIEX is above its 200-day SMA.
    """)

run_backtest = st.sidebar.button("Run Quantitative Check")

# Main Dashboard
st.title("TW Buffett Quant Framework")

if run_backtest:
    st.header("Quantitative Scan Status")
    
    # 1. Get Universe
    universe_df = get_stock_universe()
    universe = universe_df['stock_id'].tolist()
    sector_map = dict(zip(universe_df['stock_id'], universe_df['industry_category']))
    
    # 2. Check Regime
    regime = market_regime()
    if not regime:
        st.warning("⚠️ Market Regime: TAIEX is below 200DMA. Defensive Stance (No New Entries).")
    else:
        st.success("✅ Market Regime: TAIEX is bullish. Value Scanning Active.")

    selected = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, ticker in enumerate(universe):
        status_text.text(f"Scanning {ticker}... ({i+1}/{len(universe)})")
        progress_bar.progress((i + 1) / len(universe))
        
        try:
            df = get_financials(ticker)
            if validate_financial_data(df):
                q = quality_filter(df)
                v = valuation_filter(ticker)
                if entry_rule(q, v, regime):
                    selected.append(ticker)
                    st.caption(f"💎 Stock Qualified: {ticker}")
        except:
            continue

    status_text.text("Scan Complete!")

    if not selected:
        st.warning("No stocks currently meet all 10-year consistency criteria.")
    else:
        st.header("Institutional Portfolio Construction")
        portfolio = build_portfolio(selected, sector_map)
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Qualified Stocks", len(selected))
            st.table(pd.DataFrame(list(portfolio.items()), columns=["Ticker", "Weight"]))
            
        with col2:
            st.info("The selected stocks above have passed the 10-year Buffett consistency test. Weights are optimized by sector exposure.")
        
    st.header("Institutional Execution Workflow")
    st.write("1. **Download**: Annual financials and historical prices (YFinance).")
    st.write("2. **Quality**: Apply ROE consistency & Cash Flow filters.")
    st.write("3. **Valuation**: Apply P/E and Yield hurdles.")
    st.write("4. **Regime**: Confirm TAIEX Trend-Following status.")
    st.write("5. **Portfolio**: Balanced weighting with sector concentration caps.")
    st.write("6. **Rebalance**: Portfolio realignment.")

else:
    st.info("👈 Click **Run Quantitative Check** in the sidebar to begin scanning the TSE 150 Institutional Universe.")