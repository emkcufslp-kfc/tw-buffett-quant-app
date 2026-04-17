import pandas as pd
import streamlit as st

from data_loader import CacheManager, fetch_twse_daily_stats
from screening import scan_universe


st.set_page_config(page_title="TW Buffett Quant Dashboard", layout="wide")

if "cache" not in st.session_state:
    st.session_state.cache = CacheManager.load()
if "daily_stats" not in st.session_state:
    st.session_state.daily_stats = pd.DataFrame()

st.title("TW Buffett Quant Dashboard")
st.caption("Taiwan value-quality stock screen with live TWSE valuation data and yfinance fundamentals.")

with st.sidebar:
    st.header("Screen Settings")
    roe_avg_tgt = st.slider("Average ROE threshold (%)", 5, 25, 15)
    roe_min_tgt = st.slider("Minimum acceptable ROE (%)", 0, 15, 10)
    roe_min_count = st.slider("Max low-ROE years allowed", 0, 5, 2)
    fcf_years = st.slider("Positive OCF/FCF lookback (years)", 1, 10, 10)
    yield_tgt = st.slider("Dividend yield threshold (%)", 0.0, 8.0, 4.0, 0.5)
    max_stock_w = st.slider("Max stock weight (%)", 5, 20, 10) / 100
    max_sector_w = st.slider("Max sector weight (%)", 10, 60, 40) / 100
    force_refresh = st.checkbox("Refresh cached financial data", value=False)
    refresh_stats = st.button("Refresh TWSE daily stats")
    run_scan = st.button("Run candidate scan", type="primary")

if refresh_stats or st.session_state.daily_stats.empty:
    with st.spinner("Fetching TWSE daily valuation stats..."):
        st.session_state.daily_stats = fetch_twse_daily_stats()

stats_rows = len(st.session_state.daily_stats)
if stats_rows:
    st.sidebar.success(f"Loaded {stats_rows} TWSE valuation rows.")
else:
    st.sidebar.warning("TWSE valuation data is unavailable right now. Valuation checks will fail until it loads.")

st.sidebar.info(f"Cached ticker snapshots: {len(st.session_state.cache)}")

with st.expander("Strategy Logic", expanded=True):
    st.markdown(
        """
        - Quality: high average ROE, limited weak ROE years, and consistently positive operating/free cash flow.
        - Valuation: financials use historical P/B bands, while non-financials use historical P/E plus yield support.
        - Regime: the dashboard reports the broad market regime using the Taiwan index versus its 200-day moving average.
        - Portfolio: weights respect hard per-stock and per-sector caps. Unused capital stays undeployed rather than violating the caps.
        """
    )

if run_scan:
    if st.session_state.daily_stats.empty:
        st.error("Cannot run the scan without TWSE daily valuation stats.")
        st.stop()

    with st.spinner("Scanning the Taiwan stock universe for current candidates..."):
        results = scan_universe(
            cache=st.session_state.cache,
            daily_stats_df=st.session_state.daily_stats,
            force_refresh=force_refresh,
            roe_avg_tgt=roe_avg_tgt,
            roe_min_tgt=roe_min_tgt,
            roe_min_count=roe_min_count,
            fcf_years=fcf_years,
            yield_tgt=yield_tgt,
            max_stock_weight=max_stock_w,
            max_sector_weight=max_sector_w,
        )
        st.session_state.cache = results["cache"]

    if results["regime"]:
        st.success("Market regime is currently bullish versus the 200-day moving average.")
    else:
        st.warning("Market regime is currently defensive versus the 200-day moving average.")

    col1, col2, col3 = st.columns(3)
    col1.metric("Universe Size", results["universe_size"])
    col2.metric("Candidates", len(results["selected"]))
    col3.metric("Capital Deployed", f"{sum(results['portfolio'].values()):.1%}")

    if results["selected"]:
        candidate_rows = results["diagnostics"][results["diagnostics"]["selected"]].copy()
        portfolio_df = pd.DataFrame(
            [{"ticker": ticker, "weight": weight} for ticker, weight in results["portfolio"].items()]
        )

        st.subheader("Current Candidates")
        st.dataframe(candidate_rows, use_container_width=True)

        st.subheader("Suggested Portfolio")
        st.dataframe(portfolio_df, use_container_width=True)
    else:
        st.info("No stocks passed the current quality and valuation thresholds.")

    st.subheader("Scan Diagnostics")
    st.dataframe(results["diagnostics"], use_container_width=True)

    if not results["errors"].empty:
        st.subheader("Ticker Errors")
        st.dataframe(results["errors"], use_container_width=True)
else:
    st.info("Load the latest TWSE stats and run the scan to see potential candidates.")
