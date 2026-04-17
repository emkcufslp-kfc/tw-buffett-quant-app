import streamlit as st
import pandas as pd
from datetime import datetime
import yfinance as yf
from data_loader import get_stock_universe, get_financials, get_historical_valuation
from data_validation import validate_financial_data
from factor_engine import quality_filter
from valuation_engine import valuation_filter
from regime_filter import market_regime
from strategy import entry_rule
from portfolio_engine import build_portfolio

# --- Page Config & Styling ---
st.set_page_config(page_title="台版巴菲特 2.0 智能選股", layout="wide")

st.markdown("""
    <style>
    .main {
        background-color: #f5f7f9;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    </style>
""", unsafe_allow_html=True)

# --- Sidebar: 進階過濾參數 ---
st.sidebar.title("💎 策略控制中心")
st.sidebar.caption("Institutional Quantitative Engine v2.0")

st.sidebar.header("品質過濾參數")
roe_avg_val = st.sidebar.slider("10年平均 ROE (%)", 5, 25, 15)
roe_min_val = st.sidebar.slider("ROE 每年最低容忍 (%)", 0, 15, 10)

st.sidebar.header("環境參數")
as_of_date = st.sidebar.date_input("基準日期", datetime.today())
st.sidebar.info("📡 **數據源**: 統一使用 Yahoo Finance (已對齊台股除權息)")

run_backtest = st.sidebar.button("開始量化掃描")

# --- Main Panel: 策略邏輯說明 ---
st.title("台版巴菲特 2.0 量化選股系統")

with st.container():
    st.header("🎯 策略核心邏輯 (Methodology)")
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("策略架構說明")
        st.write("""
        這套「台版巴菲特 2.0」建議拆成四個模組：**Universe 過濾、品質分數、估值進場、基本面出場**。
        
        1. **Universe 過濾**: 先排除上市未滿 10 年與 KY 股，因為我們要的是經歷完整景氣循環後仍能維持獲利的公司，而不是短期財報漂亮的新股。
        2. **品質分數**: 篩選 10 年維持高 ROE（股東權益報酬率）且營運現金流、自由現金流（FCF）均為正數的公司，確保這是一檔「賺真錢」的公司。
        """)
        
    with col2:
        st.subheader("板塊特化估值")
        st.write("""
        * **科技/製造業**: 使用「當前 P/E vs 過去 10 年自身中位數」進行歷史分布比較。這比同業橫向對比更合理，因為台股 P/E 本身會隨景氣循環動態更新。
        * **金融業**: 由於金融業特性，改用「當前 P/B 是否低於過去 10 年平均減 1 倍標準差」。
        
        > **註**: 目前 Yahoo Finance 缺乏銀行 NPL (逾期放款比率) 數據，系統改以 **ROE 穩定性** 作為資產品質代理指標。
        """)

st.divider()

# --- Execution Phase ---
if run_backtest:
    st.header("📊 即時量化掃描狀態")
    
    # 1. Get Universe
    universe_df = get_stock_universe()
    universe = universe_df['stock_id'].tolist()
    sector_map = dict(zip(universe_df['stock_id'], universe_df['industry_category']))
    
    # 2. Check Regime
    regime = market_regime()
    if not regime:
        st.warning("⚠️ **市場環境**: 加權指數位於 200DMA 以下。策略處於防禦狀態，不建議新資金進場。")
    else:
        st.success("✅ **市場環境**: 加權指數位於 200DMA 以上。多頭選股模式啟動。")

    selected = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, ticker in enumerate(universe):
        status_text.text(f"正在掃描模組... {ticker} ({i+1}/{len(universe)})")
        progress_bar.progress((i + 1) / len(universe))
        
        try:
            # Module 2: Quality Check
            df = get_financials(ticker)
            if validate_financial_data(df):
                if quality_filter(df, roe_avg_threshold=roe_avg_val, roe_min_threshold=roe_min_val):
                    
                    # Module 3: Valuation (Historical Self-Comparison)
                    val_history = get_historical_valuation(ticker, df)
                    if valuation_filter(ticker, val_history):
                        selected.append(ticker)
                        st.caption(f"💎 符合巴菲特 2.0 標準: {ticker}")
        except Exception as e:
            continue

    status_text.text("✨ 掃描完成！")

    if not selected:
        st.warning("目前市場中沒有股票符合所有 10 年一致性與估值安全邊際條件。建議調整左側 ROE 門檻。")
    else:
        st.header("🏛 最終入選名單與投資組合")
        portfolio = build_portfolio(selected, sector_map)
        
        col_res1, col_res2 = st.columns([1, 2])
        with col_res1:
            st.metric("入選標的數量", len(selected))
            st.table(pd.DataFrame(list(portfolio.items()), columns=["股票代碼", "配置權重"]))
            
        with col_res2:
            st.info("這份清單中的公司均通過了 10 年 ROE 穩定性測試、現金流健康測試，且目前股價相對於其自身歷史估值具有安全邊際。")
            st.write("**建議操作**: 每月檢視 fundamental 出場訊號（ROE < 10% 兩年以上或 FCF 轉負）。")

else:
    st.info("👈 請點擊左側「開始量化掃描」按鈕，對台股 150 檔權值股進行巴菲特 2.0 模組化分析。")