import streamlit as st
import pandas as pd
from datetime import datetime
import time
import os
from data_loader import (
    get_stock_universe, get_financials, get_historical_valuation, 
    fetch_twse_daily_stats, CacheManager, get_industry_info
)
from data_validation import validate_financial_data
from factor_engine import quality_filter, fundamental_exit_check
from valuation_engine import valuation_filter, overvaluation_exit_check
from regime_filter import market_regime
from portfolio_engine import build_portfolio

# --- Page Config & Styling ---
st.set_page_config(page_title="台版巴菲特 2.0 L", layout="wide")

# --- Initialize Cache ---
if 'cache' not in st.session_state:
    st.session_state.cache = CacheManager.load()
if 'daily_stats' not in st.session_state:
    st.session_state.daily_stats = pd.DataFrame()

# --- Sidebar: 進階控制中心 ---
st.sidebar.title("💎 策略控制中心")
st.sidebar.caption("Institutional Quantitative Engine v2.5")

with st.sidebar.expander("📝 核心品質參數", expanded=True):
    roe_avg_tgt = st.slider("10年平均 ROE (%)", 5, 25, 15)
    roe_min_tgt = st.slider("ROE 最低容忍 (%)", 0, 15, 10)
    roe_min_count = st.slider("容許低於門檻次數", 0, 5, 2)
    fcf_years = st.slider("現金流連續正向年數", 1, 10, 10)

with st.sidebar.expander("💰 估值與資金控制", expanded=False):
    yield_tgt = st.slider("最低股息收益率 (%)", 0.0, 8.0, 4.0)
    max_stock_w = st.slider("單一個股權重上限 (%)", 5, 20, 10) / 100
    max_sector_w = st.slider("單一產業權重上限 (%)", 10, 60, 40) / 100

st.sidebar.header("環境同步")
force_refresh = st.sidebar.button("強制重新同步雲端數據")

if force_refresh or st.session_state.daily_stats.empty:
    with st.sidebar:
        with st.spinner("同步 TWSE OpenAPI 最新數據..."):
            st.session_state.daily_stats = fetch_twse_daily_stats()
            st.success("市場數據同步成功")

st.sidebar.info(f"💾 快取狀態: 已存儲 {len(st.session_state.cache)} 檔股票歷史數據")
run_scan = st.sidebar.button("啟動量化篩選")

# --- Main Dashboard ---
st.title("台版巴菲特 2.0 L：機構級量化篩選器")

with st.expander("📚 台版巴菲特 2.0 策略核心說明 (Methodology)", expanded=True):
    st.markdown("""
    ### 1. 存活者偏差過濾 (Survival & Reality)
    - **營運韌性**: 排除上市未滿 10 年的公司，確保經歷過至少一次完整的多空循環。
    - **透明度**: 排除 KY 股，遵循「不碰不懂、財報不透明」的原則。
    
    ### 2. 獲利品質與護城河 (Profitability & Moat)
    - **穩定獲利**: 過去 10 年平均 ROE > 15%，且低於 10% 的年度不超過 2 次。
    - **真金白銀**: 必須具備連續 10 年的營運現金流 (OCF) 與 自由現金流 (FCF) 正向表現。
    
    ### 3. 動態安全邊際 (Dynamic Valuation)
    - **科技/製造業**: 目前 P/E 低於過去 10 年中位數，且股息率滿足門檻。
    - **金融業**: 目前 P/B 低於過去 10 年 (平均值 - 1倍標準差) 的評價低點。
    """)

if run_scan:
    st.header("🔍 即時量化掃描與診斷")
    
    # 0. Check Daily Stats
    if st.session_state.daily_stats.empty:
        st.error("無法取得 TWSE 市場數據，請檢查網路連接。")
        st.stop()
        
    # 1. Get Universe
    universe_df = get_stock_universe()
    universe = universe_df['stock_id'].tolist()
    
    # 2. Market Regime
    regime = market_regime()
    if not regime:
        st.warning("⚠️ **市場警戒**: 加權指數位於 200DMA 以下。策略建議防禦，僅進行診斷不建議進場。")
    else:
        st.success("✅ **市場趨勢**: 加權指數位於 200DMA 以上。多頭量化選股模組執行中。")

    selected = []
    diag_data = [] # For diagnostic table
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, ticker in enumerate(universe):
        status_text.text(f"正在分析模組... {ticker} ({i+1}/{len(universe)})")
        progress_bar.progress((i + 1) / len(universe))
        
        try:
            # Check Cache First
            if ticker in st.session_state.cache and not force_refresh:
                df = st.session_state.cache[ticker]['financials']
                val_history = st.session_state.cache[ticker]['valuation']
                sector = st.session_state.cache[ticker]['sector']
            else:
                # Fetch New
                df = get_financials(ticker)
                if df.empty: continue
                val_history = get_historical_valuation(ticker, df)
                sector, name = get_industry_info(ticker)
                # Store in Cache
                st.session_state.cache[ticker] = {
                    'financials': df,
                    'valuation': val_history,
                    'sector': sector
                }
            
            # Application of Rules
            q_pass, q_msg = quality_filter(df, roe_avg_tgt, roe_min_tgt, roe_min_count, fcf_years)
            v_pass, v_msg = valuation_filter(ticker, val_history, st.session_state.daily_stats, sector, yield_tgt)
            
            if q_pass and v_pass:
                selected.append(ticker)
                st.caption(f"💎 符合資格: {ticker}")
            
            diag_data.append({"代碼": ticker, "品質結果": q_msg, "估值結果": v_msg})
            
        except Exception as e:
            continue

    # Save Cache
    CacheManager.save(st.session_state.cache)
    status_text.text("✨ 量化掃描儀執行完成！")

    if not selected:
        st.error("❌ 目前市場中沒有股票符合所有設定的 10 年一致性與估值門檻。")
    else:
        st.header("⚖️ 投資組合構建與權重分配")
        sector_map = {t: st.session_state.cache[t]['sector'] for t in selected}
        portfolio = build_portfolio(selected, sector_map, max_stock_w, max_sector_w)
        
        col_res1, col_res2 = st.columns([1, 2])
        with col_res1:
            st.metric("入選標的", len(selected))
            st.table(pd.DataFrame(list(portfolio.items()), columns=["股票代碼", "建議權重"]))
            
        with col_res2:
            st.info("權重已依照「10% 單一限制」與「40% 產業限制」進行風險對沖最佳化。")
            st.write("**出場紀律執行 (Exit Protocol):**")
            st.write("1. 財務惡化: ROE 連續兩季 < 10% 或 FCF 轉負。")
            st.write("2. 極度高估: 當前 P/E 突破自身歷史 90% 分位數。")

    with st.expander("🔍 掃描診斷報告 (為什麼我的股票沒入選？)", expanded=False):
        st.dataframe(pd.DataFrame(diag_data), use_container_width=True)

else:
    st.info("👈 請於左側設定您的投資參數，並點擊「啟動量化篩選」。")
    st.caption("首次運行將花費較長時間建立數據快取，後續切換參數將為即時響應。")