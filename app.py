import os
from datetime import datetime

import pandas as pd
import streamlit as st

from data_loader import CacheManager, fetch_twse_daily_stats, get_universe_reference
from screening import scan_universe


def _format_ts(path):
    if not os.path.exists(path):
        return "尚未建立"
    return datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M:%S")


def _build_validity_summary(reference_df, daily_stats_df, results=None):
    universe_size = len(reference_df)
    twse_rows = len(daily_stats_df)
    market_cap_covered = 0
    if not reference_df.empty and "市值(十億台幣)" in reference_df.columns:
        market_cap_covered = int((reference_df["市值(十億台幣)"] > 0).sum())

    summary_rows = [
        {"檢查項目": "TWSE 每日估值資料筆數", "結果": twse_rows, "說明": "用於即時 P/E、P/B、殖利率估值判斷"},
        {"檢查項目": "股票宇宙總數", "結果": universe_size, "說明": "來自上市公司名單，依產業取市值前 N 檔"},
        {"檢查項目": "可取得市值的股票數", "結果": market_cap_covered, "說明": "若偏低，代表 yfinance 市值資訊不足"},
        {"檢查項目": "股票財報快取筆數", "結果": len(st.session_state.cache), "說明": "已快取的財報/估值資料量"},
        {"檢查項目": "股票宇宙快取時間", "結果": _format_ts("universe_cache.pkl"), "說明": "超過 30 天建議刷新"},
        {"檢查項目": "財報快取時間", "結果": _format_ts("data_cache.pkl"), "說明": "超過 7 天建議刷新"},
    ]

    if results is not None:
        diagnostics_df = results.get("diagnostics", pd.DataFrame())
        error_df = results.get("errors", pd.DataFrame())
        summary_rows.extend(
            [
                {"檢查項目": "掃描完成股票數", "結果": len(diagnostics_df), "說明": "已產出診斷結果的股票數"},
                {"檢查項目": "候選股票數", "結果": len(results.get("selected", [])), "說明": "同時通過品質與估值條件"},
                {"檢查項目": "掃描錯誤數", "結果": len(error_df), "說明": "API 或欄位異常導致的個股錯誤"},
            ]
        )

    return pd.DataFrame(summary_rows)


def _localize_scan_tables(results):
    candidate_df = results["diagnostics"][results["diagnostics"]["selected"]].copy()
    diagnostics_df = results["diagnostics"].copy()
    errors_df = results["errors"].copy()
    portfolio_df = pd.DataFrame(
        [{"股票代號": ticker, "建議權重": weight} for ticker, weight in results["portfolio"].items()]
    )

    rename_map = {
        "ticker": "股票代號",
        "sector": "產業別",
        "data_source": "資料來源",
        "quality": "品質結果",
        "valuation": "估值結果",
        "selected": "是否入選",
        "error": "錯誤訊息",
    }
    candidate_df = candidate_df.rename(columns=rename_map)
    diagnostics_df = diagnostics_df.rename(columns=rename_map)
    errors_df = errors_df.rename(columns=rename_map)
    return candidate_df, diagnostics_df, errors_df, portfolio_df


st.set_page_config(page_title="TW 巴菲特量化儀表板", layout="wide")

if "cache" not in st.session_state:
    st.session_state.cache = CacheManager.load()
if "daily_stats" not in st.session_state:
    st.session_state.daily_stats = pd.DataFrame()
if "show_universe_reference" not in st.session_state:
    st.session_state.show_universe_reference = False

st.title("TW 巴菲特量化儀表板")
st.caption("以台灣上市公司為範圍，結合 TWSE 即時估值資料與 yfinance 財報資料的價值品質選股工具。")

with st.sidebar:
    st.header("掃描設定")
    top_n_per_sector = st.slider("每個產業取前幾檔市值股票", 20, 150, 100, 10)
    roe_avg_tgt = st.slider("平均 ROE 門檻 (%)", 5, 25, 15)
    roe_min_tgt = st.slider("最低可接受 ROE (%)", 0, 15, 10)
    roe_min_count = st.slider("低於門檻的容忍年數", 0, 5, 2)
    fcf_years = st.slider("連續正向現金流檢查年數", 1, 10, 10)
    yield_tgt = st.slider("殖利率門檻 (%)", 0.0, 8.0, 4.0, 0.5)
    max_stock_w = st.slider("單一股票權重上限 (%)", 5, 20, 10) / 100
    max_sector_w = st.slider("單一產業權重上限 (%)", 10, 60, 40) / 100
    force_refresh = st.checkbox("重新抓取並覆寫快取資料", value=False)
    refresh_stats = st.button("更新 TWSE 每日估值資料")
    if st.button("顯示各產業市值前 100 檔", use_container_width=True):
        st.session_state.show_universe_reference = not st.session_state.show_universe_reference
    run_scan = st.button("開始掃描候選股", type="primary", use_container_width=True)

if refresh_stats or st.session_state.daily_stats.empty:
    with st.spinner("正在抓取 TWSE 每日估值資料..."):
        st.session_state.daily_stats = fetch_twse_daily_stats()

reference_df = get_universe_reference(
    top_n_per_sector=top_n_per_sector,
    force_refresh=force_refresh,
)

stats_rows = len(st.session_state.daily_stats)
if stats_rows:
    st.sidebar.success(f"已載入 {stats_rows} 筆 TWSE 每日估值資料")
else:
    st.sidebar.warning("目前抓不到 TWSE 每日估值資料，估值條件將無法正確判斷。")

st.sidebar.info(f"目前財報/估值快取筆數：{len(st.session_state.cache)}")

with st.expander("策略說明", expanded=True):
    st.markdown(
        """
        - 品質條件：高平均 ROE、低 ROE 年數受限、且營業現金流與自由現金流維持正值。
        - 估值條件：金融股以歷史 P/B 區間判斷，非金融股以歷史 P/E 與殖利率門檻判斷。
        - 市場狀態：以台灣加權指數相對 200 日均線的位置作為風險環境判斷。
        - 股票宇宙：以上市公司名單為基礎，按產業擷取市值前 N 檔。
        - 組合控制：單一股票與單一產業都有硬上限，未配置資金會保留，不會為了滿倉而突破限制。
        """
    )

st.subheader("資料有效性檢查")
validity_df = _build_validity_summary(reference_df, st.session_state.daily_stats)
st.dataframe(validity_df, use_container_width=True, hide_index=True)

if st.session_state.show_universe_reference:
    st.subheader(f"各產業市值前 {top_n_per_sector} 檔參考名單")
    st.dataframe(reference_df, use_container_width=True, hide_index=True)

if run_scan:
    if st.session_state.daily_stats.empty:
        st.error("缺少 TWSE 每日估值資料，暫時無法執行掃描。")
        st.stop()

    with st.spinner("正在掃描台灣股票宇宙，請稍候..."):
        results = scan_universe(
            cache=st.session_state.cache,
            daily_stats_df=st.session_state.daily_stats,
            force_refresh=force_refresh,
            top_n_per_sector=top_n_per_sector,
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
        st.success("市場趨勢偏多：加權指數位於 200 日均線之上。")
    else:
        st.warning("市場趨勢偏保守：加權指數位於 200 日均線之下。")

    candidate_df, diagnostics_df, errors_df, portfolio_df = _localize_scan_tables(results)

    col1, col2, col3 = st.columns(3)
    col1.metric("股票宇宙數量", results["universe_size"])
    col2.metric("候選股票數量", len(results["selected"]))
    col3.metric("資金配置比率", f"{sum(results['portfolio'].values()):.1%}")

    st.subheader("掃描後資料有效性")
    st.dataframe(
        _build_validity_summary(reference_df, st.session_state.daily_stats, results),
        use_container_width=True,
        hide_index=True,
    )

    if not candidate_df.empty:
        st.subheader("目前候選股票")
        st.dataframe(candidate_df, use_container_width=True, hide_index=True)

        st.subheader("建議投資組合")
        st.dataframe(portfolio_df, use_container_width=True, hide_index=True)
    else:
        st.info("目前沒有股票同時通過品質與估值條件。")

    st.subheader("掃描診斷明細")
    st.dataframe(diagnostics_df, use_container_width=True, hide_index=True)

    if not errors_df.empty:
        st.subheader("個股錯誤與資料異常")
        st.dataframe(errors_df, use_container_width=True, hide_index=True)
else:
    st.info("請先確認資料有效性，再點選左側的「開始掃描候選股」。")
