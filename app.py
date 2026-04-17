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
    market_cap_covered = 0
    if not reference_df.empty and "市值(十億台幣)" in reference_df.columns:
        market_cap_covered = int((reference_df["市值(十億台幣)"] > 0).sum())

    rows = [
        {"檢查項目": "TWSE 即時估值資料筆數", "結果": len(daily_stats_df), "說明": "用於目前 P/E、P/B、殖利率判斷"},
        {"檢查項目": "股票宇宙總數", "結果": len(reference_df), "說明": "依產業取市值前 N 檔後的總股票數"},
        {"檢查項目": "可取得市值的股票數", "結果": market_cap_covered, "說明": "若偏低，代表 yfinance 市值覆蓋率不足"},
        {"檢查項目": "財報快取筆數", "結果": len(st.session_state.cache), "說明": "目前已快取的財報/估值/價格資料"},
        {"檢查項目": "股票宇宙快取時間", "結果": _format_ts("universe_cache.pkl"), "說明": "超過 30 天建議刷新"},
        {"檢查項目": "財報快取時間", "結果": _format_ts("data_cache.pkl"), "說明": "超過 7 天建議刷新"},
    ]

    if results is not None:
        rows.extend(
            [
                {"檢查項目": "排名完成股票數", "結果": len(results["ranked"]), "說明": "已完成嚴格模式評分的股票數"},
                {"檢查項目": "可投資股票數", "結果": len(results["selected"]), "說明": "動作為 STRONG BUY 或 ACCUMULATE"},
                {"檢查項目": "掃描錯誤數", "結果": len(results["errors"]), "說明": "API 或欄位問題導致個股無法評分"},
            ]
        )

    return pd.DataFrame(rows)


def _localize_ranked_table(df):
    if df.empty:
        return df
    table = df.copy()
    if "relative_strength_4w" in table.columns:
        table["relative_strength_4w"] = (table["relative_strength_4w"] * 100).round(2)
    rename_map = {
        "ticker": "股票代號",
        "name": "股票名稱",
        "sector": "產業別",
        "quality_status": "品質狀態",
        "roe_ttm": "近期待表 ROE",
        "river_signal": "河流圖估值",
        "peg_signal": "即時 PEG",
        "yield_signal": "殖利率支撐",
        "momentum_signal": "動能",
        "latest_revenue_month": "最新月營收月份",
        "latest_revenue_yoy": "最新月營收 YoY(%)",
        "avg_3m_revenue_yoy": "近3月平均營收 YoY(%)",
        "composite_score": "綜合分數",
        "action_plan": "機構動作",
        "primary_driver": "主要驅動因子",
        "key_risk": "關鍵風險",
        "current_yield": "目前殖利率",
        "current_pe": "目前 P/E",
        "current_pb": "目前 P/B",
        "relative_strength_4w": "近四週相對強弱(%)",
        "quality_score": "品質分數",
        "valuation_score": "估值分數",
        "momentum_score": "動能分數",
    }
    table = table.rename(columns=rename_map)
    preferred_columns = [
        "股票代號",
        "股票名稱",
        "產業別",
        "綜合分數",
        "機構動作",
        "品質狀態",
        "河流圖估值",
        "即時 PEG",
        "殖利率支撐",
        "動能",
        "主要驅動因子",
        "關鍵風險",
    ]
    ordered = [column for column in preferred_columns if column in table.columns]
    remainder = [column for column in table.columns if column not in ordered]
    return table[ordered + remainder]


def _build_snapshot_table(best_pick_df):
    if best_pick_df.empty:
        return pd.DataFrame()

    row = best_pick_df.iloc[0]
    snapshot_rows = [
        {"指標": "ROE 品質趨勢", "資料 / 計算": f"{row.get('quality_status', 'Data N/A')} / 近期待表 ROE {row.get('roe_ttm', 'Data N/A')}", "訊號": "Bull" if row.get("quality_score", 0) >= 20 else "Bear"},
        {"指標": "河流圖位置", "資料 / 計算": row.get("river_signal", "Data N/A"), "訊號": "Bull" if row.get("river_signal") in {"Cheap", "Deep Value"} else "Bear"},
        {"指標": "即時 PEG", "資料 / 計算": f"{row.get('peg_signal', 'Data N/A')} / {row.get('peg_value', 'Data N/A')}", "訊號": "Bull" if row.get("peg_signal") == "Undervalued" else "Bear"},
        {"指標": "月營收動能", "資料 / 計算": f"{row.get('latest_revenue_month', 'Data N/A')} / 近3月平均 YoY {row.get('avg_3m_revenue_yoy', 'Data N/A')}", "訊號": "Bull" if (row.get("avg_3m_revenue_yoy") or 0) > 0 else "Bear"},
        {"指標": "殖利率支撐", "資料 / 計算": f"{row.get('yield_signal', 'Data N/A')} / 殖利率 {row.get('current_yield', 'Data N/A')}", "訊號": "Bull" if row.get("yield_signal") == "Floor Reached" else "Bear"},
        {"指標": "動能", "資料 / 計算": row.get("momentum_signal", "Data N/A"), "訊號": "Bull" if row.get("momentum_score", 0) >= 10 else "Bear"},
    ]
    return pd.DataFrame(snapshot_rows)


def _localize_diagnostics(df):
    if df.empty:
        return df
    return df.rename(
        columns={
            "ticker": "股票代號",
            "name": "股票名稱",
            "sector": "產業別",
            "data_source": "資料來源",
            "quality": "品質判斷",
            "valuation": "估值判斷",
            "action_plan": "機構動作",
            "selected": "是否入選",
        }
    )


def _localize_errors(df):
    if df.empty:
        return df
    return df.rename(
        columns={
            "ticker": "股票代號",
            "name": "股票名稱",
            "sector": "產業別",
            "error": "錯誤訊息",
        }
    )


st.set_page_config(page_title="TWSE 機構嚴格模式估值框架", layout="wide")

if "cache" not in st.session_state:
    st.session_state.cache = CacheManager.load()
if "daily_stats" not in st.session_state:
    st.session_state.daily_stats = pd.DataFrame()
if "show_universe_reference" not in st.session_state:
    st.session_state.show_universe_reference = False

st.title("TWSE 機構嚴格模式估值框架")
st.caption("不需要輸入單一股票，系統會依市場宇宙自動掃描、評分並排序目前最值得關注的買進標的。")

with st.sidebar:
    st.header("市場掃描設定")
    top_n_per_sector = st.slider("每個產業取前幾檔市值股票", 20, 150, 100, 10)
    max_stock_w = st.slider("單一股票權重上限 (%)", 5, 20, 10) / 100
    max_sector_w = st.slider("單一產業權重上限 (%)", 10, 60, 40) / 100
    force_refresh = st.checkbox("重新抓取並覆寫快取資料", value=False)
    refresh_stats = st.button("更新 TWSE 即時估值資料", use_container_width=True)
    if st.button("顯示各產業市值前 100 檔", use_container_width=True):
        st.session_state.show_universe_reference = not st.session_state.show_universe_reference
    run_scan = st.button("掃描全市場最佳買點", type="primary", use_container_width=True)

if refresh_stats or st.session_state.daily_stats.empty:
    with st.spinner("正在抓取 TWSE 即時估值資料..."):
        st.session_state.daily_stats = fetch_twse_daily_stats()

reference_df = get_universe_reference(
    top_n_per_sector=top_n_per_sector,
    force_refresh=force_refresh,
)

if len(st.session_state.daily_stats):
    st.sidebar.success(f"已載入 {len(st.session_state.daily_stats)} 筆 TWSE 即時估值資料")
else:
    st.sidebar.warning("目前無法取得 TWSE 即時估值資料，估值判斷會失真。")

st.sidebar.info(f"目前快取筆數：{len(st.session_state.cache)}")

with st.expander("框架說明", expanded=True):
    st.markdown(
        """
        - 品質關卡：以 ROE 為核心，先排除低品質與疑似價值陷阱。
        - 估值三角：目前可用資料中，以河流圖估值與殖利率支撐為主；月營收 PEG 若資料源不足會標示 `Data N/A`。
        - 動能檢查：加入 MA200 與近四週相對大盤強弱，避免單純接落下刀。
        - 最終輸出：全市場排名、最佳買進標的、機構式動作建議，以及關鍵風險提示。
        """
    )

st.subheader("資料有效性檢查")
st.dataframe(_build_validity_summary(reference_df, st.session_state.daily_stats), use_container_width=True, hide_index=True)

if st.session_state.show_universe_reference:
    st.subheader(f"各產業市值前 {top_n_per_sector} 檔參考名單")
    st.dataframe(reference_df, use_container_width=True, hide_index=True)

if run_scan:
    if st.session_state.daily_stats.empty:
        st.error("缺少 TWSE 即時估值資料，暫時無法掃描。")
        st.stop()

    with st.spinner("正在依嚴格模式掃描全市場並計算最佳買點排序..."):
        results = scan_universe(
            cache=st.session_state.cache,
            daily_stats_df=st.session_state.daily_stats,
            force_refresh=force_refresh,
            top_n_per_sector=top_n_per_sector,
            max_stock_weight=max_stock_w,
            max_sector_weight=max_sector_w,
        )
        st.session_state.cache = results["cache"]

    best_pick_df = _localize_ranked_table(results["best_pick"])
    ranked_df = _localize_ranked_table(results["ranked"])
    diagnostics_df = _localize_diagnostics(results["diagnostics"])
    errors_df = _localize_errors(results["errors"])
    portfolio_df = pd.DataFrame(
        [{"股票代號": ticker, "建議權重": weight} for ticker, weight in results["portfolio"].items()]
    )

    if results["regime"]:
        st.success("市場環境偏多：加權指數位於 200 日均線之上。")
    else:
        st.warning("市場環境偏保守：加權指數位於 200 日均線之下。")

    col1, col2, col3 = st.columns(3)
    col1.metric("股票宇宙數量", results["universe_size"])
    col2.metric("可投資股票數", len(results["selected"]))
    col3.metric("資金配置比率", f"{sum(results['portfolio'].values()):.1%}")

    st.subheader("掃描後資料有效性")
    st.dataframe(
        _build_validity_summary(reference_df, st.session_state.daily_stats, results),
        use_container_width=True,
        hide_index=True,
    )

    if not best_pick_df.empty:
        st.subheader("今日最佳買進標的")
        st.dataframe(best_pick_df, use_container_width=True, hide_index=True)

        raw_best = results["best_pick"].iloc[0]
        st.subheader("估值快照")
        st.dataframe(_build_snapshot_table(results["best_pick"]), use_container_width=True, hide_index=True)

        st.subheader("機構判斷")
        st.markdown(
            f"""
            **綜合分數：** {raw_best.get('composite_score', 'Data N/A')}  
            **主要驅動因子：** {raw_best.get('primary_driver', 'Data N/A')}  
            **機構動作：** {raw_best.get('action_plan', 'Data N/A')}
            """
        )

        st.subheader("行動計畫")
        st.write(raw_best.get("primary_driver", "Data N/A"))

        st.subheader("關鍵風險")
        st.write(raw_best.get("key_risk", "Data N/A"))

    if not ranked_df.empty:
        st.subheader("全市場最佳買點排名")
        st.dataframe(ranked_df.head(30), use_container_width=True, hide_index=True)

    if not portfolio_df.empty:
        st.subheader("依分數篩出的建議投資組合")
        st.dataframe(portfolio_df, use_container_width=True, hide_index=True)

    st.subheader("掃描診斷明細")
    st.dataframe(diagnostics_df, use_container_width=True, hide_index=True)

    if not errors_df.empty:
        st.subheader("個股錯誤與資料異常")
        st.dataframe(errors_df, use_container_width=True, hide_index=True)
else:
    st.info("點選左側「掃描全市場最佳買點」，系統會自動從全市場股票宇宙中排序出目前最值得關注的標的。")
