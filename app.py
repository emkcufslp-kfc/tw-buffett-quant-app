import os
from datetime import datetime

import pandas as pd
import streamlit as st

from criteria_config import get_default_filter_criteria
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
        {"檢查項目": "股票宇宙筆數", "結果": len(reference_df), "說明": "依 sidebar 條件篩出的股票清單"},
        {"檢查項目": "成功抓到市值的股票數", "結果": market_cap_covered, "說明": "由 yfinance 補上市值資訊"},
        {"檢查項目": "目前快取筆數", "結果": len(st.session_state.cache), "說明": "已快取的財報、估值與股價資料"},
        {"檢查項目": "股票宇宙快取時間", "結果": _format_ts("universe_cache.pkl"), "說明": "預設 30 天有效"},
        {"檢查項目": "財報快取時間", "結果": _format_ts("data_cache.pkl"), "說明": "預設 7 天有效"},
    ]

    if results is not None:
        rows.extend(
            [
                {"檢查項目": "完成評分的股票數", "結果": len(results["ranked"]), "說明": "成功跑完完整評估流程"},
                {"檢查項目": "實際入選股票數", "結果": len(results["selected"]), "說明": "目前動作屬於 STRONG BUY 或 ACCUMULATE"},
                {"檢查項目": "被大盤濾網擋下數量", "結果": results.get("regime_blocked_count", 0), "說明": "基本面過關但目前大盤不允許進場"},
                {"檢查項目": "掃描錯誤筆數", "結果": len(results["errors"]), "說明": "資料抓取或個股評估失敗數"},
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
        "quality_status": "品質分類",
        "roe_ttm": "平均 ROE",
        "river_signal": "估值狀態",
        "peg_signal": "PEG 訊號",
        "yield_signal": "殖利率訊號",
        "momentum_signal": "動能訊號",
        "latest_revenue_month": "最新營收月份",
        "latest_revenue_yoy": "最新營收 YoY(%)",
        "avg_3m_revenue_yoy": "近 3 月平均營收 YoY(%)",
        "composite_score": "綜合分數",
        "action_plan": "建議動作",
        "primary_driver": "主要加分原因",
        "key_risk": "主要風險",
        "current_yield": "目前殖利率",
        "current_pe": "目前 P/E",
        "current_pb": "目前 P/B",
        "relative_strength_4w": "相對強弱(%)",
        "quality_score": "品質分數",
        "valuation_score": "估值分數",
        "momentum_score": "動能分數",
        "regime_blocked": "被大盤濾網擋下",
    }
    table = table.rename(columns=rename_map)

    preferred_columns = [
        "股票代號",
        "股票名稱",
        "產業別",
        "綜合分數",
        "建議動作",
        "品質分類",
        "估值狀態",
        "PEG 訊號",
        "殖利率訊號",
        "動能訊號",
        "主要加分原因",
        "主要風險",
        "被大盤濾網擋下",
    ]
    ordered = [column for column in preferred_columns if column in table.columns]
    remainder = [column for column in table.columns if column not in ordered]
    return table[ordered + remainder]


def _build_snapshot_table(best_pick_df):
    if best_pick_df.empty:
        return pd.DataFrame()

    row = best_pick_df.iloc[0]
    return pd.DataFrame(
        [
            {"面向": "ROE 品質", "數值 / 判讀": f"{row.get('quality_status', 'Data N/A')} / 平均 ROE {row.get('roe_ttm', 'Data N/A')}", "方向": "Bull" if row.get("quality_score", 0) >= 20 else "Bear"},
            {"面向": "估值", "數值 / 判讀": row.get("river_signal", "Data N/A"), "方向": "Bull" if row.get("river_signal") in {"Cheap", "Deep Value"} else "Bear"},
            {"面向": "PEG", "數值 / 判讀": f"{row.get('peg_signal', 'Data N/A')} / {row.get('peg_value', 'Data N/A')}", "方向": "Bull" if row.get("peg_signal") == "Undervalued" else "Bear"},
            {"面向": "營收成長", "數值 / 判讀": f"{row.get('latest_revenue_month', 'Data N/A')} / 近 3 月平均 YoY {row.get('avg_3m_revenue_yoy', 'Data N/A')}", "方向": "Bull" if (row.get('avg_3m_revenue_yoy') or 0) > 0 else "Bear"},
            {"面向": "殖利率", "數值 / 判讀": f"{row.get('yield_signal', 'Data N/A')} / 目前殖利率 {row.get('current_yield', 'Data N/A')}", "方向": "Bull" if row.get("yield_signal") == "Floor Reached" else "Bear"},
            {"面向": "動能", "數值 / 判讀": row.get("momentum_signal", "Data N/A"), "方向": "Bull" if row.get("momentum_score", 0) >= 10 else "Bear"},
        ]
    )


def _localize_diagnostics(df):
    if df.empty:
        return df

    return df.rename(
        columns={
            "ticker": "股票代號",
            "name": "股票名稱",
            "sector": "產業別",
            "data_source": "資料來源",
            "quality": "品質結果",
            "valuation": "估值結果",
            "action_plan": "建議動作",
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


def _build_filter_summary(criteria, min_listing_years, validation_min_rows, validation_max_na_ratio, use_market_regime):
    return pd.DataFrame(
        [
            {"類別": "股票宇宙", "目前設定": f"上市年數 >= {min_listing_years} 年"},
            {"類別": "財報驗證", "目前設定": f"至少 {validation_min_rows} 期；缺值比例 <= {validation_max_na_ratio:.0%}"},
            {"類別": "品質", "目前設定": f"Compounder ROE >= {criteria['quality_compounder_min_roe']:.1f}；Investable ROE >= {criteria['quality_investable_min_roe']:.1f}"},
            {"類別": "營收成長", "目前設定": f"Turnaround 需近 3 月平均 YoY >= {criteria['turnaround_min_revenue_yoy']:.1f}%"},
            {"類別": "PEG", "目前設定": f"低估 <= {criteria['peg_undervalued_max']:.2f}；合理 <= {criteria['peg_fair_value_max']:.2f}"},
            {"類別": "殖利率", "目前設定": f"Floor >= {criteria['yield_floor_min']:.1f}%；Neutral >= {criteria['yield_neutral_min']:.1f}%"},
            {"類別": "動能", "目前設定": f"均線窗格 {criteria['momentum_ma_window']} 日；相對強弱回看 {criteria['momentum_lookback_days']} 日"},
            {"類別": "評分動作", "目前設定": f"Strong Buy >= {criteria['action_strong_buy_min']}；Accumulate >= {criteria['action_accumulate_min']}"},
            {"類別": "大盤濾網", "目前設定": "啟用" if use_market_regime else "停用"},
        ]
    )


st.set_page_config(page_title="TWSE 機構嚴格模式估值框架", layout="wide")

if "cache" not in st.session_state:
    st.session_state.cache = CacheManager.load()
if "daily_stats" not in st.session_state:
    st.session_state.daily_stats = pd.DataFrame()
if "show_universe_reference" not in st.session_state:
    st.session_state.show_universe_reference = False

defaults = get_default_filter_criteria()

st.title("TWSE 機構嚴格模式估值框架")
st.caption("左側 sidebar 現在可以直接調整股票宇宙、財報驗證、品質、估值、動能與評分動作門檻，不需要再修改程式碼。")

with st.sidebar:
    st.header("市場掃描設定")
    top_n_per_sector = st.slider("每個產業取前幾檔市值股票", 20, 150, 100, 10)
    min_listing_years = st.slider("最低上市年數", 1, 30, 10, 1)
    max_stock_w = st.slider("單一股票權重上限 (%)", 5, 30, 10, 1) / 100
    max_sector_w = st.slider("單一產業權重上限 (%)", 10, 70, 40, 5) / 100
    use_market_regime = st.checkbox("啟用大盤濾網", value=True)
    force_refresh = st.checkbox("重新抓取並覆蓋快取資料", value=False)
    refresh_stats = st.button("更新 TWSE 即時估值資料", use_container_width=True)

    with st.expander("財報驗證門檻", expanded=False):
        validation_min_rows = st.slider("最少財報期數", 3, 12, 4, 1)
        validation_max_na_ratio = st.slider("最大缺值比例 (%)", 0, 60, 30, 5) / 100

    with st.expander("品質條件", expanded=False):
        quality_compounder_min_roe = st.slider("Compounder 最低平均 ROE", 8.0, 30.0, float(defaults["quality_compounder_min_roe"]), 0.5)
        quality_investable_min_roe = st.slider("Investable 最低平均 ROE", 0.0, 20.0, float(defaults["quality_investable_min_roe"]), 0.5)
        quality_compounder_max_decline = st.slider("Compounder 可接受 ROE 趨勢下滑", -5.0, 5.0, float(defaults["quality_compounder_max_decline"]), 0.1)
        quality_investable_max_decline = st.slider("Investable 可接受 ROE 趨勢下滑", -5.0, 5.0, float(defaults["quality_investable_max_decline"]), 0.1)
        turnaround_min_revenue_yoy = st.slider("Turnaround 最低近 3 月平均營收 YoY(%)", 0.0, 80.0, float(defaults["turnaround_min_revenue_yoy"]), 1.0)
        value_trap_revenue_yoy = st.slider("Value Trap 營收 YoY 分界(%)", -30.0, 20.0, float(defaults["value_trap_revenue_yoy"]), 1.0)

    with st.expander("估值條件", expanded=False):
        valuation_min_history = st.slider("歷史估值至少需要幾期", 3, 12, int(defaults["valuation_min_history"]), 1)
        valuation_deep_value_std = st.slider("Deep Value 標準差倍數", 0.5, 3.0, float(defaults["valuation_deep_value_std"]), 0.1)
        valuation_cheap_std = st.slider("Cheap 標準差倍數", 0.3, 2.5, float(defaults["valuation_cheap_std"]), 0.1)
        valuation_expensive_std = st.slider("Expensive 標準差倍數", 0.3, 2.5, float(defaults["valuation_expensive_std"]), 0.1)
        peg_min_growth = st.slider("啟用 PEG 所需最低營收成長(%)", -10.0, 20.0, float(defaults["peg_min_growth"]), 0.5)
        peg_undervalued_max = st.slider("PEG 低估上限", 0.2, 2.0, float(defaults["peg_undervalued_max"]), 0.05)
        peg_fair_value_max = st.slider("PEG 合理上限", 0.3, 3.0, float(defaults["peg_fair_value_max"]), 0.05)
        peg_overvalued_min = st.slider("PEG 高估起點", 0.5, 4.0, float(defaults["peg_overvalued_min"]), 0.05)
        yield_floor_min = st.slider("殖利率 Floor 門檻(%)", 0.0, 12.0, float(defaults["yield_floor_min"]), 0.5)
        yield_neutral_min = st.slider("殖利率 Neutral 門檻(%)", 0.0, 12.0, float(defaults["yield_neutral_min"]), 0.5)

    with st.expander("動能與決策條件", expanded=False):
        momentum_ma_window = st.slider("長期均線窗格(日)", 60, 250, int(defaults["momentum_ma_window"]), 10)
        momentum_lookback_days = st.slider("相對強弱回看天數", 5, 60, int(defaults["momentum_lookback_days"]), 1)
        action_strong_buy_min = st.slider("STRONG BUY 最低分數", 50, 100, int(defaults["action_strong_buy_min"]), 1)
        action_accumulate_min = st.slider("ACCUMULATE 最低分數", 40, 95, int(defaults["action_accumulate_min"]), 1)
        action_hold_min = st.slider("HOLD 最低分數", 20, 90, int(defaults["action_hold_min"]), 1)
        action_trim_min = st.slider("TRIM / REDUCE 最低分數", 0, 80, int(defaults["action_trim_min"]), 1)

    if st.button(f"顯示各產業市值前 {top_n_per_sector} 檔", use_container_width=True):
        st.session_state.show_universe_reference = not st.session_state.show_universe_reference

    run_scan = st.button("掃描全市場最佳買點", type="primary", use_container_width=True)

criteria = {
    "quality_compounder_min_roe": quality_compounder_min_roe,
    "quality_investable_min_roe": quality_investable_min_roe,
    "quality_compounder_max_decline": quality_compounder_max_decline,
    "quality_investable_max_decline": quality_investable_max_decline,
    "turnaround_min_revenue_yoy": turnaround_min_revenue_yoy,
    "value_trap_revenue_yoy": value_trap_revenue_yoy,
    "valuation_min_history": valuation_min_history,
    "valuation_deep_value_std": valuation_deep_value_std,
    "valuation_cheap_std": valuation_cheap_std,
    "valuation_expensive_std": valuation_expensive_std,
    "peg_min_growth": peg_min_growth,
    "peg_undervalued_max": peg_undervalued_max,
    "peg_fair_value_max": peg_fair_value_max,
    "peg_overvalued_min": peg_overvalued_min,
    "yield_floor_min": yield_floor_min,
    "yield_neutral_min": yield_neutral_min,
    "momentum_ma_window": momentum_ma_window,
    "momentum_lookback_days": momentum_lookback_days,
    "action_strong_buy_min": action_strong_buy_min,
    "action_accumulate_min": action_accumulate_min,
    "action_hold_min": action_hold_min,
    "action_trim_min": action_trim_min,
}

if refresh_stats or st.session_state.daily_stats.empty:
    with st.spinner("正在更新 TWSE 即時估值資料..."):
        st.session_state.daily_stats = fetch_twse_daily_stats()

reference_df = get_universe_reference(
    top_n_per_sector=top_n_per_sector,
    min_listing_years=min_listing_years,
    force_refresh=force_refresh,
)

if len(st.session_state.daily_stats):
    st.sidebar.success(f"已載入 {len(st.session_state.daily_stats)} 筆 TWSE 即時估值資料")
else:
    st.sidebar.warning("尚未取得 TWSE 即時估值資料，估值與殖利率判斷可能失真")

st.sidebar.info(f"目前快取筆數：{len(st.session_state.cache)}")

with st.expander("目前篩選條件", expanded=True):
    st.dataframe(
        _build_filter_summary(
            criteria,
            min_listing_years=min_listing_years,
            validation_min_rows=validation_min_rows,
            validation_max_na_ratio=validation_max_na_ratio,
            use_market_regime=use_market_regime,
        ),
        use_container_width=True,
        hide_index=True,
    )

st.subheader("資料完整度檢查")
st.dataframe(_build_validity_summary(reference_df, st.session_state.daily_stats), use_container_width=True, hide_index=True)

if st.session_state.show_universe_reference:
    st.subheader(f"各產業市值前 {top_n_per_sector} 檔候選股票")
    st.dataframe(reference_df, use_container_width=True, hide_index=True)

if run_scan:
    if st.session_state.daily_stats.empty:
        st.error("請先更新 TWSE 即時估值資料後再進行掃描。")
        st.stop()

    with st.spinner("正在依照 sidebar 條件掃描全市場股票，請稍候..."):
        results = scan_universe(
            cache=st.session_state.cache,
            daily_stats_df=st.session_state.daily_stats,
            force_refresh=force_refresh,
            top_n_per_sector=top_n_per_sector,
            min_listing_years=min_listing_years,
            max_stock_weight=max_stock_w,
            max_sector_weight=max_sector_w,
            validation_min_rows=validation_min_rows,
            validation_max_na_ratio=validation_max_na_ratio,
            criteria=criteria,
            use_market_regime=use_market_regime,
        )
        st.session_state.cache = results["cache"]

    best_pick_df = _localize_ranked_table(results["best_pick"])
    ranked_df = _localize_ranked_table(results["ranked"])
    diagnostics_df = _localize_diagnostics(results["diagnostics"])
    errors_df = _localize_errors(results["errors"])
    portfolio_df = pd.DataFrame(
        [{"股票代號": ticker, "建議權重": weight} for ticker, weight in results["portfolio"].items()]
    )

    if use_market_regime:
        if results["regime"]:
            st.success("目前大盤濾網為多頭允許狀態，符合條件的股票可進入候選名單。")
        else:
            st.warning("目前大盤濾網為保守狀態，符合基本面條件的股票會先標示為等待大盤轉強。")
    else:
        st.info("目前已停用大盤濾網，選股結果不受大盤方向限制。")

    col1, col2, col3 = st.columns(3)
    col1.metric("股票宇宙數量", results["universe_size"])
    col2.metric("實際入選數量", len(results["selected"]))
    col3.metric("建議配置總和", f"{sum(results['portfolio'].values()):.1%}")

    st.subheader("掃描後資料摘要")
    st.dataframe(
        _build_validity_summary(reference_df, st.session_state.daily_stats, results),
        use_container_width=True,
        hide_index=True,
    )

    if not best_pick_df.empty:
        st.subheader("最佳候選股")
        st.dataframe(best_pick_df, use_container_width=True, hide_index=True)

        raw_best = results["best_pick"].iloc[0]
        st.subheader("投資快照")
        st.dataframe(_build_snapshot_table(results["best_pick"]), use_container_width=True, hide_index=True)

        st.subheader("投資結論")
        st.markdown(
            f"""
            **綜合分數：** {raw_best.get('composite_score', 'Data N/A')}  
            **主要加分原因：** {raw_best.get('primary_driver', 'Data N/A')}  
            **建議動作：** {raw_best.get('action_plan', 'Data N/A')}
            """
        )

        st.subheader("加分重點")
        st.write(raw_best.get("primary_driver", "Data N/A"))

        st.subheader("主要風險")
        st.write(raw_best.get("key_risk", "Data N/A"))

    if not ranked_df.empty:
        st.subheader("完整排名")
        st.dataframe(ranked_df.head(30), use_container_width=True, hide_index=True)

    if not portfolio_df.empty:
        st.subheader("建議配置")
        st.dataframe(portfolio_df, use_container_width=True, hide_index=True)

    st.subheader("診斷明細")
    st.dataframe(diagnostics_df, use_container_width=True, hide_index=True)

    if not errors_df.empty:
        st.subheader("錯誤明細")
        st.dataframe(errors_df, use_container_width=True, hide_index=True)
else:
    st.info("請先在左側 sidebar 調整條件，再按下「掃描全市場最佳買點」開始分析。")
