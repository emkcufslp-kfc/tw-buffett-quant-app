import math

import pandas as pd

from criteria_config import get_default_filter_criteria


FINANCIAL_SECTORS = {"金融保險業", "金融業", "Financial Services"}


def _safe_float(value):
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _classify_quality(df, revenue_metrics, criteria):
    latest_roe = _safe_float(df["ROE"].iloc[-1]) if not df.empty else None
    recent = df["ROE"].tail(min(4, len(df))).astype(float) if not df.empty else pd.Series(dtype=float)
    roe_trend = _safe_float(recent.diff().mean()) if len(recent) >= 2 else None
    roe_ttm = _safe_float(recent.mean()) if not recent.empty else None
    avg_3m_revenue_yoy = _safe_float(revenue_metrics.get("avg_3m_revenue_yoy"))

    compounder_min_roe = criteria["quality_compounder_min_roe"]
    investable_min_roe = criteria["quality_investable_min_roe"]
    compounder_max_decline = criteria["quality_compounder_max_decline"]
    investable_max_decline = criteria["quality_investable_max_decline"]
    turnaround_min_revenue_yoy = criteria["turnaround_min_revenue_yoy"]
    value_trap_revenue_yoy = criteria["value_trap_revenue_yoy"]

    if roe_ttm is None:
        return "Data N/A", 0, latest_roe, roe_ttm, roe_trend, "缺少有效 ROE 資料"

    if roe_ttm >= compounder_min_roe and (roe_trend is None or roe_trend >= compounder_max_decline):
        return "COMPOUNDER", 30, latest_roe, roe_ttm, roe_trend, "平均 ROE 達到高標且趨勢穩定"
    if investable_min_roe <= roe_ttm < compounder_min_roe and (roe_trend is None or roe_trend >= investable_max_decline):
        return "INVESTABLE", 20, latest_roe, roe_ttm, roe_trend, "平均 ROE 達到可投資門檻"
    if roe_ttm < investable_min_roe and avg_3m_revenue_yoy is not None and avg_3m_revenue_yoy >= turnaround_min_revenue_yoy:
        return "TURNAROUND", 12, latest_roe, roe_ttm, roe_trend, "ROE 偏弱但近 3 月營收成長強勁"
    if roe_ttm < investable_min_roe:
        if avg_3m_revenue_yoy is not None and avg_3m_revenue_yoy < value_trap_revenue_yoy:
            return "VALUE TRAP", 5, latest_roe, roe_ttm, roe_trend, "ROE 偏低且近 3 月營收動能不佳"
        return "VALUE TRAP", 5, latest_roe, roe_ttm, roe_trend, "ROE 偏低，品質不足"
    return "INVESTABLE", 15, latest_roe, roe_ttm, roe_trend, "品質中性"


def _classify_river(ticker, sector, val_history_df, daily_stats_df, criteria):
    if val_history_df.empty or ticker not in daily_stats_df.index:
        return "Data N/A", 0, None, None, None, "缺少估值資料"

    current_pe = _safe_float(daily_stats_df.loc[ticker, "PE"])
    current_pb = _safe_float(daily_stats_df.loc[ticker, "PB"])
    min_history = int(criteria["valuation_min_history"])
    deep_value_std = criteria["valuation_deep_value_std"]
    cheap_std = criteria["valuation_cheap_std"]
    expensive_std = criteria["valuation_expensive_std"]
    is_financial = sector in FINANCIAL_SECTORS

    if is_financial:
        pb_series = val_history_df["PB"].dropna()
        if len(pb_series) < min_history or current_pb is None:
            return "Data N/A", 0, None, current_pb, None, "金融股缺少足夠 P/B 歷史"
        mean_pb = float(pb_series.mean())
        std_pb = float(pb_series.std()) if not math.isnan(float(pb_series.std())) else 0.0
        deep_value = mean_pb - deep_value_std * std_pb
        cheap = mean_pb - cheap_std * std_pb
        expensive = mean_pb + expensive_std * std_pb

        if current_pb < deep_value:
            return "Deep Value", 25, None, current_pb, mean_pb, "目前 P/B 遠低於歷史中樞"
        if current_pb < cheap:
            return "Cheap", 20, None, current_pb, mean_pb, "目前 P/B 低於歷史均值"
        if current_pb > expensive:
            return "Expensive", 5, None, current_pb, mean_pb, "目前 P/B 高於歷史區間"
        return "Fair", 12, None, current_pb, mean_pb, "目前 P/B 接近歷史合理區間"

    pe_series = val_history_df["PE"].dropna()
    if len(pe_series) < min_history or current_pe is None:
        return "Data N/A", 0, current_pe, None, None, "缺少足夠 P/E 歷史"
    mean_pe = float(pe_series.mean())
    std_pe = float(pe_series.std()) if not math.isnan(float(pe_series.std())) else 0.0
    deep_value = mean_pe - deep_value_std * std_pe
    cheap = mean_pe - cheap_std * std_pe
    expensive = mean_pe + expensive_std * std_pe

    if current_pe < deep_value:
        return "Deep Value", 25, current_pe, None, mean_pe, "目前 P/E 遠低於歷史中樞"
    if current_pe < cheap:
        return "Cheap", 20, current_pe, None, mean_pe, "目前 P/E 低於歷史均值"
    if current_pe > expensive:
        return "Expensive", 5, current_pe, None, mean_pe, "目前 P/E 高於歷史區間"
    return "Fair", 12, current_pe, None, mean_pe, "目前 P/E 接近歷史合理區間"


def _classify_peg(ticker, daily_stats_df, revenue_metrics, criteria):
    if ticker not in daily_stats_df.index:
        return "Data N/A", 0, None, "缺少即時 P/E"

    pe_value = _safe_float(daily_stats_df.loc[ticker, "PE"])
    growth = _safe_float(revenue_metrics.get("avg_3m_revenue_yoy"))
    min_growth = criteria["peg_min_growth"]
    undervalued_max = criteria["peg_undervalued_max"]
    fair_value_max = criteria["peg_fair_value_max"]
    overvalued_min = criteria["peg_overvalued_min"]

    if pe_value is None:
        return "Data N/A", 0, None, "缺少即時 P/E"
    if growth is None or growth <= min_growth:
        return "Data N/A", 4, None, "成長率不足，PEG 不具參考性"

    peg = pe_value / growth
    if peg <= undervalued_max:
        return "Undervalued", 15, peg, "PEG 落在低估區間"
    if peg <= fair_value_max:
        return "Fair Value", 10, peg, "PEG 落在合理區間"
    if peg >= overvalued_min:
        return "Overvalued", 3, peg, "PEG 顯示偏高估"
    return "Fair Value", 7, peg, "PEG 介於合理與高估之間"


def _classify_yield_support(ticker, daily_stats_df, criteria):
    if ticker not in daily_stats_df.index:
        return "Data N/A", 0, None, "缺少殖利率資料"

    current_yield = _safe_float(daily_stats_df.loc[ticker, "Yield"])
    yield_floor_min = criteria["yield_floor_min"]
    yield_neutral_min = criteria["yield_neutral_min"]

    if current_yield is None:
        return "Data N/A", 0, None, "缺少殖利率資料"
    if current_yield >= yield_floor_min:
        return "Floor Reached", 15, current_yield, "殖利率達到明顯支撐區"
    if current_yield >= yield_neutral_min:
        return "Neutral", 10, current_yield, "殖利率處於中性區"
    return "Ceiling", 4, current_yield, "殖利率偏低，保護性不足"


def _classify_momentum(price_df, benchmark_df, criteria):
    ma_window = int(criteria["momentum_ma_window"])
    lookback_days = int(criteria["momentum_lookback_days"])
    required_rows = max(30, lookback_days + 1)

    if price_df.empty or benchmark_df.empty or len(price_df) < required_rows or len(benchmark_df) < required_rows:
        return "Data N/A", 0, None, None, "缺少足夠股價資料"

    close = price_df["Close"].astype(float).dropna().reset_index(drop=True)
    bench = benchmark_df["Close"].astype(float).dropna().reset_index(drop=True)
    if len(close) < required_rows or len(bench) < required_rows:
        return "Data N/A", 0, None, None, "缺少足夠股價資料"

    moving_average = close.rolling(ma_window).mean().iloc[-1] if len(close) >= ma_window else None
    latest_price = float(close.iloc[-1])
    above_ma = moving_average is not None and latest_price > float(moving_average)

    stock_return = latest_price / float(close.iloc[-(lookback_days + 1)]) - 1 if len(close) >= (lookback_days + 1) else None
    bench_return = float(bench.iloc[-1]) / float(bench.iloc[-(lookback_days + 1)]) - 1 if len(bench) >= (lookback_days + 1) else None
    relative_strength = None
    outperform = None
    if stock_return is not None and bench_return is not None:
        relative_strength = stock_return - bench_return
        outperform = relative_strength > 0

    score = 0
    if above_ma:
        score += 10
    if outperform:
        score += 10

    if score >= 20:
        return "Uptrend / Outperform", score, above_ma, relative_strength, "股價站上長期均線且強於大盤"
    if score >= 10:
        return "Mixed", score, above_ma, relative_strength, "動能強弱參半"
    return "Weak", score, above_ma, relative_strength, "尚未出現明確強勢動能"


def _action_from_score(score, quality_status, criteria):
    strong_buy_min = criteria["action_strong_buy_min"]
    accumulate_min = criteria["action_accumulate_min"]
    hold_min = criteria["action_hold_min"]
    trim_min = criteria["action_trim_min"]

    if quality_status == "VALUE TRAP":
        return "SELL / AVOID"
    if score >= strong_buy_min:
        return "STRONG BUY"
    if score >= accumulate_min:
        return "ACCUMULATE"
    if score >= hold_min:
        return "HOLD"
    if score >= trim_min:
        return "TRIM / REDUCE"
    return "SELL / AVOID"


def evaluate_stock_strict_mode(
    ticker,
    sector,
    financials_df,
    valuation_df,
    daily_stats_df,
    revenue_metrics,
    price_df,
    benchmark_df,
    criteria=None,
):
    criteria = criteria or get_default_filter_criteria()

    quality_status, quality_score, latest_roe, roe_ttm, roe_trend, quality_note = _classify_quality(
        financials_df, revenue_metrics, criteria
    )
    river_signal, river_score, current_pe, current_pb, hist_center, river_note = _classify_river(
        ticker, sector, valuation_df, daily_stats_df, criteria
    )
    peg_signal, peg_score, peg_value, peg_note = _classify_peg(ticker, daily_stats_df, revenue_metrics, criteria)
    yield_signal, yield_score, current_yield, yield_note = _classify_yield_support(ticker, daily_stats_df, criteria)
    momentum_signal, momentum_score, above_ma200, relative_strength, momentum_note = _classify_momentum(
        price_df, benchmark_df, criteria
    )

    composite_score = quality_score + river_score + peg_score + yield_score + momentum_score
    action = _action_from_score(composite_score, quality_status, criteria)

    notes = [
        ("品質", quality_score, quality_note),
        ("估值", river_score, river_note),
        ("PEG", peg_score, peg_note),
        ("殖利率", yield_score, yield_note),
        ("動能", momentum_score, momentum_note),
    ]
    primary_driver = max(notes, key=lambda item: item[1])[2]
    key_risk = "請留意財報與估值資料可能不完整，並搭配產業與市場情境交叉判讀"
    if action in {"TRIM / REDUCE", "SELL / AVOID"}:
        key_risk = "目前品質、估值或動能未能支撐積極加碼"

    return {
        "ticker": ticker,
        "sector": sector,
        "quality_status": quality_status,
        "roe_ttm": roe_ttm,
        "latest_roe": latest_roe,
        "roe_trend": roe_trend,
        "river_signal": river_signal,
        "current_pe": current_pe,
        "current_pb": current_pb,
        "river_center": hist_center,
        "peg_signal": peg_signal,
        "peg_value": peg_value,
        "yield_signal": yield_signal,
        "current_yield": current_yield,
        "momentum_signal": momentum_signal,
        "latest_revenue_month": revenue_metrics.get("latest_revenue_month"),
        "latest_revenue": revenue_metrics.get("latest_revenue"),
        "latest_revenue_yoy": revenue_metrics.get("latest_revenue_yoy"),
        "avg_3m_revenue_yoy": revenue_metrics.get("avg_3m_revenue_yoy"),
        "above_ma200": above_ma200,
        "relative_strength_4w": relative_strength,
        "composite_score": composite_score,
        "action_plan": action,
        "primary_driver": primary_driver,
        "key_risk": key_risk,
        "quality_score": quality_score,
        "valuation_score": river_score + peg_score + yield_score,
        "momentum_score": momentum_score,
        "selected": action in {"STRONG BUY", "ACCUMULATE"},
    }
