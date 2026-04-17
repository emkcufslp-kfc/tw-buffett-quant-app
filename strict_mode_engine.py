import math

import pandas as pd


def _safe_float(value):
    try:
        if pd.isna(value):
            return None
        return float(value)
    except Exception:
        return None


def _classify_quality(df, revenue_metrics):
    latest_roe = _safe_float(df["ROE"].iloc[-1]) if not df.empty else None
    recent = df["ROE"].tail(min(4, len(df))).astype(float) if not df.empty else pd.Series(dtype=float)
    roe_trend = _safe_float(recent.diff().mean()) if len(recent) >= 2 else None
    roe_ttm = _safe_float(recent.mean()) if not recent.empty else None
    avg_3m_revenue_yoy = _safe_float(revenue_metrics.get("avg_3m_revenue_yoy"))

    if roe_ttm is None:
        return "Data N/A", 0, latest_roe, roe_ttm, roe_trend, "缺少足夠 ROE 資料。"

    if roe_ttm > 15 and (roe_trend is None or roe_trend >= -0.5):
        return "COMPOUNDER", 30, latest_roe, roe_ttm, roe_trend, "ROE 高且維持穩定，屬高品質複利型公司。"
    if 8 <= roe_ttm <= 15 and (roe_trend is None or roe_trend >= -1.0):
        return "INVESTABLE", 20, latest_roe, roe_ttm, roe_trend, "ROE 處於可投資區間，品質尚可。"
    if roe_ttm < 8 and avg_3m_revenue_yoy is not None and avg_3m_revenue_yoy > 20:
        return "TURNAROUND", 12, latest_roe, roe_ttm, roe_trend, "ROE 偏低，但近 3 個月月營收 YoY 明顯加速，具轉機特徵。"
    if roe_ttm < 8:
        if avg_3m_revenue_yoy is not None and avg_3m_revenue_yoy < 0:
            return "VALUE TRAP", 5, latest_roe, roe_ttm, roe_trend, "ROE 偏低且近 3 個月月營收 YoY 下滑，疑似價值陷阱。"
        return "VALUE TRAP", 5, latest_roe, roe_ttm, roe_trend, "ROE 偏低，轉機證據不足。"
    return "INVESTABLE", 15, latest_roe, roe_ttm, roe_trend, "品質中性。"


def _classify_river(ticker, sector, val_history_df, daily_stats_df):
    if val_history_df.empty or ticker not in daily_stats_df.index:
        return "Data N/A", 0, None, None, None, "估值資料不足。"

    is_financial = sector in {"金融保險業", "金融業", "Financial Services"}
    current_pe = _safe_float(daily_stats_df.loc[ticker, "PE"])
    current_pb = _safe_float(daily_stats_df.loc[ticker, "PB"])

    if is_financial:
        pb_series = val_history_df["PB"].dropna()
        if len(pb_series) < 5 or current_pb is None:
            return "Data N/A", 0, None, current_pb, None, "金融股歷史 P/B 資料不足。"
        mean_pb = float(pb_series.mean())
        std_pb = float(pb_series.std()) if not math.isnan(float(pb_series.std())) else 0.0
        deep_value = mean_pb - 2.0 * std_pb
        cheap = mean_pb - 1.0 * std_pb
        expensive = mean_pb + 1.0 * std_pb

        if current_pb < deep_value:
            return "Deep Value", 25, None, current_pb, None, "金融股 P/B 處於深度低檔。"
        if current_pb < cheap:
            return "Cheap", 20, None, current_pb, None, "金融股 P/B 低於長期均值一個標準差。"
        if current_pb > expensive:
            return "Expensive", 5, None, current_pb, None, "金融股 P/B 偏高。"
        return "Fair", 12, None, current_pb, None, "金融股 P/B 位於合理區間。"

    pe_series = val_history_df["PE"].dropna()
    if len(pe_series) < 5 or current_pe is None:
        return "Data N/A", 0, current_pe, None, None, "歷史 P/E 資料不足。"
    mean_pe = float(pe_series.mean())
    std_pe = float(pe_series.std()) if not math.isnan(float(pe_series.std())) else 0.0
    deep_value = mean_pe - 2.0 * std_pe
    cheap = mean_pe - 1.0 * std_pe
    expensive = mean_pe + 1.0 * std_pe

    if current_pe < deep_value:
        return "Deep Value", 25, current_pe, None, mean_pe, "本益比位於五年河流圖深度低檔。"
    if current_pe < cheap:
        return "Cheap", 20, current_pe, None, mean_pe, "本益比低於五年均值一個標準差。"
    if current_pe > expensive:
        return "Expensive", 5, current_pe, None, mean_pe, "本益比高於五年均值一個標準差。"
    return "Fair", 12, current_pe, None, mean_pe, "本益比位於合理河道。"


def _classify_peg(ticker, daily_stats_df, revenue_metrics):
    if ticker not in daily_stats_df.index:
        return "Data N/A", 0, None, "缺少即時 P/E。"
    pe_value = _safe_float(daily_stats_df.loc[ticker, "PE"])
    growth = _safe_float(revenue_metrics.get("avg_3m_revenue_yoy"))
    if pe_value is None:
        return "Data N/A", 0, None, "缺少即時 P/E。"
    if growth is None or growth <= 0:
        return "Data N/A", 4, None, "缺少有效的近 3 個月平均月營收 YoY，PEG 採保守分數。"

    peg = pe_value / growth
    if peg < 0.75:
        return "Undervalued", 15, peg, "PEG < 0.75，屬強買訊號。"
    if peg <= 1.2:
        return "Fair Value", 10, peg, "PEG 位於合理區間。"
    if peg > 1.5:
        return "Overvalued", 3, peg, "PEG 偏高。"
    return "Fair Value", 7, peg, "PEG 略高於中性區。"


def _classify_yield_support(ticker, daily_stats_df):
    if ticker not in daily_stats_df.index:
        return "Data N/A", 0, None, "缺少殖利率資料。"
    current_yield = _safe_float(daily_stats_df.loc[ticker, "Yield"])
    if current_yield is None:
        return "Data N/A", 0, None, "缺少殖利率資料。"
    if current_yield >= 6.0:
        return "Floor Reached", 15, current_yield, "殖利率偏高，具防守支撐。"
    if current_yield >= 4.0:
        return "Neutral", 10, current_yield, "殖利率位於可接受區間。"
    return "Ceiling", 4, current_yield, "殖利率偏低，防守支撐有限。"


def _classify_momentum(price_df, benchmark_df):
    if price_df.empty or benchmark_df.empty or len(price_df) < 30 or len(benchmark_df) < 30:
        return "Data N/A", 0, None, None, "價格歷史不足，無法判斷動能。"

    close = price_df["Close"].astype(float).dropna().reset_index(drop=True)
    bench = benchmark_df["Close"].astype(float).dropna().reset_index(drop=True)
    if len(close) < 30 or len(bench) < 30:
        return "Data N/A", 0, None, None, "價格歷史不足，無法判斷動能。"

    ma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else None
    latest_price = float(close.iloc[-1])
    above_ma200 = ma200 is not None and latest_price > float(ma200)

    stock_4w = latest_price / float(close.iloc[-21]) - 1 if len(close) >= 21 else None
    bench_4w = float(bench.iloc[-1]) / float(bench.iloc[-21]) - 1 if len(bench) >= 21 else None
    relative_strength = None
    outperform = None
    if stock_4w is not None and bench_4w is not None:
        relative_strength = stock_4w - bench_4w
        outperform = relative_strength > 0

    score = 0
    if above_ma200:
        score += 10
    if outperform:
        score += 10

    if score >= 20:
        signal = "Uptrend / Outperform"
        note = "股價位於 MA200 之上且近四週強於大盤。"
    elif score >= 10:
        signal = "Mixed"
        note = "動能中性，僅部分條件成立。"
    else:
        signal = "Weak"
        note = "動能偏弱，尚未形成價值與動能共振。"
    return signal, score, above_ma200, relative_strength, note


def _action_from_score(score, quality_status):
    if quality_status == "VALUE TRAP":
        return "SELL / AVOID"
    if score >= 80:
        return "STRONG BUY"
    if score >= 65:
        return "ACCUMULATE"
    if score >= 45:
        return "HOLD"
    if score >= 25:
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
):
    quality_status, quality_score, latest_roe, roe_ttm, roe_trend, quality_note = _classify_quality(
        financials_df, revenue_metrics
    )
    river_signal, river_score, current_pe, current_pb, hist_center, river_note = _classify_river(
        ticker, sector, valuation_df, daily_stats_df
    )
    peg_signal, peg_score, peg_value, peg_note = _classify_peg(ticker, daily_stats_df, revenue_metrics)
    yield_signal, yield_score, current_yield, yield_note = _classify_yield_support(ticker, daily_stats_df)
    momentum_signal, momentum_score, above_ma200, relative_strength, momentum_note = _classify_momentum(
        price_df, benchmark_df
    )

    composite_score = quality_score + river_score + peg_score + yield_score + momentum_score
    action = _action_from_score(composite_score, quality_status)

    notes = [
        ("品質", quality_score, quality_note),
        ("河流圖估值", river_score, river_note),
        ("即時 PEG", peg_score, peg_note),
        ("殖利率支撐", yield_score, yield_note),
        ("動能", momentum_score, momentum_note),
    ]
    primary_driver = max(notes, key=lambda item: item[1])[2]
    key_risk = "月營收與前瞻股利資料目前為 Data N/A，嚴格模式中的部分子模型以保守中性分處理。"
    if action in {"TRIM / REDUCE", "SELL / AVOID"}:
        key_risk = "估值與品質沒有形成足夠安全邊際，且動能保護有限。"

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
