import pandas as pd

from data_loader import (
    CacheManager,
    fetch_twse_daily_stats,
    get_financials,
    get_historical_valuation,
    get_latest_monthly_revenue_metrics,
    fetch_monthly_revenue_history,
    get_price_history,
    get_stock_universe,
    get_taiex_history,
)
from data_validation import validate_financial_data
from portfolio_engine import build_portfolio
from regime_filter import market_regime
from strict_mode_engine import evaluate_stock_strict_mode


def scan_universe(
    cache=None,
    daily_stats_df=None,
    force_refresh=False,
    top_n_per_sector=100,
    max_stock_weight=0.10,
    max_sector_weight=0.40,
):
    cache = cache or {}
    if daily_stats_df is None:
        daily_stats_df = fetch_twse_daily_stats()

    universe_df = get_stock_universe(
        top_n_per_sector=top_n_per_sector,
        force_refresh=force_refresh,
    )
    regime = market_regime()
    benchmark_df = get_taiex_history(period="1y")
    revenue_history_df = fetch_monthly_revenue_history(force_refresh=force_refresh)

    ranked_rows = []
    diagnostics = []
    errors = []
    selected = []
    sector_map = {}

    for row in universe_df.itertuples(index=False):
        ticker = row.stock_id
        sector = row.industry_category
        name = getattr(row, "stock_name", ticker)

        try:
            if ticker in cache and not force_refresh:
                cached = cache[ticker]
                financials_df = cached["financials"]
                valuation_df = cached["valuation"]
                price_df = cached.get("price_history", pd.DataFrame())
                source = "cache"
            else:
                financials_df = get_financials(ticker)
                valuation_df = get_historical_valuation(ticker, financials_df) if not financials_df.empty else pd.DataFrame()
                price_df = get_price_history(ticker, period="1y")
                cache[ticker] = {
                    "financials": financials_df,
                    "valuation": valuation_df,
                    "sector": sector,
                    "price_history": price_df,
                }
                source = "live"

            if financials_df.empty:
                diagnostics.append(
                    {
                        "ticker": ticker,
                        "name": name,
                        "sector": sector,
                        "data_source": source,
                        "quality": "No financial data",
                        "valuation": "Data N/A",
                        "action_plan": "SELL / AVOID",
                        "selected": False,
                    }
                )
                continue

            if not validate_financial_data(financials_df):
                diagnostics.append(
                    {
                        "ticker": ticker,
                        "name": name,
                        "sector": sector,
                        "data_source": source,
                        "quality": "Financial data validation failed",
                        "valuation": "Data N/A",
                        "action_plan": "SELL / AVOID",
                        "selected": False,
                    }
                )
                continue

            evaluation = evaluate_stock_strict_mode(
                ticker=ticker,
                sector=sector,
                financials_df=financials_df,
                valuation_df=valuation_df,
                daily_stats_df=daily_stats_df,
                revenue_metrics=get_latest_monthly_revenue_metrics(ticker, revenue_history_df),
                price_df=price_df,
                benchmark_df=benchmark_df,
            )
            evaluation["name"] = name
            evaluation["data_source"] = source
            ranked_rows.append(evaluation)

            diagnostics.append(
                {
                    "ticker": ticker,
                    "name": name,
                    "sector": sector,
                    "data_source": source,
                    "quality": evaluation["quality_status"],
                    "valuation": evaluation["river_signal"],
                    "action_plan": evaluation["action_plan"],
                    "selected": evaluation["selected"],
                }
            )

            if evaluation["selected"]:
                selected.append(ticker)
                sector_map[ticker] = sector
        except Exception as exc:
            errors.append({"ticker": ticker, "name": name, "sector": sector, "error": str(exc)})

    ranked_df = pd.DataFrame(ranked_rows)
    if not ranked_df.empty:
        ranked_df = ranked_df.sort_values(
            ["composite_score", "quality_score", "momentum_score"],
            ascending=[False, False, False],
        ).reset_index(drop=True)

    portfolio = build_portfolio(
        selected,
        sector_map,
        max_stock_weight=max_stock_weight,
        max_sector_weight=max_sector_weight,
    )
    CacheManager.save(cache)

    return {
        "regime": regime,
        "universe_size": len(universe_df),
        "selected": selected,
        "sector_map": sector_map,
        "portfolio": portfolio,
        "ranked": ranked_df,
        "best_pick": ranked_df.head(1) if not ranked_df.empty else pd.DataFrame(),
        "diagnostics": pd.DataFrame(diagnostics),
        "errors": pd.DataFrame(errors),
        "daily_stats": daily_stats_df,
        "cache": cache,
        "universe": universe_df,
    }
