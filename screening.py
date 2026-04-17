import pandas as pd

from data_loader import (
    CacheManager,
    fetch_twse_daily_stats,
    get_financials,
    get_historical_valuation,
    get_industry_info,
    get_stock_universe,
)
from data_validation import validate_financial_data
from factor_engine import quality_filter
from portfolio_engine import build_portfolio
from regime_filter import market_regime
from valuation_engine import valuation_filter


def scan_universe(
    cache=None,
    daily_stats_df=None,
    force_refresh=False,
    top_n_per_sector=100,
    roe_avg_tgt=15,
    roe_min_tgt=10,
    roe_min_count=2,
    fcf_years=10,
    yield_tgt=4.0,
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
    universe = universe_df["stock_id"].tolist()
    universe_sector_map = universe_df.set_index("stock_id")["industry_category"].to_dict()
    regime = market_regime()

    selected = []
    sector_map = {}
    diagnostics = []
    errors = []

    for ticker in universe:
        try:
            if ticker in cache and not force_refresh:
                cached = cache[ticker]
                df = cached["financials"]
                val_history = cached["valuation"]
                sector = cached["sector"]
                source = "cache"
            else:
                df = get_financials(ticker)
                if df.empty:
                    diagnostics.append(
                        {
                            "ticker": ticker,
                            "sector": "Unknown",
                            "data_source": "yfinance",
                            "quality": "No financial data",
                            "valuation": "Not evaluated",
                            "selected": False,
                        }
                    )
                    continue

                val_history = get_historical_valuation(ticker, df)
                sector = universe_sector_map.get(ticker, "Unknown")
                if sector == "Unknown":
                    sector, _ = get_industry_info(ticker)
                cache[ticker] = {
                    "financials": df,
                    "valuation": val_history,
                    "sector": sector,
                }
                source = "live"

            if not validate_financial_data(df):
                diagnostics.append(
                    {
                        "ticker": ticker,
                        "sector": sector,
                        "data_source": source,
                        "quality": "Financial data validation failed",
                        "valuation": "Not evaluated",
                        "selected": False,
                    }
                )
                continue

            q_pass, q_msg = quality_filter(
                df,
                roe_avg_tgt=roe_avg_tgt,
                roe_min_tgt=roe_min_tgt,
                roe_min_count=roe_min_count,
                fcf_consecutive=fcf_years,
            )
            v_pass, v_msg = valuation_filter(
                ticker,
                val_history,
                daily_stats_df,
                sector,
                yield_tgt=yield_tgt,
            )
            selected_flag = q_pass and v_pass
            if selected_flag:
                selected.append(ticker)
                sector_map[ticker] = sector

            diagnostics.append(
                {
                    "ticker": ticker,
                    "sector": sector,
                    "data_source": source,
                    "quality": q_msg,
                    "valuation": v_msg,
                    "selected": selected_flag,
                }
            )
        except Exception as exc:
            errors.append({"ticker": ticker, "error": str(exc)})

    portfolio = build_portfolio(
        selected,
        sector_map,
        max_stock_weight=max_stock_weight,
        max_sector_weight=max_sector_weight,
    )
    CacheManager.save(cache)

    return {
        "regime": regime,
        "universe_size": len(universe),
        "selected": selected,
        "sector_map": sector_map,
        "portfolio": portfolio,
        "diagnostics": pd.DataFrame(diagnostics),
        "errors": pd.DataFrame(errors),
        "daily_stats": daily_stats_df,
        "cache": cache,
    }
