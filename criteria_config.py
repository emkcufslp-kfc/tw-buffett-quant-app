from copy import deepcopy


DEFAULT_FILTER_CRITERIA = {
    "quality_compounder_min_roe": 15.0,
    "quality_investable_min_roe": 8.0,
    "quality_compounder_max_decline": -0.5,
    "quality_investable_max_decline": -1.0,
    "turnaround_min_revenue_yoy": 20.0,
    "value_trap_revenue_yoy": 0.0,
    "valuation_min_history": 5,
    "valuation_deep_value_std": 2.0,
    "valuation_cheap_std": 1.0,
    "valuation_expensive_std": 1.0,
    "peg_min_growth": 0.0,
    "peg_undervalued_max": 0.75,
    "peg_fair_value_max": 1.2,
    "peg_overvalued_min": 1.5,
    "yield_floor_min": 6.0,
    "yield_neutral_min": 4.0,
    "momentum_ma_window": 200,
    "momentum_lookback_days": 21,
    "action_strong_buy_min": 80,
    "action_accumulate_min": 65,
    "action_hold_min": 45,
    "action_trim_min": 25,
}


def get_default_filter_criteria():
    return deepcopy(DEFAULT_FILTER_CRITERIA)
