from collections import defaultdict

def build_portfolio(stocks, sector_map):
    max_stock_weight = 0.10
    max_sector_weight = 0.40
    weights = {}
    sector_weights = defaultdict(float)
    n = len(stocks)
    base_weight = min(1/n, max_stock_weight)
    for s in stocks:
        sector = sector_map[s]
        if sector_weights[sector] + base_weight <= max_sector_weight:
            weights[s] = base_weight
            sector_weights[sector] += base_weight
    return weights