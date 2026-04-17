from collections import defaultdict

def build_portfolio(stocks, sector_map, max_stock_weight=0.10, max_sector_weight=0.40):
    """
    Institutional Position Sizing Engine:
    - Max 10% per stock.
    - Max 40% per industry sector.
    """
    if not stocks:
        return {}

    weights = {stock: 0.0 for stock in stocks}
    sector_weights = defaultdict(float)

    remaining_capital = 1.0
    min_step = 1e-9

    while remaining_capital > min_step:
        eligible = []
        for stock in stocks:
            sector = sector_map.get(stock, "Unknown")
            stock_room = max_stock_weight - weights[stock]
            sector_room = max_sector_weight - sector_weights[sector]
            alloc_room = min(stock_room, sector_room)
            if alloc_room > min_step:
                eligible.append((stock, sector, alloc_room))

        if not eligible:
            break

        target_weight = remaining_capital / len(eligible)
        allocated_this_round = 0.0

        for stock, sector, _ in eligible:
            stock_room = max_stock_weight - weights[stock]
            sector_room = max_sector_weight - sector_weights[sector]
            allocation = min(target_weight, stock_room, sector_room, remaining_capital)
            if allocation <= min_step:
                continue

            weights[stock] += allocation
            sector_weights[sector] += allocation
            remaining_capital -= allocation
            allocated_this_round += allocation

        if allocated_this_round <= min_step:
            break

    return weights
