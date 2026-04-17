from collections import defaultdict

def build_portfolio(stocks, sector_map, max_stock_weight=0.10, max_sector_weight=0.40):
    """
    Institutional Position Sizing Engine:
    - Max 10% per stock.
    - Max 40% per industry sector.
    """
    if not stocks:
        return {}

    weights = {}
    sector_weights = defaultdict(float)
    
    # Simple Equal Weight with Constraints
    # We attempt to distribute weights equally, capping at the limits.
    target_weight = 1.0 / len(stocks)
    
    # 1. Apply Individual Cap
    individual_weight = min(target_weight, max_stock_weight)
    
    # 2. Iterate and apply Industry Cap
    for s in stocks:
        sector = sector_map.get(s, "Unknown")
        # Can we add this stock without breaking industry cap?
        if sector_weights[sector] + individual_weight <= max_sector_weight:
            weights[s] = individual_weight
            sector_weights[sector] += individual_weight
        else:
            # Add up to the cap
            remaining_room = max_sector_weight - sector_weights[sector]
            if remaining_room > 0:
                weights[s] = remaining_room
                sector_weights[sector] += remaining_room
            else:
                weights[s] = 0.0
                
    return weights
