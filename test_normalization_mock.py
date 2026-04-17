import pandas as pd
from data_loader import _normalize_financial_columns, _pivot_financial_df

def test_normalization():
    print("Mock Testing Normalization Logic...")
    
    # Mock data based on the 5481 debug output
    mock_cash_flow_data = [
        {'date': '2023-12-31', 'type': 'NetCashInflowFromOperatingActivities', 'value': 1000},
        {'date': '2023-12-31', 'type': 'PropertyAndPlantAndEquipment', 'value': -200},
        {'date': '2023-09-30', 'type': 'NetCashInflowFromOperatingActivities', 'value': 800},
        {'date': '2023-09-30', 'type': 'PropertyAndPlantAndEquipment', 'value': -150},
    ]
    
    df = pd.DataFrame(mock_cash_flow_data)
    pivoted = _pivot_financial_df(df)
    normalized = _normalize_financial_columns(pivoted)
    
    print(f"Columns after normalization: {list(normalized.columns)}")
    
    success = True
    if 'OperatingCashFlow' not in normalized.columns:
        print("Failed: 'OperatingCashFlow' not found in normalized columns")
        success = False
    else:
        print("Success: 'OperatingCashFlow' mapped correctly.")

    if 'CapitalExpenditure' not in normalized.columns:
        print("Failed: 'CapitalExpenditure' not found in normalized columns")
        success = False
    else:
        print("Success: 'CapitalExpenditure' mapped correctly.")
        
    if success:
        print("\nALL MOCK TESTS PASSED!")
    else:
        print("\nUSEFUL TESTS FAILED.")

if __name__ == "__main__":
    test_normalization()
