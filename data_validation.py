import pandas as pd

REQUIRED_COLUMNS = {"date", "ROE", "OperatingCashFlow", "FCF"}


def validate_financial_data(df, min_rows=4, max_na_ratio=0.3):
    if df is None:
        return False
    if df.empty:
        return False
    if len(df) < min_rows:
        return False
    if not REQUIRED_COLUMNS.issubset(df.columns):
        return False

    working_df = df.loc[:, sorted(REQUIRED_COLUMNS)].copy()
    working_df["date"] = working_df["date"].astype(str).str.strip()
    if (working_df["date"] == "").all():
        return False

    numeric_df = working_df.drop(columns=["date"]).apply(pd.to_numeric, errors="coerce")
    if numeric_df.isna().all().all():
        return False
    if numeric_df.isna().sum().sum() > max_na_ratio * numeric_df.size:
        return False

    return True
