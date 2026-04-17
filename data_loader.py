import pandas as pd
from FinMind.data import DataLoader
from FinMind.utility.request import request_get
import yfinance as yf

class SafeDataLoader(DataLoader):
    def get_data(
        self,
        dataset,
        data_id: str = "",
        securities_trader_id: str = "",
        stock_id: str = "",
        start_date: str = "",
        data_id_list: list = None,
        securities_trader_id_list: list = None,
        end_date: str = "",
        timeout: int = 60,
        use_async: bool = False,
        max_retry_times: int = 30,
    ) -> pd.DataFrame:
        try:
            return super().get_data(
                dataset=dataset,
                data_id=data_id,
                securities_trader_id=securities_trader_id,
                stock_id=stock_id,
                start_date=start_date,
                data_id_list=data_id_list,
                securities_trader_id_list=securities_trader_id_list,
                end_date=end_date,
                timeout=timeout,
                use_async=use_async,
                max_retry_times=max_retry_times,
            )
        except KeyError:
            url = self._dispatcher_url(dataset)
            params = dict(
                dataset=dataset,
                data_id=data_id,
                securities_trader_id=securities_trader_id,
                stock_id=stock_id,
                start_date=start_date,
                end_date=end_date,
                user_id=self._FinMindApi__user_id,
                password=self._FinMindApi__password,
                device=self._FinMindApi__device,
            )
            params = self._compatible_api_version(params)
            params = self._compatible_endpoints_param(params)
            response = request_get(
                self._FinMindApi__session,
                url,
                params=params,
                timeout=timeout,
            )
            response_json = response.json()
            if isinstance(response_json, dict):
                if 'data' in response_json:
                    return pd.DataFrame(response_json['data'])
                for fallback_key in ['result', 'items', 'dataList', 'stock_data', 'stockInfo']:
                    if fallback_key in response_json and isinstance(response_json[fallback_key], list):
                        return pd.DataFrame(response_json[fallback_key])
                if len(response_json) == 1:
                    value = list(response_json.values())[0]
                    if isinstance(value, list):
                        return pd.DataFrame(value)
            if isinstance(response_json, list):
                return pd.DataFrame(response_json)
            raise RuntimeError(
                f"FinMind get_data returned unexpected response shape: {response_json}"
            )


def get_stock_universe(api):
    try:
        info = api.taiwan_stock_info()
    except Exception as exc:
        raise RuntimeError(
            "Failed to load FinMind stock universe. "
            "Please verify your FinMind API key, network access, and FinMind token permissions. "
            f"Error: {exc.__class__.__name__}: {exc}"
        ) from exc

    if info is None or info.empty:
        raise RuntimeError(
            "FinMind taiwan_stock_info returned no data. "
            "Please verify your FinMind API key and try again."
        )

    if 'stock_name' in info.columns:
        info = info[~info['stock_name'].str.contains("KY", na=False)]

    date_column = None
    for candidate in ['start_date', 'listing_date', 'issue_date', '上市日期', '上市日']:
        if candidate in info.columns:
            date_column = candidate
            break

    if date_column is not None:
        info['start_date'] = pd.to_datetime(info[date_column], errors='coerce')
        if info['start_date'].notna().any():
            info['listing_years'] = (pd.Timestamp.today() - info['start_date']).dt.days / 365
            info = info[info['listing_years'] >= 10]
    else:
        # No date column found, keep the universe but continue without the 10-year filter
        info = info.copy()

    expected_columns = {'stock_id', 'industry_category'}
    missing = expected_columns - set(info.columns)
    if missing:
        raise RuntimeError(
            "FinMind response is missing expected columns: "
            f"{', '.join(sorted(missing))}. "
            f"Available columns: {', '.join(info.columns)}"
        )

    return info[['stock_id', 'industry_category']].drop_duplicates()

def get_price_history(ticker):
    ticker_tw = ticker + ".TW"
    data = yf.download(ticker_tw, start="2006-01-01", progress=False)
    return data

def _pivot_financial_df(df):
    if df is None or df.empty:
        return pd.DataFrame()

    if 'date' in df.columns and 'value' in df.columns:
        pivot_column = 'type' if 'type' in df.columns else 'origin_name' if 'origin_name' in df.columns else None
        if pivot_column is not None:
            # Using pivot_table with aggfunc='last' to handle potential duplicate date/type pairs 
            # (e.g. restatements in the financial data)
            df = df.pivot_table(index='date', columns=pivot_column, values='value', aggfunc='last')
            df.columns = [col if isinstance(col, str) else str(col) for col in df.columns]
            df = df.reset_index()
            return df

    return df


def _normalize_financial_columns(df):
    if df is None or df.empty:
        return df

    mapping = {}
    for col in list(df.columns):
        lower = str(col).lower()
        # --- Operating Cash Flow Aliases ---
        if any(x in lower for x in ['roe', '權益報酬率', '股東權益報酬率']):
            mapping[col] = 'ROE'
        elif any(x in lower for x in ['operating', 'cash']) and ('flow' in lower or 'activity' in lower):
            mapping[col] = 'OperatingCashFlow'
        elif '營業' in lower and '現金' in lower and ('流入' in lower or '支出' in lower or '活動' in lower):
            mapping[col] = 'OperatingCashFlow'
        elif 'netcashinflowfromoperatingactivities' in lower:
            mapping[col] = 'OperatingCashFlow'
            
        # --- Capital Expenditure / PPE Aliases ---
        elif ('capital' in lower and 'expend' in lower) or '購置不動產' in lower:
            mapping[col] = 'CapitalExpenditure'
        elif '資本' in lower and ('支出' in lower or '支出' in lower):
            mapping[col] = 'CapitalExpenditure'
        elif 'propertyandplantandequipment' in lower or '不動產、廠房及設備' in lower:
            # PPE is a common proxy when direct CapEx is missing
            mapping[col] = 'CapitalExpenditure'
        elif 'free' in lower and 'cash' in lower:
            mapping[col] = 'FCF'

    if mapping:
        df = df.rename(columns=mapping)
    return df


def get_financials(api, ticker):
    try:
        roe_df = api.taiwan_stock_financial_statement(stock_id=ticker, start_date="2006-01-01")
        cash_df = api.taiwan_stock_cash_flows_statement(stock_id=ticker, start_date="2006-01-01")
    except Exception as exc:
        raise RuntimeError(
            f"Failed to load financials for {ticker}. "
            f"Please verify the FinMind API token and ticker value. ({exc})"
        ) from exc

    roe_df = _pivot_financial_df(roe_df)
    cash_df = _pivot_financial_df(cash_df)

    roe_df = _normalize_financial_columns(roe_df)
    cash_df = _normalize_financial_columns(cash_df)

    if roe_df.empty or cash_df.empty:
        raise RuntimeError(
            f"No financial or cash flow data returned for {ticker}. "
            f"ROE columns: {list(roe_df.columns)}; cash columns: {list(cash_df.columns)}"
        )

    if 'date' not in roe_df.columns or 'date' not in cash_df.columns:
        raise RuntimeError(
            f"Unexpected financial data format for {ticker}. "
            f"ROE columns: {list(roe_df.columns)}; cash columns: {list(cash_df.columns)}"
        )

    merged = pd.merge(roe_df, cash_df, on='date', how='inner')
    if merged.empty:
        raise RuntimeError(
            f"No merged financial data available for {ticker}. "
            f"ROE columns: {list(roe_df.columns)}; cash columns: {list(cash_df.columns)}"
        )

    if 'OperatingCashFlow' not in merged.columns:
        # If still missing, we check for 'CashFlowsFromOperatingActivities' or similar directly if normalization missed it
        raise RuntimeError(
            f"Missing Operating Cash Flow for {ticker}. "
            f"Merged columns: {list(merged.columns)}"
        )

    if 'CapitalExpenditure' not in merged.columns:
        # Graceful fallback: Treat as 0 if missing, rather than failing the entire stock.
        merged['CapitalExpenditure'] = 0

    merged['FCF'] = merged['OperatingCashFlow'] - merged['CapitalExpenditure'].abs()
    return merged