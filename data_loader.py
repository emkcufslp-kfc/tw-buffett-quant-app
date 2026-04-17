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


# Taiwan Top 50 (Blue Chip) Fallback Universe
STATIC_UNIVERSE_TW50 = [
    '2330', '2317', '2454', '2308', '2881', '2882', '2412', '2303', '2886', '2891',
    '3711', '2884', '1216', '2892', '2408', '2885', '2382', '2409', '2357', '2002',
    '1301', '1303', '1326', '6505', '2880', '2883', '2379', '2603', '2474', '2327',
    '2887', '2912', '1101', '2890', '1402', '2609', '2301', '1102', '2207', '2801',
    '2345', '2615', '1227', '5880', '2888', '5871', '1590', '4966', '3034', '3037'
]


def get_stock_universe(api):
    try:
        info = api.taiwan_stock_info()
    except Exception as exc:
        # Fallback to Static Taiwan 50 Universe if API is limited (e.g. 402 Error)
        return pd.DataFrame({
            'stock_id': STATIC_UNIVERSE_TW50,
            'industry_category': ['Blue Chip Fallback'] * len(STATIC_UNIVERSE_TW50)
        }), "Static (Taiwan 50 Fallback)"
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

    return info[['stock_id', 'industry_category']].drop_duplicates(), "FinMind (Primary)"

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


def get_financials_yf(ticker):
    """
    Fallback driver using yfinance to extract fundamental data.
    """
    ticker_tw = ticker + ".TW"
    t = yf.Ticker(ticker_tw)
    
    # Fetch Annual Data
    income = t.financials.T
    cashflow = t.cashflow.T
    balance = t.balance_sheet.T
    
    if income.empty or cashflow.empty or balance.empty:
        raise RuntimeError(f"YFinance returned empty data for {ticker}")

    # Map YFinance fields to Internal Schema
    df = pd.DataFrame(index=income.index)
    df.index.name = "date"
    
    # Basic Income & Cash Flow
    df['NetIncome'] = income.get('Net Income', 0)
    df['OperatingCashFlow'] = cashflow.get('Operating Cash Flow', 0)
    df['CapitalExpenditure'] = cashflow.get('Capital Expenditure', 0)
    
    # Calculate ROE from Net Income / Stockholders Equity
    equity = balance.get('Stockholders Equity', 0)
    if isinstance(equity, pd.Series):
        # Align dates: YFinance might have different dates across statements
        df = df.join(equity.to_frame('Equity'), how='left')
        df['ROE'] = (df['NetIncome'] / df['Equity'] * 100).fillna(0)
    else:
        df['ROE'] = 0
        
    df = df.reset_index()
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    
    # Calculate FCF
    df['FCF'] = df['OperatingCashFlow'] - df['CapitalExpenditure'].abs()
    
    return df, "Yahoo Finance (Fallback)"



def get_financials(api, ticker):
    """
    Universal entry point with automatic High-Availability Fallback.
    """
    # 1. Primary Driver: FinMind
    try:
        roe_df = api.taiwan_stock_financial_statement(stock_id=ticker, start_date="2006-01-01")
        cash_df = api.taiwan_stock_cash_flows_statement(stock_id=ticker, start_date="2006-01-01")
        
        roe_df = _pivot_financial_df(roe_df)
        cash_df = _pivot_financial_df(cash_df)
        roe_df = _normalize_financial_columns(roe_df)
        cash_df = _normalize_financial_columns(cash_df)

        if roe_df.empty or cash_df.empty:
            raise KeyError("Empty FinMind response")

        merged = pd.merge(roe_df, cash_df, on='date', how='inner')
        if merged.empty:
            raise KeyError("No overlap in FinMind data")

        if 'OperatingCashFlow' not in merged.columns:
             raise KeyError("Missing OCF")

        if 'CapitalExpenditure' not in merged.columns:
            merged['CapitalExpenditure'] = 0

        merged['FCF'] = merged['OperatingCashFlow'] - merged['CapitalExpenditure'].abs()
        return merged, "FinMind (Primary)"

    except Exception as exc:
        # 2. Secondary Driver: Yahoo Finance Fallback
        # We trigger fallback for 402 errors or any data availability issue
        try:
            return get_financials_yf(ticker)
        except Exception as yf_exc:
            raise RuntimeError(
                f"Both FinMind and Yahoo Finance failed for {ticker}. "
                f"FinMind Error: {exc} | YFinance Error: {yf_exc}"
            ) from yf_exc