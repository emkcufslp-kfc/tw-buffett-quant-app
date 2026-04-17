# TW Buffett Quant Framework (2006-2026)

A production-grade quantitative value investment framework for Taiwan stocks, inspired by Warren Buffett. This Streamlit app allows users to access the strategy, view backtests, and make buy decisions.

## Features
- Dashboard with current status, portfolio weights, and metrics.
- Historical backtest charts (2006-2026).
- Stock selection for buy decisions.
- Implementation action plan.

## Installation
1. Clone the repo.
2. Install dependencies: `pip install -r requirements.txt`
3. Run the app: `streamlit run app.py`

## Deployment
Deploy to Streamlit Cloud via GitHub.

### Streamlit Secrets
To keep your FinMind API key secure, add it to Streamlit Secrets instead of entering it manually in the app.

1. In Streamlit Cloud, open your app.
2. Go to "Settings" > "Secrets".
3. Add this key/value pair:

```toml
FINMIND_API_KEY = "your-finmind-api-key"
```

4. Save and redeploy.

The app will automatically use `FINMIND_API_KEY` from Streamlit Secrets if available.

## Data Sources
- FinMind API for financial data.
- yfinance for price data.

## License
Public domain.