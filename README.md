# 🏖️ Silicon Beach Tech Companies

Interactive map of LA tech companies with commute analysis, transit routes, and network referral tracking.

## Features

- 📍 Interactive map with company locations
- 🚇 Commute analysis and transit routes
- 🔗 Network referral tracking
- 💼 VC firms and tech jobs
- 🆓 Free forever (DuckDB, no cloud costs)

## Tech Stack

- **Streamlit** - Web app framework
- **DuckDB** - Fast analytics database
- **Folium** - Interactive maps
- **Pandas** - Data processing

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run app
streamlit run app.py
```

## Deploy to Streamlit Cloud

1. Push to GitHub
2. Go to https://share.streamlit.io/
3. Deploy with:
   - **Main file:** `app.py`
   - **No secrets needed!**

## Data

Data stored in `data/silicon_beach.duckdb` - a local DuckDB database file.

