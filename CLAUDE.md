# Groupon Analytical Engineer Case Study

## Project Overview

This repo contains the solution for the Groupon Analytical Engineer case study. The work is structured across three assignments covering data cleaning, SQL analysis, and data engineering thinking. All results are presented via an interactive Streamlit app.

## Tech Stack

- **Python 3.12** managed via **uv**
- **pandas** — data cleaning and merging (Assignment 1)
- **DuckDB** — BigQuery-compatible SQL engine running locally (Assignment 2)
- **Streamlit** — interactive presentation layer
- **Plotly** — charts and visualizations
- **pytest** — data quality tests

## Project Structure

```
groupon-interview/
├── data/
│   ├── raw/                  # Original CSVs (do not modify)
│   │   ├── orders_historical.csv
│   │   └── orders_2024_2025.csv
│   └── cleaned/              # Output of Assignment 1
│       └── orders_merged.csv
├── sql/                      # BigQuery-dialect SQL files
│   ├── master_customer_table.sql
│   ├── q1_revenue_mix.sql
│   └── q2_platform_performance.sql
├── src/
│   ├── cleaning.py           # Assignment 1: data cleaning logic
│   ├── analysis.py           # Assignment 2: query execution via DuckDB
│   └── utils.py              # Shared helpers
├── tests/
│   └── test_cleaning.py      # Data quality assertions
├── app.py                    # Streamlit app (main entry point)
├── pyproject.toml            # uv-managed dependencies
└── CLAUDE.md
```

## Running the App

```bash
uv run streamlit run app.py
```

## Running Tests

```bash
uv run pytest tests/
```

## Key Domain Rules

- **Activation**: a customer's very first order ever.
- **Reactivation**: any order where the gap since the customer's previous order exceeds 365 days.
- **Retained regular**: all other returning orders.
- **USD conversion**: `gross_bookings_usd = gross_bookings_operational * fx_rate_loc_to_usd_fxn`
- All financial columns (`*_operational`) are in local currency.
- SQL targets **BigQuery standard SQL dialect**.

## AI Usage Log

Each assignment includes a note on how AI (Claude) was used and estimated time spent, as required by the case study instructions.

## Dataset

| File | Rows | Date Range |
|------|------|------------|
| orders_historical.csv | 778 | Jan 2021 – Jun 2023 |
| orders_2024_2025.csv | 464 | Jul 2023 – Feb 2025 |

Both files share the same 15-column schema and are merged in Assignment 1.
