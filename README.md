# Groupon Analytical Engineer – Case Study

Interactive solution for the Groupon Analytical Engineer case study, covering data cleaning, SQL analysis, and data engineering thinking. Presented as a Streamlit app backed by DuckDB (BigQuery-compatible SQL) and pandas.

> **Formal submission document:** [SUBMISSION.md](./SUBMISSION.md)

The app has five tabs:

| Tab | Contents |
|-----|----------|
| Assignment 1 · Data Cleaning | Issues found/fixed, before/after view of the 8 corrected country rows, charts, cleaned dataset with download button |
| Assignment 2 · SQL Analysis | Results, charts, interpretations, and the full BigQuery SQL for each query in a collapsible panel |
| Assignment 3 · Engineering Thinking | Written answers to all three questions |
| Raw Data | Original source files side by side with null counts and data types |
| SQL Playground | Free-text SQL editor running DuckDB against the cleaned dataset |

---

## Quick Start

```bash
git clone <repo-url>
cd groupon-interview
uv sync
uv run streamlit run app.py
```

Run tests:

```bash
uv run pytest tests/ -v
```

---

## Project Structure

```
groupon-interview/
├── data/
│   ├── raw/                        # Original CSVs (unmodified)
│   │   ├── orders_historical.csv   # 778 rows, Jan 2021 – Jun 2023
│   │   └── orders_2024_2025.csv    # 464 rows, Jul 2023 – Feb 2025
│   └── cleaned/
│       └── orders_merged.csv       # 1,242 rows, output of Assignment 1
├── sql/                            # BigQuery standard SQL (read-only reference)
│   ├── master_customer_table.sql
│   ├── q1_revenue_mix.sql
│   └── q2_platform_performance.sql
├── src/
│   ├── cleaning.py                 # Assignment 1: merge + data quality pipeline
│   └── analysis.py                 # Assignment 2: DuckDB query execution
├── tests/
│   └── test_cleaning.py            # 14 data quality tests (all passing)
├── app.py                          # Streamlit app (entry point)
├── pyproject.toml                  # uv-managed dependencies
└── CLAUDE.md                       # Development guide for Claude Code
```

---

## Tech Stack

| Tool | Role |
|------|------|
| Python 3.12 via **uv** | Runtime and package management |
| **pandas** | Data cleaning and transformation |
| **DuckDB** | Local BigQuery-compatible SQL engine |
| **Streamlit** | Interactive presentation layer |
| **Plotly** | Charts and visualisations |
| **pytest** | Data quality assertions |

---

## Dataset

Two CSV files share the same 15-column schema and together form one logical dataset.

| File | Rows | Period |
|------|------|--------|
| `orders_historical.csv` | 778 | Jan 2021 – Jun 2023 |
| `orders_2024_2025.csv` | 464 | Jul 2023 – Feb 2025 |
| **Merged** | **1,242** | **Jan 2021 – Feb 2025** |

**Schema summary**

| Column | Type | Description |
|--------|------|-------------|
| `operational_view_date` | date | Order event date |
| `user_uuid` | string | Customer identifier |
| `customer_city` / `customer_country` | string | Customer location (ISO-3166 alpha-2) |
| `order_uuid` / `parent_order_uuid` | string | Order identifiers |
| `platform` | string | `app`, `web`, or `touch` (mobile browser) |
| `fx_rate_loc_to_usd_fxn` | float | Local currency → USD conversion rate |
| `list_price_operational` | float | Original price (local currency) |
| `deal_discount_operational` | float | Discount applied (local currency) |
| `gross_bookings_operational` | float | Net booking value (local currency) |
| `margin_1_operational` | float | First-level margin (local currency) |
| `vfm_operational` | float | Variable fulfilment margin (local currency) |
| `incentive_promo_code` | string | Promo code used (empty = no promo) |
| `last_status` | string | `redeemed`, `unredeemed`, `refunded`, `expired` |

---

## Assignment 1 – Data Cleaning & Preparation

### Issues Found and Fixed

| Issue | Count | Fix Applied |
|-------|-------|-------------|
| Null `customer_country` | 8 rows | Filled via deterministic city → country lookup (see table below) |
| Null `incentive_promo_code` | 863 rows | Standardised to `""` — null means no promo was used |
| `operational_view_date` as string | all rows | Parsed to `datetime.date` |
| No USD equivalents in raw data | — | Derived `*_usd` columns: `value_usd = value_operational × fx_rate` |

**City → country lookup used for null `customer_country`**

| City | Country |
|------|---------|
| Birmingham | GB |
| Manchester | GB |
| Lyon | FR |
| Barcelona | ES |
| Madrid | ES |
| Phoenix | US |
| Warsaw | PL |
| Munich | DE |

**Assumption:** each city name is unambiguous within this dataset. Cross-referenced against existing rows with the same city and confirmed no conflicts.

The app shows a before/after table with the exact 8 rows, the raw `NULL` value, and the inferred country code side by side.

### Items Kept (Expected Business Behaviour)

- **120 orders with `gross_bookings_operational ≤ 0`** — all have `last_status = 'refunded'`. Non-positive booking values for refunds are correct; removing them would distort volume and profitability metrics.
- **Platform value `touch`** — represents a mobile web browser session. Kept as a distinct value in the cleaned dataset; grouped with `web` only for platform-comparison analysis in Assignment 2.

### Derived Columns Added

```
gross_bookings_usd  = gross_bookings_operational  × fx_rate_loc_to_usd_fxn
margin_1_usd        = margin_1_operational         × fx_rate_loc_to_usd_fxn
vfm_usd             = vfm_operational              × fx_rate_loc_to_usd_fxn
gross_profit_usd    = margin_1_usd + vfm_usd
```

### Cleaned Dataset Stats

```
Total rows        : 1,242
Unique orders     : 1,242  (no duplicates)
Unique customers  : 266
Date range        : 2021-01-01 → 2025-02-27
Countries         : DE, ES, FR, GB, IT, PL, US
Platforms         : app, touch, web
Order statuses    : redeemed 729 | unredeemed 274 | refunded 120 | expired 119
```

---

## Assignment 2 – SQL Analysis

All queries are written in **BigQuery standard SQL** (see `sql/`). They are executed locally via **DuckDB** through `src/analysis.py`, with minimal syntax adaptations noted inline. The BigQuery SQL for each query is also visible directly in the app under a collapsible panel in each sub-tab.

> **Note on table references:** The `.sql` files use `` `project.dataset.orders_merged` `` as a placeholder for the BigQuery table name. Replace it with your actual project and dataset when running in BigQuery.

### Part A – Master Customer Table

Customer-grain table built with window functions to classify every order as:

- **`new`** — first order ever for that `user_uuid`
- **`reactivated`** — gap from previous order > 365 days
- **`retained`** — all other returning orders

**Output columns (266 rows, one per customer):**

| Column | Description |
|--------|-------------|
| `first_order_date` | Date of first ever order |
| `last_order_date` | Date of most recent order |
| `acquisition_cohort` | Month of first order (for cohort analysis) |
| `total_orders` | Lifetime order count |
| `total_gross_bookings_usd` | Lifetime revenue |
| `avg_order_value_usd` | Average order size |
| `total_gross_profit_usd` | Lifetime gross profit (margin_1 + vfm) |
| `avg_gross_profit_per_order_usd` | Average GP per order |
| `days_since_last_order` | Recency (from dataset max date) |
| `reactivation_count` | Number of reactivation events |
| `primary_platform` | Platform with highest order count |

**Portfolio totals:** $136,580 gross bookings USD · $73,536 gross profit USD across 266 customers.

---

### Part B – Business Questions

#### Question 1 – Customer Revenue Mix & Retention

**Last 6 months (Aug 2024 – Feb 2025):**

| Segment | Gross Bookings (USD) | Share |
|---------|----------------------|-------|
| Retained regulars | $13,803 | **83.3%** |
| Reactivated | $2,754 | 16.6% |
| New (activations) | $16 | **0.1%** |

**Interpretation:** The business is almost entirely dependent on its existing customer base. New customer acquisition is near zero in the most recent period — just 0.1% of bookings. Reactivations at 17% indicate that lapsed customers are returning, which is healthy, but the complete absence of new activations is a risk signal. If retained regulars churn, there is no acquisition pipeline to replace them.

**Historical mix shift:** The new/reactivated share has fluctuated between 10–25% in most months, but 2025 data shows a sharp drop in new activations. This trend warrants investigation — it could reflect seasonality, a change in acquisition spend, or data incompleteness for 2025.

---

#### Question 2 – Platform Performance & Strategy

**Customer-level comparison (touch grouped with web):**

| Metric | App | Web (incl. touch) |
|--------|-----|-------------------|
| Unique customers | 149 | 205 |
| Avg orders/customer | 3.38 | 3.60 |
| Avg order value (USD) | $102.41 | $113.19 |
| Avg gross profit/customer (USD) | $186.60 | $223.09 |
| Total gross bookings (USD) | $51,559 | $85,021 |

**Yearly app share of gross bookings:**

| Year | App % | Web % |
|------|-------|-------|
| 2021 | 38.8% | 61.2% |
| 2022 | 40.7% | 59.3% |
| 2023 | 33.3% | 66.7% |
| 2024 | 40.2% | 59.8% |
| 2025 | 23.9% | 76.1% |

**Interpretation:** Web customers consistently outperform app customers on every financial metric — 11% higher AOV, 6% more orders per customer, and 20% more gross profit per customer. The app's share of gross bookings has been unstable, with 2025 showing a significant drop (though 2025 has only 2 months of data). Given the current performance gap, prioritising app acquisition over web would not be justified by the data alone. The recommendation would be to investigate *why* web customers are more valuable before shifting acquisition spend — it may reflect product mix, geography, or customer demographics rather than the channel itself.

---

## Assignment 3 – Data Quality & Engineering Thinking

### Q1 – Financial Column Conventions

The `_operational` suffix indicates that financial values are expressed in the **local transaction currency** (the currency in which the customer was charged). This is standard practice in multi-currency e-commerce: storing values at their local source preserves the original transaction truth before any FX conversion introduces rounding or rate variability.

To convert to USD for cross-country comparison:
```
value_usd = value_operational × fx_rate_loc_to_usd_fxn
```

The analytical error introduced by aggregating local-currency figures across countries without conversion is **currency mixing** — summing euros, pounds, zlotys, and dollars as if they were all the same unit. This produces a nonsensical total that is neither in USD nor in any meaningful composite unit. For example, €100 and $100 would both contribute "100" to the sum, but their USD equivalents differ. This would distort any revenue totals, averages, or trend comparisons built on top of such aggregates.

---

### Q2 – Inflated Customer Count

If the total unique customer count is ~15% higher than expected, the three most likely root causes and validation queries are:

**1. Duplicate `user_uuid` values created by identity stitching or a merge error**

A single real customer may have multiple UUIDs if records were migrated from different systems or if a JOIN in the pipeline generated fan-out.

```sql
-- Check for UUIDs sharing the same city/country and active on the same dates
SELECT user_uuid, customer_city, customer_country, COUNT(*) AS orders
FROM orders_merged
GROUP BY user_uuid, customer_city, customer_country
HAVING COUNT(*) >= 1
ORDER BY customer_city, customer_country;
-- Then compare UUID count against (city, country, first_order_date) clusters
```

**2. The new model counts order-level rows where the previous report counted distinct customers**

If the upstream table was an order-level event log and `COUNT(DISTINCT user_uuid)` was not applied, or if a `GROUP BY` was missing, row-level duplication would inflate the count.

```sql
-- Verify grain: should be 1 row per user_uuid
SELECT user_uuid, COUNT(*) AS row_count
FROM master_customer_table
GROUP BY user_uuid
HAVING COUNT(*) > 1;
-- Any result here means the table is not at customer grain
```

**3. The baseline report used a different customer definition (e.g., excluded refunded-only customers)**

The previous report may have filtered out customers whose only orders were refunded, expired, or unredeemed — effectively counting only "active" buyers.

```sql
-- Count customers who have at least one redeemed order vs. total
SELECT
  COUNT(DISTINCT user_uuid)                                                AS total_customers,
  COUNT(DISTINCT CASE WHEN last_status = 'redeemed' THEN user_uuid END)   AS customers_with_redemption,
  COUNT(DISTINCT CASE WHEN last_status = 'redeemed' THEN user_uuid END)
    / COUNT(DISTINCT user_uuid) * 100                                     AS redemption_pct
FROM orders_merged;
```

---

### Q3 – Making Your Model Trustworthy

Five concrete things to put in place for `master_customer_table`:

**1. Row-count and grain assertion on every run**
Every pipeline execution checks that the output has exactly one row per `user_uuid` and that the count is within ±5% of the previous run. A sudden 15% jump fails loudly before anyone queries stale data.

**2. Column-level data contracts (dbt tests or custom assertions)**
Define `not_null`, `unique`, and `accepted_values` tests on key columns:
- `user_uuid`: unique, not null
- `first_order_date` ≤ `last_order_date`
- `total_orders` ≥ 1
- `reactivation_count` ≤ `total_orders - 1`
These fail the build rather than silently producing wrong numbers downstream.

**3. Reconciliation check against the raw order table**
After every build, verify that the sum of `total_gross_bookings_usd` in the customer table equals the sum in `orders_merged` (excluding refunds if applicable). Any discrepancy signals a JOIN fan-out, missing rows, or double-counting introduced by the aggregation logic.

```sql
SELECT
  (SELECT SUM(gross_bookings_usd) FROM orders_merged)            AS raw_total,
  (SELECT SUM(total_gross_bookings_usd) FROM master_customer_table) AS model_total;
```

**4. Documented classification logic with an audit column**
Add an `order_type_sample` column (e.g., one example order UUID per type) or expose the `order_classified` CTE as its own table/view. When an analyst questions why a customer is classified as "reactivated", they can trace it back to the specific order and gap — rather than reverse-engineering the window function.

**5. A freshness SLA and lineage annotation**
Document in the table description: which source tables it reads from, what schedule it runs on, and what the maximum acceptable data lag is. If `orders_merged` stops updating, downstream consumers of `master_customer_table` should be alerted — not silently served stale data.

---

## AI Usage Log

| Assignment | How Claude was used | Approx. time |
|------------|---------------------|--------------|
| Setup | Generated project scaffold, CLAUDE.md, pyproject.toml with uv | 10 min |
| Assignment 1 | Designed cleaning pipeline; Claude wrote `cleaning.py` and `test_cleaning.py` after data inspection | 20 min |
| Assignment 2 | SQL logic designed collaboratively; Claude wrote BigQuery `.sql` files and DuckDB-adapted `analysis.py` | 30 min |
| Assignment 3 | Written answers drafted by candidate, reviewed and refined with Claude | 20 min |
| Streamlit app | Claude scaffolded the app; candidate reviewed charts and layout | — |

---

## Domain Rules Reference

| Rule | Definition |
|------|-----------|
| **Activation** | A customer's very first order ever |
| **Reactivation** | Any order where the gap since the previous order exceeds 365 days |
| **Retained regular** | All other returning orders |
| **USD conversion** | `value_usd = value_operational × fx_rate_loc_to_usd_fxn` |
| **SQL dialect** | BigQuery standard SQL |
