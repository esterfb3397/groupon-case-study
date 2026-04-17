# Groupon Analytical Engineer — Case Study Submission

**Role:** Analytical Engineer  
**Interactive app:** `uv run streamlit run app.py` → [http://localhost:8501](http://localhost:8501)  
**Repository:** [github.com/esterfb3397/groupon-case-study](https://github.com/esterfb3397/groupon-case-study)

---

## Approach

I treated this case study as I would a real onboarding task: understand the data before writing any query, make every assumption explicit, and build something another analyst could pick up and run without having to reverse-engineer the logic.

The solution is structured as a Python project managed with `uv`, using DuckDB as a local BigQuery-compatible SQL engine and Streamlit to present all findings interactively. The SQL files in `sql/` are the canonical BigQuery-dialect queries; `src/analysis.py` executes them locally via DuckDB with minor syntax adaptations documented inline.

---

## Assignment 1 — Data Cleaning & Preparation

### Merge

The two files share the same 15-column schema and have no overlapping `order_uuid` values, so a simple concatenation produces a clean merge. The combined dataset has **1,242 rows** covering January 2021 to February 2025.

### Issues found and fixed

**1. Null `customer_country` (8 rows)**

Eight rows had no country code. Rather than dropping them or flagging them as unknown, I filled them using a city-to-country lookup derived from the rest of the dataset. Each city appears unambiguously in one country elsewhere in the data:

| City | Inferred country |
|------|-----------------|
| Birmingham, Manchester | GB |
| Lyon | FR |
| Barcelona, Madrid | ES |
| Phoenix | US |
| Warsaw | PL |
| Munich | DE |

Assumption: city names in this dataset are not ambiguous (no two cities with the same name in different countries). This holds for all eight cases.

**2. Null `incentive_promo_code` (863 rows)**

Null here means no promo code was applied, not that the value is unknown. I standardised these to an empty string so that filtering (`WHERE incentive_promo_code != ''`) works consistently without needing `IS NOT NULL` checks.

**3. `operational_view_date` stored as a string**

Parsed to `datetime.date` with strict `%Y-%m-%d` format to enable date arithmetic in all downstream queries.

**4. No USD equivalents in the raw data**

The raw files only contain local-currency values. I derived four USD columns at cleaning time so that every downstream query can work in a single currency without repeating the conversion formula:

```
gross_bookings_usd  = gross_bookings_operational  × fx_rate_loc_to_usd_fxn
margin_1_usd        = margin_1_operational         × fx_rate_loc_to_usd_fxn
vfm_usd             = vfm_operational              × fx_rate_loc_to_usd_fxn
gross_profit_usd    = margin_1_usd + vfm_usd
```

### Items kept intentionally

**120 orders with `gross_bookings_operational ≤ 0`:** All 120 have `last_status = 'refunded'`. Non-positive booking values for refunds are correct business behaviour. Removing them would understate order volume and distort profitability metrics for refund analysis.

**Platform value `touch`:** Represents a mobile web browser session (as distinct from the native app). I kept it as a distinct value in the cleaned dataset and grouped it with `web` only for the platform-comparison analysis in Assignment 2, where the business question is app vs. browser intent.

### Cleaned dataset summary

| Metric | Value |
|--------|-------|
| Total rows | 1,242 |
| Unique orders | 1,242 (no duplicates) |
| Unique customers | 266 |
| Date range | 2021-01-01 to 2025-02-27 |
| Countries | DE, ES, FR, GB, IT, PL, US |
| Platforms | app, touch, web |

The cleaning pipeline is in `src/cleaning.py` and validated by 14 automated tests in `tests/test_cleaning.py`, all passing. The app includes a before/after table showing the exact 8 rows, their raw `NULL` value, and the inferred country code side by side. The cleaned dataset is available as a direct download from the app.

---

## Assignment 2 — SQL Analysis

All queries target **BigQuery standard SQL** and are in the `sql/` folder. They are executed locally via DuckDB; syntax adaptations are noted as comments in `src/analysis.py`. The full BigQuery SQL for each query is shown directly in the app — each sub-tab has a collapsible panel with the source code so the evaluator can read the query and the result side by side.

> The table references in the `.sql` files use `` `project.dataset.orders_merged` `` as a placeholder. This would be replaced with the actual BigQuery project and dataset path in a production environment.

### Part A — Master Customer Table

The table aggregates the order-level dataset to customer grain using window functions to classify every order as one of three types:

- **new:** the customer's very first order ever
- **reactivated:** any order where the gap since the previous order exceeds 365 days
- **retained:** all other returning orders

Tiebreaking for same-date orders uses `order_uuid` alphabetically to ensure deterministic window function results.

The output has one row per customer (266 rows) with the following columns:

| Column | Purpose |
|--------|---------|
| `first_order_date`, `last_order_date` | Lifecycle boundaries |
| `acquisition_cohort` | Month of first order, for cohort analysis |
| `total_orders` | Lifetime volume |
| `total_gross_bookings_usd` | Lifetime revenue |
| `avg_order_value_usd` | Average order size |
| `total_gross_profit_usd` | Lifetime profit (margin_1 + vfm) |
| `avg_gross_profit_per_order_usd` | Profit efficiency per order |
| `days_since_last_order` | Recency signal for churn analysis |
| `reactivation_count` | Number of lapse-and-return events |
| `primary_platform` | Platform with most orders (tiebroken alphabetically) |

**Portfolio totals:** $136,580 gross bookings and $73,536 gross profit across 266 customers.

The full query is in `sql/master_customer_table.sql`.

---

### Part B — Business Questions

#### Question 1 — Customer Revenue Mix & Retention

> What share of gross bookings in the last 6 months came from newly activated customers, reactivated customers, and retained regulars? How has this mix shifted over history?

"Last 6 months" is defined relative to the dataset's maximum date (2025-02-27), covering August 2024 to February 2025. I used dataset max rather than `CURRENT_DATE()` to keep results reproducible on static data.

**Last 6 months:**

| Segment | Gross bookings (USD) | Share |
|---------|----------------------|-------|
| Retained regulars | $13,803 | 83.3% |
| Reactivated | $2,754 | 16.6% |
| New activations | $16 | 0.1% |

**Interpretation**

The business is almost entirely dependent on its existing customer base. New customer acquisition is near zero in the most recent window — just 0.1% of bookings. This is a risk signal: if the retained base churns, there is no incoming cohort to replace it.

Reactivations at 17% are a healthy sign — lapsed customers are returning — but reactivation cannot substitute for sustained new acquisition. Looking at the full historical trend, the new and reactivated share has typically sat between 10% and 25% per month. The sharp drop in new activations visible in early 2025 warrants investigation: it could reflect seasonality, a reduction in acquisition spend, or simply that the most recent weeks of data are incomplete.

The SQL is in `sql/q1_revenue_mix.sql`.

---

#### Question 2 — Platform Performance & Strategy

> Compare mobile app customers vs. web customers across average order value, purchase frequency, and gross profit per customer. Has the app's share of gross bookings grown or declined? Based on the data, would you recommend prioritising app acquisition?

**Customer-level comparison** (`touch` grouped with `web`, as it represents browser intent):

| Metric | App | Web (incl. touch) |
|--------|-----|-------------------|
| Unique customers | 149 | 205 |
| Avg order value (USD) | $102.41 | $113.19 |
| Avg orders per customer | 3.38 | 3.60 |
| Avg gross profit per customer (USD) | $186.60 | $223.09 |
| Total gross bookings (USD) | $51,559 | $85,021 |

**Yearly app share of gross bookings:**

| Year | App | Web |
|------|-----|-----|
| 2021 | 38.8% | 61.2% |
| 2022 | 40.7% | 59.3% |
| 2023 | 33.3% | 66.7% |
| 2024 | 40.2% | 59.8% |
| 2025 | 23.9% | 76.1% |

**Interpretation**

Web customers consistently outperform app customers on every financial metric: 11% higher average order value ($113 vs $102), 6% more orders per customer, and 20% more gross profit per customer ($223 vs $187). The app's share of gross bookings has been volatile with no sustained upward trend, and 2025 shows a significant drop — though 2025 only covers two months, so caution is needed.

**Recommendation:** Based on the available data, I would not recommend prioritising app acquisition over web. The financial case does not support it. However, before drawing a final conclusion I would want to investigate three things:

1. Is the value gap driven by **product mix** (different deal categories purchased by channel)?
2. Is it driven by **geography** (higher-value markets skewing toward web)?
3. Does the gap persist when **controlling for customer tenure** (app customers may be newer on average)?

If the gap is explained by these factors rather than inherent channel quality, there may still be a case for app investment. The current aggregate data does not make it.

The SQL is in `sql/q2_platform_performance.sql`.

---

## Assignment 3 — Data Quality & Engineering Thinking

### Q1 — Financial column conventions

The `_operational` suffix indicates that financial values are in the **local transaction currency** — the currency the customer was actually charged in. This is standard practice in multi-currency e-commerce: storing values at their local source preserves the original transaction truth before any FX conversion introduces rounding or rate variability.

To convert to USD for cross-country comparison:
```
value_usd = value_operational × fx_rate_loc_to_usd_fxn
```

The analytical error introduced by aggregating local-currency figures without conversion is **currency mixing** — summing euros, pounds, zlotys, and dollars as if they were the same unit. The result is a number that is neither in USD nor in any meaningful composite currency. For example, €100 and $100 would both contribute "100" to the sum, but their USD equivalents differ. Revenue totals, cohort averages, and trend lines built on such aggregates would all be wrong, and the error would scale with the proportion of non-USD transactions — hard to detect in markets with low FX exposure but catastrophic in datasets with heavy EUR or GBP volume.

---

### Q2 — Inflated customer count

After the master customer table goes live, a stakeholder reports that the total unique customer count is about 15% higher than expected. My investigation would check these three root causes first:

**Root cause 1 — Duplicate `user_uuid` values from identity stitching or migration fan-out**

A single real customer may have multiple UUIDs if records were migrated from different systems, or if a pipeline JOIN produced fan-out.

```sql
SELECT
  customer_city,
  customer_country,
  COUNT(DISTINCT user_uuid)   AS uuid_count,
  MIN(operational_view_date)  AS first_seen,
  MAX(operational_view_date)  AS last_seen
FROM orders_merged
GROUP BY customer_city, customer_country
HAVING COUNT(DISTINCT user_uuid) > 1
ORDER BY uuid_count DESC;
```

If the 15% excess maps to a specific country or cohort, that points to a migration event.

**Root cause 2 — Model grain is not one row per customer**

If a `GROUP BY` was dropped or a subquery returned multiple rows per user, `COUNT(*)` counts rows instead of customers.

```sql
SELECT user_uuid, COUNT(*) AS row_count
FROM master_customer_table
GROUP BY user_uuid
HAVING COUNT(*) > 1;
```

Any result here means the table is not at customer grain and the aggregation logic is broken.

**Root cause 3 — Different customer definition in the baseline report**

The previous report may have counted only customers with at least one redeemed order, while the new model counts all `user_uuid` values regardless of status.

```sql
SELECT
  COUNT(DISTINCT user_uuid)                                               AS all_customers,
  COUNT(DISTINCT CASE WHEN last_status = 'redeemed' THEN user_uuid END)  AS customers_with_redemption,
  ROUND(
    100.0 * COUNT(DISTINCT CASE WHEN last_status = 'redeemed' THEN user_uuid END)
    / COUNT(DISTINCT user_uuid), 1
  )                                                                       AS redemption_pct
FROM orders_merged;
```

If `redemption_pct` is around 87%, removing non-redeemers would shrink the count by roughly 13%, which is close to the reported 15% gap.

---

### Q3 — Making the model trustworthy

Five concrete things I would put in place for `master_customer_table`:

**1. Grain assertion on every run**

Every build checks that the output has exactly one row per `user_uuid` and that the total row count is within ±5% of the previous run. A 15% jump fails loudly before any downstream consumer queries stale data.

**2. Column-level data contracts**

Define `not_null`, `unique`, and logical-range tests on key columns:
- `user_uuid`: unique and not null
- `first_order_date` is on or before `last_order_date`
- `total_orders` is at least 1
- `reactivation_count` is at most `total_orders - 1`

These fail the build rather than silently producing wrong numbers downstream.

**3. Financial reconciliation against the raw table**

After every build, verify that the sum of `total_gross_bookings_usd` in the customer table equals the sum in `orders_merged`:

```sql
SELECT
  (SELECT SUM(gross_bookings_usd) FROM orders_merged)                AS raw_total,
  (SELECT SUM(total_gross_bookings_usd) FROM master_customer_table)  AS model_total;
```

Any discrepancy signals a JOIN fan-out, missing rows, or double-counting introduced by the aggregation logic.

**4. Traceable classification logic**

Expose the `order_classified` CTE as its own view or intermediate table. When an analyst questions why a customer is "reactivated", they can trace it back to the specific order and the gap in days — rather than reverse-engineering a window function. I would also add a `classification_rule_version` column so that if the definition changes (for example from 365 to 180 days), historical classifications can be recomputed and compared without ambiguity.

**5. Freshness SLA and lineage in the table description**

Document directly on the table: which source tables it reads from, the refresh schedule, the maximum acceptable data lag, and the team or person responsible. If `orders_merged` stops updating, consumers of `master_customer_table` should be alerted via a freshness check rather than silently served stale data.

---

## AI Usage Log

As requested, here is how I used AI at each stage and the approximate time each assignment took.

| Assignment | How Claude Code was used | Time |
|------------|--------------------------|------|
| Setup | Generated project scaffold, `CLAUDE.md`, `pyproject.toml` with `uv`, folder structure | 10 min |
| Assignment 1 | Designed the cleaning strategy after manual data inspection; Claude wrote `src/cleaning.py` and `tests/test_cleaning.py` based on the issues I identified | 20 min |
| Assignment 2 | SQL logic designed collaboratively; I defined the classification rules and business questions, Claude wrote the BigQuery `.sql` files and the DuckDB-adapted `src/analysis.py` | 30 min |
| Assignment 3 | Written answers drafted by me, reviewed and sharpened with Claude | 20 min |
| Streamlit app | Claude scaffolded the layout and chart code; I reviewed the output and directed adjustments to the visualisations, written interpretations, before/after data views, SQL expanders, and download button | 35 min |

Claude was used as a coding assistant throughout, not as a decision-maker. Every business interpretation, assumption, and recommendation in this submission is my own.

---

## Technical Setup

```bash
git clone https://github.com/esterfb3397/groupon-case-study
cd groupon-case-study
uv sync
uv run streamlit run app.py   # opens at http://localhost:8501
uv run pytest tests/ -v       # runs 14 data quality tests
```

Python 3.12 · DuckDB 1.5 · Streamlit 1.56 · pandas 3.0 · Plotly 6.7
