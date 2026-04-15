"""
Assignment 2: SQL Analysis
Executes the BigQuery-dialect queries against the cleaned dataset using DuckDB.

DuckDB is used as a local BigQuery-compatible engine. Syntax adaptations vs BigQuery:
  - DATE_DIFF(end, start, DAY)    → DATEDIFF('day', start, end)
  - DATE_TRUNC(date, MONTH)       → DATE_TRUNC('month', date)
  - DATE_SUB(date, INTERVAL n M)  → date - INTERVAL '6 months'
  - COUNTIF(cond)                 → COUNT(*) FILTER (WHERE cond)
  - CURRENT_DATE()                → CURRENT_DATE
  - Table ref `project.dataset.t` → just `orders` (registered DuckDB relation)
"""

import duckdb
import pandas as pd
from pathlib import Path

CLEANED_PATH = Path("data/cleaned/orders_merged.csv")


def get_connection() -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB connection with the cleaned dataset registered."""
    con = duckdb.connect()
    orders = pd.read_csv(CLEANED_PATH, parse_dates=["operational_view_date"])
    con.register("orders", orders)
    return con


# ── Shared classification CTE (DuckDB-adapted) ───────────────────────────────

_CLASSIFICATION_CTE = """
order_with_gaps AS (
  SELECT
    user_uuid,
    order_uuid,
    operational_view_date,
    platform,
    gross_bookings_usd,
    margin_1_usd,
    vfm_usd,
    gross_profit_usd,
    last_status,
    LAG(operational_view_date) OVER (
      PARTITION BY user_uuid
      ORDER BY operational_view_date, order_uuid
    ) AS prev_order_date,
    ROW_NUMBER() OVER (
      PARTITION BY user_uuid
      ORDER BY operational_view_date, order_uuid
    ) AS order_seq
  FROM orders
),

order_classified AS (
  SELECT
    *,
    CASE
      WHEN order_seq = 1
        THEN 'new'
      WHEN DATEDIFF('day', prev_order_date, operational_view_date) > 365
        THEN 'reactivated'
      ELSE 'retained'
    END AS order_type
  FROM order_with_gaps
)
"""

# ── Part A: Master Customer Table ─────────────────────────────────────────────

MASTER_CUSTOMER_SQL = f"""
WITH
{_CLASSIFICATION_CTE},

platform_ranked AS (
  SELECT
    user_uuid,
    platform,
    ROW_NUMBER() OVER (
      PARTITION BY user_uuid
      ORDER BY COUNT(*) DESC, platform
    ) AS rn
  FROM order_classified
  GROUP BY user_uuid, platform
)

SELECT
  oc.user_uuid,
  MIN(oc.operational_view_date)                                           AS first_order_date,
  MAX(oc.operational_view_date)                                           AS last_order_date,
  DATE_TRUNC('month', MIN(oc.operational_view_date))                     AS acquisition_cohort,
  COUNT(oc.order_uuid)                                                    AS total_orders,
  ROUND(SUM(oc.gross_bookings_usd), 2)                                   AS total_gross_bookings_usd,
  ROUND(AVG(oc.gross_bookings_usd), 2)                                   AS avg_order_value_usd,
  ROUND(SUM(oc.margin_1_usd), 2)                                         AS total_margin_1_usd,
  ROUND(SUM(oc.vfm_usd), 2)                                              AS total_vfm_usd,
  ROUND(SUM(oc.gross_profit_usd), 2)                                     AS total_gross_profit_usd,
  ROUND(AVG(oc.gross_profit_usd), 2)                                     AS avg_gross_profit_per_order_usd,
  DATEDIFF('day', MAX(oc.operational_view_date), CURRENT_DATE)           AS days_since_last_order,
  COUNT(*) FILTER (WHERE oc.order_type = 'reactivated')                  AS reactivation_count,
  pr.platform                                                             AS primary_platform
FROM order_classified oc
LEFT JOIN platform_ranked pr
  ON oc.user_uuid = pr.user_uuid AND pr.rn = 1
GROUP BY oc.user_uuid, pr.platform
ORDER BY total_gross_bookings_usd DESC
"""

# ── Q1a: Last-6-month revenue mix ────────────────────────────────────────────

Q1_LAST_6M_SQL = f"""
WITH
{_CLASSIFICATION_CTE},

dataset_bounds AS (
  SELECT
    MAX(operational_view_date)                                AS max_date,
    MAX(operational_view_date) - INTERVAL '6 months'         AS cutoff_6m
  FROM orders
),

last_6m_mix AS (
  SELECT
    oc.order_type,
    ROUND(SUM(oc.gross_bookings_usd), 2)                     AS gross_bookings_usd
  FROM order_classified oc
  CROSS JOIN dataset_bounds db
  WHERE oc.operational_view_date > db.cutoff_6m
  GROUP BY oc.order_type
)

SELECT
  order_type,
  gross_bookings_usd,
  ROUND(100.0 * gross_bookings_usd / SUM(gross_bookings_usd) OVER (), 1) AS pct_share
FROM last_6m_mix
ORDER BY pct_share DESC
"""

# ── Q1b: Monthly revenue mix over full history ────────────────────────────────

Q1_MONTHLY_SQL = f"""
WITH
{_CLASSIFICATION_CTE}

SELECT
  DATE_TRUNC('month', operational_view_date)                AS month,
  order_type,
  ROUND(SUM(gross_bookings_usd), 2)                         AS gross_bookings_usd,
  ROUND(
    100.0 * SUM(gross_bookings_usd)
    / SUM(SUM(gross_bookings_usd)) OVER (
        PARTITION BY DATE_TRUNC('month', operational_view_date)
    ), 1
  )                                                         AS pct_share
FROM order_classified
GROUP BY month, order_type
ORDER BY month, order_type
"""

# ── Q2a: Platform comparison (customer-level metrics) ─────────────────────────

Q2_PLATFORM_SUMMARY_SQL = f"""
WITH
{_CLASSIFICATION_CTE},

order_classified_platform AS (
  SELECT
    user_uuid,
    order_uuid,
    gross_bookings_usd,
    gross_profit_usd,
    CASE WHEN platform = 'touch' THEN 'web' ELSE platform END AS platform_group
  FROM order_classified
),

customer_platform AS (
  SELECT
    user_uuid,
    platform_group,
    COUNT(order_uuid)                  AS orders,
    AVG(gross_bookings_usd)            AS avg_order_value_usd,
    SUM(gross_profit_usd)              AS total_gross_profit_usd
  FROM order_classified_platform
  GROUP BY user_uuid, platform_group
)

SELECT
  platform_group,
  COUNT(DISTINCT user_uuid)                            AS unique_customers,
  ROUND(AVG(orders), 2)                               AS avg_orders_per_customer,
  ROUND(AVG(avg_order_value_usd), 2)                  AS avg_order_value_usd,
  ROUND(AVG(total_gross_profit_usd), 2)               AS avg_gross_profit_per_customer_usd,
  ROUND(SUM(orders * avg_order_value_usd), 2)         AS total_gross_bookings_usd
FROM customer_platform
GROUP BY platform_group
ORDER BY platform_group
"""

# ── Q2b: Yearly app share trend ───────────────────────────────────────────────

Q2_YEARLY_SHARE_SQL = f"""
WITH
{_CLASSIFICATION_CTE},

yearly AS (
  SELECT
    EXTRACT(YEAR FROM operational_view_date)                              AS year,
    CASE WHEN platform = 'touch' THEN 'web' ELSE platform END            AS platform_group,
    SUM(gross_bookings_usd)                                               AS gross_bookings_usd
  FROM order_classified
  GROUP BY year, platform_group
)

SELECT
  year,
  platform_group,
  ROUND(gross_bookings_usd, 2)                                           AS gross_bookings_usd,
  ROUND(
    100.0 * gross_bookings_usd / SUM(gross_bookings_usd) OVER (PARTITION BY year),
    1
  )                                                                      AS pct_share
FROM yearly
ORDER BY year, platform_group
"""


# ── Public API ────────────────────────────────────────────────────────────────

def run_query(sql: str) -> pd.DataFrame:
    con = get_connection()
    return con.execute(sql).df()


def master_customer_table() -> pd.DataFrame:
    return run_query(MASTER_CUSTOMER_SQL)


def q1_last_6m_mix() -> pd.DataFrame:
    return run_query(Q1_LAST_6M_SQL)


def q1_monthly_mix() -> pd.DataFrame:
    return run_query(Q1_MONTHLY_SQL)


def q2_platform_summary() -> pd.DataFrame:
    return run_query(Q2_PLATFORM_SUMMARY_SQL)


def q2_yearly_share() -> pd.DataFrame:
    return run_query(Q2_YEARLY_SHARE_SQL)


if __name__ == "__main__":
    print("=== Master Customer Table (top 5) ===")
    print(master_customer_table().head())

    print("\n=== Q1 – Last 6-month revenue mix ===")
    print(q1_last_6m_mix())

    print("\n=== Q1 – Monthly mix (last 6 rows) ===")
    print(q1_monthly_mix().tail(6))

    print("\n=== Q2 – Platform summary ===")
    print(q2_platform_summary())

    print("\n=== Q2 – Yearly app share ===")
    print(q2_yearly_share())
