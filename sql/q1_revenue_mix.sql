-- =============================================================================
-- Assignment 2 – Part B, Question 1: Customer Revenue Mix & Retention
-- Target dialect: BigQuery standard SQL
--
-- Q: What share of gross bookings in the last 6 months came from newly activated
--    customers, reactivated customers, and retained regulars?
--    How has this mix shifted over the available history?
--
-- "Last 6 months" is defined relative to the dataset's maximum date (2025-02-27),
-- i.e. from 2024-08-27 onward. Using dataset max rather than CURRENT_DATE()
-- ensures reproducibility on static data.
-- =============================================================================

WITH order_with_gaps AS (
  SELECT
    user_uuid,
    order_uuid,
    operational_view_date,
    gross_bookings_usd,
    LAG(operational_view_date) OVER (
      PARTITION BY user_uuid
      ORDER BY operational_view_date, order_uuid
    ) AS prev_order_date,
    ROW_NUMBER() OVER (
      PARTITION BY user_uuid
      ORDER BY operational_view_date, order_uuid
    ) AS order_seq
  FROM `project.dataset.orders_merged`  -- replace with your BigQuery table reference, e.g. `my_project.my_dataset.orders_merged`
),

order_classified AS (
  SELECT
    *,
    CASE
      WHEN order_seq = 1                                                    THEN 'new'
      WHEN DATE_DIFF(operational_view_date, prev_order_date, DAY) > 365    THEN 'reactivated'
      ELSE                                                                       'retained'
    END AS order_type
  FROM order_with_gaps
),

dataset_bounds AS (
  SELECT
    MAX(operational_view_date)                                         AS max_date,
    DATE_SUB(MAX(operational_view_date), INTERVAL 6 MONTH)            AS cutoff_6m
  FROM `project.dataset.orders_merged`  -- replace with your BigQuery table reference, e.g. `my_project.my_dataset.orders_merged`
),

-- ── Part 1: Last-6-month revenue mix ─────────────────────────────────────────
last_6m_mix AS (
  SELECT
    oc.order_type,
    ROUND(SUM(oc.gross_bookings_usd), 2)                                           AS gross_bookings_usd,
    ROUND(100.0 * SUM(oc.gross_bookings_usd) / SUM(SUM(oc.gross_bookings_usd))
          OVER (), 1)                                                               AS pct_share
  FROM order_classified oc
  CROSS JOIN dataset_bounds db
  WHERE oc.operational_view_date > db.cutoff_6m
  GROUP BY oc.order_type
),

-- ── Part 2: Monthly mix over full history ────────────────────────────────────
monthly_mix AS (
  SELECT
    DATE_TRUNC(operational_view_date, MONTH)                           AS month,
    order_type,
    ROUND(SUM(gross_bookings_usd), 2)                                  AS gross_bookings_usd,
    ROUND(100.0 * SUM(gross_bookings_usd) / SUM(SUM(gross_bookings_usd))
          OVER (PARTITION BY DATE_TRUNC(operational_view_date, MONTH)), 1) AS pct_share
  FROM order_classified
  GROUP BY month, order_type
)

-- Switch between the two result sets by running one SELECT at a time:

-- Last-6-month mix:
SELECT * FROM last_6m_mix ORDER BY pct_share DESC;

-- Monthly historical mix (comment out the above and uncomment below):
-- SELECT * FROM monthly_mix ORDER BY month, order_type;
