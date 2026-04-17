-- =============================================================================
-- Assignment 2 – Part A: Master Customer Table
-- Target dialect: BigQuery standard SQL
--
-- Builds a customer-grain analytical table from the merged orders dataset.
-- Designed to support retention, cohort, and profitability analysis.
--
-- Order classification rules (from case study):
--   new         → customer's very first order ever
--   reactivated → order where gap since previous order > 365 days
--   retained    → all other returning orders
-- =============================================================================

WITH order_with_gaps AS (
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
    -- Previous order date per customer (chronological)
    LAG(operational_view_date) OVER (
      PARTITION BY user_uuid
      ORDER BY operational_view_date, order_uuid  -- order_uuid breaks ties deterministically
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

-- Identify each customer's most-used platform (for primary_platform field)
platform_ranked AS (
  SELECT
    user_uuid,
    platform,
    ROW_NUMBER() OVER (
      PARTITION BY user_uuid
      ORDER BY COUNT(*) DESC, platform  -- platform as tiebreaker for determinism
    ) AS rn
  FROM order_classified
  GROUP BY user_uuid, platform
)

SELECT
  oc.user_uuid,

  -- Lifecycle dates
  MIN(oc.operational_view_date)                                   AS first_order_date,
  MAX(oc.operational_view_date)                                   AS last_order_date,
  DATE_TRUNC(MIN(oc.operational_view_date), MONTH)               AS acquisition_cohort,

  -- Volume
  COUNT(oc.order_uuid)                                            AS total_orders,

  -- Revenue (USD)
  ROUND(SUM(oc.gross_bookings_usd), 2)                           AS total_gross_bookings_usd,
  ROUND(AVG(oc.gross_bookings_usd), 2)                           AS avg_order_value_usd,

  -- Profitability (USD)
  ROUND(SUM(oc.margin_1_usd), 2)                                 AS total_margin_1_usd,
  ROUND(SUM(oc.vfm_usd), 2)                                      AS total_vfm_usd,
  ROUND(SUM(oc.gross_profit_usd), 2)                             AS total_gross_profit_usd,
  ROUND(AVG(oc.gross_profit_usd), 2)                             AS avg_gross_profit_per_order_usd,

  -- Engagement
  DATE_DIFF(CURRENT_DATE(), MAX(oc.operational_view_date), DAY)  AS days_since_last_order,
  COUNTIF(oc.order_type = 'reactivated')                         AS reactivation_count,

  -- Platform
  pr.platform                                                     AS primary_platform

FROM order_classified oc
LEFT JOIN platform_ranked pr
  ON oc.user_uuid = pr.user_uuid AND pr.rn = 1
GROUP BY
  oc.user_uuid,
  pr.platform
