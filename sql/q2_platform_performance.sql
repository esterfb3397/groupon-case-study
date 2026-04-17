-- =============================================================================
-- Assignment 2 – Part B, Question 2: Platform Performance & Strategy
-- Target dialect: BigQuery standard SQL
--
-- Q: Compare mobile app vs. web customers across avg order value, purchase
--    frequency, and gross profit per customer. Has app share of gross bookings
--    grown or declined? Would you recommend prioritising app acquisition?
--
-- Assumption: platform='touch' is mobile web (browser on mobile device).
--   It is grouped with 'web' for the app-vs-web comparison since it represents
--   the same channel intent. This is flagged explicitly in platform_group.
-- =============================================================================

WITH order_classified AS (
  -- Re-use the same classification CTE from master_customer_table.sql
  SELECT
    user_uuid,
    order_uuid,
    operational_view_date,
    -- Normalise platform: touch → web (mobile browser = web channel)
    CASE WHEN platform = 'touch' THEN 'web' ELSE platform END   AS platform_group,
    gross_bookings_usd,
    gross_profit_usd,
    LAG(operational_view_date) OVER (
      PARTITION BY user_uuid ORDER BY operational_view_date, order_uuid
    ) AS prev_order_date,
    ROW_NUMBER() OVER (
      PARTITION BY user_uuid ORDER BY operational_view_date, order_uuid
    ) AS order_seq
  FROM `project.dataset.orders_merged`  -- replace with your BigQuery table reference, e.g. `my_project.my_dataset.orders_merged`
),

-- ── Part 1: Customer-level metrics per platform ───────────────────────────────
customer_platform AS (
  SELECT
    user_uuid,
    platform_group,
    COUNT(order_uuid)                  AS orders,
    ROUND(AVG(gross_bookings_usd), 2)  AS avg_order_value_usd,
    ROUND(SUM(gross_profit_usd), 2)    AS total_gross_profit_usd
  FROM order_classified
  GROUP BY user_uuid, platform_group
),

platform_summary AS (
  SELECT
    platform_group,
    COUNT(DISTINCT user_uuid)                   AS unique_customers,
    ROUND(AVG(orders), 2)                       AS avg_orders_per_customer,
    ROUND(AVG(avg_order_value_usd), 2)          AS avg_order_value_usd,
    ROUND(AVG(total_gross_profit_usd), 2)       AS avg_gross_profit_per_customer_usd,
    ROUND(SUM(orders * avg_order_value_usd), 2) AS total_gross_bookings_usd
  FROM customer_platform
  GROUP BY platform_group
),

-- ── Part 2: App share of gross bookings over time (yearly) ───────────────────
yearly_platform_mix AS (
  SELECT
    EXTRACT(YEAR FROM operational_view_date)                                      AS year,
    platform_group,
    ROUND(SUM(gross_bookings_usd), 2)                                             AS gross_bookings_usd,
    ROUND(100.0 * SUM(gross_bookings_usd) / SUM(SUM(gross_bookings_usd))
          OVER (PARTITION BY EXTRACT(YEAR FROM operational_view_date)), 1)        AS pct_share
  FROM order_classified
  GROUP BY year, platform_group
)

-- Platform comparison:
SELECT * FROM platform_summary ORDER BY platform_group;

-- Yearly app share trend (comment out above, uncomment below):
-- SELECT * FROM yearly_platform_mix ORDER BY year, platform_group;
