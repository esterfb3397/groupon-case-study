"""
Groupon Analytical Engineer – Case Study
Interactive Streamlit presentation of all three assignments.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import duckdb
from pathlib import Path

from src.cleaning import clean, CITY_COUNTRY_MAP
from src.analysis import (
    master_customer_table,
    q1_last_6m_mix,
    q1_monthly_mix,
    q2_platform_summary,
    q2_yearly_share,
)

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Groupon Case Study",
    page_icon="🟢",
    layout="wide",
)

# ── Load data (cached) ────────────────────────────────────────────────────────

@st.cache_data
def load_all():
    df, report = clean(save=False)
    mct = master_customer_table()
    last6m = q1_last_6m_mix()
    monthly = q1_monthly_mix()
    platform = q2_platform_summary()
    yearly = q2_yearly_share()
    return df, report, mct, last6m, monthly, platform, yearly


@st.cache_data
def load_raw():
    hist = pd.read_csv("data/raw/orders_historical.csv")
    recent = pd.read_csv("data/raw/orders_2024_2025.csv")
    return hist, recent


def run_sql(query: str, df_cleaned: pd.DataFrame) -> pd.DataFrame:
    """Execute a SQL query against the cleaned dataset using DuckDB."""
    con = duckdb.connect()
    con.register("orders", df_cleaned)
    return con.execute(query).df()


@st.cache_data
def before_after_country():
    """Return the 8 rows that had null customer_country, showing raw vs cleaned values."""
    raw = pd.concat([
        pd.read_csv("data/raw/orders_historical.csv"),
        pd.read_csv("data/raw/orders_2024_2025.csv"),
    ], ignore_index=True)
    mask = raw["customer_country"].isnull()
    result = raw[mask][["order_uuid", "customer_city", "customer_country"]].copy()
    result["customer_country"] = result["customer_country"].fillna("NULL")
    result["after"] = result["customer_city"].map(CITY_COUNTRY_MAP)
    result.columns = ["order_uuid", "customer_city", "before (raw)", "after (fixed)"]
    return result


def read_sql_file(name: str) -> str:
    return (Path("sql") / name).read_text()


df, report, mct, last6m, monthly, platform_df, yearly_df = load_all()
hist_raw, recent_raw = load_raw()

# ── Header ────────────────────────────────────────────────────────────────────

st.title("Groupon Analytical Engineer – Case Study")
st.caption("Candidate solution · Dataset: Jan 2021 – Feb 2025 · SQL dialect: BigQuery standard SQL")

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Assignment 1 · Data Cleaning",
    "Assignment 2 · SQL Analysis",
    "Assignment 3 · Engineering Thinking",
    "Raw Data",
    "SQL Playground",
])


# ═══════════════════════════════════════════════════════════════════════════════
# ASSIGNMENT 1
# ═══════════════════════════════════════════════════════════════════════════════

with tab1:
    st.header("Data Cleaning & Preparation")
    st.markdown(
        "Merged `orders_historical.csv` (778 rows) and `orders_2024_2025.csv` (464 rows) "
        "into a single analysis-ready dataset. Identified and resolved all data quality issues "
        "before any analysis began."
    )

    # ── KPI row ───────────────────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total rows", f"{report['total_rows']:,}")
    c2.metric("Unique orders", f"{report['unique_orders']:,}")
    c3.metric("Unique customers", f"{report['unique_customers']:,}")
    c4.metric("Date range", f"{report['date_range'][0][:7]} → {report['date_range'][1][:7]}")
    c5.metric("Countries", len(report["countries"]))

    st.divider()

    # ── Issues found & fixed ──────────────────────────────────────────────────
    st.subheader("Issues Found & Fixed")

    issues = pd.DataFrame([
        {
            "Issue": "Null `customer_country`",
            "Count": 8,
            "Fix": "Filled via deterministic city → country lookup (Birmingham→GB, Lyon→FR, etc.)",
            "Type": "Fixed",
        },
        {
            "Issue": "Null `incentive_promo_code`",
            "Count": 863,
            "Fix": "Standardised to empty string `\"\"` null means no promo applied",
            "Type": "Fixed",
        },
        {
            "Issue": "`operational_view_date` stored as string",
            "Count": 1242,
            "Fix": "Parsed to `datetime.date` with strict `%Y-%m-%d` format",
            "Type": "Fixed",
        },
        {
            "Issue": "No USD equivalents in raw data",
            "Count": 0,
            "Fix": "Derived `gross_bookings_usd`, `margin_1_usd`, `vfm_usd`, `gross_profit_usd`",
            "Type": "Enhanced",
        },
        {
            "Issue": "`gross_bookings ≤ 0` (120 rows)",
            "Count": 120,
            "Fix": "All are `last_status = 'refunded'` valid business data, kept with documentation",
            "Type": "Kept (expected)",
        },
        {
            "Issue": "Platform value `touch`",
            "Count": 152,
            "Fix": "Mobile web browser kept as-is; grouped with `web` only in platform analysis",
            "Type": "Kept (expected)",
        },
    ])

    colors = {"Fixed": "🟢", "Enhanced": "🔵", "Kept (expected)": "🟡"}
    issues[""] = issues["Type"].map(colors)
    st.dataframe(
        issues[["", "Issue", "Count", "Fix"]],
        use_container_width=True,
        hide_index=True,
    )

    # ── Status & platform distribution ───────────────────────────────────────
    st.divider()
    st.subheader("Cleaned Dataset Overview")

    col_l, col_r = st.columns(2)

    with col_l:
        status_data = pd.DataFrame(
            report["last_status_counts"].items(), columns=["Status", "Orders"]
        ).sort_values("Orders", ascending=False)
        fig_status = px.bar(
            status_data, x="Status", y="Orders",
            color="Status",
            color_discrete_sequence=px.colors.qualitative.Set2,
            title="Orders by Last Status",
        )
        fig_status.update_layout(showlegend=False)
        st.plotly_chart(fig_status, use_container_width=True)

    with col_r:
        platform_raw = df["platform"].value_counts().reset_index()
        platform_raw.columns = ["Platform", "Orders"]
        fig_plat = px.pie(
            platform_raw, names="Platform", values="Orders",
            color_discrete_sequence=px.colors.qualitative.Set2,
            title="Orders by Platform",
        )
        st.plotly_chart(fig_plat, use_container_width=True)

    # ── Orders over time ──────────────────────────────────────────────────────
    df["month"] = pd.to_datetime(df["operational_view_date"]).dt.to_period("M").dt.to_timestamp()
    monthly_orders = df.groupby("month").size().reset_index(name="orders")
    fig_time = px.line(
        monthly_orders, x="month", y="orders",
        title="Monthly Order Volume (merged dataset)",
        labels={"month": "", "orders": "Orders"},
    )
    fig_time.add_vline(
        x=pd.Timestamp("2023-07-01").timestamp() * 1000,
        line_dash="dash", line_color="grey",
        annotation_text="File boundary (Jul 2023)",
        annotation_position="top left",
    )
    st.plotly_chart(fig_time, use_container_width=True)

    # ── Before / after: the 8 fixed country rows ─────────────────────────────
    st.divider()
    st.subheader("Before / After: country_null fix")
    st.markdown(
        "The 8 rows where `customer_country` was null, showing the raw value vs. "
        "the value inferred from the city lookup."
    )
    st.dataframe(before_after_country(), hide_index=True, use_container_width=True)

    # ── Cleaned data preview + download ──────────────────────────────────────
    st.divider()
    st.subheader("Cleaned Dataset")
    col_prev, col_dl = st.columns([4, 1])
    col_prev.caption(f"{len(df):,} rows · {len(df.columns)} columns · ready for analysis")
    col_dl.download_button(
        label="Download CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name="orders_merged.csv",
        mime="text/csv",
        use_container_width=True,
    )
    st.dataframe(df, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# ASSIGNMENT 2
# ═══════════════════════════════════════════════════════════════════════════════

with tab2:
    st.header("SQL Analysis")
    st.markdown(
        "All queries written in **BigQuery standard SQL** (see `sql/`). "
        "Executed locally via **DuckDB**. "
        "Order classification: *new* = first order ever · *reactivated* = gap > 365 days · *retained* = all other returning orders."
    )

    sub_a, sub_q1, sub_q2 = st.tabs([
        "Part A · Master Customer Table",
        "Part B · Q1 Revenue Mix",
        "Part B · Q2 Platform Strategy",
    ])

    # ── Part A ────────────────────────────────────────────────────────────────
    with sub_a:
        st.subheader("Master Customer Table")
        st.markdown(
            "Customer-grain analytical table built with window functions. "
            "Designed as a reusable foundation for retention, cohort, and profitability analysis."
        )

        # KPIs
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Customers", f"{len(mct):,}")
        c2.metric("Total Gross Bookings (USD)", f"${mct['total_gross_bookings_usd'].sum():,.0f}")
        c3.metric("Total Gross Profit (USD)", f"${mct['total_gross_profit_usd'].sum():,.0f}")
        c4.metric("Avg Orders / Customer", f"{mct['total_orders'].mean():.1f}")

        st.divider()

        col_l, col_r = st.columns(2)

        with col_l:
            # Cohort acquisition heatmap
            mct["cohort_year"] = pd.to_datetime(mct["acquisition_cohort"]).dt.year
            cohort_counts = mct.groupby(["cohort_year", "primary_platform"]).size().reset_index(name="customers")
            fig_cohort = px.bar(
                cohort_counts, x="cohort_year", y="customers",
                color="primary_platform",
                barmode="stack",
                title="Customer Acquisitions by Year & Primary Platform",
                labels={"cohort_year": "Acquisition Year", "customers": "Customers", "primary_platform": "Platform"},
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            st.plotly_chart(fig_cohort, use_container_width=True)

        with col_r:
            # Orders distribution
            fig_orders = px.histogram(
                mct, x="total_orders", nbins=12,
                title="Distribution of Total Orders per Customer",
                labels={"total_orders": "Total Orders", "count": "Customers"},
                color_discrete_sequence=["#2ecc71"],
            )
            st.plotly_chart(fig_orders, use_container_width=True)

        # Profitability scatter
        fig_scatter = px.scatter(
            mct,
            x="total_gross_bookings_usd",
            y="total_gross_profit_usd",
            size="total_orders",
            color="primary_platform",
            hover_data=["user_uuid", "total_orders", "reactivation_count"],
            title="Customer Profitability: Gross Bookings vs Gross Profit (bubble = order count)",
            labels={
                "total_gross_bookings_usd": "Total Gross Bookings (USD)",
                "total_gross_profit_usd": "Total Gross Profit (USD)",
                "primary_platform": "Platform",
            },
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

        st.subheader("Full Table")
        st.dataframe(
            mct.sort_values("total_gross_bookings_usd", ascending=False),
            use_container_width=True,
        )

        st.divider()
        with st.expander("BigQuery SQL master_customer_table.sql"):
            st.code(read_sql_file("master_customer_table.sql"), language="sql")

    # ── Q1 ────────────────────────────────────────────────────────────────────
    with sub_q1:
        st.subheader("Q1 – Customer Revenue Mix & Retention")
        st.markdown(
            "_What share of gross bookings in the last 6 months came from newly activated customers, "
            "reactivated customers, and retained regulars? How has this mix shifted over history?_"
        )
        st.caption("'Last 6 months' = Aug 2024 – Feb 2025 (relative to dataset max date 2025-02-27)")

        col_l, col_r = st.columns([1, 2])

        with col_l:
            st.markdown("**Last 6 months – Revenue Mix**")
            COLOR_MAP = {"new": "#2ecc71", "reactivated": "#f39c12", "retained": "#3498db"}
            fig_pie = px.pie(
                last6m,
                names="order_type", values="gross_bookings_usd",
                color="order_type",
                color_discrete_map=COLOR_MAP,
                hole=0.4,
            )
            fig_pie.update_traces(textinfo="percent+label")
            st.plotly_chart(fig_pie, use_container_width=True)

            st.dataframe(
                last6m.rename(columns={
                    "order_type": "Segment",
                    "gross_bookings_usd": "Gross Bookings (USD)",
                    "pct_share": "Share (%)",
                }),
                hide_index=True,
                use_container_width=True,
            )

        with col_r:
            st.markdown("**Historical Monthly Mix (stacked %)**")
            monthly["month"] = pd.to_datetime(monthly["month"])
            fig_area = px.area(
                monthly,
                x="month", y="pct_share",
                color="order_type",
                color_discrete_map=COLOR_MAP,
                labels={"month": "", "pct_share": "Share (%)", "order_type": "Segment"},
                groupnorm="",
            )
            fig_area.update_layout(yaxis_ticksuffix="%", legend_title="Segment")
            st.plotly_chart(fig_area, use_container_width=True)

        st.divider()
        st.subheader("Interpretation")
        st.markdown("""
**Last 6 months:** The business is overwhelmingly driven by existing customers **83.3% of gross bookings
came from retained regulars**, 16.6% from reactivated customers, and just **0.1% from new activations**.

**What this means for the business:**
- The acquisition pipeline is essentially empty in the most recent window. If retained regulars churn,
  there is no incoming cohort to replace them.
- Reactivations at 17% are a positive signal lapsed customers are returning but this cannot
  substitute for sustained new customer growth.
- The mix has been structurally similar across history (~10–25% new/reactivated), but 2025 shows
  a stark drop in new activations that warrants investigation: is it a seasonal effect, a reduction
  in acquisition spend, or a data completeness issue for the most recent months?
- **Recommendation:** Treat the near-zero new activation rate as a red flag. Monitor cohort sizes
  monthly and set an activation-share floor (e.g. 10%) as an early warning metric.
        """)

        with st.expander("BigQuery SQL q1_revenue_mix.sql"):
            st.code(read_sql_file("q1_revenue_mix.sql"), language="sql")

    # ── Q2 ────────────────────────────────────────────────────────────────────
    with sub_q2:
        st.subheader("Q2 – Platform Performance & Strategy")
        st.markdown(
            "_Compare mobile app vs. web customers across AOV, purchase frequency, and gross profit. "
            "Has app share grown or declined? Should we prioritise app acquisition?_"
        )
        st.caption("Assumption: `platform = 'touch'` (mobile browser) is grouped with `web`.")

        # Platform KPI cards
        app = platform_df[platform_df["platform_group"] == "app"].iloc[0]
        web = platform_df[platform_df["platform_group"] == "web"].iloc[0]

        st.markdown("**Platform Comparison**")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("App – Unique Customers", int(app["unique_customers"]))
        c2.metric("Web – Unique Customers", int(web["unique_customers"]))
        c1.metric("App – Avg Order Value", f"${app['avg_order_value_usd']:.2f}",
                  delta=f"{app['avg_order_value_usd'] - web['avg_order_value_usd']:.2f} vs web")
        c2.metric("Web – Avg Order Value", f"${web['avg_order_value_usd']:.2f}")
        c3.metric("App – Avg Orders/Customer", f"{app['avg_orders_per_customer']:.2f}",
                  delta=f"{app['avg_orders_per_customer'] - web['avg_orders_per_customer']:.2f} vs web")
        c4.metric("Web – Avg Orders/Customer", f"{web['avg_orders_per_customer']:.2f}")
        c3.metric("App – GP/Customer (USD)", f"${app['avg_gross_profit_per_customer_usd']:.2f}",
                  delta=f"{app['avg_gross_profit_per_customer_usd'] - web['avg_gross_profit_per_customer_usd']:.2f} vs web")
        c4.metric("Web – GP/Customer (USD)", f"${web['avg_gross_profit_per_customer_usd']:.2f}")

        st.divider()

        col_l, col_r = st.columns(2)

        with col_l:
            # Grouped bar: key metrics side by side
            metrics = ["avg_order_value_usd", "avg_gross_profit_per_customer_usd", "avg_orders_per_customer"]
            labels = ["Avg Order Value (USD)", "Avg Gross Profit / Customer (USD)", "Avg Orders / Customer"]
            fig_bar = go.Figure()
            for plat, color in [("app", "#2ecc71"), ("web", "#3498db")]:
                row = platform_df[platform_df["platform_group"] == plat].iloc[0]
                fig_bar.add_trace(go.Bar(
                    name=plat.capitalize(),
                    x=labels,
                    y=[row[m] for m in metrics],
                    marker_color=color,
                ))
            fig_bar.update_layout(
                barmode="group",
                title="App vs Web – Key Metrics",
                yaxis_title="Value",
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        with col_r:
            # Yearly share trend
            yearly_df["year"] = yearly_df["year"].astype(int)
            fig_trend = px.line(
                yearly_df, x="year", y="pct_share",
                color="platform_group",
                markers=True,
                title="App vs Web – Share of Gross Bookings by Year (%)",
                labels={"year": "Year", "pct_share": "Share (%)", "platform_group": "Platform"},
                color_discrete_map={"app": "#2ecc71", "web": "#3498db"},
            )
            fig_trend.update_layout(yaxis_ticksuffix="%")
            st.plotly_chart(fig_trend, use_container_width=True)

        st.dataframe(
            platform_df.rename(columns={
                "platform_group": "Platform",
                "unique_customers": "Customers",
                "avg_orders_per_customer": "Avg Orders",
                "avg_order_value_usd": "Avg Order Value (USD)",
                "avg_gross_profit_per_customer_usd": "Avg GP / Customer (USD)",
                "total_gross_bookings_usd": "Total Gross Bookings (USD)",
            }),
            hide_index=True,
            use_container_width=True,
        )

        st.divider()
        st.subheader("Interpretation")
        st.markdown("""
**Performance gap:** Web customers consistently outperform app customers on every financial metric:
11% higher average order value ($113 vs $102), 6% more orders per customer (3.60 vs 3.38),
and **20% more gross profit per customer** ($223 vs $187).

**Share trend:** The app's share of gross bookings has been **volatile and overall declining** —
from 39% in 2021 to 24% in 2025 (though 2025 only covers 2 months and may not be representative).
There was no sustained upward trend at any point in the dataset.

**Recommendation do not blindly prioritise app acquisition.**
The data does not support reallocating acquisition spend toward the app channel:
web customers generate more revenue and profit per head, and the app's bookings share is declining.
Before any channel strategy shift, the right questions to investigate are:
- Is the value gap driven by **product mix** (different deal categories purchased on each channel)?
- Is it driven by **geography** (high-value markets predominantly using web)?
- Does the gap persist when controlling for **customer tenure** (newer vs. older customers)?
If the gap is explained by these factors rather than inherent channel quality, there may still be
a case for app investment but the current aggregate data does not make it.
        """)

        with st.expander("BigQuery SQL q2_platform_performance.sql"):
            st.code(read_sql_file("q2_platform_performance.sql"), language="sql")


# ═══════════════════════════════════════════════════════════════════════════════
# ASSIGNMENT 3
# ═══════════════════════════════════════════════════════════════════════════════

with tab3:
    st.header("Data Quality & Engineering Thinking")
    st.markdown("Written answers no SQL required. Max half a page per question.")

    st.divider()

    # ── Q1 ────────────────────────────────────────────────────────────────────
    st.subheader("Q1 – Financial Column Conventions")
    st.markdown("""
The `_operational` suffix indicates that financial values are expressed in the **local transaction
currency** the currency in which the customer was charged. This is standard practice in
multi-currency e-commerce: storing values at their local source preserves the original
transaction truth before any FX conversion introduces rounding or rate variability.

**To convert to USD for cross-country comparison:**
```
value_usd = value_operational × fx_rate_loc_to_usd_fxn
```

**The analytical error from aggregating without conversion is currency mixing** summing euros,
pounds, zlotys, and dollars as if they were the same unit. This produces a number that is neither
in USD nor in any meaningful composite currency. For example, €100 and $100 would both contribute
"100" to the sum, but their USD equivalents differ by ~8% at current rates. Revenue totals, cohort
averages, and trend lines built on such aggregates would all be wrong and the error would scale
with the proportion of non-USD transactions, making it harder to detect in markets with low FX
exposure but catastrophic in datasets with heavy EUR or GBP volume.
    """)

    st.divider()

    # ── Q2 ────────────────────────────────────────────────────────────────────
    st.subheader("Q2 – Inflated Customer Count (~15% higher than expected)")
    st.markdown("Walk through the three most likely root causes and the validation query for each.")

    with st.expander("Root cause 1 Duplicate `user_uuid` values (identity stitching / migration fan-out)", expanded=True):
        st.markdown("""
A single real customer may have been assigned multiple UUIDs if records were migrated from
different systems, or if a pipeline JOIN produced fan-out (e.g. a many-to-many relationship
between orders and users that was not deduped).

**Validation:**
```sql
-- Look for UUIDs that share the same city, country, and overlapping order dates
-- (a proxy for the same physical person)
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
Also check whether the 15% excess maps to a specific acquisition cohort or country that
would point to a specific migration event.
        """)

    with st.expander("Root cause 2 Model grain is not 1 row per customer (missing GROUP BY or DISTINCT)"):
        st.markdown("""
If the aggregation CTE was accidentally built at order level instead of customer level
(e.g. a `GROUP BY` was dropped, or a subquery returned multiple rows per user),
`COUNT(*)` would count rows rather than customers.

**Validation:**
```sql
-- The table must have exactly one row per user_uuid
SELECT user_uuid, COUNT(*) AS row_count
FROM master_customer_table
GROUP BY user_uuid
HAVING COUNT(*) > 1;
-- Any result here means the model is not at customer grain
```
        """)

    with st.expander("Root cause 3 Different customer definition vs. the previous report"):
        st.markdown("""
The baseline report may have filtered out customers whose only interactions were
refunded, expired, or unredeemed effectively counting only customers who completed
at least one redemption. The new model counts all `user_uuid` values regardless of status.

**Validation:**
```sql
SELECT
  COUNT(DISTINCT user_uuid)                                               AS all_customers,
  COUNT(DISTINCT CASE WHEN last_status = 'redeemed' THEN user_uuid END)  AS customers_with_redemption,
  ROUND(
    100.0 * COUNT(DISTINCT CASE WHEN last_status = 'redeemed' THEN user_uuid END)
    / COUNT(DISTINCT user_uuid), 1
  )                                                                       AS redemption_pct
FROM orders_merged;
-- If redemption_pct is ~87%, removing non-redeemers would shrink the count by ~13% close to the 15% gap
```
        """)

    st.divider()

    # ── Q3 ────────────────────────────────────────────────────────────────────
    st.subheader("Q3 – Making Your Model Trustworthy")
    st.markdown("Five concrete things to put in place for `master_customer_table`:")

    items = [
        (
            "1 · Row-count and grain assertion on every run",
            """
Every pipeline execution checks:
- Output has **exactly one row per `user_uuid`** (no duplicates)
- Total row count is within **±5% of the previous run** (guards against silent data loss or explosion)

A 15% jump fails the build loudly before any downstream consumer queries stale data.
            """,
        ),
        (
            "2 · Column-level data contracts (dbt tests or custom assertions)",
            """
Define `not_null`, `unique`, and logical-range tests on key columns:
- `user_uuid`: unique + not null
- `first_order_date` ≤ `last_order_date`
- `total_orders` ≥ 1
- `reactivation_count` ≤ `total_orders - 1`

These fail the build rather than silently producing wrong numbers downstream.
            """,
        ),
        (
            "3 · Financial reconciliation against the raw order table",
            """
After every build, verify that the sum of `total_gross_bookings_usd` in the customer table
equals the sum in `orders_merged`:

```sql
SELECT
  (SELECT SUM(gross_bookings_usd) FROM orders_merged)                AS raw_total,
  (SELECT SUM(total_gross_bookings_usd) FROM master_customer_table)  AS model_total;
```

Any discrepancy signals a JOIN fan-out, missing rows, or double-counting.
            """,
        ),
        (
            "4 · Documented classification logic with a traceable audit column",
            """
Expose the `order_classified` CTE as its own view or intermediate table.
When an analyst questions why a customer is classified as "reactivated", they can trace it back
to the specific order UUID and gap rather than reverse-engineering a window function.

Also: add a `classification_rule_version` column so that if the definition of reactivation changes
(e.g. from 365 to 180 days), historical classifications can be recomputed and compared.
            """,
        ),
        (
            "5 · Freshness SLA and lineage annotation in the table description",
            """
Document directly on the table:
- **Source tables**: `orders_merged` (and its upstream sources)
- **Refresh schedule**: e.g. daily at 06:00 UTC
- **Max acceptable lag**: 24 hours
- **Owner**: team/person responsible

If `orders_merged` stops updating, consumers of `master_customer_table` should be alerted
via a freshness check not silently served stale data. In dbt this is a `freshness` block;
in BigQuery it can be enforced via scheduled query alerts or a monitoring table.
            """,
        ),
    ]

    for title, body in items:
        with st.expander(title, expanded=False):
            st.markdown(body)


# ═══════════════════════════════════════════════════════════════════════════════
# RAW DATA
# ═══════════════════════════════════════════════════════════════════════════════

with tab4:
    st.header("Raw Source Data")
    st.markdown(
        "Original files as received unmodified. "
        "Use this tab to compare against the cleaned dataset in Assignment 1."
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("orders_historical.csv", f"{len(hist_raw):,} rows", "Jan 2021 – Jun 2023")
    c2.metric("orders_2024_2025.csv", f"{len(recent_raw):,} rows", "Jul 2023 – Feb 2025")
    c3.metric("Combined", f"{len(hist_raw) + len(recent_raw):,} rows", "before cleaning")

    st.divider()

    raw_tab1, raw_tab2 = st.tabs(["orders_historical.csv", "orders_2024_2025.csv"])

    with raw_tab1:
        st.caption(f"{len(hist_raw):,} rows · {len(hist_raw.columns)} columns")
        col_l, col_r = st.columns(2)
        with col_l:
            st.markdown("**Null counts**")
            nulls = hist_raw.isnull().sum().reset_index()
            nulls.columns = ["Column", "Nulls"]
            st.dataframe(nulls[nulls["Nulls"] > 0], hide_index=True, use_container_width=True)
        with col_r:
            st.markdown("**Data types**")
            dtypes = hist_raw.dtypes.reset_index()
            dtypes.columns = ["Column", "Type"]
            st.dataframe(dtypes, hide_index=True, use_container_width=True)
        st.dataframe(hist_raw, use_container_width=True)

    with raw_tab2:
        st.caption(f"{len(recent_raw):,} rows · {len(recent_raw.columns)} columns")
        col_l, col_r = st.columns(2)
        with col_l:
            st.markdown("**Null counts**")
            nulls2 = recent_raw.isnull().sum().reset_index()
            nulls2.columns = ["Column", "Nulls"]
            st.dataframe(nulls2[nulls2["Nulls"] > 0], hide_index=True, use_container_width=True)
        with col_r:
            st.markdown("**Data types**")
            dtypes2 = recent_raw.dtypes.reset_index()
            dtypes2.columns = ["Column", "Type"]
            st.dataframe(dtypes2, hide_index=True, use_container_width=True)
        st.dataframe(recent_raw, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# SQL PLAYGROUND
# ═══════════════════════════════════════════════════════════════════════════════

with tab5:
    st.header("SQL Playground")
    st.markdown(
        "Write any SQL against the **cleaned dataset** (`orders` table). "
        "Uses DuckDB BigQuery-compatible syntax. "
        "Results appear below the query editor."
    )

    st.info(
        "**Available table:** `orders` 1,242 rows, 19 columns (cleaned + USD columns added).\n\n"
        "**Columns:** `operational_view_date`, `user_uuid`, `customer_city`, `customer_country`, "
        "`order_uuid`, `parent_order_uuid`, `platform`, `fx_rate_loc_to_usd_fxn`, "
        "`list_price_operational`, `deal_discount_operational`, `gross_bookings_operational`, "
        "`margin_1_operational`, `vfm_operational`, `incentive_promo_code`, `last_status`, "
        "`gross_bookings_usd`, `margin_1_usd`, `vfm_usd`, `gross_profit_usd`",
        icon="ℹ️",
    )

    # ── Example queries ───────────────────────────────────────────────────────
    examples = {
        "— pick an example —": "",
        "Orders by country": "SELECT customer_country, COUNT(*) AS orders, ROUND(SUM(gross_bookings_usd), 2) AS gross_bookings_usd\nFROM orders\nGROUP BY customer_country\nORDER BY gross_bookings_usd DESC",
        "Top 10 customers by revenue": "SELECT user_uuid, COUNT(*) AS orders, ROUND(SUM(gross_bookings_usd), 2) AS total_usd\nFROM orders\nGROUP BY user_uuid\nORDER BY total_usd DESC\nLIMIT 10",
        "Monthly revenue trend": "SELECT DATE_TRUNC('month', operational_view_date) AS month,\n       COUNT(*) AS orders,\n       ROUND(SUM(gross_bookings_usd), 2) AS gross_bookings_usd\nFROM orders\nGROUP BY month\nORDER BY month",
        "Promo vs non-promo AOV": "SELECT\n  CASE WHEN incentive_promo_code = '' THEN 'No promo' ELSE 'Promo' END AS promo,\n  COUNT(*) AS orders,\n  ROUND(AVG(gross_bookings_usd), 2) AS avg_order_value_usd\nFROM orders\nGROUP BY promo",
        "Refunded orders detail": "SELECT order_uuid, user_uuid, customer_country, operational_view_date,\n       gross_bookings_usd, last_status\nFROM orders\nWHERE last_status = 'refunded'\nORDER BY operational_view_date DESC\nLIMIT 20",
        "Customer order classification": "WITH gaps AS (\n  SELECT user_uuid, order_uuid, operational_view_date,\n         LAG(operational_view_date) OVER (PARTITION BY user_uuid ORDER BY operational_view_date) AS prev_date,\n         ROW_NUMBER() OVER (PARTITION BY user_uuid ORDER BY operational_view_date) AS seq\n  FROM orders\n)\nSELECT *,\n  CASE\n    WHEN seq = 1 THEN 'new'\n    WHEN DATEDIFF('day', prev_date, operational_view_date) > 365 THEN 'reactivated'\n    ELSE 'retained'\n  END AS order_type\nFROM gaps\nORDER BY user_uuid, operational_view_date",
    }

    selected = st.selectbox("Load an example query", options=list(examples.keys()))

    default_sql = examples[selected] if selected != "— pick an example —" else "SELECT *\nFROM orders\nLIMIT 10"

    query = st.text_area(
        "SQL query",
        value=default_sql,
        height=200,
        placeholder="SELECT * FROM orders LIMIT 10",
    )

    col_run, col_info = st.columns([1, 5])
    run = col_run.button("Run query", type="primary", use_container_width=True)

    if run:
        if not query.strip():
            st.warning("Write a query first.")
        else:
            try:
                result = run_sql(query, df)
                col_info.caption(f"{len(result):,} rows · {len(result.columns)} columns returned")
                st.dataframe(result, use_container_width=True)
            except Exception as e:
                st.error(f"Query error: {e}")
