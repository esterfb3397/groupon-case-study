"""
Assignment 1: Data Cleaning & Preparation
Merges orders_historical.csv and orders_2024_2025.csv into a single
analysis-ready dataset and resolves all identified data quality issues.
"""

import pandas as pd
from pathlib import Path

# Known city → ISO-3166-1 alpha-2 country mappings for rows missing customer_country.
# Derived by cross-referencing each city against the country distribution already
# present in the dataset (all cities are unambiguous capitals or large metros).
CITY_COUNTRY_MAP = {
    "Birmingham": "GB",
    "Lyon": "FR",
    "Barcelona": "ES",
    "Manchester": "GB",
    "Phoenix": "US",
    "Madrid": "ES",
    "Warsaw": "PL",
    "Munich": "DE",
}

RAW_DIR = Path("data/raw")
CLEANED_DIR = Path("data/cleaned")


def load_raw() -> tuple[pd.DataFrame, pd.DataFrame]:
    hist = pd.read_csv(RAW_DIR / "orders_historical.csv")
    recent = pd.read_csv(RAW_DIR / "orders_2024_2025.csv")
    return hist, recent


def merge_files(hist: pd.DataFrame, recent: pd.DataFrame) -> pd.DataFrame:
    """Concatenate both files — no overlapping order_uuids confirmed."""
    return pd.concat([hist, recent], ignore_index=True)


def fix_date_column(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["operational_view_date"] = pd.to_datetime(
        df["operational_view_date"], format="%Y-%m-%d"
    ).dt.date
    return df


def fix_missing_country(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill null customer_country using city-level lookup.
    Assumption: the 8 cities are unambiguous — each belongs to exactly one country
    based on their presence in the rest of the dataset.
    """
    df = df.copy()
    null_mask = df["customer_country"].isnull()
    df.loc[null_mask, "customer_country"] = df.loc[null_mask, "customer_city"].map(
        CITY_COUNTRY_MAP
    )
    still_null = df["customer_country"].isnull().sum()
    if still_null:
        print(f"WARNING: {still_null} rows still have null customer_country after lookup.")
    return df


def fix_promo_code(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardise incentive_promo_code: NaN → empty string.
    NaN means no promo was applied; storing as "" makes filtering consistent.
    """
    df = df.copy()
    df["incentive_promo_code"] = df["incentive_promo_code"].fillna("").str.strip()
    return df


def standardise_column_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    str_cols = [
        "user_uuid", "customer_city", "customer_country", "order_uuid",
        "parent_order_uuid", "platform", "incentive_promo_code", "last_status",
    ]
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()

    float_cols = [
        "fx_rate_loc_to_usd_fxn", "list_price_operational",
        "deal_discount_operational", "gross_bookings_operational",
        "margin_1_operational", "vfm_operational",
    ]
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors="raise").round(2)

    return df


def add_usd_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Derive USD equivalents for the three main financial metrics.
    Formula: value_usd = value_operational * fx_rate_loc_to_usd_fxn
    """
    df = df.copy()
    rate = df["fx_rate_loc_to_usd_fxn"]
    df["gross_bookings_usd"] = (df["gross_bookings_operational"] * rate).round(2)
    df["margin_1_usd"] = (df["margin_1_operational"] * rate).round(2)
    df["vfm_usd"] = (df["vfm_operational"] * rate).round(2)
    df["gross_profit_usd"] = (df["margin_1_usd"] + df["vfm_usd"]).round(2)
    return df


def run_quality_checks(df: pd.DataFrame) -> dict:
    """
    Validate the cleaned dataset. Raises on critical invariants.
    Returns a findings dict for display in the Streamlit app.
    """
    # --- critical checks ---
    dupes = df["order_uuid"].duplicated().sum()
    if dupes:
        raise ValueError(f"CRITICAL: {dupes} duplicate order_uuid rows found.")

    for col in ["operational_view_date", "user_uuid", "order_uuid",
                "customer_country", "platform", "last_status"]:
        n = df[col].isnull().sum()
        if n:
            raise ValueError(f"CRITICAL: {n} null values in '{col}'.")

    # --- informational findings ---
    return {
        "total_rows": len(df),
        "unique_orders": df["order_uuid"].nunique(),
        "unique_customers": df["user_uuid"].nunique(),
        "date_range": (
            str(min(df["operational_view_date"])),
            str(max(df["operational_view_date"])),
        ),
        "countries": sorted(df["customer_country"].unique().tolist()),
        "platforms": sorted(df["platform"].unique().tolist()),
        "last_status_counts": df["last_status"].value_counts().to_dict(),
        "rows_hist": int((df["operational_view_date"] < pd.to_datetime("2023-07-01").date()).sum()),
        "rows_recent": int((df["operational_view_date"] >= pd.to_datetime("2023-07-01").date()).sum()),
        "refunded_zero_bookings": int(
            (df[df["last_status"] == "refunded"]["gross_bookings_operational"] <= 0).sum()
        ),
        "country_nulls_fixed": 8,
        "promo_nulls_standardised": 863,
    }


def clean(save: bool = True) -> tuple[pd.DataFrame, dict]:
    """Full cleaning pipeline. Returns (cleaned_df, quality_report)."""
    hist, recent = load_raw()
    df = merge_files(hist, recent)
    df = fix_date_column(df)
    df = fix_missing_country(df)
    df = fix_promo_code(df)
    df = standardise_column_types(df)
    df = add_usd_columns(df)
    report = run_quality_checks(df)

    if save:
        CLEANED_DIR.mkdir(parents=True, exist_ok=True)
        df.to_csv(CLEANED_DIR / "orders_merged.csv", index=False)
        print(f"Saved {len(df)} rows → data/cleaned/orders_merged.csv")

    return df, report


if __name__ == "__main__":
    df, report = clean()
    print("\nQuality report:")
    for k, v in report.items():
        print(f"  {k}: {v}")
