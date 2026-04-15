"""Data quality tests for Assignment 1 cleaning pipeline."""

import pandas as pd
import pytest
from src.cleaning import (
    merge_files,
    fix_date_column,
    fix_missing_country,
    fix_promo_code,
    add_usd_columns,
    run_quality_checks,
    clean,
)


@pytest.fixture()
def raw_dfs():
    hist = pd.read_csv("data/raw/orders_historical.csv")
    recent = pd.read_csv("data/raw/orders_2024_2025.csv")
    return hist, recent


@pytest.fixture()
def cleaned_df():
    df, _ = clean(save=False)
    return df


# --- merge ---

def test_merge_row_count(raw_dfs):
    hist, recent = raw_dfs
    merged = merge_files(hist, recent)
    assert len(merged) == len(hist) + len(recent)


def test_no_overlap_between_files(raw_dfs):
    hist, recent = raw_dfs
    overlap = set(hist["order_uuid"]) & set(recent["order_uuid"])
    assert len(overlap) == 0, f"Unexpected overlapping order_uuids: {overlap}"


# --- date ---

def test_date_parsed(cleaned_df):
    import datetime
    assert all(isinstance(d, datetime.date) for d in cleaned_df["operational_view_date"])


def test_date_range(cleaned_df):
    import datetime
    assert min(cleaned_df["operational_view_date"]) == datetime.date(2021, 1, 1)
    assert max(cleaned_df["operational_view_date"]) <= datetime.date(2025, 12, 31)


# --- country ---

def test_no_null_country(cleaned_df):
    assert cleaned_df["customer_country"].isnull().sum() == 0


def test_country_values(cleaned_df):
    valid = {"DE", "ES", "FR", "GB", "IT", "PL", "US"}
    found = set(cleaned_df["customer_country"].unique())
    assert found <= valid, f"Unexpected country codes: {found - valid}"


# --- promo code ---

def test_no_null_promo_code(cleaned_df):
    assert cleaned_df["incentive_promo_code"].isnull().sum() == 0


def test_promo_code_no_whitespace(cleaned_df):
    has_leading = cleaned_df["incentive_promo_code"].str.startswith(" ").any()
    has_trailing = cleaned_df["incentive_promo_code"].str.endswith(" ").any()
    assert not has_leading and not has_trailing


# --- financial columns ---

def test_usd_columns_exist(cleaned_df):
    for col in ["gross_bookings_usd", "margin_1_usd", "vfm_usd", "gross_profit_usd"]:
        assert col in cleaned_df.columns


def test_usd_conversion_formula(cleaned_df):
    expected = (
        cleaned_df["gross_bookings_operational"] * cleaned_df["fx_rate_loc_to_usd_fxn"]
    ).round(2)
    pd.testing.assert_series_equal(
        cleaned_df["gross_bookings_usd"], expected, check_names=False
    )


def test_refunded_orders_non_positive_bookings(cleaned_df):
    refunded = cleaned_df[cleaned_df["last_status"] == "refunded"]
    assert (refunded["gross_bookings_operational"] <= 0).all()


# --- uniqueness & nulls ---

def test_no_duplicate_orders(cleaned_df):
    assert cleaned_df["order_uuid"].duplicated().sum() == 0


def test_no_nulls_in_key_columns(cleaned_df):
    key_cols = ["operational_view_date", "user_uuid", "order_uuid",
                "customer_country", "platform", "last_status"]
    for col in key_cols:
        assert cleaned_df[col].isnull().sum() == 0, f"Nulls found in {col}"


# --- quality report ---

def test_quality_report_keys():
    _, report = clean(save=False)
    expected_keys = {
        "total_rows", "unique_orders", "unique_customers", "date_range",
        "countries", "platforms", "last_status_counts", "rows_hist",
        "rows_recent", "refunded_zero_bookings",
    }
    assert expected_keys <= set(report.keys())
