---
name: check-data
description: Run the full data quality test suite (14 pytest tests) against the cleaned dataset and report results. Use this after any change to cleaning.py or the raw CSVs.
disable-model-invocation: false
---

Run the data quality test suite and report results.

Steps:
1. Run `uv run pytest tests/test_cleaning.py -v` from the project root.
2. Show the full test output.
3. If all 14 tests pass: confirm the dataset is clean and ready for analysis.
4. If any test fails:
   - Show the exact failure message.
   - Identify which data quality check failed (merge, date parsing, country nulls, promo code, USD conversion, duplicates, etc.).
   - Propose a fix in `src/cleaning.py`.

Expected passing tests (14 total):
- test_merge_row_count
- test_no_overlap_between_files
- test_date_parsed
- test_date_range
- test_no_null_country
- test_country_values
- test_no_null_promo_code
- test_promo_code_no_whitespace
- test_usd_columns_exist
- test_usd_conversion_formula
- test_refunded_orders_non_positive_bookings
- test_no_duplicate_orders
- test_no_nulls_in_key_columns
- test_quality_report_keys
