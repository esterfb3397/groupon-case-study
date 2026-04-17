---
name: run-analysis
description: Run all five Assignment 2 SQL queries via DuckDB and print results. Use this to verify query output, check numbers, or refresh findings after any code change.
disable-model-invocation: false
---

Run all Assignment 2 queries by executing `src/analysis.py` and display the results clearly.

Steps:
1. Run `uv run python src/analysis.py` from the project root.
2. Show the full output for all five queries:
   - Master Customer Table (first 10 rows + shape)
   - Q1 – Last 6-month revenue mix
   - Q1 – Monthly mix (last 12 rows)
   - Q2 – Platform summary
   - Q2 – Yearly share trend
3. Highlight any numbers that look unexpected (nulls, negative shares, missing segments).
4. Confirm the dataset stats: 266 customers, $136,580 total gross bookings USD, date range 2021-01-01 → 2025-02-27.
