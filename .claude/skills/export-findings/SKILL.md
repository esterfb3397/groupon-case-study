---
name: export-findings
description: Generate a concise summary of all key findings from the three assignments. Use this to prepare talking points, review the analysis, or verify the README is up to date.
disable-model-invocation: false
---

Generate a structured findings summary for the Groupon case study.

Steps:
1. Run `uv run python src/analysis.py` to get fresh numbers.
2. Run `uv run python src/cleaning.py` to get the quality report.
3. Print a structured summary with three sections:

---

## Assignment 1 – Data Cleaning
- Total rows after merge and issues fixed (nulls, types, derived columns).
- List each issue found with count and fix applied.

## Assignment 2 – SQL Analysis

**Master Customer Table**
- Row count, total gross bookings USD, total gross profit USD, avg orders per customer.

**Q1 – Revenue Mix (last 6 months)**
- % share for new / reactivated / retained with the gross bookings USD value for each.
- Key business insight in 1–2 sentences.

**Q2 – Platform Performance**
- App vs web: avg order value, avg orders/customer, avg gross profit/customer.
- Yearly app share trend (is it growing or declining?).
- Recommendation in 1 sentence.

## Assignment 3 – Engineering Thinking
- Bullet summary of the answer to each of the 3 questions (2–3 bullets each).

---

4. Flag any finding that diverges from the README — that means the README needs updating.
