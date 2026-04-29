---
name: vic-root-gold-features
description: Gold feature grain, PK, and Databricks-facing contract for VicRoot
---

# Gold features — VicRoot

1. Grain: one row per `tree_id` + `as_of_date` (see `docs/architecture.md`).
2. Join weather aggregates for same `as_of_date` / window; compute `moisture_deficit_index`.
3. Add optional `resilience_score_rule` and `risk_tier` for Power BI demos.
4. Do **not** embed training scoring here if Databricks owns ML—keep Gold as **features only** plus optional rule-based columns.
5. Document any new column in `docs/architecture.md` before Databricks consumes.
