---
name: vic-root-databricks-handoff
description: OneLake shortcut, UC, and prediction table pattern for VicRoot
---

# Databricks handoff — VicRoot

1. Follow checklist in `docs/architecture.md` § Databricks handoff.
2. Confirm read path: Gold Delta via shortcut / external location—no duplicate copy.
3. Training: version features by `as_of_date`; avoid leakage (document join windows).
4. Scoring: write `gold_tree_health_predictions` with `model_version`, `scored_at`; Power BI add relationship to features if needed.
