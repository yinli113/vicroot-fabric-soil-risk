# Plan execution — file artifacts (reference)

# TL;DR

The scaffold now lives **directly in the repo** (`.cursor/`, `notebooks/`, `fabric/`, `config/`, `sql/`, `.env.example`). This page remains a **mirror** of those templates for easy copy-paste or diffing.

---

## 1. `.gitignore` (repository root)

```
# Python
.venv/
venv/
__pycache__/
*.py[cod]
.pytest_cache/
.mypy_cache/

# Jupyter
.ipynb_checkpoints/

# Data / secrets (never commit)
*.env
.env.*
secrets/
data/raw/
*.csv
*.parquet
!config/**/*.json

# IDE
.idea/
.vscode/

# OS
.DS_Store
```

---

## 2. Cursor rules — `.cursor/rules/`

Save each block as the named `.mdc` file.

### `vic-root-core.mdc`

```markdown
---
description: VicRoot core—blueprint, medallion naming, CRS, no secrets in git
alwaysApply: true
---

# VicRoot Insights — core

- **Vision:** [PROMPT.md](../../PROMPT.md) — Melbourne urban forestry; soil stress + weather → risk signals.
- **Medallion naming:** Delta tables/paths use `bronze_*`, `silver_*`, `gold_*` (or `feature_*` for ML-ready outputs).
- **CRS:** Tree coords typically **EPSG:4326** ingested; local spatial math **EPSG:7855** (GDA2020 / MGA zone 55 context). Document exceptions in [docs/architecture.md](../../docs/architecture.md).
- **Parameters first:** Prefer Fabric pipeline **parameters** + **variables** and notebook `params` dict—no workspace-specific IDs hard-coded in business logic.
- **Secrets:** Never commit API keys or connection strings. Use Fabric / Azure Key Vault.
- **Source of truth:** Technical detail in `docs/architecture.md`; session state in `memory-bank/activeContext.md`.
```

### `vic-root-fabric.mdc`

```markdown
---
description: Fabric pipelines, parameters vs variables, idempotency, notebook params
globs: fabric/**,**/notebooks/**,docs/architecture.md
alwaysApply: false
---

# VicRoot — Microsoft Fabric

- **Parameters:** Declare ingestion endpoints, `medallion_root`, `snapshot_date`, CRS codes, environment—see [docs/architecture.md](../../docs/architecture.md).
- **Variables:** Use for paths built from `utcnow()`, activity outputs, and conditional branches (`Set variable`).
- **Notebook activities:** Pass the same keys as a structured `params` object; first notebook cell resolves defaults via `notebooks/lib/pipeline_params.py` pattern.
- **Idempotency:** Read/update `metadata.ingestion_watermarks` (or equivalent) so reruns do not duplicate Bronze facts.
- **Lookup:** Prefer a small config JSON in Files or a `metadata.datasets` Delta table over duplicating URLs in many activities.
- **Git:** Prefer Fabric Git integration to Azure DevOps for pipeline/notebook definitions when available.
```

### `vic-root-geospatial.mdc`

```markdown
---
description: Sedona/geospatial, CRS, validation ranges for soil/tree work
globs: "**/notebooks/**"
alwaysApply: false
---

# VicRoot — geospatial

- **Transform:** Silver layer must project tree points from `source_crs` (default 4326) to `target_crs` (default **7855**) before distance/overlay operations.
- **Join:** Spatial join trees (points) to soil (polygons); document tolerance/buffer in `docs/architecture.md` if used.
- **Libraries:** Prefer Apache Sedona in PySpark on Fabric; geopandas acceptable for small dev proofs only—production path is cluster-scale Spark.
- **Validation:** pH roughly 0–14; flag salinity and missing geometry as quarantine rows, not silent nulls.
- **Output:** Persist geometry as WKB or well-known functions compatible with downstream Power BI / Databricks consumers.
```

### `vic-root-ai-routing.mdc`

```markdown
---
description: When to summarize vs explore vs generalPurpose; evidence-based debug
alwaysApply: true
---

# VicRoot — AI routing (token + quality)

- **Route first:** Classify: doc summary / repo search / implement / debug—then load **one** domain rule + **one** skill.
- **Cheap & parallel:** Use readonly **explore** subtasks for disjoint file maps or API field extraction; tight prompts, no full blueprint in every subagent.
- **Deep work:** Use **generalPurpose** for multi-step Fabric pipeline design, Sedona/CRS edge cases, root-cause with logs.
- **Debug:** Require **evidence** (log snippet path, failing line, parameter snapshot)—hypothesis → minimal repro → single fix.
- **Memory:** After meaningful changes, add one bullet to `memory-bank/activeContext.md`; keep long specs in `docs/architecture.md` only.
```

---

## 3. Project skills — `.cursor/skills/<name>/SKILL.md`

Each skill is a folder with `SKILL.md`. Frontmatter: `name` + `description`.

### `.cursor/skills/vic-root-fabric-params/SKILL.md`

```markdown
---
name: vic-root-fabric-params
description: Standard Fabric pipeline parameters, variables, and notebook wiring for VicRoot
---

# Fabric parameters — VicRoot

1. Open `docs/architecture.md` at repository root — parameter table.
2. In Fabric pipeline: define matching **pipeline parameters**; defaults for `dev`.
3. Add **variables** for computed paths: `concat` / `formatDateTime` patterns off `snapshot_date`.
4. Notebook activity: pass JSON or individual arguments matching `resolve_params()` keys in `notebooks/lib/pipeline_params.py`.
5. Verify run: check notebook receives `target_crs`, `medallion_root`, `snapshot_date`, API bases.
```

### `.cursor/skills/vic-root-bronze-ingest/SKILL.md`

```markdown
---
name: vic-root-bronze-ingest
description: Bronze API ingestion, landing layout, watermark updates for VicRoot
---

# Bronze ingestion — VicRoot

1. Landing paths: `{medallion_root}/bronze/{dataset}/dt={snapshot_date}/` (adjust to org standard).
2. Before ingest: read `metadata.ingestion_watermarks` for dataset; apply incremental rule if API supports cursors.
3. Write raw JSON/CSV to Files or Bronze Delta as designed; avoid duplicate fact keys on rerun (merge/idempotent write).
4. After success: upsert watermark row with `pipeline_run_id`, `last_success_utc`, `snapshot_date`.
5. Log record counts via `Set variable` + optional notification.
```

### `.cursor/skills/vic-root-silver-spatial-join/SKILL.md`

```markdown
---
name: vic-root-silver-spatial-join
description: Silver CRS transform, spatial join, GE from config for VicRoot
---

# Silver spatial join — VicRoot

1. Read Bronze tables/Files; project tree coords to `target_crs` (7855).
2. Load soil polygons; ensure consistent CRS before join.
3. Spatial join: point-in-polygon; capture `soil_ph`, salinity fields, join quality flags.
4. Load Great Expectations (or validation) rules from `config/ge_validation_rules.json` or metadata Delta.
5. Write `silver_trees_soil` (name per architecture); quarantine failed expectations to `silver_quarantine` or equivalent.
```

### `.cursor/skills/vic-root-gold-features/SKILL.md`

```markdown
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
```

### `.cursor/skills/vic-root-debug-pipeline/SKILL.md`

```markdown
---
name: vic-root-debug-pipeline
description: Fabric pipeline + notebook failure checklist for VicRoot
---

# Debug pipeline — VicRoot

1. Capture **Run ID**; open activity output / error message verbatim.
2. Check parameter pass-through: notebook `resolve_params()` vs pipeline definitions (typos, missing defaults).
3. For notebook: stderr first cell import errors (Sedona init, jar attach).
4. For APIs: HTTP status, pagination exhaustion, throttle—log response headers if available.
5. One change at a time; re-run single activity if Fabric supports scoped debug.
```

### `.cursor/skills/vic-root-doc-summary/SKILL.md`

```markdown
---
name: vic-root-doc-summary
description: Summarize long API/docs into tables for VicRoot without code changes
---

# Doc summary — VicRoot

1. Read-only: extract endpoints, auth scheme, rate limits, key JSON fields.
2. Output: markdown table + “open questions” list; cite section headings only, not full paste.
3. If doc updates `docs/architecture.md`, list proposed diff as bullets—do not apply without user confirm.
```

### `.cursor/skills/vic-root-databricks-handoff/SKILL.md`

```markdown
---
name: vic-root-databricks-handoff
description: OneLake shortcut, UC, and prediction table pattern for VicRoot
---

# Databricks handoff — VicRoot

1. Follow checklist in `docs/architecture.md` § Databricks handoff.
2. Confirm read path: Gold Delta via shortcut / external location—no duplicate copy.
3. Training: version features by `as_of_date`; avoid leakage (document join windows).
4. Scoring: write `gold_tree_health_predictions` with `model_version`, `scored_at`; Power BI add relationship to features if needed.
```

---

## 4. `notebooks/lib/pipeline_params.py`

```python
"""Resolve notebook parameters from Fabric pipeline or local defaults."""

from __future__ import annotations

import json
import os
from typing import Any, Dict

DEFAULT_PARAMS: Dict[str, Any] = {
    "environment": "dev",
    "lakehouse_name": "lh_vicroot_melbourne",
    "medallion_root": "Files/medallion",
    "api_base_url_com": "https://data.melbourne.vic.gov.au/api/records/1.0/search/",
    "api_base_url_soil": "",
    "api_base_url_bom": "",
    "bom_station_id": "",
    "source_crs": "EPSG:4326",
    "target_crs": "EPSG:7855",
    "snapshot_date": "2026-04-24",
}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def resolve_params(overrides: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Merge defaults with Fabric/notebook-injected params.

    Fabric: pass overrides from pipeline (e.g. mssparkutils or notebook arguments).
    Local: set VICROOT_PARAMS_JSON env var to a JSON object.
    """
    params = dict(DEFAULT_PARAMS)
    raw = os.environ.get("VICROOT_PARAMS_JSON")
    if raw:
        params = _deep_merge(params, json.loads(raw))
    if overrides:
        params = _deep_merge(params, overrides)
    return params
```

---

## 5. `notebooks/01_bronze_ingest.py`

PySpark notebook script stub (paste as cells in Fabric). **Imports:** On Fabric, if `from notebooks.lib...` fails, use `%run ./lib/pipeline_params` (adjust path to where you uploaded `pipeline_params.py`) or add the folder to `sys.path`.

```python
# Cell 1 — params
from notebooks.lib.pipeline_params import resolve_params

# In Fabric, replace {} with dict from pipeline activity when wiring.
params = resolve_params({})
snapshot_date = params["snapshot_date"]
medallion_root = params["medallion_root"]
print(params)
```

```python
# Cell 2 — placeholder ingest (implement HTTP / copy activity output)
# - Read metadata.ingestion_watermarks
# - Pull CoM / Soil / BOM per params
# - Write to f"{medallion_root}/bronze/..." with idempotent layout
# - Upsert watermarks on success
raise NotImplementedError("Implement Bronze ingest in Fabric notebook")
```

---

## 6. `notebooks/02_silver_spatial_join.py`

```python
# Cell 1 — params
from notebooks.lib.pipeline_params import resolve_params

params = resolve_params({})
source_crs = params["source_crs"]
target_crs = params["target_crs"]
```

```python
# Cell 2 — Sedona / transform / join (implement)
# - Read Bronze
# - ST_Transform tree coords to target_crs
# - Spatial join soil polygons
# - Validate using config rules
raise NotImplementedError("Implement Silver join in Fabric notebook")
```

---

## 7. `notebooks/03_gold_features.py`

```python
# Cell 1 — params
from notebooks.lib.pipeline_params import resolve_params

params = resolve_params({})
```

```python
# Cell 2 — feature assembly (implement)
# - Read silver_trees_soil + weather aggregates
# - Emit gold_tree_features at grain (tree_id, as_of_date)
raise NotImplementedError("Implement Gold features in Fabric notebook")
```

---

## 8. `config/ge_validation_rules.json`

```json
{
  "soil_ph": { "min": 0, "max": 14 },
  "salinity_ms_cm": { "min": 0, "max": 50 },
  "notes": "Tune bounds per Agriculture Victoria documentation; quarantine out-of-range."
}
```

---

## 9. `sql/metadata_ddl.sql`

```sql
-- Fabric / Spark SQL — adapt database/schema names to your lakehouse

CREATE TABLE IF NOT EXISTS metadata.ingestion_watermarks (
  dataset_name STRING,
  last_success_run_id STRING,
  last_success_utc TIMESTAMP,
  high_watermark STRING,
  snapshot_date STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS metadata.datasets (
  dataset_name STRING,
  api_base STRING,
  enabled BOOLEAN,
  priority INT
) USING DELTA;
```

---

## 10. `fabric/README.md`

```markdown
# Fabric pipeline notes — Bronze

1. Create pipeline parameters matching [docs/architecture.md](../docs/architecture.md).
2. Activities: optional **Lookup** on `metadata.datasets`; **Copy** or **Notebook** per source; **Set variable** for paths.
3. Child pipeline or sequential: CoM trees → AV soil → BOM weather; then watermark notebook.
4. Git: connect workspace to Azure DevOps per org standards.
```

---

## 11. `fabric/pipeline_bronze.parameters.template.json`

Template for documenting parameter names (not a full ARM export):

```json
{
  "pipeline_parameters": {
    "environment": { "type": "string", "default": "dev" },
    "medallion_root": { "type": "string", "default": "Files/medallion" },
    "api_base_url_com": { "type": "string" },
    "api_base_url_soil": { "type": "string" },
    "api_base_url_bom": { "type": "string" },
    "bom_station_id": { "type": "string" },
    "source_crs": { "type": "string", "default": "EPSG:4326" },
    "target_crs": { "type": "string", "default": "EPSG:7855" },
    "snapshot_date": { "type": "string" }
  }
}
```

---

## 12. `src/__init__.py`

Optional package marker for local tooling:

```python
# VicRoot Insights — local Python package placeholder
```

---

After creating files, update `memory-bank/activeContext.md` to remove the “blocked” caveat and list completed paths.
