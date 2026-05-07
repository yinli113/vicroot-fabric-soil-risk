# VicRoot — Fabric pipeline activity checklist

# TL;DR

Use this as a **runbook**: **notebook activities in order**, with **shared pipeline parameters** passed into each notebook (`resolve_params()` in `notebooks/lib/pipeline_params.py`). **Bronze** domain notebooks *can* parallelize on large capacity; on **F2 / trial** prefer **serial** Bronze (Soil → Site → Weather). **Silver → Gold → ML** stay **sequential**. Adjust **table names** only if your lakehouse uses different identifiers.

---

## 1. Pipeline parameters (set once per pipeline)

Define these at **pipeline** level; pass the same keys into **each** notebook activity (Fabric: *Base parameters* / dynamic JSON, or equivalent).

| Parameter | Example / note |
|-----------|------------------|
| `snapshot_date` | `2026-05-06` — partition / trace label for this run |
| `pipeline_run_id` | `@pipeline().RunId` |
| `environment` | `dev` |
| `medallion_root` | `Files/medallion` |
| `source_crs` | `EPSG:4326` |
| `target_crs` | `EPSG:7855` |
| `readings_years_csv` | `2022,2023,2024,2025` |
| `bronze_ingest_mode` | `zip_and_files` (or your mode) |
| `soil_zip_input_path` | Path to **2022** zip in Lakehouse **Files** |
| `soil_csv_input_dir` | **Single** folder containing yearly CSVs **flat** next to each other: `dir/soil-sensor-readings-historical-data-2023.csv`, … (current `01_bronze_ingest_soil.py` file branch uses `{dir}/{filename}` only). For partitioned layouts (`year=YYYY/raw/file/`), set `soil_csv_input_dir` to the parent that contains those folders or adjust the notebook. Example parent: `Files/medallion/bronze/com_soil_sensor_readings` if you store files there **flat** or mirror that path. Not three separate params. |
| `site_file_input_path` | Optional; `Files/raw/site/...` |
| `weather_file_input_path` | Optional; or enable API branch |
| `weather_api_enabled` | `true`/`false` per your Bronze weather notebook |
| `silver_records_table` | `silver_soil_sensor_readings` |
| `weather_daily_table` | `silver_weather_daily` (Gold dim weather built from this if present) |
| `dq_fail_on_error` | `true` for strict runs |

**Secrets:** load in Fabric (Key Vault / notebook credentials), **not** in pipeline JSON in git. See [.env.example](../.env.example) for env names only.

---

## 2. Activity order and repo mapping

### Stage A — Bronze (can parallelize per domain)

| # | Suggested activity name | Repo notebook | Purpose |
|---|-------------------------|---------------|---------|
| A1 | `NB_Bronze_Soil` | `notebooks/01_bronze_ingest_soil.py` | Soil sensor zip/CSV/API → Bronze paths |
| A2 | `NB_Bronze_Site` | `notebooks/01_bronze_ingest_site.py` | Site reference → Bronze |
| A3 | `NB_Bronze_Weather` | `notebooks/01_bronze_ingest_weather.py` | Weather file/API → Bronze |

**Capacity (F2 / trial):** run **A1 → A2 → A3** in **series** (each activity depends on the previous success). Parallel Bronze activities often trigger **TooManyRequestsForCapacity** (HTTP 430).

**Optional:** legacy combined `notebooks/01_bronze_ingest.py` if you still use it — prefer **split** notebooks to isolate failures.

**Gate:** each activity should log rows written / paths; fix **input guard** errors before Silver.

---

### Stage B — Silver (sequential within Silver; soil before spatial join if join reads silver soil)

| # | Suggested activity name | Repo notebook | Purpose |
|---|-------------------------|---------------|---------|
| B1 | `NB_Silver_Soil` | `notebooks/02_silver_clean_soil.py` | `silver_soil_sensor_readings` (+ quarantine if configured) |
| B2 | `NB_Silver_Site` | `notebooks/02_silver_clean_site.py` | `silver_site_file_reference` (or equivalent) |
| B3 | `NB_Silver_Weather` | `notebooks/02_silver_clean_weather.py` | `silver_weather_daily` |
| B4 | `NB_Silver_SpatialJoin` | `notebooks/02_silver_spatial_join.py` | CRS + spatial join / Sedona path per your implementation |

**Gate:** `silver_soil_sensor_readings` and `silver_weather_daily` (or your params) **exist** before Gold.

---

### Stage C — Gold

| # | Suggested activity name | Repo notebook | Purpose |
|---|-------------------------|---------------|---------|
| C1 | `NB_Gold_Features` | `notebooks/03_gold_features.py` | Dims + depth facts + `gold_fact_irrigation_risk` + peer tables |

**Gate:** Gold tables listed in [docs/architecture.md](architecture.md) present; DQ logs clean if `dq_fail_on_error=true`.

---

### Stage D — ML baseline (optional; after Gold)

| # | Suggested activity name | Repo notebook | Purpose |
|---|-------------------------|---------------|---------|
| D1 | `NB_Gold_ML_MoistureBaseline` | `notebooks/05_ml_moisture_baseline.py` | `gold_ml_moisture_baseline` |

**Extra params (defaults in `pipeline_params.py`):** `ml_target_depth_cm`, `ml_train_end_date`, `ml_rolling_days`, `ml_min_train_rows`, `ml_model_id`, `gold_ml_moisture_baseline_table`.

---

## 3. Post-run checks (quick)

| Check | Where |
|--------|--------|
| Watermarks / logs | Notebook output; `metadata.ingestion_watermarks` if used |
| Row counts | Bronze/Silver/Gold/ML print lines |
| BI | Refresh semantic model if it reads Gold / ML tables |

---

## 4. Failure triage (first knobs)

1. **Bronze strict guard** — broaden/narrow input paths; confirm zip/CSV exist under **Files**.  
2. **Silver empty** — wrong Bronze path or `snapshot_date` mismatch.  
3. **Gold DQ** — duplicate PKs, missing weather table, empty facts.  
4. **ML** — missing `gold_dim_weather_daily` or moisture fact; `ml_min_train_rows` too high for short history.

Skill reference: [.cursor/skills/vic-root-debug-pipeline/SKILL.md](../.cursor/skills/vic-root-debug-pipeline/SKILL.md).

---

## 5. Related repo files

- Parameter defaults: [`notebooks/lib/pipeline_params.py`](../notebooks/lib/pipeline_params.py)  
- Bronze template: [`fabric/pipeline_bronze.parameters.template.json`](pipeline_bronze.parameters.template.json)  
- Architecture / naming: [`docs/architecture.md`](../docs/architecture.md)  
- Ops model (Git + Deployment Pipeline + manual sync): [`docs/power_bi_dashboard_report.md`](../docs/power_bi_dashboard_report.md) §9.5  
