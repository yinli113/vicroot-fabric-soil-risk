# VicRoot Insights — architecture

# TL;DR

Medallion pipeline on Microsoft Fabric (Bronze → Silver → Gold) with **parameter-driven** pipelines and notebooks; Gold is the **ML feature contract** for later Databricks training and batch scoring via OneLake shortcuts. This file is the technical source of truth—update it when parameters or schemas change.

**Scaffold:** Project layout is in-repo (`.cursor/`, `notebooks/`, `fabric/`, `config/`, `sql/`). A historical copy-paste mirror remains in [plan-execution-artifacts.md](plan-execution-artifacts.md). Local secrets: copy [.env.example](../.env.example) to `.env`.

## Workstreams

1. Bronze: API ingestion, raw landing, watermarks.
2. Silver: CRS transform (EPSG:4326 → EPSG:7855), spatial join, validation.
3. Gold: feature table + optional rule-based risk tier; Power BI DirectLake.
4. Later: Databricks reads Gold; writes prediction Delta back to the same lakehouse or agreed path.

### Split notebooks for scale (implemented)

- Bronze soil: `notebooks/01_bronze_ingest_soil.py`
- Bronze site: `notebooks/01_bronze_ingest_site.py`
- Bronze weather: `notebooks/01_bronze_ingest_weather.py`
- Silver soil: `notebooks/02_silver_clean_soil.py`
- Silver site: `notebooks/02_silver_clean_site.py`
- Silver weather: `notebooks/02_silver_clean_weather.py`
- GeoJSON export helper: `notebooks/04_export_site_risk_geojson.py`

These run independently so one domain can fail/retry without blocking others.

## Domain + layer layout (staged migration)

Keep the existing working soil ingestion paths during transition, and migrate by **copy/shortcut + fallback** instead of cutover.

- **Raw domain folders (target state):**
  - `Files/raw/soil/...` (source zip/csv/json only)
  - `Files/raw/site/...`
  - `Files/raw/weather/...`
- **Processed medallion folders (keep as source of truth for tables):**
  - `Files/medallion/bronze/...`
  - Silver/Gold Delta tables in Lakehouse Tables.

Current practical rule: shortcut at broader scope is acceptable, but notebook inputs must point to exact files/folders (`soil_csv_input_dir`, `soil_zip_input_path`, `site_file_input_path`, `weather_file_input_path`) to avoid accidental ingestion.

## Fabric resource naming (suggested)

Use a **consistent prefix** (`vicroot`) and **environment** (`dev` / `prod`) so pipeline runs, support tickets, and chat debug all refer to the same objects. Replace these with your **actual** portal names when you create resources, and keep this table updated.

| Kind | Suggested name | Notes |
|------|------------------|--------|
| Fabric **workspace** | `ws_vicroot_dev` | All VicRoot items for the trial / dev phase. (Underscores align with Fabric naming rules.) |
| **Lakehouse** | `lh_vicroot_melbourne` | Medallion Delta + **Files** (Bronze zip/JSON/API landing). |
| Power BI **semantic model** (later) | `pbir_vicroot_gold` or workspace default | DirectLake to Gold tables. |
| **Pipeline** — Bronze | `pl_vicroot_bronze_com_ingest` | CoM APIs, watermarks, optional 2022 manual branch. |
| **Pipeline** — Silver (optional) | `pl_vicroot_silver_geospatial` | CRS, spatial joins, validation. |
| **Pipeline** — Gold (optional) | `pl_vicroot_gold_features` | Feature tables for BI / Databricks. |
| **Notebook** — Bronze | `nb_vicroot_bronze_ingest` | HTTP/API pulls, zip extract, raw writes. |
| **Notebook** — Silver | `nb_vicroot_silver_spatial` | Aligns with repo `notebooks/02_silver_spatial_join.py`. |
| **Notebook** — Gold | `nb_vicroot_gold_features` | Aligns with repo `notebooks/03_gold_features.py`. |

**Debug shorthand:** When asking for help, paste **workspace**, **lakehouse**, **pipeline name**, **Run ID**, and **Files path** (or notebook error cell).

### Bronze path example — 2022 soil sensor zip

Lakehouse **Files** can store **`.zip`** binaries as-is; unpack in a notebook (`zipfile`) before or while promoting to parsed JSON/CSV/Delta.

- **Local repo file (2022 archive):** `Soil Sensor Readings - Historical data (2022).zip` (repository root).
- **Suggested lakehouse path (slugged filename):**  
  `Files/medallion/bronze/com_soil_sensor_readings/source=historical/year=2022/raw/soil_sensor_readings_historical_2022.zip`
- **After extract / parsed outputs (example):**  
  `Files/medallion/bronze/com_soil_sensor_readings/source=historical/year=2022/jsonl/` (or `parsed/`)

**API years (2023+):** same folder family with `year=2023` … or partitions driven by pipeline parameters (`snapshot_date`).

## Pipeline parameters (recommended)

Use these as **pipeline parameters** in Fabric Data Factory; pass the same keys into notebook activities as a `params` JSON object or individual arguments.

| Parameter | Type | Example | Purpose |
|-----------|------|---------|---------|
| `environment` | string | `dev` / `prod` | Naming and path segregation |
| `lakehouse_name` | string | `lh_vicroot_melbourne` | Logical name for logs (match portal lakehouse) |
| `medallion_root` | string | `Files/medallion` | Root under lakehouse Files for raw paths |
| `api_base_url_com` | string | City of Melbourne Open Data base URL | Trees API |
| `api_base_url_soil` | string | Agriculture Victoria Soils API base | Soil polygons/properties |
| `api_base_url_bom` | string | BOM Data.vic base | Rainfall / ET |
| `bom_station_id` | string | Station identifier | Weather slice |
| `source_crs` | string | `EPSG:4326` | Incoming tree coords |
| `target_crs` | string | `EPSG:7855` | GDA2020 / MGA zone 55 local math |
| `snapshot_date` | string | `2026-04-24` | Partition key for idempotent runs |
| `pipeline_run_id` | string | `@pipeline().RunId` | Correlation id |
| `bronze_ingest_mode` | string | `zip_and_api` | Run `zip_only`, `api_only`, or both for soil sensor bronze |
| `readings_years_csv` | string | `2023,2024,2025` | API years to ingest |
| `soil_zip_input_path` | string | `Files/Soil Sensor Readings - Historical data (2022).zip` | Uploaded 2022 archive path in Lakehouse Files |
| `site_file_input_path` | string | `Files/raw/site/soil-sensor-locations.csv` | Optional uploaded site reference file |
| `weather_file_input_path` | string | `Files/raw/weather/weather-daily.csv` | Optional uploaded weather file |
| `weather_api_enabled` | bool | `false` | Enable weather API ingestion branch in Bronze |
| `weather_api_provider` | string | `openweather` | Current supported provider |
| `openweather_base_url` | string | `https://api.openweathermap.org` | OpenWeather base URL |
| `weather_latitude` | string | `-37.8136` | Weather query latitude (optional; inferred from site file if blank) |
| `weather_longitude` | string | `144.9631` | Weather query longitude (optional; inferred from site file if blank) |
| `weather_location_name` | string | `melbourne` | Label stored in weather rows |
| `bronze_strict_input_guard` | bool | `true` | Fail fast if input paths are broad/unsafe or files missing |

**Variables** (not parameters): build dynamic paths with `utcnow()`, activity outputs, and `Set variable`—e.g. `bronze_path_today = concat(medallion_root, '/bronze/com_trees/dt=', snapshot_date)`.

## Control table: idempotency

Delta table `metadata.ingestion_watermarks` (or equivalent) suggested schema:

- `dataset_name` (string): e.g. `com_trees`, `av_soil`, `bom_weather`
- `last_success_run_id` (string)
- `last_success_utc` (timestamp)
- `high_watermark` (string, optional): cursor for incremental APIs
- `snapshot_date` (string): last fully materialized partition

Notebooks or pipeline **Lookup** can read this before ingest; **append** or **merge** after success.

## API inventory (blueprint)

| Source | Use | Key fields |
|--------|-----|------------|
| City of Melbourne Open Data | Tree inventory | Lat/Lon, species, ULE, health |
| Agriculture Victoria Soils | Soil stress | Top/sub soil pH, salinity, geometry |
| BOM Data.vic | Moisture story | Rainfall, evapotranspiration |

Replace base URLs with pipeline parameters; do not hard-code secrets—use Fabric credentials / Key Vault integration when needed.

## Gold layer contract (ML-ready)

**Grain:** one row per tree per `as_of_date` (or per `snapshot_date` if batch-only).

**Primary key:** (`tree_id`, `as_of_date`) — adjust `tree_id` to match CoM stable identifier.

**Core columns (minimum for Databricks handoff):**

| Column | Type | Notes |
|--------|------|--------|
| `tree_id` | string | Stable CoM id |
| `as_of_date` | date | Feature snapshot |
| `geom_tree_7855` | binary or WKB | Optional; or centroid x/y |
| `species` | string | |
| `soil_ph` | double | From spatial join |
| `soil_salinity_proxy` | double | Domain-specific column name |
| `rainfall_mm_period` | double | From BOM window |
| `et_mm_period` | double | |
| `moisture_deficit_index` | double | Derived |
| `resilience_score_rule` | double | Optional deterministic score for demos |
| `risk_tier` | string | Optional `low`/`med`/`high` for Power BI |

**Future Databricks output table (separate):** e.g. `gold_tree_health_predictions` with `tree_id`, `as_of_date`, `health_decline_prob`, `model_version`, `scored_at`.

## Risk definition references (for Gold rules)

Use these references to keep the Gold risk logic explainable and auditable.

| Domain signal | Current project columns | Recommended reference | Why it supports the rule |
|---------------|-------------------------|-----------------------|--------------------------|
| Salinity hazard | `salinity_surface_7d_avg`, `salinity_surface_30d_avg`, `salinity_trend_30d` | FAO irrigation water quality guidance (Ayers & Westcot, FAO paper 29) | Widely used salinity hazard classes and crop stress interpretation baseline for irrigation contexts. |
| Soil EC interpretation | Sensor/unit-normalized salinity in Silver/Gold | USDA NRCS soil EC interpretation guides | Practical classes (non-saline to strongly saline) useful for analyst communication and threshold calibration. |
| Weather demand pressure | `evap_mm`, `rainfall_mm`, `evap_moisture_pressure` | FAO-56 crop evapotranspiration framework | Establishes ET0 and water balance framing (`ETc`, rainfall/effective rainfall) for moisture-stress pressure logic. |
| Local climate context (AU) | `evap_mm`, seasonal weather behavior | Bureau of Meteorology evapotranspiration references | Adds Australian climatology context for Melbourne seasonality and expected ET ranges. |

### First-pass threshold policy (calibration baseline)

These are **starting thresholds** for rule-based tiers and should be calibrated with observed outcomes.

| Signal | Low concern | Medium concern | High concern | Notes |
|--------|-------------|----------------|--------------|-------|
| Surface salinity level (`salinity_surface_7d_avg`) | `< 2 dS/m equivalent` | `2 to < 4 dS/m` | `>= 4 dS/m` | Aligns to common agronomic salinity class breakpoints. Validate sensor unit conversion first. |
| Salinity trend (`salinity_trend_30d`) | `<= 0` | `> 0 and < 0.2 dS/m per 30d` | `>= 0.2 dS/m per 30d` | Captures increasing salt pressure, even when absolute level is moderate. |
| Evap-moisture pressure (`evap_moisture_pressure`) | `< 0.8` | `0.8 to < 1.2` | `>= 1.2` | Ratio-style index: higher means atmospheric demand exceeds moisture support. |
| Rainfall support (`rainfall_mm_7d` optional feature) | `>= 20 mm` | `5 to < 20 mm` | `< 5 mm` | Optional weather modifier: low rain increases drought/salt concentration risk. |

### Unit normalization requirement (must do before final thresholds)

Because salinity thresholds depend on units, standardize to one project unit before locking policy:

- If sensor is `uS/cm`: convert to `dS/m` with `dS/m = uS/cm / 1000`.
- If sensor is `mS/cm`: convert to `dS/m` with `dS/m = mS/cm`.
- Persist both raw and normalized values in Silver if needed for audit.
- Apply Gold risk thresholds only to the normalized salinity field.

### Suggested references (link list)

- FAO irrigation water quality guidance: [Ayers & Westcot](https://www.fao.org/4/x5870e/x5870e07.htm)
- FAO-56 evapotranspiration standard: [Crop Evapotranspiration](https://www.fao.org/4/X0490E/X0490E00.htm)
- Australian evapotranspiration context: [Bureau of Meteorology ET](http://www.bom.gov.au/watl/eto/index.shtml)
- Practical soil EC interpretation: [USDA NRCS soil EC indicator](https://nrcs.usda.gov/sites/default/files/2022-10/Soil%20Electrical%20Conductivity.pdf)
- Crop salinity response overview: [UC Davis summary paper](https://vric.ucdavis.edu/pdf/irrigation/IrrigationWaterSalinityandCropProduction.pdf)

## Power BI

Use **DirectLake** against Gold Delta tables (and later prediction table). Avoid Import mode for large fact tables so refreshes stay aligned with pipeline runs.

## Databricks handoff checklist

- [ ] OneLake **shortcut** (or supported path) from Databricks to Fabric lakehouse Delta roots.
- [ ] Unity Catalog **external location** aligned with the same physical storage (if using UC).
- [ ] Frozen **Gold column list** and PK documented in this file.
- [ ] Batch scoring cadence and **single write location** for predictions agreed.
- [ ] `model_version` and `scored_at` on all prediction writes.

## Repo layout (reference)

- `notebooks/` — PySpark scripts / notebook exports; use `notebooks/lib/pipeline_params.py` for params.
- `fabric/` — pipeline notes and parameter templates for the Fabric portal.
- `config/` — optional JSON for validation rules and thresholds.
- `sql/` — DDL snippets for metadata tables.
