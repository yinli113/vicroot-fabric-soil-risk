# VicRoot Insights — architecture

# TL;DR

Medallion pipeline on Microsoft Fabric (Bronze → Silver → Gold) with **parameter-driven** pipelines and notebooks; Gold is the **ML feature contract** for later Databricks training and batch scoring via OneLake shortcuts. This file is the technical source of truth—update it when parameters or schemas change.

**Scaffold:** Project layout is in-repo (`.cursor/`, `notebooks/`, `fabric/`, `config/`, `sql/`). A historical copy-paste mirror remains in [plan-execution-artifacts.md](plan-execution-artifacts.md). Local secrets: copy [.env.example](../.env.example) to `.env`.

## Workstreams

1. Bronze: API ingestion, raw landing, watermarks.
2. Silver: CRS transform (EPSG:4326 → EPSG:7855), spatial join, validation.
3. Gold: star schema (dims + facts) + rule-based risk tier; Power BI DirectLake.
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
- **After extract / parsed outputs:**  
  `Files/medallion/bronze/bronze_soil_readings/year=2022/raw/extracted/soil_data_2022_*.json` (shards) promoted to `.../raw/records_file.jsonl`

**API/file years (2022+):** same folder family with `year=YYYY`; 2022 is normally sourced from the uploaded zip, while 2023+ can come from uploaded CSVs or API.

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
| `readings_years_csv` | string | `2022,2023,2024,2025` | Soil reading years to load into Silver/Gold; Bronze promotes 2022 from the uploaded zip and uses file/API branches for the other years |
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

## Gold layer — star schema (current)

Gold is modeled for **Power BI DirectLake** and minimal duplication: dimensions hold slow-changing or shared attributes; facts hold measurable grains.

Column-level meanings and units are maintained in [`docs/dic.md`](dic.md).

### Analytical framing (VicRoot soil sensor network)

**Primary questions (moisture + temperature, site grain):** When CoM monitoring locations are **close enough** to treat **daily regional weather as shared**, compare **sites** and **within-site depth** on:

- **Buffering / damping** — how subsurface moisture and temperature track or lag **weather extremes** (cold, heat, wet, dry) over time; depth facts plus rolling windows support this in BI without a single baked-in “stability score.”
- **Shallow–deep profile** — e.g. warmer shallow than deep under heat, or **deeper moisture** holding up in dry intervals (storage). Unusual or unstable gradients are **monitoring signals** (volume, compaction, drainage, irrigation, microclimate)—not automated **tree suitability**.

**Salinity as secondary:** `salinity_ec_*` stays in Gold as a **sensor EC proxy** (not lab `ECe`). It is **de-emphasised** in the product narrative because: (1) **semantics** — the numeric scale is instrument-specific; agronomic bands need documentation/calibration; (2) **moisture coupling** — very dry soil skews EC; peer analytics use **`ec_reading_reliable`** (moisture floors) so like-for-like comparisons are explicit; (3) **scope** — VicRoot does **not** claim species–soil suitability from sensors alone; salinity remains **separate tiers** and **peer z-scores** for optional screening.

**Mapping to Gold tables:**

| Intent | Tables |
|--------|--------|
| Site × date × depth moisture/temperature | `gold_fact_moisture_depth_daily`, `gold_fact_temperature_depth_daily` |
| Regional weather context | `gold_dim_weather_daily` |
| Site × date shallow + deep profile + salinity context | `gold_fact_irrigation_risk` (shallow 10→20→30 cm; deep 80→70→60→50 cm; **deltas** in `docs/dic.md`) |
| Salinity vs peers under coarse weather bins | `gold_fact_soil_depth_peer_daily`, `gold_site_depth_peer_summary` |

**Dashboard discipline:** Prefer explicit metrics for “stability” (e.g. **rolling variance**, **seasonal range**, **soil vs air temperature amplitude**) instead of an unlabeled “stable/unstable” flag.

### Dimensions

| Table | Grain | Role |
|-------|--------|------|
| `gold_dim_site` | `site_id` | Site name, `latitude`, `longitude` (geo analytics). |
| `gold_dim_depth` | `depth_cm` | Reference depths **10–80 cm** (10 cm steps). Sensor readings snap to nearest bucket. |
| `gold_dim_weather_daily` | `as_of_date` | `rainfall_mm`, `evap_mm`, `humidity_avg_pct`, `temp_avg_c`, `temp_min_c`, `temp_max_c`, `temp_range_c` (max−min), `wind_speed_avg_ms`. |

### Facts

| Table | Grain | Role |
|-------|--------|------|
| `gold_fact_salinity_depth_daily` | (`site_id`, `as_of_date`, `depth_cm`) | `reading_count` (salinity readings that day/depth), `salinity_ec_avg`, `salinity_ec_30d_avg`. Salinity preserves the source numeric scale as a **sensor EC proxy**. Day avg + **30d only** (no min/max). |
| `gold_fact_moisture_depth_daily` | (`site_id`, `as_of_date`, `depth_cm`) | `reading_count`, `moisture_vwc_avg`, `moisture_vwc_30d_avg`. |
| `gold_fact_temperature_depth_daily` | (`site_id`, `as_of_date`, `depth_cm`) | `reading_count`, `temp_c_avg`, `temp_c_30d_avg`. |
| `gold_fact_irrigation_risk` | (`site_id`, `as_of_date`) | **Primary context:** shallow + deep **moisture/temperature** (`moisture_shallow_*`, `moisture_deep_*`, `temp_*`, **`moisture_shallow_minus_deep_vwc`**, **`temp_shallow_minus_deep_c`**) plus `moisture_status`, `evap_moisture_pressure`. **Secondary:** shallow **salinity** tiers (`salinity_risk_tier`, etc.) kept separate from moisture/temperature. Join weather on `as_of_date` → `gold_dim_weather_daily`. |
| `gold_fact_soil_depth_peer_daily` | (`site_id`, `as_of_date`, `depth_cm`) | Joins salinity/moisture/temperature depth facts with **weather bins** (`rain_band`, `temp_band`, `weather_bin_key`), **`ec_reading_reliable`** (moisture floor by depth), and **peer z-score** for qualified salinity vs other sites same day/depth/bin. Params: `gold_ec_moisture_*`, `gold_peer_min_sites` (see `docs/dic.md`). |
| `gold_site_depth_peer_summary` | (`site_id`, `depth_cm`) | Aggregates peer z behaviour per site×depth: reliability rate, mean z, mean \|z\|, tail counts. For ranking “who diverges under similar weather.” |
| `gold_ml_moisture_baseline` | (`site_id`, `as_of_date`, `depth_cm`) | Optional **ML skeleton** output from `notebooks/05_ml_moisture_baseline.py`: lagged weather features, **`pred_moisture_vwc_avg`**, **`moisture_residual`**, **`is_train_row`**, **`model_id`**, **`snapshot_date`**. Default **single depth** via `ml_target_depth_cm`. **Store in the lakehouse** unless you add an **ADLS Gen2** shortcut for external analytics. |

The previous single table `gold_fact_sensor_feature` is **removed** — drop it in the lakehouse if it still exists after repointing Power BI.

**Legacy / tree-level ML contract (future blueprint)**  
Original tree-centric Gold sketch (tree id, species, spatial join) remains a **future** target when tree inventory is joined; current implementation is **soil sensor site** grain.

## Risk definition references (for Gold rules)

**Note:** Moisture/temperature **profile and status** fields in `gold_fact_irrigation_risk` are the **primary monitoring contract** for the current VicRoot sensor story. The references below **chiefly support salinity tier text** and **evap/rain water-balance context**—salinity remains **secondary** to moisture/temperature site analytics; see *Analytical framing* above.

Use these references to keep the Gold risk logic explainable and auditable.

| Domain signal | Current project columns | Recommended reference | Why it supports the rule |
|---------------|-------------------------|-----------------------|--------------------------|
| Salinity hazard | `gold_fact_irrigation_risk` shallow + trend; per-depth in `gold_fact_salinity_depth_daily` | FAO irrigation water quality guidance (Ayers & Westcot, FAO paper 29) | Widely used salinity hazard classes and crop stress interpretation baseline for irrigation contexts. |
| Soil EC interpretation | Sensor/unit-normalized salinity in Silver/Gold | USDA NRCS soil EC interpretation guides | Practical classes (non-saline to strongly saline) useful for analyst communication and threshold calibration. |
| Weather demand pressure | `gold_dim_weather_daily` (`evap_mm`, `rainfall_mm`); fact `evap_moisture_pressure` | FAO-56 crop evapotranspiration framework | Establishes ET0 and water balance framing (`ETc`, rainfall/effective rainfall) for moisture-stress pressure logic. |
| Local climate context (AU) | `evap_mm`, seasonal weather behavior | Bureau of Meteorology evapotranspiration references | Adds Australian climatology context for Melbourne seasonality and expected ET ranges. |

### First-pass threshold policy (calibration baseline)

These are **starting thresholds** for rule-based tiers and should be calibrated with observed outcomes. **Salinity** bands below are **secondary** to moisture/temperature monitoring; see *Analytical framing*.

| Signal | Low concern | Medium concern | High concern | Notes |
|--------|-------------|----------------|--------------|-------|
| Shallow salinity level (`salinity_shallow_ec_avg`) | `0 to < 2` (`non_saline`) | `2 to < 4` (`slightly_saline`) | `4 to < 8` (`moderately_saline`); `>= 8` (`highly_saline`) | Salinity tiers use the observed sensor EC proxy scale and are not mixed with moisture/temperature. |
| Salinity trend (`salinity_trend_vs_30d`) | `<= 0` | `> 0 and < 0.2 per 30d` | `>= 0.2 per 30d` | Captures increasing salt pressure on the sensor EC proxy scale. |
| Moisture status (`moisture_shallow_vwc_avg`) | `40 to 80% VWC` (`target_zone`) | `< 40% VWC` (`low_moisture`) or `> 80% VWC` (`high_moisture`) | Not used as salinity risk | Moisture stays as context/status rather than part of a combined risk score. |
| Evap-moisture pressure (`evap_moisture_pressure`) | Context only | Context only | Context only | Kept as weather/moisture pressure context, not part of the salinity tier. |

### Risk reasons in `gold_fact_irrigation_risk`

| Output field | Value | Rule |
|--------------|-------|------|
| `salinity_risk_tier` | `unknown` | `salinity_shallow_ec_avg` is null. |
| `salinity_risk_tier` | `non_saline` | `salinity_shallow_ec_avg < 2`. |
| `salinity_risk_tier` | `slightly_saline` | `2 <= salinity_shallow_ec_avg < 4`. |
| `salinity_risk_tier` | `moderately_saline` | `4 <= salinity_shallow_ec_avg < 8`. |
| `salinity_risk_tier` | `highly_saline` | `salinity_shallow_ec_avg >= 8`. |
| `salinity_risk_reason` | `no shallow salinity reading` | No shallow salinity value available. |
| `salinity_risk_reason` | `0-2 sensor EC proxy` | Non-saline band. |
| `salinity_risk_reason` | `2-4 sensor EC proxy` | Slightly saline band. |
| `salinity_risk_reason` | `4-8 sensor EC proxy` | Moderately saline band. |
| `salinity_risk_reason` | `>8 sensor EC proxy` | Highly saline band. |
| `moisture_status` | `unknown` | `moisture_shallow_vwc_avg` is null. |
| `moisture_status` | `low_moisture` | `moisture_shallow_vwc_avg < 40`. |
| `moisture_status` | `target_zone` | `40 <= moisture_shallow_vwc_avg <= 80`. |
| `moisture_status` | `high_moisture` | `moisture_shallow_vwc_avg > 80`. |
| `salinity_observed_band` | `unknown` | `salinity_shallow_ec_avg` is null. |
| `salinity_observed_band` | `low_observed` | `salinity_shallow_ec_avg < 0.3`. |
| `salinity_observed_band` | `medium_observed` | `0.3 <= salinity_shallow_ec_avg < 0.8`. |
| `salinity_observed_band` | `high_observed` | `salinity_shallow_ec_avg >= 0.8`. |

### Salinity scale decision

The source unit label is `uS/cm` / `µS/cm`, but the observed range (`0` to about `2.16`) behaves like a mS/cm or dS/m-scale sensor EC proxy. Dividing by 1000 flattened all sites to effectively zero and removed useful monitoring signal.

Gold therefore preserves the raw salinity numeric value and names it `salinity_ec_*` / `salinity_shallow_ec_avg`. Treat these columns as **sensor EC proxy** values, not lab-measured `ECe`.

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

## CI/CD (source control vs Fabric runtime)

**Principle:** Git is the **versioned source** for notebooks and `pipeline_params.py`; **Microsoft Fabric** is where **Spark** runs and Delta tables are written. Trial or small **F-skus** still hit **capacity** limits when too many notebook sessions start at once — CI in Azure DevOps does **not** use Fabric capacity; it only validates files in the repo.

For **Azure DevOps Pipelines** (PR-safe checks without Livy), see [azure-devops-fabric-ci.md](azure-devops-fabric-ci.md) and `azure-pipelines.yml` at repo root.

## Repo layout (reference)

- `notebooks/` — PySpark scripts / notebook exports; use `notebooks/lib/pipeline_params.py` for params.
- `fabric/` — pipeline notes and parameter templates for the Fabric portal.
- `config/` — optional JSON for validation rules and thresholds.
- `sql/` — DDL snippets for metadata tables.
- `azure-pipelines.yml` — Azure DevOps YAML for offline validation (optional).
