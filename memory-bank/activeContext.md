# Active context — VicRoot Insights

# TL;DR

**Phase in focus:** Phase 1 (Bronze) scaffolding complete in-repo; implement ingestion and watermarks in Fabric next. **If you read one file:** [docs/architecture.md](docs/architecture.md) (includes **Fabric resource naming** and Bronze paths for the 2022 soil zip).

## Current phase

- **1 — Bronze:** Parameter catalog and control-table design documented; notebook stub + `fabric/` templates added.
- **2 — Silver:** Notebook stub + validation config example; Sedona/join logic to be filled in Fabric.
- **3 — Gold:** Schema contract documented; feature notebook stub only.

## Recent decisions

- 2026-04-24: Adopt **metadata-driven** Fabric pipelines (parameters + variables + notebook `params` dict); **Databricks later** for ML/scoring; Gold remains the feature contract.
- 2026-04-24: **EPSG:7855** as default `target_crs` for Victorian local spatial math (MGA zone 55 context per blueprint).
- 2026-04-24: **Scaffold applied:** `.cursor/rules`, `.cursor/skills`, `notebooks/`, `fabric/`, `config/`, `sql/`, `src/`, `.gitignore`, `requirements.txt`, [.env.example](../.env.example) (and `.env_example` symlink). Secrets only in local `.env` (gitignored).
- 2026-04-28: Hardened `notebooks/02_silver_spatial_join.py` for mixed-source ingestion: explicit bool parsing for Fabric params, deterministic dedupe across API+file overlaps, and analyst-facing `silver_soil_sensor_quarantine` with retained DQ reason columns (`dq_*` + `dq_reasons`).
- 2026-04-28: Updated Silver fallback validation defaults to align with native-unit checks: `salinity_ms_cm`, `moisture_vwc`, and `temp_c` (removed `soil_ph` fallback key).
- 2026-04-28: Added Gold Delta schema-evolution write option in `notebooks/03_gold_features.py` (`mode("overwrite")` + `option("overwriteSchema","true")`) for `gold_soil_sensor_features` and `gold_soil_sensor_depth_daily`.
- 2026-04-28: Added DQ test blocks across medallion notebooks: Bronze ingest completeness checks (`bronze_dq_min_rows_per_year`), Silver health checks (duplicate keys + quarantine ratio threshold), and Gold output contract checks (site/day uniqueness, key nulls, risk tier domain), controlled by `dq_fail_on_error`.
- 2026-04-28: Added optional non-API file ingestion path for `site_reference` and `weather_daily` in Bronze, plus Silver materialization to `silver_site_file_reference` and `silver_weather_daily`; Gold default weather join now targets `silver_weather_daily`.
- 2026-04-28: Added Bronze input preflight guard (`bronze_strict_input_guard`) to log resolved paths and fail fast on broad/unsafe shortcut inputs or missing files; updated `docs/architecture.md` with staged raw-domain migration guidance and new file-input params.
- 2026-04-28: Extended Bronze/Silver weather flow with optional OpenWeather API ingestion (`weather_api_enabled`, `openweather_api_key`, coord params), writing Bronze `weather_daily/source=api/...` and auto-loading to `silver_weather_daily`; Bronze guard now explicitly recognizes `Files/medallion/bronze/com_soil_sensor_readings` as a known-safe transition subpath.
- 2026-04-28: Implemented domain-split scale pattern with separate Bronze notebooks (`01_bronze_ingest_soil.py`, `01_bronze_ingest_site.py`, `01_bronze_ingest_weather.py`) and Silver notebooks (`02_silver_clean_soil.py`, `02_silver_clean_site.py`, `02_silver_clean_weather.py`).
- 2026-04-28: Hardened split soil Bronze ingest zip handling with legacy-path and discovery fallback when `soil_zip_input_path` is missing (supports staged migration where 2022 zip still resides under medallion Bronze paths).
- 2026-04-28: Added split soil Bronze CSV fallback for yearly files (2023/2024/2025) to legacy medallion bronze paths when `Files/raw/soil/...` is unavailable, enabling staged raw-path migration without breaking runs.
- 2026-04-28: Added transition auto-narrowing in legacy combined `01_bronze_ingest.py`: if `soil_csv_input_dir` is set to broad `Files/medallion/bronze`, notebook now rewrites it to `Files/medallion/bronze/com_soil_sensor_readings` before strict guard checks.
- 2026-04-28: Extended legacy combined Bronze migration compatibility: when auto-narrowed to `com_soil_sensor_readings`, notebook now auto-adjusts `soil_csv_file_pattern` to `year={year}/raw/file/...`, guard accepts fallback candidates under year/source legacy layouts, and file-branch writes use basename-safe output paths.
- 2026-04-28: Hardened split site Silver ingestion for mixed legacy/split Bronze paths: `02_silver_clean_site.py` now discovers available `snapshot_date=*` folders under both `bronze_site_reference` and legacy roots (`site_reference`, `com_soil_sensor_locations`) and can fallback to direct raw site file read when Bronze site file source is absent.
- 2026-04-28: Added no-param site source fallback in `02_silver_clean_site.py` to probe common raw paths (`Files/raw/site/...` and `File/raw/site/...`) when `site_file_input_path` is not provided, reducing snapshot/path mismatch failures during transition.
- 2026-04-28: Hardened split weather Silver ingestion for transition mode: `02_silver_clean_weather.py` now supports split+legacy Bronze roots, snapshot auto-discovery, raw weather file fallback when params are absent, delimiter auto-detect for CSV, corrupt-JSON skip, and column-name sanitization before Delta write.
- 2026-04-28: Hardened split Bronze weather notebook to prevent silent no-op runs: now logs weather input mode, fails fast when both file and API sources are unset, and errors if no weather rows were written before watermark update.
- 2026-04-28: Added weather API convenience defaults in split Bronze weather flow: auto-enable API when `openweather_api_key` is present and file source is unset, default Melbourne coordinates in params/fallback, and warning log when coordinate fallback is used.
- 2026-04-28: Fixed Fabric notebook activity parameter wiring gap in `01_bronze_ingest_weather.py` by merging runtime-injected base parameters from notebook globals (plus alias `p_openweather_api_key`), so pipeline Notebook activity values now override defaults as intended.
- 2026-04-29: Added direct Azure Key Vault secret retrieval path in `01_bronze_ingest_weather.py` using service principal credentials (`azure_tenant_id`, `azure_client_id`, `azure_client_secret`) plus vault/secret params, with automatic API enable when secret is loaded; updated `.env.example`/`pipeline_params.py` placeholders and removed accidentally embedded real key from `.env.example`.
- 2026-04-29: Updated weather secret retrieval priority to prefer Fabric notebook utils (`mssparkutils/notebookutils` `credentials.getSecret`) via `key_vault_linked_service`/vault name/url, with service-principal REST as fallback; added `key_vault_linked_service` to params and `.env.example`.
- 2026-04-29: Added direct Fabric connection-id secret retrieval support in `01_bronze_ingest_weather.py` (`notebookutils.credentials.getSecretWithConnection`) via new `key_vault_connection_id` param/env mapping, so notebook can use a known-working connection id without linked service name ambiguity.
- 2026-04-29: Hardened `02_silver_clean_weather.py` to merge runtime notebook activity parameters from globals and notebook utils argument APIs (including `p_` aliases), preventing parameter-loss issues between Fabric pipeline activity and notebook execution.
- 2026-04-29: Added explicit observability/fail-fast to `02_silver_clean_weather.py` (resolved param log, source/normalized row counts, and optional empty-write guard via `silver_weather_allow_empty_write`) to diagnose cases where no weather table appears.
- 2026-04-29: Switched weather Bronze API strategy to support full soil-aligned history (2022–2025): `01_bronze_ingest_weather.py` now supports `weather_api_provider=open_meteo_archive` with date-range pull derived from `weather_years_csv` (or `readings_years_csv`) and writes historical daily weather to Bronze API path for downstream Silver use.
- 2026-04-29: Enhanced Open-Meteo historical mapping in weather Bronze: populate `evap_mm` from daily `et0_fao_evapotranspiration` and derive `humidity_avg_pct` by aggregating hourly `relative_humidity_2m` to daily mean.
- 2026-04-29: Synced legacy combined notebook `01_bronze_ingest.py` weather API branch with split-weather behavior: added `weather_api_provider`/`weather_years_csv`, Open-Meteo archive historical pull for 2022–2025, evap/humidity enrichment, and provider-aware fallback to OpenWeather forecast only when explicitly selected.
- 2026-04-29: Expanded Gold notebook outputs in `03_gold_features.py` to business-aligned tables: `gold_irrigation_risk_daily`, `gold_plant_suitability_zone`, and `gold_ops_alert`, including salinity 7d/30d trends with weather-linked risk and suitability logic, while keeping existing `gold_soil_sensor_features` and `gold_soil_sensor_depth_daily` for backward compatibility.
- 2026-04-29: Fixed weather Key Vault runtime regression by expanding Fabric parameter aliases (`p_key_vault_connection_id`, `p_openweather_secret_name`), adding deterministic default connection-id fallback, and replacing silent secret retrieval failures with explicit INFO/WARN attempt-path logging.
- 2026-04-29: Added source-backed Gold risk policy documentation in `docs/architecture.md` (FAO/NRCS/BOM references, first-pass threshold table, and salinity unit-normalization requirement) to make risk tiers auditable and easier to calibrate.
- 2026-04-29: Added `notebooks/04_export_site_risk_geojson.py` to export ready-to-use `site_risk.geojson` from `gold_irrigation_risk_daily` (latest row per site with lat/lon) to Lakehouse Files for map visuals that require GeoJSON.
- 2026-04-30: Split `gold_fact_sensor_feature` into **`gold_fact_salinity_depth_daily`**, **`gold_fact_moisture_depth_daily`**, **`gold_fact_temperature_depth_daily`** (each: site×date×depth, day avg + 30d only, per-measure reading_count). Rebuilt **`gold_fact_irrigation_risk`** shallow moisture/salinity from moisture+salinity facts; shallow temp from temperature fact. Removed write of combined sensor feature table.
- 2026-05-01: Reworked Gold risk definition: `gold_fact_irrigation_risk` now uses explicit `salinity_risk_tier`/`salinity_risk_reason` bands (`non_saline`, `slightly_saline`, `moderately_saline`, `highly_saline`), and moisture/temperature remain separate context fields (`moisture_status`, actual temp), removing the mixed generic `risk_tier`.
- 2026-05-01: Added `docs/dic.md` as the Gold-layer data dictionary with table grains, column meanings, units, and the exact salinity/moisture risk/status settings used by `gold_fact_irrigation_risk`; linked it from `README.md` and `docs/architecture.md`.
- 2026-05-01: Corrected Gold salinity scale after inspecting actual Silver distribution (`unit=µS/cm`, raw max about `2.16`): preserve raw salinity numeric value as **sensor EC proxy** (`salinity_ec_*`, `salinity_shallow_ec_avg`) instead of dividing by 1000, and updated docs to state this is not lab `ECe`.
- 2026-05-01: Added physical `salinity_observed_band` to `gold_fact_irrigation_risk` for Power BI/Fabric maps where DirectLake calculated columns are not allowed; bands are `low_observed` (<0.3), `medium_observed` (0.3-0.8), and `high_observed` (>=0.8).
- 2026-05-04: Fixed 2022 soil history promotion: Bronze soil zip ingestion now merges extracted 2022 JSON shards into `year=2022/raw/records_file.jsonl`, with CSV as a legacy fallback; default soil years include `2022` so Silver/Gold can carry the full 2022-2025 range.
- 2026-05-04: Removed stray `com_app_token` references from `_run_bronze_input_guard` in legacy `01_bronze_ingest.py` (they were out of scope and could crash the guard); `zip_and_files` still requires on-disk CSVs for non-2022 years in `readings_years_csv` or a mode that includes API.
- 2026-05-04: Simplified **no-API** soil Bronze: guard + zip branch treat **`files_only` + valid `soil_zip_input_path`** like zip+files for 2022; fixed guard year compare (`int` 2022 vs `"2022"` bug); added `local` / `local_files` / `files_and_zip` aliases → `zip_and_files` in `resolve_params`.
- 2026-05-04: Silver soil: coalesce **2022 JSON timestamp fields** (`date_time`, `date`+`time`, etc.) into `local_time_raw` (and dedupe key); **`_source_year` as int** for SQL; split-soil notebook **fallback-reads** `com_soil_sensor_readings` when default `bronze_soil_root` is the split layout.

- 2026-05-04: Added Gold **weather-bin + peer z-score** tables: `gold_fact_soil_depth_peer_daily` (site×date×depth with moisture-qualified EC and peer stats) and `gold_site_depth_peer_summary`; params `gold_ec_moisture_*`, `gold_peer_min_sites`; documented in `docs/dic.md` and `docs/architecture.md`.
- 2026-05-04: **Analytical framing + Gold profile columns:** Documented site-centric moisture/temperature focus and **salinity secondary** in `PROMPT.md`, `README.md`, `docs/architecture.md` (*Analytical framing*), `docs/dic.md`. `gold_fact_irrigation_risk` now includes `moisture_deep_vwc_avg`, `temp_deep_c_avg`, `moisture_shallow_minus_deep_vwc`, `temp_shallow_minus_deep_c`; GeoJSON export updated; `.cursor/skills/vic-root-gold-features/SKILL.md` aligned.
- 2026-05-06: Added [docs/power_bi_dashboard_report.md](docs/power_bi_dashboard_report.md): narrative for **Pages 1–3** (embedded `docs/images/page_1.png`–`page_3.png`), salinity rationale, BI limits vs **ML next steps** on Gold/OneLake.

## Open risks / follow-ups

- Confirm exact **CoM API** endpoints and pagination in Fabric (replace placeholders in bronze stub).
- Confirm **Agriculture Victoria Soils** response schema for polygon vs property fields.
- Register **secrets** only in Fabric / Azure Key Vault—not in git.

## Next actions (human)

1. Create lakehouse and tables in Fabric; run `sql/metadata_ddl.sql` (adapt to workspace).
2. Import `fabric/pipeline_bronze.parameters.template.json` concepts into a real pipeline; wire notebook activities.
3. Fill `notebooks/01_bronze_ingest.py` with real HTTP + write logic using pipeline parameters.
