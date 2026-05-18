# Fabric pipeline notes — Bronze

**CI (no Spark):** Azure DevOps can run [`azure-pipelines.yml`](../azure-pipelines.yml) for offline validation only — see [docs/azure-devops-fabric-ci.md](../docs/azure-devops-fabric-ci.md).

**Split soil Bronze:** sync [`notebooks/lib/bronze_soil_ingest.py`](../notebooks/lib/bronze_soil_ingest.py) with `01_bronze_ingest_soil.py` (same `Files/notebooks/lib` path Fabric uses).

1. Create pipeline parameters using `pipeline_bronze.parameters.template.json`.
2. **End-to-end activity order:** see [`pipeline_activity_checklist.md`](pipeline_activity_checklist.md) (Bronze → Silver → Gold → optional ML).
3. Add one Notebook activity that runs `notebooks/01_bronze_ingest.py` (`nb_vicroot_bronze_ingest`) — or use **split** Bronze notebooks per [`pipeline_activity_checklist.md`](pipeline_activity_checklist.md).
4. Pass pipeline parameters directly into notebook `params`:
   - `bronze_ingest_mode`: `zip_only` | `files_only` | `api_only` | `zip_and_files` | `zip_and_api`
   - `soil_zip_input_path`: where your 2022 zip was uploaded in Lakehouse Files
   - `readings_years_csv`: e.g. `2022,2023,2024,2025`
   - `soil_csv_input_dir`: folder containing uploaded yearly CSV files
   - `soil_csv_file_pattern`: filename pattern with `{year}` placeholder
   - `time_window_minutes`: API query window size (default `60`)
   - `resume_from_utc`: optional resume point, e.g. `2023-07-28T08:00:00Z`
5. Keep `api_base_url_com` as `https://data.melbourne.vic.gov.au`; optionally pass `com_app_token` for higher request limits.
6. The notebook writes:
   - 2022 zip copy + extracted JSON shards promoted to `year=2022/raw/records_file.jsonl`
   - 2023+ uploaded CSVs under `.../year=YYYY/raw/file/` with normalized JSONL as `records_file.jsonl`
   - API JSONL by year as `.../year=YYYY/raw/records_api.jsonl`
   - soil sensor locations snapshot under `Files/medallion/bronze/com_soil_sensor_locations/...`
