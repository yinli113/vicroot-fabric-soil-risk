# Fabric pipeline notes — Bronze

1. Create pipeline parameters using `pipeline_bronze.parameters.template.json`.
2. Add one Notebook activity that runs `notebooks/01_bronze_ingest.py` (`nb_vicroot_bronze_ingest`).
3. Pass pipeline parameters directly into notebook `params`:
   - `bronze_ingest_mode`: `zip_only` | `files_only` | `api_only` | `zip_and_files` | `zip_and_api`
   - `soil_zip_input_path`: where your 2022 zip was uploaded in Lakehouse Files
   - `readings_years_csv`: e.g. `2023,2024,2025`
   - `soil_csv_input_dir`: folder containing uploaded yearly CSV files
   - `soil_csv_file_pattern`: filename pattern with `{year}` placeholder
   - `time_window_minutes`: API query window size (default `60`)
   - `resume_from_utc`: optional resume point, e.g. `2023-07-28T08:00:00Z`
4. Keep `api_base_url_com` as `https://data.melbourne.vic.gov.au`; optionally pass `com_app_token` for higher request limits.
5. The notebook writes:
   - 2022 zip copy + extract manifest under `Files/medallion/bronze/com_soil_sensor_readings/year=2022/...`
   - 2023+ uploaded CSVs under `.../year=YYYY/raw/file/` with normalized JSONL as `records_file.jsonl`
   - 2023+ API JSONL by year as `.../year=YYYY/raw/records_api.jsonl`
   - soil sensor locations snapshot under `Files/medallion/bronze/com_soil_sensor_locations/...`
