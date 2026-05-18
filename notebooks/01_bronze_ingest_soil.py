"""Bronze soil ingestion (domain-split).

This notebook ingests only soil sensor data into Bronze paths.
Heavy IO/API helpers live in ``notebooks.lib.bronze_soil_ingest``.
"""

from __future__ import annotations

import json
import os
import sys
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Tuple

NOTEBOOK_VERSION = "v2026-05-09-bronze-soil-split-lib-03"

try:
    from notebooks.lib.pipeline_params import resolve_params
except ModuleNotFoundError:
    for candidate in [
        "/lakehouse/default/Files/notebooks/lib",
        "/lakehouse/default/Files/lib",
        str(Path.cwd() / "notebooks" / "lib"),
        str(Path.cwd() / "lib"),
    ]:
        if os.path.isdir(candidate) and candidate not in sys.path:
            sys.path.append(candidate)
    try:
        from pipeline_params import resolve_params  # type: ignore
    except ModuleNotFoundError:
        def resolve_params(overrides: Dict[str, Any] | None = None) -> Dict[str, Any]:
            params: Dict[str, Any] = {
                "medallion_root": "Files/medallion",
                "bronze_soil_root": "Files/medallion/bronze/bronze_soil_readings",
                "snapshot_date": datetime.now().strftime("%Y-%m-%d"),
                "pipeline_run_id": "",
                "watermark_table": "metadata.ingestion_watermarks",
                "bronze_ingest_mode": "zip_and_files",
                "readings_years_csv": "2022,2023,2024,2025",
                "soil_csv_input_dir": "Files/raw/soil",
                "soil_csv_file_pattern": "soil-sensor-readings-historical-data-{year}.csv",
                "soil_zip_input_path": "Files/raw/soil/Soil Sensor Readings - Historical data (2022).zip",
                "soil_zip_filename_slug": "soil_sensor_readings_historical_2022.zip",
                "api_base_url_com": "https://data.melbourne.vic.gov.au",
                "com_dataset_soil_readings": "soil-sensor-readings-historical-data",
                "com_page_size": 100,
                "time_window_minutes": 60,
                "com_app_token": "",
            }
            if os.environ.get("VICROOT_PARAMS_JSON"):
                params.update(json.loads(os.environ["VICROOT_PARAMS_JSON"]))
            if os.environ.get("VICROOT_COM_APP_TOKEN"):
                params["com_app_token"] = os.environ["VICROOT_COM_APP_TOKEN"]
            if overrides:
                params.update(overrides)
            return params

try:
    from notebooks.lib.bronze_soil_ingest import (
        copy_bronze_file,
        csv_to_jsonl,
        effective_pipeline_run_id,
        iter_year_records,
        lakehouse_to_local,
        parse_years,
        promote_2022_extract_to_records_jsonl,
        soil_zip_input_ready,
        update_watermarks,
        write_json,
        write_jsonl,
    )
except ModuleNotFoundError:
    for candidate in [
        "/lakehouse/default/Files/notebooks/lib",
        "/lakehouse/default/Files/lib",
        str(Path.cwd() / "notebooks" / "lib"),
        str(Path.cwd() / "lib"),
    ]:
        if os.path.isdir(candidate) and candidate not in sys.path:
            sys.path.append(candidate)
    from bronze_soil_ingest import (  # type: ignore
        copy_bronze_file,
        csv_to_jsonl,
        effective_pipeline_run_id,
        iter_year_records,
        lakehouse_to_local,
        parse_years,
        promote_2022_extract_to_records_jsonl,
        soil_zip_input_ready,
        update_watermarks,
        write_json,
        write_jsonl,
    )

# --- parameters -------------------------------------------------------------

params = resolve_params({})
print(f"[INFO] notebook_version={NOTEBOOK_VERSION}")

medallion_root = params.get("medallion_root", "Files/medallion").rstrip("/")
bronze_soil_root = params.get("bronze_soil_root", f"{medallion_root}/bronze/bronze_soil_readings").rstrip("/")
snapshot_date = params.get("snapshot_date", datetime.now().strftime("%Y-%m-%d"))
pipeline_run_id = effective_pipeline_run_id(params, snapshot_date)
watermark_table = params.get("watermark_table", "metadata.ingestion_watermarks")
print(f"[INFO] pipeline_run_id={pipeline_run_id}")

ingest_mode = params.get("bronze_ingest_mode", "zip_and_files")
years = parse_years(params.get("readings_years_csv", "2022,2023,2024,2025"))
soil_csv_input_dir = params.get("soil_csv_input_dir", "Files/raw/soil").rstrip("/")
soil_csv_file_pattern = params.get("soil_csv_file_pattern", "soil-sensor-readings-historical-data-{year}.csv")
zip_input_path = params.get("soil_zip_input_path", "Files/raw/soil/Soil Sensor Readings - Historical data (2022).zip")
zip_slug = params.get("soil_zip_filename_slug", "soil_sensor_readings_historical_2022.zip")

base_url = params.get("api_base_url_com", "https://data.melbourne.vic.gov.au")
dataset = params.get("com_dataset_soil_readings", "soil-sensor-readings-historical-data")
app_token = params.get("com_app_token", "")
page_size = int(params.get("com_page_size", 100))
time_window_minutes = int(params.get("time_window_minutes", 60))

# --- ingest -----------------------------------------------------------------

watermarks: List[Tuple[str, str, str]] = []
zip_2022_records_written = False

if ingest_mode in {"zip_only", "zip_and_files", "zip_and_api"} or (
    ingest_mode == "files_only" and soil_zip_input_ready(zip_input_path)
):
    year_base = f"{bronze_soil_root}/year=2022"
    zip_out = f"{year_base}/raw/zip/{zip_slug}"
    extract_dir = lakehouse_to_local(f"{year_base}/raw/extracted")
    copy_bronze_file(zip_input_path, zip_out)
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(lakehouse_to_local(zip_out), "r") as z:
        z.extractall(extract_dir)
    bronze_jsonl = f"{year_base}/raw/records_file.jsonl"
    rows, shard_sources = promote_2022_extract_to_records_jsonl(extract_dir=extract_dir, records_jsonl=bronze_jsonl)
    zip_2022_records_written = True
    write_json(
        f"{year_base}/metadata/ingest_summary.json",
        {
            "source_type": "zip",
            "source_file": zip_input_path,
            "bronze_file": zip_out,
            "records_file": bronze_jsonl,
            "row_count": rows,
            "shard_files": shard_sources,
            "snapshot_date": snapshot_date,
        },
    )
    print(f"[ZIP] soil year=2022 rows={rows}")
    watermarks.append(("bronze_soil_2022_zip", pipeline_run_id, "2022-12-31"))

if ingest_mode in {"files_only", "zip_and_files"}:
    for year in years:
        if year == 2022 and zip_2022_records_written:
            print("[INFO] skipping separate 2022 CSV file branch; zip branch already wrote records_file.jsonl")
            continue
        filename = soil_csv_file_pattern.format(year=year)
        src = f"{soil_csv_input_dir}/{filename}"
        year_base = f"{bronze_soil_root}/year={year}"
        bronze_csv = f"{year_base}/raw/file/{filename}"
        bronze_jsonl = f"{year_base}/raw/records_file.jsonl"
        copy_bronze_file(src, bronze_csv)
        rows = csv_to_jsonl(bronze_csv, bronze_jsonl)
        write_json(
            f"{year_base}/metadata/ingest_summary.json",
            {"source_type": "file", "source_file": src, "row_count": rows, "snapshot_date": snapshot_date},
        )
        print(f"[FILE] soil year={year} rows={rows}")
        watermarks.append((f"bronze_soil_{year}_file", pipeline_run_id, f"{year}-12-31"))

if ingest_mode in {"api_only", "zip_and_api"}:
    if not app_token:
        raise RuntimeError("com_app_token is required for api_only/zip_and_api runs.")
    for year in years:
        if year == 2022 and zip_2022_records_written:
            print("[INFO] skipping 2022 API branch; zip branch already wrote records_file.jsonl")
            continue
        year_base = f"{bronze_soil_root}/year={year}"
        jsonl = f"{year_base}/raw/records_api.jsonl"
        local = lakehouse_to_local(jsonl)
        parent = os.path.dirname(local)
        if parent:
            os.makedirs(parent, exist_ok=True)
        if os.path.exists(local):
            os.remove(local)
        total = 0
        for batch in iter_year_records(
            base_url=base_url,
            dataset=dataset,
            year=year,
            page_size=page_size,
            app_token=app_token,
            time_window_minutes=time_window_minutes,
        ):
            total += write_jsonl(jsonl, batch)
        write_json(
            f"{year_base}/metadata/ingest_summary.json",
            {"source_type": "api", "row_count": total, "snapshot_date": snapshot_date},
        )
        print(f"[API] soil year={year} rows={total}")
        watermarks.append((f"bronze_soil_{year}_api", pipeline_run_id, f"{year}-12-31"))

if watermarks:
    update_watermarks(watermarks, watermark_table, snapshot_date=snapshot_date)
print("[DONE] Bronze soil ingestion complete.")
