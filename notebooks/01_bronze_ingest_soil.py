"""Bronze soil ingestion (domain-split).

This notebook ingests only soil sensor data into Bronze paths.
"""

from __future__ import annotations

import csv
import json
import os
import re
import shutil
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

NOTEBOOK_VERSION = "v2026-04-28-bronze-soil-split-01"

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
                "readings_years_csv": "2023,2024,2025",
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


def _lakehouse_to_local(path: str) -> str:
    if path.startswith("/lakehouse/"):
        return path
    if path.startswith("Files/"):
        return f"/lakehouse/default/{path}"
    return path


def _parse_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        val = value.strip().lower()
        if val in {"true", "1", "yes", "y", "on"}:
            return True
        if val in {"false", "0", "no", "n", "off", ""}:
            return False
    return bool(value)


def _parse_years(years_csv: str) -> List[int]:
    years = sorted({int(x.strip()) for x in years_csv.split(",") if x.strip()})
    if not years:
        raise ValueError("readings_years_csv is empty")
    return years


def _ensure_parent(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def _discover_zip_candidates(root_dir: str = "/lakehouse/default/Files") -> List[str]:
    candidates: List[str] = []
    if not os.path.isdir(root_dir):
        return candidates
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if not filename.lower().endswith(".zip"):
                continue
            full_path = os.path.join(dirpath, filename)
            lower = filename.lower()
            if "soil" in lower and "sensor" in lower and "2022" in lower:
                candidates.insert(0, full_path)
            else:
                candidates.append(full_path)
    return candidates


def _copy_file(src: str, dst: str) -> None:
    src_local = _lakehouse_to_local(src)
    dst_local = _lakehouse_to_local(dst)
    if not os.path.exists(src_local):
        if src_local.lower().endswith(".zip"):
            legacy_candidates = [
                "/lakehouse/default/Files/medallion/bronze/com_soil_sensor_readings/year=2022/raw/zip/soil_sensor_readings_historical_2022.zip",
                "/lakehouse/default/Files/medallion/bronze/Soil Sensor Readings - Historical data (2022).zip",
            ]
            resolved = None
            for candidate in legacy_candidates:
                if os.path.exists(candidate):
                    resolved = candidate
                    break
            if resolved is None:
                discovered = _discover_zip_candidates()
                if len(discovered) == 1:
                    resolved = discovered[0]
                elif len(discovered) > 1:
                    preferred = [
                        p
                        for p in discovered
                        if "soil" in os.path.basename(p).lower() and "2022" in os.path.basename(p).lower()
                    ]
                    if len(preferred) == 1:
                        resolved = preferred[0]
            if resolved is not None:
                print(f"[INFO] zip input not found at requested path; using fallback: {resolved}")
                src_local = resolved
            else:
                raise FileNotFoundError(f"Input not found: {src_local}")
        elif src_local.lower().endswith(".csv"):
            filename = os.path.basename(src_local)
            year_match = re.search(r"(20\d{2})", filename)
            legacy_candidates: List[str] = []
            if year_match:
                year = year_match.group(1)
                legacy_candidates.extend(
                    [
                        f"/lakehouse/default/Files/medallion/bronze/com_soil_sensor_readings/year={year}/raw/file/{filename}",
                        f"/lakehouse/default/Files/medallion/bronze/com_soil_sensor_readings/source=file/year={year}/raw/{filename}",
                    ]
                )
            resolved = None
            for candidate in legacy_candidates:
                if os.path.exists(candidate):
                    resolved = candidate
                    break
            if resolved is not None:
                print(f"[INFO] csv input not found at requested path; using fallback: {resolved}")
                src_local = resolved
            else:
                raise FileNotFoundError(f"Input not found: {src_local}")
        else:
            raise FileNotFoundError(f"Input not found: {src_local}")
    _ensure_parent(dst_local)
    if os.path.abspath(src_local) != os.path.abspath(dst_local):
        shutil.copy2(src_local, dst_local)


def _csv_to_jsonl(csv_path: str, jsonl_path: str) -> int:
    src = _lakehouse_to_local(csv_path)
    dst = _lakehouse_to_local(jsonl_path)
    _ensure_parent(dst)
    written = 0
    with open(src, "r", encoding="utf-8-sig", newline="") as in_f, open(dst, "w", encoding="utf-8") as out_f:
        reader = csv.DictReader(in_f)
        for row in reader:
            out_f.write(json.dumps(row, ensure_ascii=True))
            out_f.write("\n")
            written += 1
    return written


def _write_json(path: str, payload: Dict[str, Any]) -> None:
    local = _lakehouse_to_local(path)
    _ensure_parent(local)
    with open(local, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2)


def _build_records_url(base_url: str, dataset: str, params: Dict[str, Any]) -> str:
    return f"{base_url.rstrip('/')}/api/explore/v2.1/catalog/datasets/{dataset}/records?{urllib.parse.urlencode(params)}"


def _http_get_json(url: str, app_token: str) -> Dict[str, Any]:
    req = urllib.request.Request(url=url)
    if app_token:
        req.add_header("X-App-Token", app_token)
    try:
        with urllib.request.urlopen(req, timeout=60, context=ssl.create_default_context()) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        body = ""
        try:
            body = err.read().decode("utf-8")
        except Exception:
            body = ""
        raise RuntimeError(f"HTTP {err.code} for URL {url}; body={body[:500]}")


def _iter_year_records(
    *,
    base_url: str,
    dataset: str,
    year: int,
    page_size: int,
    app_token: str,
    time_window_minutes: int,
) -> Iterable[List[Dict[str, Any]]]:
    start = datetime(year, 1, 1)
    end = datetime(year + 1, 1, 1)
    current = start
    while current < end:
        nxt = current + timedelta(minutes=max(5, time_window_minutes))
        where = (
            f"local_time >= date'{current.strftime('%Y-%m-%d %H:%M:%S')}' and "
            f"local_time < date'{nxt.strftime('%Y-%m-%d %H:%M:%S')}'"
        )
        offset = 0
        while True:
            url = _build_records_url(
                base_url,
                dataset,
                {"where": where, "limit": max(20, page_size), "offset": offset, "order_by": "local_time asc"},
            )
            payload = _http_get_json(url, app_token)
            rows = payload.get("results", [])
            if not rows:
                break
            yield rows
            offset += len(rows)
            if len(rows) < max(20, page_size):
                break
        current = nxt


def _write_jsonl(path: str, rows: List[Dict[str, Any]]) -> int:
    local = _lakehouse_to_local(path)
    _ensure_parent(local)
    with open(local, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=True))
            f.write("\n")
    return len(rows)


def _update_watermarks(rows: List[Tuple[str, str, str]], table_name: str) -> None:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    schema = "dataset_name string, last_success_run_id string, last_success_utc timestamp, high_watermark string, snapshot_date string"
    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    data = [(n, r, now_utc, h, snapshot_date) for n, r, h in rows]
    df = spark.createDataFrame(data, schema=schema)
    if "." in table_name:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {table_name.rsplit('.', 1)[0]}")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table_name} (dataset_name STRING,last_success_run_id STRING,last_success_utc TIMESTAMP,high_watermark STRING,snapshot_date STRING) USING DELTA"
    )
    df.write.mode("append").format("delta").saveAsTable(table_name)


params = resolve_params({})
print(f"[INFO] notebook_version={NOTEBOOK_VERSION}")

medallion_root = params.get("medallion_root", "Files/medallion").rstrip("/")
bronze_soil_root = params.get("bronze_soil_root", f"{medallion_root}/bronze/bronze_soil_readings").rstrip("/")
snapshot_date = params.get("snapshot_date", datetime.now().strftime("%Y-%m-%d"))
pipeline_run_id = params.get("pipeline_run_id", f"manual-{snapshot_date}")
watermark_table = params.get("watermark_table", "metadata.ingestion_watermarks")

ingest_mode = params.get("bronze_ingest_mode", "zip_and_files")
years = _parse_years(params.get("readings_years_csv", "2023,2024,2025"))
soil_csv_input_dir = params.get("soil_csv_input_dir", "Files/raw/soil").rstrip("/")
soil_csv_file_pattern = params.get("soil_csv_file_pattern", "soil-sensor-readings-historical-data-{year}.csv")
zip_input_path = params.get("soil_zip_input_path", "Files/raw/soil/Soil Sensor Readings - Historical data (2022).zip")
zip_slug = params.get("soil_zip_filename_slug", "soil_sensor_readings_historical_2022.zip")

base_url = params.get("api_base_url_com", "https://data.melbourne.vic.gov.au")
dataset = params.get("com_dataset_soil_readings", "soil-sensor-readings-historical-data")
app_token = params.get("com_app_token", "")
page_size = int(params.get("com_page_size", 100))
time_window_minutes = int(params.get("time_window_minutes", 60))

watermarks: List[Tuple[str, str, str]] = []

if ingest_mode in {"zip_only", "zip_and_files", "zip_and_api"}:
    year_base = f"{bronze_soil_root}/year=2022"
    zip_out = f"{year_base}/raw/zip/{zip_slug}"
    extract_dir = _lakehouse_to_local(f"{year_base}/raw/extracted")
    _copy_file(zip_input_path, zip_out)
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(_lakehouse_to_local(zip_out), "r") as z:
        z.extractall(extract_dir)
    _write_json(
        f"{year_base}/metadata/ingest_summary.json",
        {"source_type": "zip", "source_file": zip_input_path, "bronze_file": zip_out, "snapshot_date": snapshot_date},
    )
    watermarks.append(("bronze_soil_2022_zip", pipeline_run_id, "2022-12-31"))

if ingest_mode in {"files_only", "zip_and_files"}:
    for year in years:
        filename = soil_csv_file_pattern.format(year=year)
        src = f"{soil_csv_input_dir}/{filename}"
        year_base = f"{bronze_soil_root}/year={year}"
        bronze_csv = f"{year_base}/raw/file/{filename}"
        bronze_jsonl = f"{year_base}/raw/records_file.jsonl"
        _copy_file(src, bronze_csv)
        rows = _csv_to_jsonl(bronze_csv, bronze_jsonl)
        _write_json(
            f"{year_base}/metadata/ingest_summary.json",
            {"source_type": "file", "source_file": src, "row_count": rows, "snapshot_date": snapshot_date},
        )
        print(f"[FILE] soil year={year} rows={rows}")
        watermarks.append((f"bronze_soil_{year}_file", pipeline_run_id, f"{year}-12-31"))

if ingest_mode in {"api_only", "zip_and_api"}:
    if not app_token:
        raise RuntimeError("com_app_token is required for api_only/zip_and_api runs.")
    for year in years:
        year_base = f"{bronze_soil_root}/year={year}"
        jsonl = f"{year_base}/raw/records_api.jsonl"
        local = _lakehouse_to_local(jsonl)
        _ensure_parent(local)
        if os.path.exists(local):
            os.remove(local)
        total = 0
        for rows in _iter_year_records(
            base_url=base_url,
            dataset=dataset,
            year=year,
            page_size=page_size,
            app_token=app_token,
            time_window_minutes=time_window_minutes,
        ):
            total += _write_jsonl(jsonl, rows)
        _write_json(
            f"{year_base}/metadata/ingest_summary.json",
            {"source_type": "api", "row_count": total, "snapshot_date": snapshot_date},
        )
        print(f"[API] soil year={year} rows={total}")
        watermarks.append((f"bronze_soil_{year}_api", pipeline_run_id, f"{year}-12-31"))

if watermarks:
    _update_watermarks(watermarks, watermark_table)
print("[DONE] Bronze soil ingestion complete.")

