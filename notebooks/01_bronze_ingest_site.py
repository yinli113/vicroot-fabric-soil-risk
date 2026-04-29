"""Bronze site ingestion (domain-split)."""

from __future__ import annotations

import json
import os
import shutil
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

NOTEBOOK_VERSION = "v2026-04-28-bronze-site-split-01"

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
                "bronze_site_root": "Files/medallion/bronze/bronze_site_reference",
                "snapshot_date": datetime.now().strftime("%Y-%m-%d"),
                "pipeline_run_id": "",
                "watermark_table": "metadata.ingestion_watermarks",
                "site_file_input_path": "",
                "site_api_enabled": True,
                "api_base_url_com": "https://data.melbourne.vic.gov.au",
                "com_dataset_soil_locations": "soil-sensor-locations",
                "com_page_size": 100,
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


def _ensure_parent(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def _copy_file(src: str, dst: str) -> None:
    src_local = _lakehouse_to_local(src)
    dst_local = _lakehouse_to_local(dst)
    if not os.path.exists(src_local):
        raise FileNotFoundError(f"Input not found: {src_local}")
    _ensure_parent(dst_local)
    if os.path.abspath(src_local) != os.path.abspath(dst_local):
        shutil.copy2(src_local, dst_local)


def _write_json(path: str, payload: Dict[str, Any]) -> None:
    local = _lakehouse_to_local(path)
    _ensure_parent(local)
    with open(local, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2)


def _http_get_json(url: str, app_token: str) -> Dict[str, Any]:
    req = urllib.request.Request(url=url)
    if app_token:
        req.add_header("X-App-Token", app_token)
    with urllib.request.urlopen(req, timeout=60, context=ssl.create_default_context()) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _fetch_locations(base_url: str, dataset: str, page_size: int, app_token: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    offset = 0
    while True:
        query = urllib.parse.urlencode({"limit": page_size, "offset": offset})
        url = f"{base_url.rstrip('/')}/api/explore/v2.1/catalog/datasets/{dataset}/records?{query}"
        payload = _http_get_json(url, app_token)
        batch = payload.get("results", [])
        rows.extend(batch)
        if not batch or len(batch) < page_size:
            break
        offset += len(batch)
    return rows


def _update_watermarks(rows: List[Tuple[str, str, str]], table_name: str, snapshot_date: str) -> None:
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
bronze_site_root = params.get("bronze_site_root", f"{medallion_root}/bronze/bronze_site_reference").rstrip("/")
snapshot_date = params.get("snapshot_date", datetime.now().strftime("%Y-%m-%d"))
pipeline_run_id = params.get("pipeline_run_id", f"manual-{snapshot_date}")
watermark_table = params.get("watermark_table", "metadata.ingestion_watermarks")

site_file_input_path = params.get("site_file_input_path", "").strip()
site_api_enabled = _parse_bool(params.get("site_api_enabled", True), True)
base_url = params.get("api_base_url_com", "https://data.melbourne.vic.gov.au")
dataset = params.get("com_dataset_soil_locations", "soil-sensor-locations")
app_token = params.get("com_app_token", "")
page_size = int(params.get("com_page_size", 100))

watermarks: List[Tuple[str, str, str]] = []

if site_file_input_path:
    filename = os.path.basename(site_file_input_path)
    out_path = f"{bronze_site_root}/source=file/snapshot_date={snapshot_date}/raw/{filename}"
    _copy_file(site_file_input_path, out_path)
    _write_json(
        f"{bronze_site_root}/source=file/snapshot_date={snapshot_date}/metadata/ingest_summary.json",
        {"source_type": "file", "source_file": site_file_input_path, "bronze_file": out_path, "snapshot_date": snapshot_date},
    )
    print(f"[FILE] site rows source copied: {out_path}")
    watermarks.append(("bronze_site_file", pipeline_run_id, snapshot_date))

if site_api_enabled:
    rows = _fetch_locations(base_url, dataset, page_size, app_token)
    out_path = f"{bronze_site_root}/source=api/snapshot_date={snapshot_date}/raw/records.json"
    _write_json(out_path, {"dataset": dataset, "snapshot_date": snapshot_date, "record_count": len(rows), "results": rows})
    print(f"[API] site rows fetched: {len(rows)}")
    watermarks.append(("bronze_site_api", pipeline_run_id, snapshot_date))

if watermarks:
    _update_watermarks(watermarks, watermark_table, snapshot_date)
print("[DONE] Bronze site ingestion complete.")

