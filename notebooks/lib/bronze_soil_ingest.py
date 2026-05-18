"""Helpers for Bronze soil file/zip/API ingestion (used by 01_bronze_ingest_soil.py)."""

from __future__ import annotations

import csv
import json
import os
import re
import shutil
import ssl
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Tuple

__all__ = [
    "lakehouse_to_local",
    "soil_zip_input_ready",
    "parse_years",
    "copy_bronze_file",
    "csv_to_jsonl",
    "promote_2022_extract_to_records_jsonl",
    "write_json",
    "iter_year_records",
    "write_jsonl",
    "update_watermarks",
    "effective_pipeline_run_id",
]


def lakehouse_to_local(path: str) -> str:
    if path.startswith("/lakehouse/"):
        return path
    if path.startswith("Files/"):
        return f"/lakehouse/default/{path}"
    return path


def soil_zip_input_ready(soil_zip_input_path: str) -> bool:
    p = (soil_zip_input_path or "").strip()
    if not p.lower().endswith(".zip"):
        return False
    return os.path.isfile(lakehouse_to_local(p))


def parse_years(years_csv: str) -> List[int]:
    years = sorted({int(x.strip()) for x in years_csv.split(",") if x.strip()})
    if not years:
        raise ValueError("readings_years_csv is empty")
    return years


def effective_pipeline_run_id(params: Dict[str, Any], snapshot_date: str) -> str:
    """Fabric pipelines pass @pipeline().RunId; standalone runs get a stable-enough notebook id."""
    rid = str(params.get("pipeline_run_id") or "").strip()
    if rid:
        return rid
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"notebook-{snapshot_date}-{stamp}"


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


def copy_bronze_file(src: str, dst: str) -> None:
    src_local = lakehouse_to_local(src)
    dst_local = lakehouse_to_local(dst)
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


def csv_to_jsonl(csv_path: str, jsonl_path: str) -> int:
    src = lakehouse_to_local(csv_path)
    dst = lakehouse_to_local(jsonl_path)
    _ensure_parent(dst)
    written = 0
    with open(src, "r", encoding="utf-8-sig", newline="") as in_f, open(dst, "w", encoding="utf-8") as out_f:
        reader = csv.DictReader(in_f)
        for row in reader:
            out_f.write(json.dumps(row, ensure_ascii=True))
            out_f.write("\n")
            written += 1
    return written


def _iter_records_from_json_payload(payload: Any) -> Iterable[Dict[str, Any]]:
    if payload is None:
        return
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item
        return
    if isinstance(payload, dict):
        if isinstance(payload.get("results"), list):
            for item in payload["results"]:
                if isinstance(item, dict):
                    yield item
            return
        yield payload


def _discover_2022_record_sources(extract_dir: str) -> Tuple[List[str], List[str]]:
    local_root = lakehouse_to_local(extract_dir)
    json_files: List[str] = []
    csv_files: List[str] = []
    all_files: List[str] = []
    for dirpath, _, filenames in os.walk(local_root):
        for filename in filenames:
            path = os.path.join(dirpath, filename)
            all_files.append(path)
            lower = filename.lower()
            if lower.endswith(".json"):
                json_files.append(path)
            elif lower.endswith(".csv"):
                csv_files.append(path)
    json_files.sort()
    csv_files.sort()
    if not json_files and not csv_files:
        preview = "\n".join(all_files[:30]) if all_files else "(archive extracted no files)"
        raise FileNotFoundError(
            f"No JSON/CSV found in extracted 2022 archive: {local_root}\nExtracted files:\n{preview}"
        )
    return json_files, csv_files


def promote_2022_extract_to_records_jsonl(*, extract_dir: str, records_jsonl: str) -> Tuple[int, List[str]]:
    json_files, csv_files = _discover_2022_record_sources(extract_dir)
    dst = lakehouse_to_local(records_jsonl)
    _ensure_parent(dst)
    written = 0
    sources: List[str] = []

    if json_files:
        with open(dst, "w", encoding="utf-8") as out_f:
            for path in json_files:
                sources.append(path)
                try:
                    with open(path, "r", encoding="utf-8-sig") as in_f:
                        payload = json.load(in_f)
                except json.JSONDecodeError as err:
                    raise RuntimeError(f"Invalid JSON in 2022 shard: {path}") from err
                for row in _iter_records_from_json_payload(payload):
                    out_f.write(json.dumps(row, ensure_ascii=True))
                    out_f.write("\n")
                    written += 1
        return written, sources

    soil_sensor_matches = [
        p for p in csv_files if "soil" in os.path.basename(p).lower() and "sensor" in os.path.basename(p).lower()
    ]
    candidates = soil_sensor_matches or csv_files
    preferred = [p for p in candidates if "2022" in os.path.basename(p).lower()]
    if len(preferred) == 1:
        csv_path = preferred[0]
    elif len(candidates) == 1:
        csv_path = candidates[0]
    else:
        preview = "\n".join(candidates[:20])
        raise RuntimeError(f"Ambiguous 2022 CSV files in archive. Candidates:\n{preview}")

    written = csv_to_jsonl(csv_path, records_jsonl)
    sources = [csv_path]
    return written, sources


def write_json(path: str, payload: Dict[str, Any]) -> None:
    local = lakehouse_to_local(path)
    _ensure_parent(local)
    with open(local, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=True, indent=2)


def _build_records_url(base_url: str, dataset: str, query: Dict[str, Any]) -> str:
    return f"{base_url.rstrip('/')}/api/explore/v2.1/catalog/datasets/{dataset}/records?{urllib.parse.urlencode(query)}"


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


def iter_year_records(
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


def write_jsonl(path: str, rows: List[Dict[str, Any]]) -> int:
    local = lakehouse_to_local(path)
    _ensure_parent(local)
    with open(local, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=True))
            f.write("\n")
    return len(rows)


def update_watermarks(rows: List[Tuple[str, str, str]], table_name: str, *, snapshot_date: str) -> None:
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
