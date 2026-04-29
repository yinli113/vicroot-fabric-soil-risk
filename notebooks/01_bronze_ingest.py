"""Bronze ingestion for CoM soil sensors.

Implements:
1) 2022 archive handling (zip already uploaded to Lakehouse Files).
2) 2023+ API pulls with year filters and pagination.

Fabric note:
- If imports fail in notebook cells, use `%run ./lib/pipeline_params`.
"""

# %% [markdown]
# # Cell 1 - Imports and parameter resolver

from __future__ import annotations

import json
import csv
import os
import shutil
import ssl
import sys
import time
import urllib.parse
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

NOTEBOOK_VERSION = "v2026-04-25-bronze-files-mode-01"

try:
    from notebooks.lib.pipeline_params import resolve_params
except ModuleNotFoundError:
    # Fabric notebooks often don't expose local repo package paths.
    # Try common runtime locations, then fall back to an inline resolver.
    candidate_dirs = [
        "/lakehouse/default/Files/notebooks/lib",
        "/lakehouse/default/Files/lib",
        str(Path.cwd() / "notebooks" / "lib"),
        str(Path.cwd() / "lib"),
    ]
    for candidate in candidate_dirs:
        if os.path.isdir(candidate) and candidate not in sys.path:
            sys.path.append(candidate)
    try:
        from pipeline_params import resolve_params  # type: ignore
    except ModuleNotFoundError:
        def resolve_params(overrides: Dict[str, Any] | None = None) -> Dict[str, Any]:
            """Inline fallback when helper module is unavailable in runtime."""
            params: Dict[str, Any] = {
                "environment": "dev",
                "lakehouse_name": "lh_vicroot_melbourne",
                "medallion_root": "Files/medallion",
                "api_base_url_com": "https://data.melbourne.vic.gov.au",
                "api_base_url_soil": "",
                "api_base_url_bom": "",
                "bom_station_id": "",
                "source_crs": "EPSG:4326",
                "target_crs": "EPSG:7855",
                "snapshot_date": datetime.now().strftime("%Y-%m-%d"),
                "pipeline_run_id": "",
                "bronze_ingest_mode": "zip_and_files",
                "readings_years_csv": "2023,2024,2025",
                "com_page_size": 100,
                "time_window_minutes": 60,
                "resume_from_utc": "",
                "com_dataset_soil_readings": "soil-sensor-readings-historical-data",
                "com_dataset_soil_locations": "soil-sensor-locations",
                "soil_zip_input_path": "Files/medallion/bronze/Soil Sensor Readings - Historical data (2022).zip",
                "soil_zip_filename_slug": "soil_sensor_readings_historical_2022.zip",
                "soil_csv_input_dir": "Files/medallion/bronze",
                "soil_csv_file_pattern": "soil-sensor-readings-historical-data-{year}.csv",
                "bronze_strict_input_guard": True,
                "weather_api_enabled": False,
                "weather_api_provider": "open_meteo_archive",
                "weather_years_csv": "2022,2023,2024,2025",
                "openweather_base_url": "https://api.openweathermap.org",
                "weather_latitude": "",
                "weather_longitude": "",
                "weather_location_name": "melbourne",
                "com_app_token": "",
                "soil_api_key": "",
                "soil_api_subscription_key": "",
                "bom_api_key": "",
                "openweather_api_key": "",
            }
            if os.environ.get("VICROOT_PARAMS_JSON"):
                params.update(json.loads(os.environ["VICROOT_PARAMS_JSON"]))
            for key, env_name in (
                ("com_app_token", "VICROOT_COM_APP_TOKEN"),
                ("soil_api_key", "VICROOT_SOIL_API_KEY"),
                ("soil_api_subscription_key", "VICROOT_SOIL_API_SUBSCRIPTION_KEY"),
                ("bom_api_key", "VICROOT_BOM_API_KEY"),
                ("api_base_url_com", "VICROOT_API_BASE_URL_COM"),
                ("api_base_url_soil", "VICROOT_API_BASE_URL_SOIL"),
                ("api_base_url_bom", "VICROOT_API_BASE_URL_BOM"),
                ("openweather_api_key", "VICROOT_OPENWEATHER_API_KEY"),
                ("weather_api_provider", "VICROOT_WEATHER_API_PROVIDER"),
                ("weather_years_csv", "VICROOT_WEATHER_YEARS_CSV"),
            ):
                if os.environ.get(env_name):
                    params[key] = os.environ[env_name]
            if overrides:
                params.update(overrides)
            return params


# %% [markdown]
# # Cell 2 - Helper functions

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_params_for_log(params: Dict[str, Any]) -> Dict[str, Any]:
    sanitized: Dict[str, Any] = {}
    for key, value in params.items():
        key_lower = key.lower()
        if "token" in key_lower or "key" in key_lower or "secret" in key_lower:
            continue
        sanitized[key] = value
    return sanitized


def _parse_years(years_csv: str) -> List[int]:
    years = []
    for item in years_csv.split(","):
        item = item.strip()
        if not item:
            continue
        years.append(int(item))
    if not years:
        raise ValueError("years_csv is empty; provide at least one year, e.g. 2023,2024,2025")
    return sorted(set(years))


def _parse_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y", "on"}:
            return True
        if lowered in {"false", "0", "no", "n", "off", ""}:
            return False
    return default


def _lakehouse_to_local(path: str) -> str:
    """Map Fabric lakehouse-style path to notebook local path."""
    trimmed = path.strip()
    if trimmed.startswith("/lakehouse/"):
        return trimmed
    if trimmed.startswith("Files/"):
        return f"/lakehouse/default/{trimmed}"
    if trimmed.startswith("Tables/"):
        return f"/lakehouse/default/{trimmed}"
    return trimmed


def _ensure_parent_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


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
            # Prefer likely soil-sensor 2022 archives first.
            if "soil" in lower and "sensor" in lower and "2022" in lower:
                candidates.insert(0, full_path)
            else:
                candidates.append(full_path)
    return candidates


def _copy_zip_to_bronze(zip_input_path: str, zip_output_path: str) -> None:
    src = _lakehouse_to_local(zip_input_path)
    dst = _lakehouse_to_local(zip_output_path)
    if not os.path.exists(src):
        candidates = _discover_zip_candidates()
        if len(candidates) == 1:
            src = candidates[0]
            print(f"[INFO] Requested zip path not found; using discovered zip: {src}")
        elif len(candidates) > 1:
            preferred = [p for p in candidates if "soil" in os.path.basename(p).lower() and "2022" in os.path.basename(p).lower()]
            if len(preferred) == 1:
                src = preferred[0]
                print(f"[INFO] Requested zip path not found; using best-match zip: {src}")
            else:
                preview = "\n".join(candidates[:10])
                raise FileNotFoundError(
                    f"Zip input not found: {src}\n"
                    f"Found multiple zip files under /lakehouse/default/Files; set `soil_zip_input_path` explicitly.\n"
                    f"Candidates (first 10):\n{preview}"
                )
        else:
            raise FileNotFoundError(
                f"Zip input not found: {src}\n"
                "No .zip files found under /lakehouse/default/Files. "
                "Upload the archive or set `soil_zip_input_path` to the correct Files path."
            )
    _ensure_parent_dir(dst)
    if os.path.abspath(src) == os.path.abspath(dst):
        return
    shutil.copy2(src, dst)


def _extract_zip(zip_path: str, extract_dir: str) -> List[str]:
    zip_local = _lakehouse_to_local(zip_path)
    out_dir = _lakehouse_to_local(extract_dir)
    _ensure_dir(out_dir)
    extracted_files: List[str] = []
    with zipfile.ZipFile(zip_local, "r") as archive:
        archive.extractall(out_dir)
        extracted_files = [os.path.join(out_dir, name) for name in archive.namelist()]
    return extracted_files


def _copy_file(src_path: str, dst_path: str) -> None:
    src = _lakehouse_to_local(src_path)
    dst = _lakehouse_to_local(dst_path)
    if not os.path.exists(src):
        raise FileNotFoundError(f"Input file not found: {src}")
    _ensure_parent_dir(dst)
    if os.path.abspath(src) == os.path.abspath(dst):
        return
    shutil.copy2(src, dst)


def _csv_to_jsonl(csv_path: str, jsonl_path: str) -> int:
    csv_local = _lakehouse_to_local(csv_path)
    jsonl_local = _lakehouse_to_local(jsonl_path)
    _ensure_parent_dir(jsonl_local)
    row_count = 0
    with open(csv_local, "r", encoding="utf-8-sig", newline="") as in_handle, open(
        jsonl_local, "w", encoding="utf-8"
    ) as out_handle:
        reader = csv.DictReader(in_handle)
        for row in reader:
            out_handle.write(json.dumps(row, ensure_ascii=True))
            out_handle.write("\n")
            row_count += 1
    return row_count


def _read_first_site_coordinates(path: str) -> Tuple[float, float] | None:
    local_path = _lakehouse_to_local(path)
    if not os.path.exists(local_path):
        return None
    lat_candidates = ("latitude", "lat", "y", "Latitude", "LATITUDE")
    lon_candidates = ("longitude", "lon", "lng", "x", "Longitude", "LONGITUDE")
    with open(local_path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            lat_val = None
            lon_val = None
            for key in lat_candidates:
                if key in row and row.get(key) not in (None, ""):
                    lat_val = row.get(key)
                    break
            for key in lon_candidates:
                if key in row and row.get(key) not in (None, ""):
                    lon_val = row.get(key)
                    break
            if lat_val is None or lon_val is None:
                continue
            try:
                return float(lat_val), float(lon_val)
            except ValueError:
                continue
    return None


def _http_get_json_no_auth(url: str) -> Dict[str, Any]:
    request = urllib.request.Request(url=url)
    context = ssl.create_default_context()
    try:
        with urllib.request.urlopen(request, timeout=60, context=context) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        body = ""
        try:
            body = err.read().decode("utf-8")
        except Exception:  # noqa: BLE001
            body = ""
        raise RuntimeError(f"HTTP {err.code} for URL {url}; body={body[:500]}")


def _fetch_openweather_daily_forecast(
    *,
    openweather_base_url: str,
    openweather_api_key: str,
    latitude: float,
    longitude: float,
    location_name: str,
    snapshot_date: str,
) -> List[Dict[str, Any]]:
    query = urllib.parse.urlencode(
        {
            "lat": latitude,
            "lon": longitude,
            "appid": openweather_api_key,
            "units": "metric",
        }
    )
    url = f"{openweather_base_url.rstrip('/')}/data/2.5/forecast?{query}"
    payload = _http_get_json_no_auth(url)
    rows = payload.get("list", [])
    daily_rollup: Dict[str, Dict[str, float]] = {}
    for row in rows:
        dt_txt = str(row.get("dt_txt", ""))
        as_of_date = dt_txt[:10]
        if not as_of_date:
            continue
        bucket = daily_rollup.setdefault(
            as_of_date,
            {
                "count": 0.0,
                "temp_sum": 0.0,
                "temp_min": 999.0,
                "temp_max": -999.0,
                "humidity_sum": 0.0,
                "wind_sum": 0.0,
                "rain_sum": 0.0,
            },
        )
        main = row.get("main", {}) or {}
        wind = row.get("wind", {}) or {}
        rain = row.get("rain", {}) or {}
        temp = float(main.get("temp", 0.0))
        humidity = float(main.get("humidity", 0.0))
        wind_speed = float(wind.get("speed", 0.0))
        rain_3h = float(rain.get("3h", 0.0))
        bucket["count"] += 1.0
        bucket["temp_sum"] += temp
        bucket["humidity_sum"] += humidity
        bucket["wind_sum"] += wind_speed
        bucket["rain_sum"] += rain_3h
        bucket["temp_min"] = min(bucket["temp_min"], temp)
        bucket["temp_max"] = max(bucket["temp_max"], temp)

    output: List[Dict[str, Any]] = []
    for as_of_date in sorted(daily_rollup):
        bucket = daily_rollup[as_of_date]
        count = max(1.0, bucket["count"])
        output.append(
            {
                "as_of_date": as_of_date,
                "location_name": location_name,
                "provider": "openweather",
                "rainfall_mm": bucket["rain_sum"],
                "temp_avg_c": bucket["temp_sum"] / count,
                "temp_min_c": bucket["temp_min"],
                "temp_max_c": bucket["temp_max"],
                "humidity_avg_pct": bucket["humidity_sum"] / count,
                "wind_speed_avg_ms": bucket["wind_sum"] / count,
                "evap_mm": None,
                "snapshot_date": snapshot_date,
                "ingested_at_utc": _utc_now_iso(),
            }
        )
    return output


def _fetch_open_meteo_archive_daily(
    *,
    latitude: float,
    longitude: float,
    location_name: str,
    snapshot_date: str,
    start_date: str,
    end_date: str,
) -> List[Dict[str, Any]]:
    base_url = "https://archive-api.open-meteo.com/v1/archive"
    query = urllib.parse.urlencode(
        {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "daily": ",".join(
                [
                    "temperature_2m_max",
                    "temperature_2m_min",
                    "temperature_2m_mean",
                    "precipitation_sum",
                    "wind_speed_10m_mean",
                    "et0_fao_evapotranspiration",
                ]
            ),
            "hourly": "relative_humidity_2m",
            "timezone": "Australia/Melbourne",
        }
    )
    payload = _http_get_json_no_auth(f"{base_url}?{query}")
    daily = payload.get("daily", {}) or {}
    dates = daily.get("time", []) or []
    temp_max = daily.get("temperature_2m_max", []) or []
    temp_min = daily.get("temperature_2m_min", []) or []
    temp_mean = daily.get("temperature_2m_mean", []) or []
    precip = daily.get("precipitation_sum", []) or []
    wind = daily.get("wind_speed_10m_mean", []) or []
    evap = daily.get("et0_fao_evapotranspiration", []) or []

    humidity_by_day: Dict[str, Tuple[float, int]] = {}
    hourly = payload.get("hourly", {}) or {}
    hourly_time = hourly.get("time", []) or []
    hourly_humidity = hourly.get("relative_humidity_2m", []) or []
    for idx, ts in enumerate(hourly_time):
        day = str(ts)[:10]
        if not day or idx >= len(hourly_humidity):
            continue
        value = hourly_humidity[idx]
        if value is None:
            continue
        try:
            value_f = float(value)
        except (TypeError, ValueError):
            continue
        total, count = humidity_by_day.get(day, (0.0, 0))
        humidity_by_day[day] = (total + value_f, count + 1)

    output: List[Dict[str, Any]] = []
    for i, as_of_date in enumerate(dates):
        day_key = str(as_of_date)
        humidity_avg = None
        if day_key in humidity_by_day and humidity_by_day[day_key][1] > 0:
            humidity_avg = humidity_by_day[day_key][0] / humidity_by_day[day_key][1]
        output.append(
            {
                "as_of_date": day_key,
                "location_name": location_name,
                "provider": "open_meteo_archive",
                "rainfall_mm": float(precip[i]) if i < len(precip) and precip[i] is not None else None,
                "temp_avg_c": float(temp_mean[i]) if i < len(temp_mean) and temp_mean[i] is not None else None,
                "temp_min_c": float(temp_min[i]) if i < len(temp_min) and temp_min[i] is not None else None,
                "temp_max_c": float(temp_max[i]) if i < len(temp_max) and temp_max[i] is not None else None,
                "humidity_avg_pct": humidity_avg,
                "wind_speed_avg_ms": float(wind[i]) if i < len(wind) and wind[i] is not None else None,
                "evap_mm": float(evap[i]) if i < len(evap) and evap[i] is not None else None,
                "snapshot_date": snapshot_date,
                "ingested_at_utc": _utc_now_iso(),
            }
        )
    return output


def _build_records_url(base_url: str, dataset: str, params: Dict[str, Any]) -> str:
    base = base_url.rstrip("/")
    query = urllib.parse.urlencode(params)
    return f"{base}/api/explore/v2.1/catalog/datasets/{dataset}/records?{query}"


def _http_get_json(url: str, com_app_token: str) -> Dict[str, Any]:
    request = urllib.request.Request(url=url)
    # CoM portal often works without token; send token only when supplied.
    if com_app_token:
        request.add_header("X-App-Token", com_app_token)
    context = ssl.create_default_context()
    try:
        with urllib.request.urlopen(request, timeout=60, context=context) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        body = ""
        try:
            body = err.read().decode("utf-8")
        except Exception:  # noqa: BLE001
            body = ""
        raise RuntimeError(f"HTTP {err.code} for URL {url}; body={body[:500]}")
    except ssl.SSLCertVerificationError:
        # Fallback for local environments missing root certificates.
        insecure = ssl._create_unverified_context()
        with urllib.request.urlopen(request, timeout=60, context=insecure) as response:
            return json.loads(response.read().decode("utf-8"))


def _iter_year_records(
    base_url: str,
    dataset: str,
    year: int,
    page_size: int,
    com_app_token: str,
    time_window_minutes: int,
    resume_from_utc: str,
) -> Iterable[Tuple[int, List[Dict[str, Any]], int]]:
    year_start = datetime(year, 1, 1)
    year_end = datetime(year + 1, 1, 1)
    if resume_from_utc:
        try:
            parsed_resume = datetime.fromisoformat(resume_from_utc.replace("Z", "+00:00"))
            parsed_resume = parsed_resume.replace(tzinfo=None)
            if year_start <= parsed_resume < year_end:
                year_start = parsed_resume
        except ValueError:
            print(
                "[WARN] Invalid resume_from_utc format. "
                "Use ISO-8601 like 2023-07-28T08:00:00Z. Ignoring resume point."
            )
    global_offset = 0
    year_total_count = 0
    current_page_size = max(20, int(page_size))
    window_minutes = max(5, int(time_window_minutes))
    token_in_use = com_app_token
    use_order_by = True
    retry_count = 0
    current_slot = year_start
    while current_slot < year_end:
        next_slot = current_slot + timedelta(minutes=window_minutes)
        slot_start = current_slot.strftime("%Y-%m-%d %H:%M:%S")
        slot_end = next_slot.strftime("%Y-%m-%d %H:%M:%S")
        where = f"local_time >= date'{slot_start}' and local_time < date'{slot_end}'"
        slot_offset = 0
        first_page_for_slot = True

        while True:
            query_params: Dict[str, Any] = {
                "where": where,
                "limit": current_page_size,
                "offset": slot_offset,
            }
            if use_order_by:
                query_params["order_by"] = "local_time asc"
            url = _build_records_url(
                base_url=base_url,
                dataset=dataset,
                params=query_params,
            )
            try:
                payload = _http_get_json(url=url, com_app_token=token_in_use)
                retry_count = 0
            except Exception as exc:  # noqa: BLE001
                message = str(exc)
                is_400 = "HTTP 400" in message
                is_429 = "HTTP 429" in message
                if is_429:
                    if not token_in_use:
                        raise RuntimeError(
                            "City of Melbourne API daily anonymous limit reached (HTTP 429).\n"
                            "Action: set `com_app_token` (or env `VICROOT_COM_APP_TOKEN`) and rerun.\n"
                            "You can create a Socrata app token, then pass it in the pipeline/notebook params.\n"
                            f"Original error: {message}"
                        )
                    retry_count += 1
                    if retry_count <= 5:
                        sleep_s = retry_count * 10
                        print(
                            f"[WARN] year={year} slot={slot_start} offset={slot_offset}: 429 throttled, "
                            f"retry {retry_count}/5 after {sleep_s}s"
                        )
                        time.sleep(sleep_s)
                        continue
                    raise RuntimeError(
                        "HTTP 429 even with app token. Reduce run scope (fewer years) or wait until reset.\n"
                        f"Original error: {message}"
                    )
                if is_400 and use_order_by:
                    use_order_by = False
                    print(
                        f"[WARN] year={year} slot={slot_start} offset={slot_offset}: "
                        "400 with order_by; retrying without order_by"
                    )
                    continue
                if is_400 and current_page_size > 20:
                    current_page_size = max(20, current_page_size // 2)
                    print(
                        f"[WARN] year={year} slot={slot_start} offset={slot_offset}: "
                        f"400; reducing page_size to {current_page_size} and retrying"
                    )
                    continue
                if is_400 and token_in_use:
                    token_in_use = ""
                    print(
                        f"[WARN] year={year} slot={slot_start} offset={slot_offset}: "
                        "400; retrying without X-App-Token"
                    )
                    continue
                retry_count += 1
                if retry_count <= 3:
                    sleep_s = retry_count * 2
                    print(
                        f"[WARN] year={year} slot={slot_start} offset={slot_offset}: request error "
                        f"({message[:160]}), retry {retry_count}/3 after {sleep_s}s"
                    )
                    time.sleep(sleep_s)
                    continue
                raise
            rows = payload.get("results", [])
            if first_page_for_slot:
                first_page_for_slot = False
                year_total_count += int(payload.get("total_count", 0))
            if not rows:
                break
            yield global_offset, rows, year_total_count
            row_count = len(rows)
            global_offset += row_count
            slot_offset += row_count
            if row_count < current_page_size:
                break
        current_slot = next_slot


def _write_jsonl(path: str, rows: List[Dict[str, Any]]) -> int:
    local_path = _lakehouse_to_local(path)
    _ensure_parent_dir(local_path)
    with open(local_path, "a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True))
            handle.write("\n")
    return len(rows)


def _write_json(path: str, payload: Dict[str, Any]) -> None:
    local_path = _lakehouse_to_local(path)
    _ensure_parent_dir(local_path)
    with open(local_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2)


def _fetch_locations(base_url: str, dataset: str, page_size: int, com_app_token: str) -> List[Dict[str, Any]]:
    offset = 0
    output: List[Dict[str, Any]] = []
    while True:
        url = _build_records_url(
            base_url=base_url,
            dataset=dataset,
            params={"limit": page_size, "offset": offset},
        )
        payload = _http_get_json(url=url, com_app_token=com_app_token)
        rows = payload.get("results", [])
        output.extend(rows)
        if not rows or len(rows) < page_size:
            break
        offset += len(rows)
    return output


def _update_watermark_rows(rows: List[Tuple[str, str, str]], watermark_table: str) -> None:
    """Upsert-ish append for metadata.ingestion_watermarks.

    This keeps notebook logic simple for now; downstream table maintenance can MERGE later.
    """
    try:
        # Spark is available in Fabric notebooks.
        from pyspark.sql import SparkSession  # type: ignore

        spark = SparkSession.builder.getOrCreate()
        row_schema = "dataset_name string, last_success_run_id string, last_success_utc timestamp, high_watermark string, snapshot_date string"
        now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        data = [(name, run_id, now_utc, high_watermark, snapshot_date) for name, run_id, high_watermark in rows]
        df = spark.createDataFrame(data, schema=row_schema)

        def _ensure_table_and_append(table_name: str) -> None:
            if "." in table_name:
                schema_name = table_name.rsplit(".", 1)[0]
                spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            spark.sql(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                  dataset_name STRING,
                  last_success_run_id STRING,
                  last_success_utc TIMESTAMP,
                  high_watermark STRING,
                  snapshot_date STRING
                ) USING DELTA
                """
            )
            df.write.mode("append").format("delta").saveAsTable(table_name)

        try:
            _ensure_table_and_append(watermark_table)
        except Exception as inner_exc:  # noqa: BLE001
            # Fabric lakehouse often defaults to dbo schema; fallback there if custom schema is unavailable.
            if watermark_table != "dbo.ingestion_watermarks":
                print(
                    f"[WARN] Could not write {watermark_table}: {inner_exc}. "
                    "Falling back to dbo.ingestion_watermarks"
                )
                _ensure_table_and_append("dbo.ingestion_watermarks")
            else:
                raise
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] Could not write metadata.ingestion_watermarks: {exc}")


def _run_bronze_dq_tests(
    *,
    dq_fail_on_error: bool,
    bronze_min_rows_per_year: int,
    year_rows_by_source: Dict[str, Dict[int, int]],
    zip_mode_used: bool,
    zip_extracted_count: int,
    watermark_count: int,
) -> None:
    failures: List[str] = []
    warnings: List[str] = []

    if year_rows_by_source:
        for source_type, rows_by_year in sorted(year_rows_by_source.items()):
            for year, row_count in sorted(rows_by_year.items()):
                if row_count < bronze_min_rows_per_year:
                    failures.append(
                        f"{source_type} year={year} has {row_count} rows (< bronze_dq_min_rows_per_year={bronze_min_rows_per_year})"
                    )
    else:
        warnings.append("No year-level row stats were recorded for file/api branches in this run.")

    if zip_mode_used and zip_extracted_count == 0:
        warnings.append("Zip mode was used but extracted file count is zero.")
    if watermark_count == 0:
        failures.append("No watermark rows prepared; ingestion run has no completion markers.")

    print("[DQ][BRONZE] checks complete.")
    if warnings:
        for message in warnings:
            print(f"[DQ][BRONZE][WARN] {message}")
    if failures:
        for message in failures:
            print(f"[DQ][BRONZE][FAIL] {message}")
        if dq_fail_on_error:
            raise RuntimeError("Bronze DQ checks failed. See [DQ][BRONZE][FAIL] logs above.")
        print("[DQ][BRONZE][WARN] dq_fail_on_error=false, continuing despite Bronze DQ failures.")
    else:
        print("[DQ][BRONZE] all checks passed.")


def _run_bronze_input_guard(
    *,
    strict: bool,
    ingest_mode: str,
    years_csv: str,
    soil_csv_input_dir: str,
    soil_csv_file_pattern: str,
    soil_zip_input_path: str,
    site_file_input_path: str,
    weather_file_input_path: str,
) -> None:
    print("[GUARD][BRONZE] input path preflight:")
    print(f"  - ingest_mode={ingest_mode}")
    print(f"  - soil_csv_input_dir={soil_csv_input_dir} -> {_lakehouse_to_local(soil_csv_input_dir)}")
    print(f"  - soil_zip_input_path={soil_zip_input_path} -> {_lakehouse_to_local(soil_zip_input_path)}")
    if site_file_input_path:
        print(f"  - site_file_input_path={site_file_input_path} -> {_lakehouse_to_local(site_file_input_path)}")
    if weather_file_input_path:
        print(f"  - weather_file_input_path={weather_file_input_path} -> {_lakehouse_to_local(weather_file_input_path)}")

    failures: List[str] = []
    warnings: List[str] = []

    csv_dir_norm = soil_csv_input_dir.strip().rstrip("/")
    if strict and csv_dir_norm in {"Files/medallion/bronze", "/lakehouse/default/Files/medallion/bronze"}:
        failures.append(
            "soil_csv_input_dir points to broad root `Files/medallion/bronze`. "
            "Use a narrow folder (for example shortcut subfolder) that contains only source CSV files."
        )
    elif "/medallion/bronze/com_soil_sensor_readings" in csv_dir_norm:
        print("[GUARD][BRONZE] using known-safe medallion soil subpath.")
    elif "/medallion/bronze" in csv_dir_norm:
        warnings.append(
            "soil_csv_input_dir is under medallion bronze. Ensure this is a source-file shortcut path, not a full bronze root."
        )

    if ingest_mode in {"files_only", "zip_and_files"}:
        years = _parse_years(years_csv)
        for year in years:
            source_rel_path = soil_csv_file_pattern.format(year=year)
            expected_path = f"{soil_csv_input_dir.rstrip('/')}/{source_rel_path}"
            expected_local = _lakehouse_to_local(expected_path)
            if not os.path.exists(expected_local):
                # Legacy fallback checks for staged migration layouts.
                source_filename = os.path.basename(source_rel_path)
                fallback_candidates = [
                    f"{soil_csv_input_dir.rstrip('/')}/year={year}/raw/file/{source_filename}",
                    f"{soil_csv_input_dir.rstrip('/')}/source=file/year={year}/raw/{source_filename}",
                ]
                fallback_exists = False
                for candidate in fallback_candidates:
                    if os.path.exists(_lakehouse_to_local(candidate)):
                        fallback_exists = True
                        print(f"[GUARD][BRONZE][WARN] expected path missing; fallback candidate exists for year={year}: {candidate}")
                        break
                if not fallback_exists:
                    failures.append(f"Missing expected CSV for year={year}: {expected_path}")
            elif os.path.isdir(expected_local):
                failures.append(f"Expected a CSV file but found a directory: {expected_path}")
            elif not source_rel_path.lower().endswith(".csv"):
                failures.append(f"soil_csv_file_pattern must resolve to .csv filenames; got: {source_rel_path}")

    if ingest_mode in {"zip_only", "zip_and_api", "zip_and_files"}:
        if not soil_zip_input_path.lower().endswith(".zip"):
            failures.append("soil_zip_input_path must end with .zip")

    for label, path in [("site_file_input_path", site_file_input_path), ("weather_file_input_path", weather_file_input_path)]:
        if not path:
            continue
        local = _lakehouse_to_local(path)
        if os.path.isdir(local):
            failures.append(f"{label} points to a directory, expected a file: {path}")
        elif not os.path.exists(local):
            failures.append(f"{label} file not found: {path}")

    for message in warnings:
        print(f"[GUARD][BRONZE][WARN] {message}")
    if failures:
        for message in failures:
            print(f"[GUARD][BRONZE][FAIL] {message}")
        raise RuntimeError("Bronze input guard failed. Fix input paths before ingestion.")
    print("[GUARD][BRONZE] preflight passed.")


# %% [markdown]
# # Cell 3 - Resolve params and execution context

print(f"[INFO] notebook_version={NOTEBOOK_VERSION}")

params = resolve_params({})
print("Resolved params (sanitized):")
print(json.dumps(_safe_params_for_log(params), indent=2))

medallion_root = params.get("medallion_root", "Files/medallion").rstrip("/")
base_url_com = params.get("api_base_url_com", "https://data.melbourne.vic.gov.au")
com_app_token = params.get("com_app_token", "")
snapshot_date = params.get("snapshot_date", datetime.now().strftime("%Y-%m-%d"))
pipeline_run_id = params.get("pipeline_run_id", f"manual-{snapshot_date}")

ingest_mode = params.get("bronze_ingest_mode", "zip_and_files")
years_csv = params.get("readings_years_csv", "2023,2024,2025")
page_size = int(params.get("com_page_size", 100))
time_window_minutes = int(params.get("time_window_minutes", 60))
resume_from_utc = params.get("resume_from_utc", "")
watermark_table = params.get("watermark_table", "metadata.ingestion_watermarks")
readings_dataset = params.get("com_dataset_soil_readings", "soil-sensor-readings-historical-data")
locations_dataset = params.get("com_dataset_soil_locations", "soil-sensor-locations")
zip_input_path = params.get(
    "soil_zip_input_path",
    "Files/medallion/bronze/Soil Sensor Readings - Historical data (2022).zip",
)
zip_bronze_filename = params.get("soil_zip_filename_slug", "soil_sensor_readings_historical_2022.zip")
soil_csv_input_dir = params.get("soil_csv_input_dir", f"{medallion_root}/bronze").rstrip("/")
soil_csv_file_pattern = params.get("soil_csv_file_pattern", "soil-sensor-readings-historical-data-{year}.csv")
site_file_input_path = params.get("site_file_input_path", "").strip()
weather_file_input_path = params.get("weather_file_input_path", "").strip()
dq_fail_on_error = _parse_bool(params.get("dq_fail_on_error", True), True)
bronze_strict_input_guard = _parse_bool(params.get("bronze_strict_input_guard", True), True)
bronze_min_rows_per_year = int(params.get("bronze_dq_min_rows_per_year", 1))
weather_api_enabled = _parse_bool(params.get("weather_api_enabled", False), False)
weather_api_provider = str(params.get("weather_api_provider", "open_meteo_archive")).strip().lower()
weather_years_csv = str(params.get("weather_years_csv", params.get("readings_years_csv", "2022,2023,2024,2025"))).strip()
openweather_base_url = str(params.get("openweather_base_url", "https://api.openweathermap.org")).strip()
weather_latitude = str(params.get("weather_latitude", "")).strip()
weather_longitude = str(params.get("weather_longitude", "")).strip()
weather_location_name = str(params.get("weather_location_name", "melbourne")).strip()
openweather_api_key = str(params.get("openweather_api_key", "")).strip()

raw_base = f"{medallion_root}/bronze/com_soil_sensor_readings"
watermark_rows: List[Tuple[str, str, str]] = []
year_rows_by_source: Dict[str, Dict[int, int]] = {}
zip_extracted_count = 0

# Transition convenience: auto-narrow broad bronze root to the soil subpath.
if soil_csv_input_dir in {"Files/medallion/bronze", "/lakehouse/default/Files/medallion/bronze"}:
    narrowed_dir = "Files/medallion/bronze/com_soil_sensor_readings"
    print(
        "[INFO] soil_csv_input_dir was broad root `Files/medallion/bronze`; "
        f"auto-narrowing to `{narrowed_dir}` for safe soil ingestion."
    )
    soil_csv_input_dir = narrowed_dir
    if soil_csv_file_pattern == "soil-sensor-readings-historical-data-{year}.csv":
        soil_csv_file_pattern = "year={year}/raw/file/soil-sensor-readings-historical-data-{year}.csv"
        print(
            "[INFO] soil_csv_file_pattern auto-adjusted for legacy medallion layout: "
            f"`{soil_csv_file_pattern}`"
        )

_run_bronze_input_guard(
    strict=bronze_strict_input_guard,
    ingest_mode=ingest_mode,
    years_csv=years_csv,
    soil_csv_input_dir=soil_csv_input_dir,
    soil_csv_file_pattern=soil_csv_file_pattern,
    soil_zip_input_path=zip_input_path,
    site_file_input_path=site_file_input_path,
    weather_file_input_path=weather_file_input_path,
)


# %% [markdown]
# # Cell 4 - 2022 zip branch (run when mode is `zip_only` or `zip_and_api`)

if ingest_mode in {"zip_only", "zip_and_api", "zip_and_files"}:
    year_base_2022 = f"{raw_base}/year=2022"
    zip_output_path = f"{year_base_2022}/raw/zip/{zip_bronze_filename}"
    parsed_output_dir = f"{year_base_2022}/raw/extracted"
    _copy_zip_to_bronze(zip_input_path=zip_input_path, zip_output_path=zip_output_path)
    extracted = _extract_zip(zip_path=zip_output_path, extract_dir=parsed_output_dir)
    zip_extracted_count = len(extracted)
    _write_json(
        f"{year_base_2022}/metadata/extract_manifest.json",
        {"zip_path": zip_output_path, "extracted_files": extracted, "extracted_at_utc": _utc_now_iso()},
    )
    print(f"[OK] 2022 zip copied + extracted: {zip_output_path}")
    watermark_rows.append(("com_soil_sensor_readings_2022_zip", pipeline_run_id, "2022-12-31"))


# %% [markdown]
# # Cell 5 - 2023+ file branch (run when mode includes `files`)

if ingest_mode in {"files_only", "zip_and_files"}:
    years = _parse_years(years_csv)
    for year in years:
        source_rel_path = soil_csv_file_pattern.format(year=year)
        source_filename = os.path.basename(source_rel_path)
        csv_source_path = f"{soil_csv_input_dir}/{source_rel_path}"
        year_base = f"{raw_base}/year={year}"
        csv_bronze_path = f"{year_base}/raw/file/{source_filename}"
        jsonl_path = f"{year_base}/raw/records_file.jsonl"
        _copy_file(csv_source_path, csv_bronze_path)
        total_written = _csv_to_jsonl(csv_bronze_path, jsonl_path)
        _write_json(
            f"{year_base}/metadata/ingest_summary.json",
            {
                "dataset": "soil-sensor-readings-historical-data",
                "year": year,
                "source_type": "file",
                "source_file": csv_source_path,
                "bronze_file": csv_bronze_path,
                "total_written": total_written,
                "ingested_at_utc": _utc_now_iso(),
                "pipeline_run_id": pipeline_run_id,
                "snapshot_date": snapshot_date,
            },
        )
        print(f"[FILE] year={year} rows={total_written} source={csv_source_path}")
        year_rows_by_source.setdefault("file", {})[year] = total_written
        watermark_rows.append((f"com_soil_sensor_readings_{year}_file", pipeline_run_id, f"{year}-12-31"))


# %% [markdown]
# # Cell 6 - 2023+ API branch (run when mode is `api_only` or `zip_and_api`)

if ingest_mode in {"api_only", "zip_and_api"}:
    if not com_app_token:
        raise RuntimeError(
            "Missing com_app_token for API ingestion. "
            "Set notebook param `com_app_token` (or env `VICROOT_COM_APP_TOKEN`) and rerun."
        )
    years = _parse_years(years_csv)
    for year in years:
        year_base = f"{raw_base}/year={year}"
        jsonl_path = f"{year_base}/raw/records_api.jsonl"
        # Reset file so reruns are idempotent for the year partition.
        local_jsonl = _lakehouse_to_local(jsonl_path)
        _ensure_parent_dir(local_jsonl)
        if os.path.exists(local_jsonl):
            os.remove(local_jsonl)
        total_written = 0
        total_count = None
        for offset, rows, count in _iter_year_records(
            base_url=base_url_com,
            dataset=readings_dataset,
            year=year,
            page_size=page_size,
            com_app_token=com_app_token,
            time_window_minutes=time_window_minutes,
            resume_from_utc=resume_from_utc,
        ):
            total_count = count
            if not rows:
                break
            total_written += _write_jsonl(jsonl_path, rows)
            print(f"[API] year={year} offset={offset} batch={len(rows)} total_written={total_written}")
        _write_json(
            f"{year_base}/metadata/ingest_summary.json",
            {
                "dataset": readings_dataset,
                "year": year,
                "total_count_reported": total_count,
                "total_written": total_written,
                "ingested_at_utc": _utc_now_iso(),
                "pipeline_run_id": pipeline_run_id,
                "snapshot_date": snapshot_date,
            },
        )
        year_rows_by_source.setdefault("api", {})[year] = total_written
        watermark_rows.append((f"com_soil_sensor_readings_{year}_api", pipeline_run_id, f"{year}-12-31"))

    locations_rows = _fetch_locations(
        base_url=base_url_com,
        dataset=locations_dataset,
        page_size=page_size,
        com_app_token=com_app_token,
    )
    locations_path = f"{medallion_root}/bronze/com_soil_sensor_locations/source=api/snapshot_date={snapshot_date}/raw/records.json"
    _write_json(
        locations_path,
        {
            "dataset": locations_dataset,
            "snapshot_date": snapshot_date,
            "record_count": len(locations_rows),
            "results": locations_rows,
        },
    )
    print(f"[OK] Locations ingested: {len(locations_rows)} rows")
    watermark_rows.append(("com_soil_sensor_locations_api", pipeline_run_id, snapshot_date))


# %% [markdown]
# # Cell 6b - Optional site/weather file branch

def _copy_optional_dataset_file(input_path: str, bronze_dataset: str) -> str:
    file_name = os.path.basename(input_path)
    output_path = f"{medallion_root}/bronze/{bronze_dataset}/source=file/snapshot_date={snapshot_date}/raw/{file_name}"
    _copy_file(input_path, output_path)
    _write_json(
        f"{medallion_root}/bronze/{bronze_dataset}/source=file/snapshot_date={snapshot_date}/metadata/ingest_summary.json",
        {
            "dataset": bronze_dataset,
            "source_type": "file",
            "source_file": input_path,
            "bronze_file": output_path,
            "ingested_at_utc": _utc_now_iso(),
            "pipeline_run_id": pipeline_run_id,
            "snapshot_date": snapshot_date,
        },
    )
    return output_path


if site_file_input_path:
    copied_site_path = _copy_optional_dataset_file(
        input_path=site_file_input_path,
        bronze_dataset="site_reference",
    )
    print(f"[FILE] site reference copied: {copied_site_path}")
    watermark_rows.append(("site_reference_file", pipeline_run_id, snapshot_date))

if weather_file_input_path:
    copied_weather_path = _copy_optional_dataset_file(
        input_path=weather_file_input_path,
        bronze_dataset="weather_daily",
    )
    print(f"[FILE] weather file copied: {copied_weather_path}")
    watermark_rows.append(("weather_daily_file", pipeline_run_id, snapshot_date))


# %% [markdown]
# # Cell 6c - Optional weather API branch

if weather_api_enabled:
    lat: float | None = None
    lon: float | None = None
    if weather_latitude and weather_longitude:
        lat = float(weather_latitude)
        lon = float(weather_longitude)
    elif site_file_input_path:
        inferred = _read_first_site_coordinates(site_file_input_path)
        if inferred is not None:
            lat, lon = inferred
            print(
                f"[WEATHER][API] inferred coordinates from site file: lat={lat}, lon={lon}. "
                "Set weather_latitude/weather_longitude to override."
            )
    if lat is None or lon is None:
        raise RuntimeError(
            "Unable to resolve weather coordinates. Provide `weather_latitude` and `weather_longitude`, "
            "or provide a site file containing latitude/longitude columns."
        )

    if weather_api_provider in {"open_meteo_archive", "openmeteo", "open-meteo"}:
        years = _parse_years(weather_years_csv)
        start_date = f"{min(years)}-01-01"
        end_date = f"{max(years)}-12-31"
        print(f"[WEATHER][API] fetching open-meteo archive history: {start_date} to {end_date}")
        weather_rows = _fetch_open_meteo_archive_daily(
            latitude=lat,
            longitude=lon,
            location_name=weather_location_name,
            snapshot_date=snapshot_date,
            start_date=start_date,
            end_date=end_date,
        )
        weather_provider = "open_meteo_archive"
    elif weather_api_provider == "openweather":
        if not openweather_api_key:
            raise RuntimeError(
                "weather_api_provider=openweather but openweather_api_key is missing. "
                "Set `openweather_api_key` (or env `VICROOT_OPENWEATHER_API_KEY`) "
                "or switch to weather_api_provider=open_meteo_archive for 2022-2025 history."
            )
        weather_rows = _fetch_openweather_daily_forecast(
            openweather_base_url=openweather_base_url,
            openweather_api_key=openweather_api_key,
            latitude=lat,
            longitude=lon,
            location_name=weather_location_name,
            snapshot_date=snapshot_date,
        )
        weather_provider = "openweather"
    else:
        raise RuntimeError(
            f"Unsupported weather_api_provider={weather_api_provider}. "
            "Supported: open_meteo_archive, openweather"
        )
    weather_jsonl_path = (
        f"{medallion_root}/bronze/weather_daily/source=api/snapshot_date={snapshot_date}/raw/weather_daily.jsonl"
    )
    local_weather_jsonl = _lakehouse_to_local(weather_jsonl_path)
    _ensure_parent_dir(local_weather_jsonl)
    if os.path.exists(local_weather_jsonl):
        os.remove(local_weather_jsonl)
    _write_jsonl(weather_jsonl_path, weather_rows)
    _write_json(
        f"{medallion_root}/bronze/weather_daily/source=api/snapshot_date={snapshot_date}/metadata/ingest_summary.json",
        {
            "dataset": "weather_daily",
            "source_type": "api",
            "provider": weather_provider,
            "record_count": len(weather_rows),
            "weather_jsonl_path": weather_jsonl_path,
            "lat": lat,
            "lon": lon,
            "pipeline_run_id": pipeline_run_id,
            "snapshot_date": snapshot_date,
            "ingested_at_utc": _utc_now_iso(),
        },
    )
    print(f"[WEATHER][API] wrote daily weather rows={len(weather_rows)} path={weather_jsonl_path}")
    watermark_rows.append(("weather_daily_api", pipeline_run_id, snapshot_date))


# %% [markdown]
# # Cell 7 - Watermark update and completion

_run_bronze_dq_tests(
    dq_fail_on_error=dq_fail_on_error,
    bronze_min_rows_per_year=bronze_min_rows_per_year,
    year_rows_by_source=year_rows_by_source,
    zip_mode_used=ingest_mode in {"zip_only", "zip_and_api", "zip_and_files"},
    zip_extracted_count=zip_extracted_count,
    watermark_count=len(watermark_rows),
)
_update_watermark_rows(rows=watermark_rows, watermark_table=watermark_table)
print("[DONE] Bronze ingestion complete.")
