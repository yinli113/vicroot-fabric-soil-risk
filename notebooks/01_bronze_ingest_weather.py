"""Bronze weather ingestion (domain-split)."""

from __future__ import annotations

import csv
import json
import os
import shutil
import ssl
import sys
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

NOTEBOOK_VERSION = "v2026-04-28-bronze-weather-split-01"
DEFAULT_KEY_VAULT_CONNECTION_ID = "5173c532-426a-498f-9730-90700afe13a6"

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
                "bronze_weather_root": "Files/medallion/bronze/bronze_weather_daily",
                "snapshot_date": datetime.now().strftime("%Y-%m-%d"),
                "pipeline_run_id": "",
                "watermark_table": "metadata.ingestion_watermarks",
                "weather_file_input_path": "",
                "weather_api_enabled": False,
                "weather_api_provider": "open_meteo_archive",
                "weather_years_csv": "2022,2023,2024,2025",
                "openweather_api_key": "",
                "openweather_base_url": "https://api.openweathermap.org",
                "use_key_vault_for_openweather": True,
                "key_vault_name": "",
                "key_vault_url": "",
                "key_vault_linked_service": "",
                "key_vault_connection_id": DEFAULT_KEY_VAULT_CONNECTION_ID,
                "openweather_secret_name": "openweather-api-key",
                "weather_location_name": "melbourne",
                "weather_latitude": "-37.8136",
                "weather_longitude": "144.9631",
                "site_file_input_path": "",
                "azure_tenant_id": "",
                "azure_client_id": "",
                "azure_client_secret": "",
            }
            if os.environ.get("VICROOT_PARAMS_JSON"):
                params.update(json.loads(os.environ["VICROOT_PARAMS_JSON"]))
            if os.environ.get("VICROOT_OPENWEATHER_API_KEY"):
                params["openweather_api_key"] = os.environ["VICROOT_OPENWEATHER_API_KEY"]
            for key, env_name in (
                ("azure_tenant_id", "AZURE_TENANT_ID"),
                ("azure_client_id", "AZURE_CLIENT_ID"),
                ("azure_client_secret", "AZURE_CLIENT_SECRET"),
                ("key_vault_name", "VICROOT_KEY_VAULT_NAME"),
                ("key_vault_url", "VICROOT_KEY_VAULT_URL"),
                ("key_vault_linked_service", "VICROOT_KEY_VAULT_LINKED_SERVICE"),
                ("key_vault_connection_id", "VICROOT_KEY_VAULT_CONNECTION_ID"),
                ("openweather_secret_name", "VICROOT_OPENWEATHER_SECRET_NAME"),
                ("weather_api_provider", "VICROOT_WEATHER_API_PROVIDER"),
                ("weather_years_csv", "VICROOT_WEATHER_YEARS_CSV"),
            ):
                if os.environ.get(env_name):
                    params[key] = os.environ[env_name]
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


def _write_jsonl(path: str, rows: List[Dict[str, Any]]) -> int:
    local = _lakehouse_to_local(path)
    _ensure_parent(local)
    with open(local, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=True))
            f.write("\n")
    return len(rows)


def _read_first_site_coordinates(path: str) -> Tuple[float, float] | None:
    local_path = _lakehouse_to_local(path)
    if not os.path.exists(local_path):
        return None
    with open(local_path, "r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            lat = row.get("latitude") or row.get("lat") or row.get("Latitude")
            lon = row.get("longitude") or row.get("lon") or row.get("Longitude")
            if lat and lon:
                try:
                    return float(lat), float(lon)
                except ValueError:
                    continue
    return None


def _http_get_json(url: str) -> Dict[str, Any]:
    req = urllib.request.Request(url=url)
    with urllib.request.urlopen(req, timeout=60, context=ssl.create_default_context()) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _http_post_form(url: str, form_data: Dict[str, str]) -> Dict[str, Any]:
    body = urllib.parse.urlencode(form_data).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60, context=ssl.create_default_context()) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _get_aad_token_with_sp(tenant_id: str, client_id: str, client_secret: str) -> str:
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = _http_post_form(
        token_url,
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": "https://vault.azure.net/.default",
            "grant_type": "client_credentials",
        },
    )
    token = payload.get("access_token", "")
    if not token:
        raise RuntimeError("Failed to obtain AAD token for Key Vault access.")
    return str(token)


def _get_secret_from_key_vault(
    *,
    key_vault_name: str,
    key_vault_url: str,
    secret_name: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
) -> str:
    if not key_vault_url:
        if not key_vault_name:
            raise RuntimeError("Key Vault is enabled but both key_vault_url and key_vault_name are empty.")
        key_vault_url = f"https://{key_vault_name}.vault.azure.net"
    access_token = _get_aad_token_with_sp(tenant_id, client_id, client_secret)
    secret_path = urllib.parse.quote(secret_name, safe="")
    secret_url = f"{key_vault_url.rstrip('/')}/secrets/{secret_path}?api-version=7.4"
    req = urllib.request.Request(
        url=secret_url,
        headers={"Authorization": f"Bearer {access_token}"},
    )
    with urllib.request.urlopen(req, timeout=60, context=ssl.create_default_context()) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    secret_value = payload.get("value", "")
    if not secret_value:
        raise RuntimeError(f"Secret `{secret_name}` not found or empty in Key Vault.")
    return str(secret_value)


def _get_secret_from_notebook_utils(
    *,
    key_vault_name: str,
    key_vault_url: str,
    key_vault_linked_service: str,
    key_vault_connection_id: str,
    secret_name: str,
) -> str:
    runtime_vars = globals()
    # Preferred path in Fabric when user has a working connection id.
    if "notebookutils" in runtime_vars and key_vault_connection_id:
        print(
            "[INFO] secret retrieval attempt: notebookutils.credentials.getSecretWithConnection "
            f"(connection_id={'set' if key_vault_connection_id else 'empty'}, secret_name={secret_name})."
        )
        creds = getattr(runtime_vars["notebookutils"], "credentials", None)
        getter = getattr(creds, "getSecretWithConnection", None) if creds is not None else None
        if getter is not None:
            try:
                val = getter(key_vault_connection_id, secret_name)
                if val:
                    print("[INFO] secret retrieval success via getSecretWithConnection.")
                    return str(val)
                print("[WARN] getSecretWithConnection returned an empty secret value.")
            except Exception as exc:  # noqa: BLE001
                print(f"[WARN] getSecretWithConnection failed: {type(exc).__name__}.")
        else:
            print("[WARN] getSecretWithConnection is unavailable on notebookutils.credentials.")
    elif key_vault_connection_id:
        print(
            "[WARN] notebookutils not available; cannot use getSecretWithConnection "
            f"for secret_name={secret_name}."
        )
    else:
        print("[INFO] secret retrieval path skipped: no key_vault_connection_id provided.")

    print("[INFO] secret retrieval fallback path: trying getSecret candidates.")
    candidates: List[Tuple[str, ...]] = []
    candidate_labels: List[str] = []
    if key_vault_linked_service:
        candidates.append((key_vault_linked_service, secret_name))
        candidate_labels.append(f"linked_service:{key_vault_linked_service}")
    if key_vault_name:
        candidates.append((key_vault_name, secret_name))
        candidate_labels.append(f"key_vault_name:{key_vault_name}")
    if key_vault_url:
        candidates.append((key_vault_url, secret_name))
        candidate_labels.append("key_vault_url")
    elif key_vault_name:
        candidates.append((f"https://{key_vault_name}.vault.azure.net/", secret_name))
        candidate_labels.append("derived_key_vault_url")

    if not candidates:
        print("[INFO] secret retrieval fallback skipped: no getSecret candidate args were configured.")
        return ""

    def _try_get_secret(getter: Any, args: Tuple[str, ...], getter_name: str, candidate_label: str) -> str:
        print(f"[INFO] secret retrieval attempt: {getter_name} using {candidate_label}.")
        try:
            val = getter(*args)
            if val:
                print(f"[INFO] secret retrieval success via {getter_name} ({candidate_label}).")
                return str(val)
            print(f"[WARN] {getter_name} returned empty value for {candidate_label}.")
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] {getter_name} failed for {candidate_label}: {type(exc).__name__}.")
        return ""

    # mssparkutils style
    if "mssparkutils" in runtime_vars:
        creds = getattr(runtime_vars["mssparkutils"], "credentials", None)
        getter = getattr(creds, "getSecret", None) if creds is not None else None
        if getter is not None:
            for args, label in zip(candidates, candidate_labels):
                val = _try_get_secret(getter, args, "mssparkutils.credentials.getSecret", label)
                if val:
                    return val
        else:
            print("[INFO] mssparkutils.credentials.getSecret unavailable.")

    # notebookutils style
    if "notebookutils" in runtime_vars:
        creds = getattr(runtime_vars["notebookutils"], "credentials", None)
        getter = getattr(creds, "getSecret", None) if creds is not None else None
        if getter is not None:
            for args, label in zip(candidates, candidate_labels):
                val = _try_get_secret(getter, args, "notebookutils.credentials.getSecret", label)
                if val:
                    return val
        else:
            print("[INFO] notebookutils.credentials.getSecret unavailable.")
    print("[WARN] secret retrieval failed via all notebook utils paths.")
    return ""


def _fetch_openweather_daily(
    *,
    base_url: str,
    api_key: str,
    latitude: float,
    longitude: float,
    location_name: str,
    snapshot_date: str,
) -> List[Dict[str, Any]]:
    q = urllib.parse.urlencode({"lat": latitude, "lon": longitude, "appid": api_key, "units": "metric"})
    url = f"{base_url.rstrip('/')}/data/2.5/forecast?{q}"
    payload = _http_get_json(url)
    daily: Dict[str, Dict[str, float]] = {}
    for row in payload.get("list", []):
        dt_txt = str(row.get("dt_txt", ""))
        day = dt_txt[:10]
        if not day:
            continue
        main = row.get("main", {}) or {}
        wind = row.get("wind", {}) or {}
        rain = row.get("rain", {}) or {}
        bucket = daily.setdefault(day, {"n": 0.0, "t_sum": 0.0, "t_min": 999.0, "t_max": -999.0, "h_sum": 0.0, "w_sum": 0.0, "r_sum": 0.0})
        t = float(main.get("temp", 0.0))
        h = float(main.get("humidity", 0.0))
        w = float(wind.get("speed", 0.0))
        r = float(rain.get("3h", 0.0))
        bucket["n"] += 1.0
        bucket["t_sum"] += t
        bucket["h_sum"] += h
        bucket["w_sum"] += w
        bucket["r_sum"] += r
        bucket["t_min"] = min(bucket["t_min"], t)
        bucket["t_max"] = max(bucket["t_max"], t)
    out: List[Dict[str, Any]] = []
    for day in sorted(daily):
        b = daily[day]
        n = max(1.0, b["n"])
        out.append(
            {
                "as_of_date": day,
                "location_name": location_name,
                "provider": "openweather",
                "rainfall_mm": b["r_sum"],
                "temp_avg_c": b["t_sum"] / n,
                "temp_min_c": b["t_min"],
                "temp_max_c": b["t_max"],
                "humidity_avg_pct": b["h_sum"] / n,
                "wind_speed_avg_ms": b["w_sum"] / n,
                "evap_mm": None,
                "snapshot_date": snapshot_date,
                "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            }
        )
    return out


def _parse_years(years_csv: str) -> List[int]:
    years = []
    for item in years_csv.split(","):
        item = item.strip()
        if not item:
            continue
        years.append(int(item))
    if not years:
        raise ValueError("weather_years_csv/readings_years_csv is empty; expected like 2022,2023,2024,2025")
    return sorted(set(years))


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
            # Humidity is available as hourly in archive API; aggregate to daily mean.
            "hourly": "relative_humidity_2m",
            "timezone": "Australia/Melbourne",
        }
    )
    url = f"{base_url}?{query}"
    payload = _http_get_json(url)
    daily = payload.get("daily", {}) or {}
    dates = daily.get("time", []) or []
    temp_max = daily.get("temperature_2m_max", []) or []
    temp_min = daily.get("temperature_2m_min", []) or []
    temp_mean = daily.get("temperature_2m_mean", []) or []
    precip = daily.get("precipitation_sum", []) or []
    wind = daily.get("wind_speed_10m_mean", []) or []
    evap = daily.get("et0_fao_evapotranspiration", []) or []

    # Build daily humidity averages from hourly series when available.
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
    n = len(dates)
    out: List[Dict[str, Any]] = []
    for i in range(n):
        out.append(
            {
                "as_of_date": str(dates[i]),
                "location_name": location_name,
                "provider": "open_meteo_archive",
                "rainfall_mm": float(precip[i]) if i < len(precip) and precip[i] is not None else None,
                "temp_avg_c": float(temp_mean[i]) if i < len(temp_mean) and temp_mean[i] is not None else None,
                "temp_min_c": float(temp_min[i]) if i < len(temp_min) and temp_min[i] is not None else None,
                "temp_max_c": float(temp_max[i]) if i < len(temp_max) and temp_max[i] is not None else None,
                "humidity_avg_pct": (
                    (humidity_by_day[str(dates[i])][0] / humidity_by_day[str(dates[i])][1])
                    if str(dates[i]) in humidity_by_day and humidity_by_day[str(dates[i])][1] > 0
                    else None
                ),
                "wind_speed_avg_ms": float(wind[i]) if i < len(wind) and wind[i] is not None else None,
                "evap_mm": float(evap[i]) if i < len(evap) and evap[i] is not None else None,
                "snapshot_date": snapshot_date,
                "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
            }
        )
    return out


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


def _runtime_param_overrides() -> Dict[str, Any]:
    """Read notebook activity base parameters injected as runtime variables.

    Fabric notebook activity often injects base parameters as top-level variables
    in the notebook runtime. Merge them so pipeline values override defaults.
    """
    tracked_keys = [
        "medallion_root",
        "bronze_weather_root",
        "snapshot_date",
        "pipeline_run_id",
        "watermark_table",
        "weather_file_input_path",
        "weather_api_enabled",
        "weather_api_provider",
        "weather_years_csv",
        "openweather_api_key",
        "use_key_vault_for_openweather",
        "key_vault_name",
        "key_vault_url",
        "key_vault_linked_service",
        "key_vault_connection_id",
        "openweather_secret_name",
        "azure_tenant_id",
        "azure_client_id",
        "azure_client_secret",
        "openweather_base_url",
        "weather_location_name",
        "weather_latitude",
        "weather_longitude",
        "site_file_input_path",
    ]
    arg_aliases: Dict[str, List[str]] = {
        "openweather_api_key": ["p_openweather_api_key", "openweather-api-key", "VICROOT_OPENWEATHER_API_KEY"],
        "key_vault_name": ["key-vault-name", "VICROOT_KEY_VAULT_NAME"],
        "key_vault_url": ["key-vault-url", "VICROOT_KEY_VAULT_URL"],
        "key_vault_linked_service": ["key-vault-linked-service", "VICROOT_KEY_VAULT_LINKED_SERVICE"],
        "key_vault_connection_id": [
            "p_key_vault_connection_id",
            "key-vault-connection-id",
            "keyVaultConnectionId",
            "VICROOT_KEY_VAULT_CONNECTION_ID",
        ],
        "openweather_secret_name": [
            "p_openweather_secret_name",
            "openweather-secret-name",
            "openWeatherSecretName",
            "weather-secret-name",
            "VICROOT_OPENWEATHER_SECRET_NAME",
        ],
        "azure_tenant_id": ["AZURE_TENANT_ID", "tenant_id", "azure-tenant-id"],
        "azure_client_id": ["AZURE_CLIENT_ID", "client_id", "azure-client-id"],
        "azure_client_secret": ["AZURE_CLIENT_SECRET", "client_secret", "azure-client-secret"],
        "use_key_vault_for_openweather": ["use-key-vault-for-openweather"],
        "weather_api_enabled": ["weather-api-enabled"],
        "weather_api_provider": ["weather-api-provider", "p_weather_api_provider", "VICROOT_WEATHER_API_PROVIDER"],
        "weather_years_csv": ["weather-years-csv", "p_weather_years_csv", "VICROOT_WEATHER_YEARS_CSV"],
        "weather_latitude": ["weather-latitude", "lat"],
        "weather_longitude": ["weather-longitude", "lon", "lng"],
        "weather_file_input_path": ["weather-file-input-path"],
    }
    overrides: Dict[str, Any] = {}
    runtime_vars = globals()
    def _is_blank(value: Any) -> bool:
        return value is None or (isinstance(value, str) and value.strip() == "")

    for key in tracked_keys:
        if key in runtime_vars and not _is_blank(runtime_vars[key]):
            overrides[key] = runtime_vars[key]

    def _read_arg_from_utils(arg_name: str) -> Any:
        # Fabric/Synapse style helpers may be available under different globals.
        if "mssparkutils" in runtime_vars:
            try:
                return runtime_vars["mssparkutils"].notebook.getArgument(arg_name, "")
            except Exception:
                pass
        if "notebookutils" in runtime_vars:
            nb = getattr(runtime_vars["notebookutils"], "notebook", None)
            if nb is not None:
                for getter in ("getArgument", "get"):
                    if hasattr(nb, getter):
                        try:
                            return getattr(nb, getter)(arg_name, "")
                        except Exception:
                            pass
        return ""

    # Merge args from notebook utils when provided.
    for key in tracked_keys:
        if key in overrides and not _is_blank(overrides[key]):
            continue
        arg_val = _read_arg_from_utils(key)
        if not _is_blank(arg_val):
            overrides[key] = arg_val
            continue
        for alias in arg_aliases.get(key, []):
            alias_val = _read_arg_from_utils(alias)
            if not _is_blank(alias_val):
                overrides[key] = alias_val
                break

    # Friendly alias support when pipeline param is named with "p_" prefix.
    if "p_openweather_api_key" in runtime_vars and "openweather_api_key" not in overrides:
        if not _is_blank(runtime_vars["p_openweather_api_key"]):
            overrides["openweather_api_key"] = runtime_vars["p_openweather_api_key"]
    elif "openweather_api_key" not in overrides:
        alias_val = _read_arg_from_utils("p_openweather_api_key")
        if not _is_blank(alias_val):
            overrides["openweather_api_key"] = alias_val
    if "openweather_api_key" not in overrides and "openweather-api-key" in runtime_vars:
        if not _is_blank(runtime_vars["openweather-api-key"]):
            overrides["openweather_api_key"] = runtime_vars["openweather-api-key"]
    return overrides


params = resolve_params(_runtime_param_overrides())
print(f"[INFO] notebook_version={NOTEBOOK_VERSION}")

medallion_root = params.get("medallion_root", "Files/medallion").rstrip("/")
bronze_weather_root = params.get("bronze_weather_root", f"{medallion_root}/bronze/bronze_weather_daily").rstrip("/")
snapshot_date = params.get("snapshot_date", datetime.now().strftime("%Y-%m-%d"))
pipeline_run_id = params.get("pipeline_run_id", f"manual-{snapshot_date}")
watermark_table = params.get("watermark_table", "metadata.ingestion_watermarks")

weather_file_input_path = params.get("weather_file_input_path", "").strip()
weather_api_enabled = _parse_bool(params.get("weather_api_enabled", False), False)
weather_api_provider = str(params.get("weather_api_provider", "open_meteo_archive")).strip().lower()
weather_years_csv = str(params.get("weather_years_csv", params.get("readings_years_csv", "2022,2023,2024,2025"))).strip()
openweather_api_key = params.get("openweather_api_key", "").strip()
use_key_vault_for_openweather = _parse_bool(params.get("use_key_vault_for_openweather", True), True)
key_vault_name = str(params.get("key_vault_name", "")).strip()
key_vault_url = str(params.get("key_vault_url", "")).strip()
key_vault_linked_service = str(params.get("key_vault_linked_service", "")).strip()
key_vault_connection_id = str(params.get("key_vault_connection_id", "")).strip()
if not key_vault_connection_id and DEFAULT_KEY_VAULT_CONNECTION_ID:
    key_vault_connection_id = DEFAULT_KEY_VAULT_CONNECTION_ID
    print("[INFO] using default key_vault_connection_id because none was provided.")
openweather_secret_name = str(params.get("openweather_secret_name", "openweather-api-key")).strip()
azure_tenant_id = str(params.get("azure_tenant_id", "")).strip()
azure_client_id = str(params.get("azure_client_id", "")).strip()
azure_client_secret = str(params.get("azure_client_secret", "")).strip()
openweather_base_url = params.get("openweather_base_url", "https://api.openweathermap.org").strip()
weather_location_name = params.get("weather_location_name", "melbourne")
site_file_input_path = params.get("site_file_input_path", "").strip()
lat_str = str(params.get("weather_latitude", "")).strip()
lon_str = str(params.get("weather_longitude", "")).strip()

watermarks: List[Tuple[str, str, str]] = []

# Convenience behavior: if API key is present and no file source is configured,
# auto-enable API ingestion so Fabric runs don't silently no-op.
if not weather_api_enabled and not weather_file_input_path and openweather_api_key:
    weather_api_enabled = True
    print("[INFO] auto-enabled weather API ingestion because openweather_api_key is set.")

# Optional direct Key Vault retrieval (without pipeline secret mapping).
if not openweather_api_key and use_key_vault_for_openweather:
    # 1) Try direct Fabric utils retrieval first.
    kv_utils_secret = _get_secret_from_notebook_utils(
        key_vault_name=key_vault_name,
        key_vault_url=key_vault_url,
        key_vault_linked_service=key_vault_linked_service,
        key_vault_connection_id=key_vault_connection_id,
        secret_name=openweather_secret_name,
    )
    if kv_utils_secret:
        openweather_api_key = kv_utils_secret
        print("[INFO] openweather_api_key retrieved via notebook utils secret API.")
        if not weather_api_enabled and not weather_file_input_path:
            weather_api_enabled = True
            print("[INFO] auto-enabled weather API ingestion because Key Vault secret was loaded.")
    # 2) Fallback to service principal REST retrieval.
    elif azure_tenant_id and azure_client_id and azure_client_secret and (key_vault_name or key_vault_url):
        try:
            openweather_api_key = _get_secret_from_key_vault(
                key_vault_name=key_vault_name,
                key_vault_url=key_vault_url,
                secret_name=openweather_secret_name,
                tenant_id=azure_tenant_id,
                client_id=azure_client_id,
                client_secret=azure_client_secret,
            )
            print("[INFO] openweather_api_key retrieved from Azure Key Vault.")
            if not weather_api_enabled and not weather_file_input_path:
                weather_api_enabled = True
                print("[INFO] auto-enabled weather API ingestion because Key Vault secret was loaded.")
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] Key Vault secret load failed: {exc}")
    else:
        print(
            "[WARN] Key Vault retrieval requested, but service-principal fallback is not configured "
            "(requires azure_tenant_id, azure_client_id, azure_client_secret, and key_vault_name or key_vault_url)."
        )

print(
    f"[INFO] weather inputs: weather_file_input_path={'set' if weather_file_input_path else 'empty'}, "
    f"weather_api_enabled={weather_api_enabled}, "
    f"weather_api_provider={weather_api_provider}, "
    f"weather_years_csv={weather_years_csv}, "
    f"openweather_api_key={'set' if openweather_api_key else 'empty'}; "
    f"runtime_overrides={','.join(sorted(_runtime_param_overrides().keys())) or 'none'}"
)
if not weather_file_input_path and not weather_api_enabled:
    raise RuntimeError(
        "No weather ingestion source configured. Set one of:\n"
        "1) weather_file_input_path=Files/raw/weather/<file>.csv\n"
        "2) weather_api_enabled=true (with openweather_api_key and coordinates)"
    )

if weather_file_input_path:
    filename = os.path.basename(weather_file_input_path)
    out_path = f"{bronze_weather_root}/source=file/snapshot_date={snapshot_date}/raw/{filename}"
    _copy_file(weather_file_input_path, out_path)
    _write_json(
        f"{bronze_weather_root}/source=file/snapshot_date={snapshot_date}/metadata/ingest_summary.json",
        {"source_type": "file", "source_file": weather_file_input_path, "bronze_file": out_path, "snapshot_date": snapshot_date},
    )
    print(f"[FILE] weather source copied: {out_path}")
    watermarks.append(("bronze_weather_file", pipeline_run_id, snapshot_date))

if weather_api_enabled:
    lat = float(lat_str) if lat_str else None
    lon = float(lon_str) if lon_str else None
    if (lat is None or lon is None) and site_file_input_path:
        inferred = _read_first_site_coordinates(site_file_input_path)
        if inferred:
            lat, lon = inferred
            print(f"[INFO] inferred weather coordinates from site file: lat={lat}, lon={lon}")
    if lat is None or lon is None:
        # Final fallback defaults to Melbourne CBD if neither params nor site file provide coordinates.
        lat = -37.8136
        lon = 144.9631
        print("[WARN] weather coordinates not provided; defaulting to Melbourne (-37.8136, 144.9631).")

    if weather_api_provider in {"open_meteo_archive", "openmeteo", "open-meteo"}:
        years = _parse_years(weather_years_csv)
        start_date = f"{min(years)}-01-01"
        end_date = f"{max(years)}-12-31"
        print(f"[INFO] fetching historical weather via open-meteo archive for {start_date} to {end_date}")
        rows = _fetch_open_meteo_archive_daily(
            latitude=lat,
            longitude=lon,
            location_name=weather_location_name,
            snapshot_date=snapshot_date,
            start_date=start_date,
            end_date=end_date,
        )
    else:
        if not openweather_api_key:
            raise RuntimeError(
                "weather_api_provider is openweather but openweather_api_key is missing. "
                "Set openweather_api_key or switch weather_api_provider=open_meteo_archive for 2022-2025 history."
            )
        rows = _fetch_openweather_daily(
            base_url=openweather_base_url,
            api_key=openweather_api_key,
            latitude=lat,
            longitude=lon,
            location_name=weather_location_name,
            snapshot_date=snapshot_date,
        )
    out_path = f"{bronze_weather_root}/source=api/snapshot_date={snapshot_date}/raw/weather_daily.jsonl"
    _write_jsonl(out_path, rows)
    _write_json(
        f"{bronze_weather_root}/source=api/snapshot_date={snapshot_date}/metadata/ingest_summary.json",
        {"source_type": "api", "provider": "openweather", "row_count": len(rows), "bronze_file": out_path, "snapshot_date": snapshot_date},
    )
    print(f"[API] weather rows fetched: {len(rows)}")
    watermarks.append(("bronze_weather_api", pipeline_run_id, snapshot_date))

if not watermarks:
    raise RuntimeError(
        "Weather notebook completed without writing any source rows. "
        "Check weather_file_input_path or weather API parameters."
    )

_update_watermarks(watermarks, watermark_table, snapshot_date)
print("[DONE] Bronze weather ingestion complete.")

