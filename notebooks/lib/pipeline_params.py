"""Resolve notebook parameters from Fabric pipeline, env, or local defaults."""

from __future__ import annotations

import json
import os
from typing import Any, Dict

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

DEFAULT_PARAMS: Dict[str, Any] = {
    "environment": "dev",
    "lakehouse_name": "lh_vicroot_melbourne",
    "medallion_root": "Files/medallion",
    "api_base_url_com": "https://data.melbourne.vic.gov.au",
    "api_base_url_soil": "",
    "api_base_url_bom": "",
    "bom_station_id": "",
    "source_crs": "EPSG:4326",
    "target_crs": "EPSG:7855",
    "snapshot_date": "2026-04-24",
    "pipeline_run_id": "",
    # Bronze soil sensor ingestion controls
    # Modes: zip_only | files_only | api_only | zip_and_files | zip_and_api
    # Alias: "local" | "local_files" | "files_and_zip" -> zip_and_files (2022 zip + yearly CSVs, no API)
    "bronze_ingest_mode": "zip_and_files",
    "readings_years_csv": "2022,2023,2024,2025",
    "com_page_size": 100,
    "time_window_minutes": 60,
    "resume_from_utc": "",
    "watermark_table": "metadata.ingestion_watermarks",
    "com_dataset_soil_readings": "soil-sensor-readings-historical-data",
    "com_dataset_soil_locations": "soil-sensor-locations",
    "soil_zip_input_path": "Files/medallion/bronze/com_soil_sensor_readings/source=historical/year=2022/raw/zip/soil_sensor_readings_historical_2022.zip",
    "soil_zip_filename_slug": "soil_sensor_readings_historical_2022.zip",
    "soil_csv_input_dir": "Files/medallion/bronze/com_soil_sensor_readings",
    "soil_csv_file_pattern": "soil-sensor-readings-historical-data-{year}.csv",
    # Optional extra file ingests (uploaded into Lakehouse Files)
    "site_file_input_path": "",
    "weather_file_input_path": "",
    "weather_api_enabled": False,
    "weather_api_provider": "open_meteo_archive",
    "weather_years_csv": "2022,2023,2024,2025",
    "openweather_base_url": "https://api.openweathermap.org",
    "use_key_vault_for_openweather": True,
    "key_vault_name": "",
    "key_vault_url": "",
    "key_vault_linked_service": "",
    "key_vault_connection_id": "5173c532-426a-498f-9730-90700afe13a6",
    "openweather_secret_name": "openweather-api-key",
    "weather_latitude": "-37.8136",
    "weather_longitude": "144.9631",
    "weather_location_name": "melbourne",
    "dq_fail_on_error": True,
    "bronze_strict_input_guard": True,
    "bronze_dq_min_rows_per_year": 1,
    "silver_dq_max_quarantine_ratio": 0.9,
    "validation_rules_path": "Files/config/ge_validation_rules.json",
    "silver_records_table": "silver_soil_sensor_readings",
    "silver_quarantine_table": "silver_soil_sensor_quarantine",
    "silver_locations_table": "silver_soil_sensor_locations",
    "silver_site_file_table": "silver_site_file_reference",
    "silver_weather_table": "silver_weather_daily",
    "silver_weather_allow_empty_write": False,
    # Gold star schema (see notebooks/03_gold_features.py) — moisture/temp-led site profile; salinity secondary
    "gold_dim_site_table": "gold_dim_site",
    "gold_dim_depth_table": "gold_dim_depth",
    "gold_dim_weather_daily_table": "gold_dim_weather_daily",
    "gold_fact_salinity_depth_daily_table": "gold_fact_salinity_depth_daily",
    "gold_fact_moisture_depth_daily_table": "gold_fact_moisture_depth_daily",
    "gold_fact_temperature_depth_daily_table": "gold_fact_temperature_depth_daily",
    "gold_fact_irrigation_risk_table": "gold_fact_irrigation_risk",
    "gold_fact_soil_depth_peer_daily_table": "gold_fact_soil_depth_peer_daily",
    "gold_site_depth_peer_summary_table": "gold_site_depth_peer_summary",
    # ML baseline (see notebooks/05_ml_moisture_baseline.py) — same lakehouse Delta by default
    "gold_ml_moisture_baseline_table": "gold_ml_moisture_baseline",
    "ml_target_depth_cm": 30,
    "ml_train_end_date": "2023-12-31",
    "ml_rolling_days": 7,
    "ml_min_train_rows": 80,
    "ml_model_id": "linreg_weather_lags_v1",
    # Moisture floors for EC interpretability (sensor EC less reliable when soil is very dry).
    "gold_ec_moisture_min_shallow_vwc": 25.0,
    "gold_ec_moisture_min_deep_vwc": 18.0,
    "gold_ec_moisture_shallow_depth_max_cm": 30,
    "gold_peer_min_sites": 5,
    "weather_daily_table": "silver_weather_daily",
    "weather_date_col": "as_of_date",
    "weather_rainfall_col": "rainfall_mm",
    "weather_evap_col": "evap_mm",
    # Populated from .env / process env (never log these)
    "com_app_token": "",
    "soil_api_key": "",
    "soil_api_subscription_key": "",
    "bom_api_key": "",
    "openweather_api_key": "",
    "azure_tenant_id": "",
    "azure_client_id": "",
    "azure_client_secret": "",
}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _merge_env_secrets(params: Dict[str, Any]) -> Dict[str, Any]:
    """Overlay secrets from environment (see .env.example)."""
    env_map = {
        "com_app_token": "VICROOT_COM_APP_TOKEN",
        "soil_api_key": "VICROOT_SOIL_API_KEY",
        "soil_api_subscription_key": "VICROOT_SOIL_API_SUBSCRIPTION_KEY",
        "bom_api_key": "VICROOT_BOM_API_KEY",
        "openweather_api_key": "VICROOT_OPENWEATHER_API_KEY",
        "azure_tenant_id": "AZURE_TENANT_ID",
        "azure_client_id": "AZURE_CLIENT_ID",
        "azure_client_secret": "AZURE_CLIENT_SECRET",
        "key_vault_name": "VICROOT_KEY_VAULT_NAME",
        "key_vault_url": "VICROOT_KEY_VAULT_URL",
        "key_vault_linked_service": "VICROOT_KEY_VAULT_LINKED_SERVICE",
        "key_vault_connection_id": "VICROOT_KEY_VAULT_CONNECTION_ID",
        "openweather_secret_name": "VICROOT_OPENWEATHER_SECRET_NAME",
    }
    for key, env_name in env_map.items():
        val = os.environ.get(env_name)
        if val:
            params[key] = val
    # Optional URL overrides for local testing
    for param_key, env_name in (
        ("api_base_url_com", "VICROOT_API_BASE_URL_COM"),
        ("api_base_url_soil", "VICROOT_API_BASE_URL_SOIL"),
        ("api_base_url_bom", "VICROOT_API_BASE_URL_BOM"),
    ):
        val = os.environ.get(env_name)
        if val:
            params[param_key] = val
    return params


_BRONZE_INGEST_MODE_ALIASES = {
    "local": "zip_and_files",
    "local_files": "zip_and_files",
    "files_and_zip": "zip_and_files",
}


def _normalize_bronze_ingest_mode(params: Dict[str, Any]) -> None:
    mode = str(params.get("bronze_ingest_mode", "")).strip().lower()
    if mode in _BRONZE_INGEST_MODE_ALIASES:
        params["bronze_ingest_mode"] = _BRONZE_INGEST_MODE_ALIASES[mode]


def resolve_params(overrides: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Merge defaults with VICROOT_PARAMS_JSON, env secrets, and caller overrides.

    Fabric: pass overrides from pipeline notebook activity.
    """
    params = dict(DEFAULT_PARAMS)
    params = _merge_env_secrets(params)
    raw = os.environ.get("VICROOT_PARAMS_JSON")
    if raw:
        params = _deep_merge(params, json.loads(raw))
    if overrides:
        params = _deep_merge(params, overrides)
    _normalize_bronze_ingest_mode(params)
    return params
