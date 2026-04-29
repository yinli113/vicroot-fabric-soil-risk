"""Silver weather cleaning (domain-split)."""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

NOTEBOOK_VERSION = "v2026-04-28-silver-weather-split-01"

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
        def resolve_params(overrides: dict | None = None) -> dict:
            params = {
                "medallion_root": "Files/medallion",
                "bronze_weather_root": "Files/medallion/bronze/bronze_weather_daily",
                "snapshot_date": datetime.now().strftime("%Y-%m-%d"),
                "weather_file_input_path": "",
                "silver_weather_table": "silver_weather_daily",
            }
            if os.environ.get("VICROOT_PARAMS_JSON"):
                params.update(json.loads(os.environ["VICROOT_PARAMS_JSON"]))
            if overrides:
                params.update(overrides)
            return params


def _lakehouse_to_local(path: str) -> str:
    if path.startswith("/lakehouse/"):
        return path
    if path.startswith("Files/"):
        return f"/lakehouse/default/{path}"
    return path


def _read_json_if_exists(spark: SparkSession, path: str) -> DataFrame | None:
    for candidate in [path, _lakehouse_to_local(path)]:
        try:
            df = spark.read.json(candidate)
            if set(df.columns) == {"_corrupt_record"}:
                continue
            return df
        except Exception:
            pass
    return None


def _read_csv_if_exists(spark: SparkSession, path: str) -> DataFrame | None:
    for candidate in [path, _lakehouse_to_local(path)]:
        try:
            best_df = None
            best_col_count = -1
            for sep in [",", ";", "\t", "|"]:
                df = spark.read.option("header", "true").option("sep", sep).csv(candidate)
                col_count = len(df.columns)
                if col_count > best_col_count:
                    best_df = df
                    best_col_count = col_count
            if best_df is None or not best_df.columns:
                continue
            if len(best_df.columns) == 1 and any(d in best_df.columns[0] for d in [";", ",", "\t", "|"]):
                continue
            return best_df
        except Exception:
            pass
    return None


def _sanitize_column_name(name: str) -> str:
    sanitized = re.sub(r"[ ,;{}()\n\t=]+", "_", name.strip())
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    return sanitized or "col"


def _sanitize_df_columns(df: DataFrame) -> DataFrame:
    out = df
    used: Dict[str, int] = {}
    for old in out.columns:
        base = _sanitize_column_name(old)
        idx = used.get(base, 0)
        new = base if idx == 0 else f"{base}_{idx}"
        while new in used:
            idx += 1
            new = f"{base}_{idx}"
        used[base] = idx + 1
        used[new] = 1
        if new != old:
            out = out.withColumnRenamed(old, new)
    return out


def _snapshot_candidates(source_base_dir: str, requested_snapshot: str) -> List[str]:
    candidates: List[str] = [requested_snapshot]
    local = _lakehouse_to_local(source_base_dir)
    if not os.path.isdir(local):
        return candidates
    discovered: List[str] = []
    for name in os.listdir(local):
        if not name.startswith("snapshot_date="):
            continue
        snap = name.split("=", 1)[1].strip()
        if snap:
            discovered.append(snap)
    for snap in sorted(set(discovered), reverse=True):
        if snap not in candidates:
            candidates.append(snap)
    return candidates


def _col_or_null(df: DataFrame, *candidates: str):
    for name in candidates:
        if name in df.columns:
            return F.col(name)
    return F.lit(None)


def _normalize_weather(df: DataFrame, snapshot_date: str) -> DataFrame:
    return (
        df.withColumn("as_of_date", F.to_date(F.coalesce(_col_or_null(df, "as_of_date", "date", "Date"), F.lit(None))))
        .withColumn("location_name", F.coalesce(_col_or_null(df, "location_name"), F.lit("melbourne")).cast("string"))
        .withColumn("provider", F.coalesce(_col_or_null(df, "provider"), F.lit("file")).cast("string"))
        .withColumn(
            "rainfall_mm",
            F.coalesce(_col_or_null(df, "rainfall_mm", "rain_mm", "precipitation_mm"), F.lit(None)).cast("double"),
        )
        .withColumn("evap_mm", F.coalesce(_col_or_null(df, "evap_mm", "evaporation_mm", "et_mm"), F.lit(None)).cast("double"))
        .withColumn(
            "temp_avg_c",
            F.coalesce(_col_or_null(df, "temp_avg_c", "temperature_avg_c", "temp_c"), F.lit(None)).cast("double"),
        )
        .withColumn("temp_min_c", F.coalesce(_col_or_null(df, "temp_min_c", "temperature_min_c"), F.lit(None)).cast("double"))
        .withColumn("temp_max_c", F.coalesce(_col_or_null(df, "temp_max_c", "temperature_max_c"), F.lit(None)).cast("double"))
        .withColumn("humidity_avg_pct", F.coalesce(_col_or_null(df, "humidity_avg_pct", "humidity_pct"), F.lit(None)).cast("double"))
        .withColumn("wind_speed_avg_ms", F.coalesce(_col_or_null(df, "wind_speed_avg_ms", "wind_speed_ms"), F.lit(None)).cast("double"))
        .withColumn("snapshot_date", F.coalesce(_col_or_null(df, "snapshot_date"), F.lit(snapshot_date)).cast("string"))
        .dropDuplicates(["as_of_date", "location_name", "provider"])
    )


def _runtime_param_overrides() -> Dict[str, str]:
    tracked_keys = [
        "medallion_root",
        "bronze_weather_root",
        "snapshot_date",
        "weather_file_input_path",
        "silver_weather_table",
    ]
    aliases = {
        "medallion_root": ["p_medallion_root"],
        "bronze_weather_root": ["p_bronze_weather_root"],
        "snapshot_date": ["p_snapshot_date"],
        "weather_file_input_path": ["p_weather_file_input_path"],
        "silver_weather_table": ["p_silver_weather_table"],
    }
    runtime_vars = globals()

    def _is_blank(value: object) -> bool:
        return value is None or (isinstance(value, str) and value.strip() == "")

    def _read_arg_from_utils(arg_name: str) -> str:
        if "mssparkutils" in runtime_vars:
            try:
                return str(runtime_vars["mssparkutils"].notebook.getArgument(arg_name, ""))
            except Exception:
                pass
        if "notebookutils" in runtime_vars:
            nb = getattr(runtime_vars["notebookutils"], "notebook", None)
            if nb is not None:
                for getter in ("getArgument", "get"):
                    if hasattr(nb, getter):
                        try:
                            return str(getattr(nb, getter)(arg_name, ""))
                        except Exception:
                            pass
        return ""

    out: Dict[str, str] = {}
    for key in tracked_keys:
        if key in runtime_vars and not _is_blank(runtime_vars[key]):
            out[key] = str(runtime_vars[key]).strip()
            continue
        val = _read_arg_from_utils(key)
        if not _is_blank(val):
            out[key] = val.strip()
            continue
        for alias in aliases.get(key, []):
            alias_val = _read_arg_from_utils(alias)
            if not _is_blank(alias_val):
                out[key] = alias_val.strip()
                break
    return out


spark = SparkSession.builder.getOrCreate()
params = resolve_params(_runtime_param_overrides())
print(f"[INFO] notebook_version={NOTEBOOK_VERSION}")

medallion_root = params.get("medallion_root", "Files/medallion").rstrip("/")
bronze_weather_root = params.get("bronze_weather_root", f"{medallion_root}/bronze/bronze_weather_daily").rstrip("/")
legacy_weather_root = f"{medallion_root}/bronze/weather_daily"
snapshot_date = params.get("snapshot_date", "2026-04-28")
weather_file_input_path = params.get("weather_file_input_path", "").strip()
silver_weather_table = params.get("silver_weather_table", "silver_weather_daily")
silver_weather_allow_empty_write = str(params.get("silver_weather_allow_empty_write", "false")).strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}

print(
    f"[INFO] weather silver params: bronze_weather_root={bronze_weather_root}, "
    f"legacy_weather_root={legacy_weather_root}, snapshot_date={snapshot_date}, "
    f"silver_weather_table={silver_weather_table}, weather_file_input_path={'set' if weather_file_input_path else 'empty'}"
)

weather_dfs: List[DataFrame] = []

if weather_file_input_path:
    file_name = os.path.basename(weather_file_input_path)
    loaded_from_bronze_file = False
    for root in [bronze_weather_root, legacy_weather_root]:
        for snap in _snapshot_candidates(f"{root}/source=file", snapshot_date):
            candidate = f"{root}/source=file/snapshot_date={snap}/raw/{file_name}"
            df = _read_csv_if_exists(spark, candidate)
            if df is None:
                df = _read_json_if_exists(spark, candidate)
            if df is not None:
                print(f"[INFO] loaded weather file source from: {candidate}")
                weather_dfs.append(df)
                loaded_from_bronze_file = True
                break
        if loaded_from_bronze_file:
            break
    if not loaded_from_bronze_file:
        df = _read_csv_if_exists(spark, weather_file_input_path)
        if df is None:
            df = _read_json_if_exists(spark, weather_file_input_path)
        if df is not None:
            print(f"[WARN] loading weather directly from raw path: {weather_file_input_path}")
            weather_dfs.append(df)
else:
    common_raw_weather_candidates = [
        "Files/raw/weather/weather-daily.csv",
        "File/raw/weather/weather-daily.csv",
        "Files/raw/weather/openweather_daily.csv",
        "File/raw/weather/openweather_daily.csv",
    ]
    for candidate in common_raw_weather_candidates:
        df = _read_csv_if_exists(spark, candidate)
        if df is None:
            df = _read_json_if_exists(spark, candidate)
        if df is not None:
            print(f"[WARN] weather_file_input_path not set; using discovered raw weather file: {candidate}")
            weather_dfs.append(df)
            break

for root in [bronze_weather_root, legacy_weather_root]:
    api_loaded = False
    for snap in _snapshot_candidates(f"{root}/source=api", snapshot_date):
        candidate = f"{root}/source=api/snapshot_date={snap}/raw/weather_daily.jsonl"
        api_df = _read_json_if_exists(spark, candidate)
        if api_df is not None:
            print(f"[INFO] loaded weather API source from: {candidate}")
            weather_dfs.append(api_df)
            api_loaded = True
            break
    if api_loaded:
        break

if not weather_dfs:
    raise FileNotFoundError("No Bronze weather sources found for this snapshot.")

weather_df = weather_dfs[0]
for nxt in weather_dfs[1:]:
    weather_df = weather_df.unionByName(nxt, allowMissingColumns=True)

raw_count = weather_df.count()
print(f"[INFO] weather source rows before normalization={raw_count}")
weather_df = _sanitize_df_columns(weather_df)
silver_weather_df = _normalize_weather(weather_df, snapshot_date).filter(F.col("as_of_date").isNotNull())
silver_count = silver_weather_df.count()
print(f"[INFO] weather rows after normalization/filter={silver_count}")
if silver_count == 0 and not silver_weather_allow_empty_write:
    raise RuntimeError(
        "silver_weather_df is empty after normalization. "
        "Set silver_weather_allow_empty_write=true to allow empty writes, "
        "or inspect source columns/date formats."
    )

silver_weather_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(silver_weather_table)
print(f"[DONE] wrote table: {silver_weather_table}")

