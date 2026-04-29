"""Silver site cleaning (domain-split)."""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

NOTEBOOK_VERSION = "v2026-04-28-silver-site-split-01"

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
                "site_file_input_path": "",
                "silver_site_file_table": "silver_site_file_reference",
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
            cols = set(df.columns)
            # Skip malformed reads (common when non-JSON file is attempted as JSON).
            if cols == {"_corrupt_record"}:
                continue
            return df
        except Exception:
            pass
    return None


def _read_csv_if_exists(spark: SparkSession, path: str) -> DataFrame | None:
    for candidate in [path, _lakehouse_to_local(path)]:
        try:
            # Try common delimiters to handle uploaded CSV variations.
            best_df = None
            best_col_count = -1
            for sep in [",", ";", "\t", "|"]:
                df = spark.read.option("header", "true").option("sep", sep).csv(candidate)
                col_count = len(df.columns)
                if col_count > best_col_count:
                    best_col_count = col_count
                    best_df = df
            if best_df is None or not best_df.columns:
                continue
            # If still one header-like column with delimiters, treat as unreadable.
            if len(best_df.columns) == 1 and any(d in best_df.columns[0] for d in [";", ",", "\t", "|"]):
                continue
            return best_df
        except Exception:
            pass
    return None


def _col_or_null(df: DataFrame, *candidates: str):
    for name in candidates:
        if name in df.columns:
            return F.col(name)
    return F.lit(None)


def _sanitize_column_name(name: str) -> str:
    sanitized = re.sub(r"[ ,;{}()\n\t=]+", "_", name.strip())
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    return sanitized or "col"


def _sanitize_df_columns(df: DataFrame) -> DataFrame:
    out = df
    used: Dict[str, int] = {}
    for old in out.columns:
        base = _sanitize_column_name(old)
        new = base
        idx = used.get(base, 0)
        while new in used:
            idx += 1
            new = f"{base}_{idx}"
        used[base] = idx
        used[new] = 0
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


def _normalize_site_df(df: DataFrame, snapshot_date: str) -> DataFrame:
    return (
        df.withColumn("site_id", F.coalesce(_col_or_null(df, "site_id", "id", "ID"), F.lit(None)).cast("string"))
        .withColumn("site_name", F.coalesce(_col_or_null(df, "site_name", "name", "Site_Name"), F.lit(None)).cast("string"))
        .withColumn(
            "latitude",
            F.coalesce(_col_or_null(df, "latitude", "lat", "Latitude", "y"), F.lit(None)).cast("double"),
        )
        .withColumn(
            "longitude",
            F.coalesce(_col_or_null(df, "longitude", "lon", "lng", "Longitude", "x"), F.lit(None)).cast("double"),
        )
        .withColumn("snapshot_date", F.lit(snapshot_date))
        .dropDuplicates(["site_id", "site_name", "latitude", "longitude"])
    )


spark = SparkSession.builder.getOrCreate()
params = resolve_params({})
print(f"[INFO] notebook_version={NOTEBOOK_VERSION}")

medallion_root = params.get("medallion_root", "Files/medallion").rstrip("/")
bronze_site_root = params.get("bronze_site_root", f"{medallion_root}/bronze/bronze_site_reference").rstrip("/")
legacy_site_file_root = f"{medallion_root}/bronze/site_reference"
legacy_site_api_root = f"{medallion_root}/bronze/com_soil_sensor_locations"
snapshot_date = params.get("snapshot_date", "2026-04-28")
site_file_input_path = params.get("site_file_input_path", "").strip()
silver_site_table = params.get("silver_site_file_table", "silver_site_file_reference")

site_dfs: List[DataFrame] = []

if site_file_input_path:
    file_name = os.path.basename(site_file_input_path)
    loaded_from_bronze_file = False
    for root in [bronze_site_root, legacy_site_file_root]:
        for snap in _snapshot_candidates(f"{root}/source=file", snapshot_date):
            candidate = f"{root}/source=file/snapshot_date={snap}/raw/{file_name}"
            df = _read_csv_if_exists(spark, candidate)
            if df is None:
                df = _read_json_if_exists(spark, candidate)
            if df is not None:
                print(f"[INFO] loaded site file source from: {candidate}")
                site_dfs.append(df)
                loaded_from_bronze_file = True
                break
        if loaded_from_bronze_file:
            break
    if not loaded_from_bronze_file:
        # Last-resort transition fallback: read directly from the raw uploaded file.
        df = _read_csv_if_exists(spark, site_file_input_path)
        if df is None:
            df = _read_json_if_exists(spark, site_file_input_path)
        if df is not None:
            print(f"[WARN] loading site reference directly from raw path: {site_file_input_path}")
            site_dfs.append(df)
else:
    # Param not provided: try common raw paths automatically.
    common_raw_site_candidates = [
        "Files/raw/site/soil-sensor-locations.csv",
        "File/raw/site/soil-sensor-locations.csv",
        "Files/raw/site/soil_sensor_locations.csv",
        "File/raw/site/soil_sensor_locations.csv",
    ]
    for candidate in common_raw_site_candidates:
        df = _read_csv_if_exists(spark, candidate)
        if df is None:
            df = _read_json_if_exists(spark, candidate)
        if df is not None:
            print(f"[WARN] site_file_input_path not set; using discovered raw site file: {candidate}")
            site_dfs.append(df)
            break

for root in [bronze_site_root, legacy_site_api_root]:
    api_loaded = False
    for snap in _snapshot_candidates(f"{root}/source=api", snapshot_date):
        candidate = f"{root}/source=api/snapshot_date={snap}/raw/records.json"
        api_df = _read_json_if_exists(spark, candidate)
        if api_df is None:
            continue
        print(f"[INFO] loaded site API source from: {candidate}")
        if "results" in api_df.columns:
            site_dfs.append(api_df.select(F.explode_outer("results").alias("r")).select("r.*"))
        else:
            site_dfs.append(api_df)
        api_loaded = True
        break
    if api_loaded:
        break

if not site_dfs:
    raise FileNotFoundError("No Bronze site sources found for this snapshot.")

site_df = site_dfs[0]
for nxt in site_dfs[1:]:
    site_df = site_df.unionByName(nxt, allowMissingColumns=True)

site_df = _sanitize_df_columns(site_df)
silver_site_df = _normalize_site_df(site_df, snapshot_date).filter(
    F.col("site_id").isNotNull() & (F.trim(F.col("site_id")) != "") & F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
)
silver_site_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(silver_site_table)
print(f"[DONE] wrote table: {silver_site_table}")

