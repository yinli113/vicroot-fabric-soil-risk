"""Export site risk points to GeoJSON for Lakehouse map visual."""

# %% [markdown]
# # Cell 1 - Imports and params

from __future__ import annotations

import datetime as dt
import json
import os
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

NOTEBOOK_VERSION = "v2026-04-29-export-site-risk-geojson-01"

try:
    from notebooks.lib.pipeline_params import resolve_params
except ModuleNotFoundError:
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
            params: Dict[str, Any] = {
                "gold_irrigation_risk_table": "gold_irrigation_risk_daily",
                "geojson_output_path": "Files/raw/map/site_risk.geojson",
                "geojson_latest_only": True,
                "geojson_max_features": 50000,
            }
            raw = os.environ.get("VICROOT_PARAMS_JSON")
            if raw:
                params.update(json.loads(raw))
            if overrides:
                params.update(overrides)
            return params


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


def _to_builtin(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _to_lakehouse_abs_path(path_like: str) -> str:
    path_like = (path_like or "").strip()
    if not path_like:
        return "/lakehouse/default/Files/raw/map/site_risk.geojson"
    if path_like.startswith("/lakehouse/default/"):
        return path_like
    if path_like.startswith("Files/"):
        return f"/lakehouse/default/{path_like}"
    if path_like.startswith("File/"):
        return f"/lakehouse/default/{path_like}"
    return f"/lakehouse/default/Files/{path_like.lstrip('/')}"


# %% [markdown]
# # Cell 2 - Read Gold table and shape latest site view

spark = SparkSession.builder.getOrCreate()
params = resolve_params({})

gold_irrigation_risk_table = params.get("gold_irrigation_risk_table", "gold_irrigation_risk_daily")
geojson_output_path = params.get("geojson_output_path", "Files/raw/map/site_risk.geojson")
geojson_latest_only = _parse_bool(params.get("geojson_latest_only", True), True)
geojson_max_features = int(params.get("geojson_max_features", 50000))

print(f"[INFO] notebook_version={NOTEBOOK_VERSION}")
print(
    json.dumps(
        {
            "gold_irrigation_risk_table": gold_irrigation_risk_table,
            "geojson_output_path": geojson_output_path,
            "geojson_latest_only": geojson_latest_only,
            "geojson_max_features": geojson_max_features,
        },
        indent=2,
    )
)

risk_df = (
    spark.table(gold_irrigation_risk_table)
    .withColumn("latitude", F.col("latitude").cast("double"))
    .withColumn("longitude", F.col("longitude").cast("double"))
    .withColumn("as_of_date", F.to_date(F.col("as_of_date")))
    .withColumn("snapshot_date", F.col("snapshot_date").cast("string"))
    .filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
)

if geojson_latest_only:
    latest_window = Window.partitionBy("site_id").orderBy(
        F.col("as_of_date").desc_nulls_last(), F.col("snapshot_date").desc_nulls_last()
    )
    risk_df = (
        risk_df.withColumn("site_row_num", F.row_number().over(latest_window))
        .filter(F.col("site_row_num") == 1)
        .drop("site_row_num")
    )

row_count = risk_df.count()
print(f"[INFO] geojson_candidate_rows={row_count}")
if row_count == 0:
    raise RuntimeError("No rows with valid latitude/longitude found in Gold risk table.")
if row_count > geojson_max_features:
    raise RuntimeError(
        f"Row count {row_count} exceeds geojson_max_features={geojson_max_features}. "
        "Increase limit or export a filtered subset."
    )


# %% [markdown]
# # Cell 3 - Build and write GeoJSON

selected_df = risk_df.select(
    "site_id",
    "site_name",
    "as_of_date",
    "snapshot_date",
    "risk_tier",
    "reading_count",
    "moisture_lt_20",
    "salinity_lt_20",
    "salinity_surface_7d_avg",
    "salinity_surface_30d_avg",
    "salinity_trend_30d",
    "rainfall_mm_period",
    "evap_mm_period",
    "evap_moisture_pressure",
    "latitude",
    "longitude",
)

features = []
for row in selected_df.collect():
    row_dict = row.asDict(recursive=True)
    lon = _to_builtin(row_dict.pop("longitude", None))
    lat = _to_builtin(row_dict.pop("latitude", None))
    properties = {k: _to_builtin(v) for k, v in row_dict.items()}
    features.append(
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": properties,
        }
    )

feature_collection = {"type": "FeatureCollection", "features": features}

abs_output_path = _to_lakehouse_abs_path(geojson_output_path)
os.makedirs(os.path.dirname(abs_output_path), exist_ok=True)
with open(abs_output_path, "w", encoding="utf-8") as handle:
    json.dump(feature_collection, handle, ensure_ascii=True)

print(f"[DONE] wrote geojson: {abs_output_path}")
print(f"[DONE] feature_count={len(features)}")
