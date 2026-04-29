"""Gold feature build from Silver soil sensor readings.

This first Gold version is site-level (sensor location + day) to support:
- Power BI monitoring
- ML-ready feature outputs for downstream training/scoring

Future extension:
- Join tree inventory + weather and move to tree-level grain.
"""

# %% [markdown]
# # Cell 1 - Imports and params

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

NOTEBOOK_VERSION = "v2026-04-28-gold-depth-weather-01"

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
                "snapshot_date": "2026-04-27",
                "silver_records_table": "silver_soil_sensor_readings",
                "gold_features_table": "gold_soil_sensor_features",
                "gold_depth_daily_table": "gold_soil_sensor_depth_daily",
                "gold_irrigation_risk_table": "gold_irrigation_risk_daily",
                "gold_plant_suitability_table": "gold_plant_suitability_zone",
                "gold_ops_alert_table": "gold_ops_alert",
                "silver_site_file_table": "silver_site_file_reference",
                "weather_daily_table": "silver_weather_daily",
                "weather_date_col": "as_of_date",
                "weather_rainfall_col": "rainfall_mm",
                "weather_evap_col": "evap_mm",
            }
            if os.environ.get("VICROOT_PARAMS_JSON"):
                params.update(json.loads(os.environ["VICROOT_PARAMS_JSON"]))
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


def _run_gold_dq_tests(
    *,
    dq_fail_on_error: bool,
    gold_df,
    depth_daily_df,
) -> None:
    failures = []

    gold_count = gold_df.count()
    depth_count = depth_daily_df.count()
    duplicate_site_day = (
        gold_df.groupBy("site_id", "as_of_date").count().filter(F.col("count") > 1).count()
    )
    null_key_rows = gold_df.filter(
        F.col("site_id").isNull() | (F.trim(F.col("site_id")) == "") | F.col("as_of_date").isNull()
    ).count()
    invalid_risk_tier = gold_df.filter(~F.col("risk_tier").isin("low", "medium", "high")).count()

    print(
        f"[DQ][GOLD] features={gold_count} depth_daily={depth_count} "
        f"duplicate_site_day={duplicate_site_day} null_key_rows={null_key_rows} invalid_risk_tier={invalid_risk_tier}"
    )

    if gold_count == 0:
        failures.append("gold_df is empty.")
    if depth_count == 0:
        failures.append("depth_daily_enriched_df is empty.")
    if duplicate_site_day > 0:
        failures.append(f"Found {duplicate_site_day} duplicate (site_id, as_of_date) rows in gold_df.")
    if null_key_rows > 0:
        failures.append(f"Found {null_key_rows} rows with null/blank site_id or null as_of_date.")
    if invalid_risk_tier > 0:
        failures.append(f"Found {invalid_risk_tier} rows with risk_tier outside [low, medium, high].")

    if failures:
        for message in failures:
            print(f"[DQ][GOLD][FAIL] {message}")
        if dq_fail_on_error:
            raise RuntimeError("Gold DQ checks failed. See [DQ][GOLD][FAIL] logs above.")
        print("[DQ][GOLD][WARN] dq_fail_on_error=false, continuing despite Gold DQ failures.")
    else:
        print("[DQ][GOLD] all checks passed.")


# %% [markdown]
# # Cell 2 - Load Silver tables

spark = SparkSession.builder.getOrCreate()
params = resolve_params({})
print(f"[INFO] notebook_version={NOTEBOOK_VERSION}")
print(
    json.dumps(
        {k: v for k, v in params.items() if "token" not in k.lower() and "key" not in k.lower()},
        indent=2,
    )
)

silver_records_table = params.get("silver_records_table", "silver_soil_sensor_readings")
gold_features_table = params.get("gold_features_table", "gold_soil_sensor_features")
gold_depth_daily_table = params.get("gold_depth_daily_table", "gold_soil_sensor_depth_daily")
gold_irrigation_risk_table = params.get("gold_irrigation_risk_table", "gold_irrigation_risk_daily")
gold_plant_suitability_table = params.get("gold_plant_suitability_table", "gold_plant_suitability_zone")
gold_ops_alert_table = params.get("gold_ops_alert_table", "gold_ops_alert")
snapshot_date = params.get("snapshot_date", "2026-04-27")
weather_daily_table = params.get("weather_daily_table", "")
weather_date_col = params.get("weather_date_col", "as_of_date")
weather_rainfall_col = params.get("weather_rainfall_col", "rainfall_mm")
weather_evap_col = params.get("weather_evap_col", "evap_mm")
silver_site_table = params.get(
    "silver_site_file_table",
    params.get("silver_site_table", "silver_site_file_reference"),
)
dq_fail_on_error = _parse_bool(params.get("dq_fail_on_error", True), True)

silver_df = spark.table(silver_records_table)
print(f"[INFO] silver_records_rows={silver_df.count()}")


# %% [markdown]
# # Cell 3 - Build depth-aware daily features

base_df = (
    silver_df.withColumn("site_id", F.col("site_id").cast("string"))
    .withColumn("as_of_date", F.col("as_of_date").cast("date"))
    .withColumn(
        "depth_band",
        F.when(F.col("depth_cm").isNull(), F.lit("unknown"))
        .when(F.col("depth_cm") < F.lit(20), F.lit("lt_20"))
        .when(F.col("depth_cm") <= F.lit(50), F.lit("20_50"))
        .otherwise(F.lit("gt_50")),
    )
)

depth_daily_df = (
    base_df.groupBy("site_id", "site_name", "as_of_date", "measure_type", "depth_band")
    .agg(
        F.count(F.lit(1)).alias("reading_count"),
        F.avg("soil_value_num").alias("value_avg"),
        F.min("soil_value_num").alias("value_min"),
        F.max("soil_value_num").alias("value_max"),
    )
)

w7 = Window.partitionBy("site_id", "measure_type", "depth_band").orderBy("as_of_date").rowsBetween(-6, 0)
w14 = Window.partitionBy("site_id", "measure_type", "depth_band").orderBy("as_of_date").rowsBetween(-13, 0)

depth_daily_enriched_df = (
    depth_daily_df.withColumn("value_7d_avg", F.avg("value_avg").over(w7))
    .withColumn("value_14d_avg", F.avg("value_avg").over(w14))
    .withColumn("snapshot_date", F.lit(snapshot_date))
)

# Pivot to site-day feature columns for BI/ML.
pivot_df = (
    depth_daily_enriched_df.withColumn("feature_key", F.concat_ws("_", F.col("measure_type"), F.col("depth_band")))
    .groupBy("site_id", "site_name", "as_of_date")
    .pivot("feature_key", ["moisture_lt_20", "moisture_20_50", "moisture_gt_50", "salinity_lt_20", "salinity_20_50", "salinity_gt_50", "temperature_lt_20", "temperature_20_50", "temperature_gt_50"])
    .agg(F.first("value_avg"))
)

site_daily_counts_df = (
    base_df.groupBy("site_id", "site_name", "as_of_date")
    .agg(F.count(F.lit(1)).alias("reading_count"))
)

irrigation_daily_df = site_daily_counts_df.join(pivot_df, on=["site_id", "site_name", "as_of_date"], how="left")

def _table_exists(table_name: str) -> bool:
    if not table_name:
        return False
    try:
        spark.table(table_name).limit(1).count()
        return True
    except Exception:
        return False

if _table_exists(weather_daily_table):
    weather_df = (
        spark.table(weather_daily_table)
        .withColumn("as_of_date", F.to_date(F.col(weather_date_col)))
        .withColumn("rainfall_mm_period", F.col(weather_rainfall_col).cast("double"))
        .withColumn("evap_mm_period", F.col(weather_evap_col).cast("double"))
        .select("as_of_date", "rainfall_mm_period", "evap_mm_period")
        .dropDuplicates(["as_of_date"])
    )
    irrigation_daily_df = irrigation_daily_df.join(weather_df, on=["as_of_date"], how="left")
else:
    irrigation_daily_df = (
        irrigation_daily_df.withColumn("rainfall_mm_period", F.lit(None).cast("double"))
        .withColumn("evap_mm_period", F.lit(None).cast("double"))
    )

if _table_exists(silver_site_table):
    site_window = Window.partitionBy("site_id").orderBy(F.col("snapshot_date").desc_nulls_last())
    site_geo_df = (
        spark.table(silver_site_table)
        .withColumn("site_id", F.col("site_id").cast("string"))
        .withColumn("site_name_geo", F.col("site_name").cast("string"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("site_row_num", F.row_number().over(site_window))
        .filter(F.col("site_row_num") == 1)
        .select("site_id", "site_name_geo", "latitude", "longitude")
    )
    irrigation_daily_df = (
        irrigation_daily_df.join(site_geo_df, on=["site_id"], how="left")
        .withColumn("site_name", F.coalesce(F.col("site_name"), F.col("site_name_geo")))
        .drop("site_name_geo")
    )
else:
    irrigation_daily_df = (
        irrigation_daily_df.withColumn("latitude", F.lit(None).cast("double"))
        .withColumn("longitude", F.lit(None).cast("double"))
    )

salinity_window_7 = Window.partitionBy("site_id").orderBy("as_of_date").rowsBetween(-6, 0)
salinity_window_30 = Window.partitionBy("site_id").orderBy("as_of_date").rowsBetween(-29, 0)
irrigation_daily_df = (
    irrigation_daily_df.withColumn(
        "salinity_surface_avg",
        F.coalesce(F.col("salinity_lt_20"), F.col("salinity_20_50"), F.col("salinity_gt_50")),
    )
    .withColumn("salinity_surface_7d_avg", F.avg("salinity_surface_avg").over(salinity_window_7))
    .withColumn("salinity_surface_30d_avg", F.avg("salinity_surface_avg").over(salinity_window_30))
    .withColumn(
        "salinity_trend_30d",
        F.when(F.col("salinity_surface_30d_avg").isNull() | F.col("salinity_surface_7d_avg").isNull(), F.lit(None))
        .otherwise(F.col("salinity_surface_7d_avg") - F.col("salinity_surface_30d_avg")),
    )
)

gold_df = (
    irrigation_daily_df.withColumn(
        "moisture_deficit_index",
        F.when(F.col("moisture_lt_20").isNull(), None).otherwise(F.lit(100.0) - F.col("moisture_lt_20")),
    )
    .withColumn(
        "evap_moisture_pressure",
        F.when(F.col("evap_mm_period").isNull() | F.col("rainfall_mm_period").isNull(), None)
        .otherwise(F.col("evap_mm_period") - F.col("rainfall_mm_period")),
    )
    .withColumn(
        "risk_tier",
        F.when(
            (F.col("moisture_lt_20") < F.lit(20))
            | (F.col("salinity_lt_20") > F.lit(2.0))
            | (F.col("evap_moisture_pressure") > F.lit(3.0)),
            F.lit("high"),
        )
        .when(
            (F.col("moisture_lt_20") < F.lit(30))
            | (F.col("salinity_lt_20") > F.lit(1.0))
            | (F.col("evap_moisture_pressure") > F.lit(1.0)),
            F.lit("medium"),
        )
        .otherwise(F.lit("low")),
    )
    .withColumn("snapshot_date", F.lit(snapshot_date))
)

# Product 1: irrigation decision table (site/day).
gold_irrigation_risk_df = gold_df.select(
    "site_id",
    "site_name",
    "latitude",
    "longitude",
    "as_of_date",
    "reading_count",
    "moisture_lt_20",
    "moisture_20_50",
    "moisture_gt_50",
    "salinity_lt_20",
    "salinity_20_50",
    "salinity_gt_50",
    "temperature_lt_20",
    "temperature_20_50",
    "temperature_gt_50",
    "rainfall_mm_period",
    "evap_mm_period",
    "evap_moisture_pressure",
    "salinity_surface_7d_avg",
    "salinity_surface_30d_avg",
    "salinity_trend_30d",
    "moisture_deficit_index",
    "risk_tier",
    "snapshot_date",
)

# Product 2: site-level suitability zone from 90d aggregates.
plant_window_90 = Window.partitionBy("site_id").orderBy("as_of_date").rowsBetween(-89, 0)
gold_plant_suitability_df = (
    gold_df.withColumn("moisture_90d_avg", F.avg("moisture_lt_20").over(plant_window_90))
    .withColumn("salinity_90d_avg", F.avg("salinity_surface_avg").over(plant_window_90))
    .withColumn("rainfall_90d_sum", F.sum("rainfall_mm_period").over(plant_window_90))
    .withColumn("evap_90d_sum", F.sum("evap_mm_period").over(plant_window_90))
    .withColumn(
        "plant_suitability_zone",
        F.when(F.col("salinity_90d_avg").isNull() | F.col("moisture_90d_avg").isNull(), F.lit("unknown"))
        .when((F.col("salinity_90d_avg") <= F.lit(0.8)) & (F.col("moisture_90d_avg").between(20, 45)), F.lit("high"))
        .when((F.col("salinity_90d_avg") <= F.lit(1.5)) & (F.col("moisture_90d_avg").between(12, 55)), F.lit("medium"))
        .otherwise(F.lit("low")),
    )
    .select(
        "site_id",
        "site_name",
        "latitude",
        "longitude",
        "as_of_date",
        "moisture_90d_avg",
        "salinity_90d_avg",
        "rainfall_90d_sum",
        "evap_90d_sum",
        "plant_suitability_zone",
        "snapshot_date",
    )
)

# Product 3: operational alerts from rule triggers.
gold_ops_alert_df = (
    gold_df.withColumn(
        "alert_type",
        F.when(F.col("risk_tier") == F.lit("high"), F.lit("high_risk_irrigation"))
        .when(F.col("salinity_trend_30d") > F.lit(0.2), F.lit("salinity_rising"))
        .when(F.col("evap_moisture_pressure") > F.lit(4.0), F.lit("high_evap_pressure"))
        .otherwise(F.lit(None)),
    )
    .withColumn(
        "alert_severity",
        F.when(F.col("alert_type") == F.lit("high_risk_irrigation"), F.lit("high"))
        .when(F.col("alert_type").isNotNull(), F.lit("medium"))
        .otherwise(F.lit(None)),
    )
    .withColumn(
        "alert_reason",
        F.when(F.col("alert_type") == F.lit("high_risk_irrigation"), F.lit("risk_tier=high"))
        .when(F.col("alert_type") == F.lit("salinity_rising"), F.lit("7d salinity above 30d baseline"))
        .when(F.col("alert_type") == F.lit("high_evap_pressure"), F.lit("evap-rain pressure > 4"))
        .otherwise(F.lit(None)),
    )
    .filter(F.col("alert_type").isNotNull())
    .select(
        "site_id",
        "site_name",
        "latitude",
        "longitude",
        "as_of_date",
        "alert_type",
        "alert_severity",
        "alert_reason",
        "snapshot_date",
    )
)

print(f"[INFO] gold_feature_rows={gold_df.count()}")
print(f"[INFO] gold_depth_daily_rows={depth_daily_enriched_df.count()}")
print(f"[INFO] gold_irrigation_risk_rows={gold_irrigation_risk_df.count()}")
print(f"[INFO] gold_plant_suitability_rows={gold_plant_suitability_df.count()}")
print(f"[INFO] gold_ops_alert_rows={gold_ops_alert_df.count()}")
_run_gold_dq_tests(
    dq_fail_on_error=dq_fail_on_error,
    gold_df=gold_df,
    depth_daily_df=depth_daily_enriched_df,
)


# %% [markdown]
# # Cell 4 - Write Gold table

gold_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(gold_features_table)
depth_daily_enriched_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(gold_depth_daily_table)
gold_irrigation_risk_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(
    gold_irrigation_risk_table
)
gold_plant_suitability_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(
    gold_plant_suitability_table
)
gold_ops_alert_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(
    gold_ops_alert_table
)
print(f"[DONE] wrote table: {gold_features_table}")
print(f"[DONE] wrote table: {gold_depth_daily_table}")
print(f"[DONE] wrote table: {gold_irrigation_risk_table}")
print(f"[DONE] wrote table: {gold_plant_suitability_table}")
print(f"[DONE] wrote table: {gold_ops_alert_table}")
