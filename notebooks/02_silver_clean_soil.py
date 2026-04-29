"""Silver soil cleaning (domain-split)."""

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
from pyspark.sql.window import Window

NOTEBOOK_VERSION = "v2026-04-28-silver-soil-split-01"

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
                "readings_years_csv": "2023,2024,2025",
                "silver_include_api_source": True,
                "silver_include_file_source": True,
                "soil_csv_input_dir": "Files/raw/soil",
                "soil_csv_file_pattern": "soil-sensor-readings-historical-data-{year}.csv",
                "silver_records_table": "silver_soil_sensor_readings",
                "silver_quarantine_table": "silver_soil_sensor_quarantine",
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


def _parse_years(years_csv: str) -> List[str]:
    values = [v.strip() for v in years_csv.split(",") if v.strip()]
    if not values:
        raise ValueError("readings_years_csv is empty.")
    return sorted(set(values))


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


def _read_jsonl_if_exists(spark: SparkSession, path: str) -> DataFrame | None:
    for candidate in [path, _lakehouse_to_local(path)]:
        try:
            return spark.read.json(candidate)
        except Exception:
            pass
    return None


def _read_csv_if_exists(spark: SparkSession, path: str) -> DataFrame | None:
    for candidate in [path, _lakehouse_to_local(path)]:
        try:
            return spark.read.option("header", "true").csv(candidate)
        except Exception:
            pass
    return None


def _csv_for_year_from_candidates(candidates: List[str], year: str) -> str | None:
    pattern = re.compile(rf"soil-sensor-readings-historical-data[-_]?{re.escape(year)}\.csv$", re.IGNORECASE)
    for c in candidates:
        if pattern.search(c):
            return c
    return None


def _dedupe_bronze_records(df: DataFrame) -> DataFrame:
    keyed = (
        df.withColumn("_k_site_id", F.coalesce(F.col("site_id"), F.col("Site_ID")).cast("string"))
        .withColumn("_k_probe_id", F.coalesce(F.col("probe_id"), F.col("Probe_ID")).cast("string"))
        .withColumn("_k_probe_measure", F.lower(F.trim(F.coalesce(F.col("probe_measure"), F.col("Probe_Measure")).cast("string"))))
        .withColumn("_k_local_time", F.coalesce(F.col("local_time"), F.col("Local_Time")).cast("string"))
        .withColumn("_k_soil_value", F.trim(F.coalesce(F.col("soil_value"), F.col("Soil_Value")).cast("string")))
        .withColumn(
            "_source_priority",
            F.when(F.col("_source_type") == F.lit("file"), F.lit(1)).when(F.col("_source_type") == F.lit("api"), F.lit(2)).otherwise(F.lit(9)),
        )
    )
    w = Window.partitionBy("_k_site_id", "_k_probe_id", "_k_probe_measure", "_k_local_time", "_k_soil_value").orderBy(
        F.col("_source_priority").asc()
    )
    return keyed.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop(
        "_k_site_id", "_k_probe_id", "_k_probe_measure", "_k_local_time", "_k_soil_value", "_source_priority", "_rn"
    )


spark = SparkSession.builder.getOrCreate()
params = resolve_params({})
print(f"[INFO] notebook_version={NOTEBOOK_VERSION}")

medallion_root = params.get("medallion_root", "Files/medallion").rstrip("/")
bronze_soil_root = params.get("bronze_soil_root", f"{medallion_root}/bronze/bronze_soil_readings").rstrip("/")
snapshot_date = params.get("snapshot_date", "2026-04-28")
years = _parse_years(params.get("readings_years_csv", "2023,2024,2025"))
include_api = _parse_bool(params.get("silver_include_api_source", True), True)
include_file = _parse_bool(params.get("silver_include_file_source", True), True)
soil_csv_input_dir = params.get("soil_csv_input_dir", "Files/raw/soil").rstrip("/")
soil_csv_file_pattern = params.get("soil_csv_file_pattern", "soil-sensor-readings-historical-data-{year}.csv")

records_dfs: List[DataFrame] = []
csv_candidates = [f"{soil_csv_input_dir}/{soil_csv_file_pattern.format(year=year)}" for year in years]

for year in years:
    if include_file:
        p = f"{bronze_soil_root}/year={year}/raw/records_file.jsonl"
        df = _read_jsonl_if_exists(spark, p)
        if df is None:
            filename = soil_csv_file_pattern.format(year=year)
            df = _read_csv_if_exists(spark, f"{bronze_soil_root}/year={year}/raw/file/{filename}")
        if df is None:
            discovered = _csv_for_year_from_candidates(csv_candidates, year)
            if discovered:
                df = _read_csv_if_exists(spark, discovered)
        if df is not None:
            records_dfs.append(df.withColumn("_source_type", F.lit("file")).withColumn("_source_year", F.lit(year)))
    if include_api:
        p = f"{bronze_soil_root}/year={year}/raw/records_api.jsonl"
        df = _read_jsonl_if_exists(spark, p)
        if df is not None:
            records_dfs.append(df.withColumn("_source_type", F.lit("api")).withColumn("_source_year", F.lit(year)))

if not records_dfs:
    raise FileNotFoundError("No Bronze soil records found.")

bronze_records_df = records_dfs[0]
for nxt in records_dfs[1:]:
    bronze_records_df = bronze_records_df.unionByName(nxt, allowMissingColumns=True)
bronze_records_df = _dedupe_bronze_records(bronze_records_df)

silver_base_df = (
    bronze_records_df.withColumn("site_id", F.col("site_id").cast("string"))
    .withColumn("record_id", F.coalesce(F.col("id"), F.col("ID")).cast("string"))
    .withColumn("probe_id", F.col("probe_id").cast("string"))
    .withColumn("site_name", F.col("site_name").cast("string"))
    .withColumn("probe_measure", F.col("probe_measure").cast("string"))
    .withColumn("unit", F.col("unit").cast("string"))
    .withColumn("soil_value_raw", F.coalesce(F.col("soil_value"), F.col("Soil_Value")).cast("string"))
    .withColumn("soil_value", F.col("soil_value_raw"))
    .withColumn("soil_value_clean", F.trim(F.regexp_replace(F.col("soil_value_raw"), ",", "")))
    .withColumn("soil_value_num", F.regexp_extract(F.col("soil_value_clean"), r"[-+]?[0-9]*\.?[0-9]+", 0).cast("double"))
    .withColumn("local_time_raw", F.coalesce(F.col("local_time"), F.col("Local_Time")).cast("string"))
    .withColumn("local_time_ts", F.to_timestamp("local_time_raw"))
    .withColumn("as_of_date", F.to_date("local_time_ts"))
    .withColumn("snapshot_date", F.lit(snapshot_date))
    .withColumn("probe_measure_lc", F.lower(F.col("probe_measure")))
    .withColumn("depth_cm", F.regexp_extract(F.col("probe_measure_lc"), r"(\d+)\s*cm", 1).cast("int"))
    .withColumn("probe_channel", F.regexp_extract(F.col("probe_measure"), r"#\s*(\d+)", 1).cast("int"))
    .withColumn(
        "measure_type",
        F.when(F.col("probe_measure_lc").contains("moisture") | (F.lower(F.col("unit")) == F.lit("%vwc")), F.lit("moisture"))
        .when(F.col("probe_measure_lc").contains("salinity") | (F.col("unit").isin("µS/cm", "mS/cm")), F.lit("salinity"))
        .when(F.col("probe_measure_lc").contains("temp") | F.col("unit").isin("ºC", "°C"), F.lit("temperature"))
        .otherwise(F.lit("other")),
    )
)

salinity_ms_cm_max = 50.0
moisture_vwc_min = 0.0
moisture_vwc_max = 100.0
temp_c_min = -20.0
temp_c_max = 80.0
salinity_us_cm_max = salinity_ms_cm_max * 1000.0

silver_checked_df = (
    silver_base_df.withColumn("dq_missing_site_id", F.col("site_id").isNull() | (F.trim(F.col("site_id")) == ""))
    .withColumn("dq_missing_value", F.col("soil_value_clean").isNull() | (F.col("soil_value_clean") == ""))
    .withColumn("dq_non_numeric_value", ~(F.col("soil_value_clean").isNull() | (F.col("soil_value_clean") == "")) & F.col("soil_value_num").isNull())
    .withColumn("dq_invalid_time", F.col("local_time_ts").isNull())
    .withColumn("dq_invalid_depth", F.col("depth_cm").isNotNull() & ((F.col("depth_cm") < 0) | (F.col("depth_cm") > 300)))
    .withColumn("dq_sensor_sentinel", F.col("soil_value_num").isin(-9999.0, -9999, -999.0, -999) | F.col("soil_value_raw").isin("-9999", "-9999.0", "-999", "-999.0"))
    .withColumn("dq_salinity_out_of_range", F.when(F.col("unit") == "mS/cm", (F.col("soil_value_num") < 0.0) | (F.col("soil_value_num") > salinity_ms_cm_max)).when(F.col("unit") == "µS/cm", (F.col("soil_value_num") < 0.0) | (F.col("soil_value_num") > salinity_us_cm_max)).otherwise(F.lit(False)))
    .withColumn("dq_moisture_out_of_range", (F.col("measure_type") == "moisture") & ((F.col("soil_value_num") < moisture_vwc_min) | (F.col("soil_value_num") > moisture_vwc_max)))
    .withColumn("dq_temp_out_of_range", (F.col("measure_type") == "temperature") & ((F.col("soil_value_num") < temp_c_min) | (F.col("soil_value_num") > temp_c_max)))
    .withColumn(
        "dq_reasons",
        F.expr(
            "filter(array("
            "if(dq_missing_site_id,'missing_site_id',null),"
            "if(dq_missing_value,'missing_value',null),"
            "if(dq_non_numeric_value,'non_numeric_value',null),"
            "if(dq_invalid_time,'invalid_time',null),"
            "if(dq_invalid_depth,'invalid_depth',null),"
            "if(dq_sensor_sentinel,'sensor_sentinel',null),"
            "if(dq_salinity_out_of_range,'salinity_out_of_range',null),"
            "if(dq_moisture_out_of_range,'moisture_out_of_range',null),"
            "if(dq_temp_out_of_range,'temp_out_of_range',null)"
            "), x -> x is not null)"
        ),
    )
    .withColumn("dq_is_valid", F.size(F.col("dq_reasons")) == 0)
)

silver_valid_df = silver_checked_df.filter(F.col("dq_is_valid")).drop(
    "probe_measure_lc",
    "soil_value_clean",
    "dq_missing_site_id",
    "dq_missing_value",
    "dq_non_numeric_value",
    "dq_invalid_time",
    "dq_invalid_depth",
    "dq_sensor_sentinel",
    "dq_salinity_out_of_range",
    "dq_moisture_out_of_range",
    "dq_temp_out_of_range",
    "dq_is_valid",
    "dq_reasons",
)
silver_quarantine_df = silver_checked_df.filter(~F.col("dq_is_valid")).drop("probe_measure_lc", "soil_value_clean")

silver_records_table = params.get("silver_records_table", "silver_soil_sensor_readings")
silver_quarantine_table = params.get("silver_quarantine_table", "silver_soil_sensor_quarantine")

silver_valid_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(silver_records_table)
silver_quarantine_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(silver_quarantine_table)
print(f"[DONE] wrote tables: {silver_records_table}, {silver_quarantine_table}")

