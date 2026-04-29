"""Silver transformation for soil sensor readings.

Current scope:
- Read Bronze records written by `01_bronze_ingest.py` (file/api branches).
- Normalize schema and types.
- Apply data-quality flags and split valid vs quarantine outputs.
- Persist Silver Delta tables.

Future scope:
- Spatially join with tree inventory / soil polygons (separate notebook step).
"""

# %% [markdown]
# # Cell 1 - Imports and params

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

NOTEBOOK_VERSION = "v2026-04-28-silver-dedupe-dq-01"

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
                "medallion_root": "Files/medallion",
                "snapshot_date": "2026-04-27",
                "readings_years_csv": "2023,2024,2025",
                "silver_records_table": "silver_soil_sensor_readings",
                "silver_quarantine_table": "silver_soil_sensor_quarantine",
                "silver_locations_table": "silver_soil_sensor_locations",
                "silver_include_api_source": True,
                "silver_include_file_source": True,
            }
            if os.environ.get("VICROOT_PARAMS_JSON"):
                params.update(json.loads(os.environ["VICROOT_PARAMS_JSON"]))
            if overrides:
                params.update(overrides)
            return params


# %% [markdown]
# # Cell 2 - Helpers

def _lakehouse_to_local(path: str) -> str:
    trimmed = path.strip()
    if trimmed.startswith("/lakehouse/"):
        return trimmed
    if trimmed.startswith("Files/"):
        return f"/lakehouse/default/{trimmed}"
    if trimmed.startswith("Tables/"):
        return f"/lakehouse/default/{trimmed}"
    return trimmed


def _parse_years(years_csv: str) -> List[str]:
    values = [v.strip() for v in years_csv.split(",") if v.strip()]
    if not values:
        raise ValueError("readings_years_csv is empty. Example: 2023,2024,2025")
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
    local_path = _lakehouse_to_local(path)
    candidates = [path, local_path]
    for candidate in candidates:
        try:
            df = spark.read.json(candidate)
            # Force evaluation to ensure path is valid/non-empty at read time.
            _ = df.columns
            return df
        except Exception:
            continue
    return None


def _read_csv_if_exists(spark: SparkSession, path: str) -> DataFrame | None:
    local_path = _lakehouse_to_local(path)
    candidates = [path, local_path]
    for candidate in candidates:
        try:
            df = spark.read.option("header", "true").csv(candidate)
            _ = df.columns
            return df
        except Exception:
            continue
    return None


def _read_structured_if_exists(spark: SparkSession, path: str) -> DataFrame | None:
    lowered = path.lower()
    if lowered.endswith(".csv"):
        return _read_csv_if_exists(spark, path)
    if lowered.endswith(".json") or lowered.endswith(".jsonl"):
        return _read_jsonl_if_exists(spark, path)
    # Fallback: try CSV first, then JSON.
    csv_df = _read_csv_if_exists(spark, path)
    if csv_df is not None:
        return csv_df
    return _read_jsonl_if_exists(spark, path)


def _discover_csv_candidates(root_path: str) -> List[str]:
    matches: List[str] = []
    search_roots = [_lakehouse_to_local(root_path), "/lakehouse/default/Files", "/lakehouse"]
    for local_root in search_roots:
        if not os.path.isdir(local_root):
            continue
        for dirpath, _, filenames in os.walk(local_root):
            for filename in filenames:
                lower = filename.lower()
                if lower.endswith(".csv") and "soil-sensor-readings-historical-data" in lower:
                    full_path = os.path.join(dirpath, filename)
                    if full_path.startswith("/lakehouse/default/"):
                        full_path = full_path.replace("/lakehouse/default/", "", 1)
                    matches.append(full_path)
    # preserve order while de-duplicating
    deduped: List[str] = []
    seen = set()
    for m in matches:
        if m in seen:
            continue
        seen.add(m)
        deduped.append(m)
    return deduped


def _csv_for_year_from_candidates(candidates: List[str], year: str) -> str | None:
    pattern = re.compile(rf"soil-sensor-readings-historical-data[-_]?{re.escape(year)}\.csv$", re.IGNORECASE)
    for c in candidates:
        if pattern.search(c):
            return c
    # fallback: if no strict match, use a unique filename token match for year.
    token_pattern = re.compile(rf"(^|[^0-9]){re.escape(year)}([^0-9]|$)")
    token_matches = [c for c in candidates if token_pattern.search(os.path.basename(c))]
    if len(token_matches) == 1:
        return token_matches[0]
    if len(token_matches) > 1:
        print(f"[WARN] ambiguous CSV candidates for year={year}; matched={len(token_matches)}. Set soil_csv_input_dir explicitly.")
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
            F.when(F.col("_source_type") == F.lit("file"), F.lit(1))
            .when(F.col("_source_type") == F.lit("api"), F.lit(2))
            .otherwise(F.lit(9)),
        )
    )
    dedupe_window = Window.partitionBy(
        "_k_site_id", "_k_probe_id", "_k_probe_measure", "_k_local_time", "_k_soil_value"
    ).orderBy(F.col("_source_priority").asc())
    return (
        keyed.withColumn("_rn", F.row_number().over(dedupe_window))
        .filter(F.col("_rn") == 1)
        .drop("_k_site_id", "_k_probe_id", "_k_probe_measure", "_k_local_time", "_k_soil_value", "_source_priority", "_rn")
    )


def _run_silver_dq_tests(
    *,
    dq_fail_on_error: bool,
    max_quarantine_ratio: float,
    silver_valid_df: DataFrame,
    silver_quarantine_df: DataFrame,
) -> None:
    valid_count = silver_valid_df.count()
    quarantine_count = silver_quarantine_df.count()
    total_count = valid_count + quarantine_count
    quarantine_ratio = (quarantine_count / total_count) if total_count else 1.0

    duplicate_keys = (
        silver_valid_df.groupBy("site_id", "probe_id", "probe_measure", "local_time_raw")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    null_key_rows = silver_valid_df.filter(
        F.col("site_id").isNull()
        | (F.trim(F.col("site_id")) == "")
        | F.col("local_time_ts").isNull()
        | F.col("soil_value_num").isNull()
    ).count()

    print(
        f"[DQ][SILVER] valid={valid_count} quarantine={quarantine_count} "
        f"ratio={quarantine_ratio:.4f} duplicate_keys={duplicate_keys} null_key_rows={null_key_rows}"
    )
    print("[DQ][SILVER] top quarantine reasons:")
    (
        silver_quarantine_df.select(F.explode_outer("dq_reasons").alias("dq_reason"))
        .groupBy("dq_reason")
        .count()
        .orderBy(F.desc("count"))
        .show(20, truncate=False)
    )

    failures: List[str] = []
    if valid_count == 0:
        failures.append("No valid Silver rows were produced.")
    if duplicate_keys > 0:
        failures.append(f"Found {duplicate_keys} duplicate business keys in silver_valid_df.")
    if quarantine_ratio > max_quarantine_ratio:
        failures.append(
            f"Quarantine ratio {quarantine_ratio:.4f} exceeds silver_dq_max_quarantine_ratio={max_quarantine_ratio:.4f}."
        )

    if failures:
        for message in failures:
            print(f"[DQ][SILVER][FAIL] {message}")
        if dq_fail_on_error:
            raise RuntimeError("Silver DQ checks failed. See [DQ][SILVER][FAIL] logs above.")
        print("[DQ][SILVER][WARN] dq_fail_on_error=false, continuing despite Silver DQ failures.")
    else:
        print("[DQ][SILVER] all checks passed.")


def _read_locations_if_exists(spark: SparkSession, path: str) -> DataFrame | None:
    local_path = _lakehouse_to_local(path)
    if not os.path.exists(local_path):
        return None
    payload_df = spark.read.json(local_path)
    if "results" not in payload_df.columns:
        return None
    return payload_df.select(F.explode_outer("results").alias("r")).select("r.*")


def _load_validation_rules(params: Dict[str, Any]) -> Dict[str, Any]:
    """Load GE-style validation rules from Lakehouse Files or fallback defaults."""
    configured_path = params.get("validation_rules_path", "Files/config/ge_validation_rules.json")
    candidate_paths = [
        configured_path,
        "Files/config/ge_validation_rules.json",
        "/lakehouse/default/Files/config/ge_validation_rules.json",
    ]
    for p in candidate_paths:
        local = _lakehouse_to_local(p)
        try:
            if os.path.exists(local):
                with open(local, "r", encoding="utf-8") as handle:
                    rules = json.load(handle)
                print(f"[INFO] loaded validation rules from: {p}")
                return rules
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] could not read validation rules at {p}: {exc}")
    print("[WARN] validation rules file not found; using built-in defaults.")
    return {
        "salinity_ms_cm": {"min": 0, "max": 50},
        "moisture_vwc": {"min": 0, "max": 100},
        "temp_c": {"min": -20, "max": 80},
    }


# %% [markdown]
# # Cell 3 - Resolve params and load Bronze sources

spark = SparkSession.builder.getOrCreate()
params = resolve_params({})
print(f"[INFO] notebook_version={NOTEBOOK_VERSION}")
print(
    json.dumps(
        {k: v for k, v in params.items() if "token" not in k.lower() and "key" not in k.lower()},
        indent=2,
    )
)

medallion_root = params.get("medallion_root", "Files/medallion").rstrip("/")
snapshot_date = params.get("snapshot_date", "2026-04-27")
years = _parse_years(params.get("readings_years_csv", "2023,2024,2025"))
include_api = _parse_bool(params.get("silver_include_api_source", True), True)
include_file = _parse_bool(params.get("silver_include_file_source", True), True)
soil_csv_input_dir = params.get("soil_csv_input_dir", f"{medallion_root}/bronze").rstrip("/")
soil_csv_file_pattern = params.get("soil_csv_file_pattern", "soil-sensor-readings-historical-data-{year}.csv")
dq_fail_on_error = _parse_bool(params.get("dq_fail_on_error", True), True)
silver_dq_max_quarantine_ratio = float(params.get("silver_dq_max_quarantine_ratio", 0.9))
site_file_input_path = params.get("site_file_input_path", "").strip()
weather_file_input_path = params.get("weather_file_input_path", "").strip()

records_dfs: List[DataFrame] = []
csv_candidates = _discover_csv_candidates(f"{medallion_root}/bronze")
if csv_candidates:
    print("[INFO] discovered CSV candidates under bronze:")
    for c in csv_candidates[:10]:
        print(f"  - {c}")
for year in years:
    if include_file:
        p = f"{medallion_root}/bronze/com_soil_sensor_readings/year={year}/raw/records_file.jsonl"
        df = _read_jsonl_if_exists(spark, p)
        if df is None:
            p_legacy = f"{medallion_root}/bronze/com_soil_sensor_readings/source=file/year={year}/raw/records.jsonl"
            df = _read_jsonl_if_exists(spark, p_legacy)
        if df is None:
            filename = soil_csv_file_pattern.format(year=year)
            p_csv_normalized = f"{medallion_root}/bronze/com_soil_sensor_readings/year={year}/raw/file/{filename}"
            p_csv_uploaded = f"{soil_csv_input_dir}/{filename}"
            df = _read_csv_if_exists(spark, p_csv_normalized)
            if df is None:
                p_csv_legacy = (
                    f"{medallion_root}/bronze/com_soil_sensor_readings/source=file/year={year}/raw/{filename}"
                )
                df = _read_csv_if_exists(spark, p_csv_legacy)
            if df is None:
                df = _read_csv_if_exists(spark, p_csv_uploaded)
            if df is None and csv_candidates:
                discovered = _csv_for_year_from_candidates(csv_candidates, year)
                if discovered:
                    print(f"[INFO] using discovered CSV for year={year}: {discovered}")
                    df = _read_csv_if_exists(spark, discovered)
            if df is not None:
                print(f"[INFO] using CSV fallback for year={year}: {p_csv_normalized} or {p_csv_uploaded}")
        if df is not None:
            records_dfs.append(df.withColumn("_source_type", F.lit("file")).withColumn("_source_year", F.lit(year)))
    if include_api:
        p = f"{medallion_root}/bronze/com_soil_sensor_readings/year={year}/raw/records_api.jsonl"
        df = _read_jsonl_if_exists(spark, p)
        if df is None:
            p_legacy = f"{medallion_root}/bronze/com_soil_sensor_readings/source=api/year={year}/raw/records.jsonl"
            df = _read_jsonl_if_exists(spark, p_legacy)
        if df is not None:
            records_dfs.append(df.withColumn("_source_type", F.lit("api")).withColumn("_source_year", F.lit(year)))

if not records_dfs:
    raise FileNotFoundError(
        "No Bronze records found. Expected one of:\n"
        "1) records_file.jsonl under year=YYYY/raw\n"
        "2) records_api.jsonl under year=YYYY/raw\n"
        "3) CSV files in year=YYYY/raw/file/ or soil_csv_input_dir with soil_csv_file_pattern.\n"
        f"Checked soil_csv_input_dir={soil_csv_input_dir}, years={years}. "
        "If files are in another folder, set soil_csv_input_dir to that exact Files path."
    )

bronze_records_df = records_dfs[0]
for next_df in records_dfs[1:]:
    bronze_records_df = bronze_records_df.unionByName(next_df, allowMissingColumns=True)

pre_dedupe_count = bronze_records_df.count()
bronze_records_df = _dedupe_bronze_records(bronze_records_df)
post_dedupe_count = bronze_records_df.count()
print(f"[INFO] loaded_bronze_records={pre_dedupe_count}")
print(f"[INFO] deduped_bronze_records={post_dedupe_count} (removed={pre_dedupe_count - post_dedupe_count})")

locations_path = (
    f"{medallion_root}/bronze/com_soil_sensor_locations/source=api/snapshot_date={snapshot_date}/raw/records.json"
)
locations_df = _read_locations_if_exists(spark, locations_path)
if locations_df is None:
    print(f"[WARN] locations file not found for snapshot_date={snapshot_date}; skipping locations silver table.")
else:
    print(f"[INFO] loaded_locations={locations_df.count()}")

site_file_bronze_path = ""
if site_file_input_path:
    site_file_bronze_path = (
        f"{medallion_root}/bronze/site_reference/source=file/snapshot_date={snapshot_date}/raw/"
        f"{os.path.basename(site_file_input_path)}"
    )
site_file_df = _read_structured_if_exists(spark, site_file_bronze_path) if site_file_bronze_path else None
if site_file_df is not None:
    print(f"[INFO] loaded_site_file_rows={site_file_df.count()}")
else:
    print("[INFO] no optional site file found for this snapshot.")

weather_file_bronze_path = ""
if weather_file_input_path:
    weather_file_bronze_path = (
        f"{medallion_root}/bronze/weather_daily/source=file/snapshot_date={snapshot_date}/raw/"
        f"{os.path.basename(weather_file_input_path)}"
    )
weather_file_df = _read_structured_if_exists(spark, weather_file_bronze_path) if weather_file_bronze_path else None
if weather_file_df is not None:
    print(f"[INFO] loaded_weather_file_rows={weather_file_df.count()}")
else:
    print("[INFO] no optional weather file found for this snapshot.")

weather_api_bronze_path = (
    f"{medallion_root}/bronze/weather_daily/source=api/snapshot_date={snapshot_date}/raw/weather_daily.jsonl"
)
weather_api_df = _read_structured_if_exists(spark, weather_api_bronze_path)
if weather_api_df is not None:
    print(f"[INFO] loaded_weather_api_rows={weather_api_df.count()}")
else:
    print("[INFO] no weather API bronze file found for this snapshot.")

if weather_file_df is not None and weather_api_df is not None:
    weather_file_df = weather_file_df.unionByName(weather_api_df, allowMissingColumns=True).dropDuplicates(["as_of_date"])
elif weather_file_df is None:
    weather_file_df = weather_api_df


# %% [markdown]
# # Cell 4 - Normalize and quality checks

# Bronze columns observed: local_time, site_name, site_id, id, probe_id, probe_measure, soil_value, unit, json_featuretype
silver_base_df = (
    bronze_records_df.withColumn("site_id", F.col("site_id").cast("string"))
    .withColumn("record_id", F.coalesce(F.col("id"), F.col("ID")).cast("string"))
    .withColumn("probe_id", F.col("probe_id").cast("string"))
    .withColumn("site_name", F.col("site_name").cast("string"))
    .withColumn("probe_measure", F.col("probe_measure").cast("string"))
    .withColumn("unit", F.col("unit").cast("string"))
    .withColumn("json_featuretype", F.col("json_featuretype").cast("string"))
    .withColumn("soil_value_raw", F.coalesce(F.col("soil_value"), F.col("Soil_Value")).cast("string"))
    # Unified value column for downstream tables (prevents empty `soil_value` when source used `Soil_Value`).
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
        "soil_texture",
        F.when(F.col("probe_measure_lc").contains("sandy loam"), F.lit("sandy_loam"))
        .when(F.col("probe_measure_lc").contains("clayey loam"), F.lit("clayey_loam"))
        .when(F.col("probe_measure_lc").contains("clay"), F.lit("clay"))
        .when(F.col("probe_measure_lc").contains("sand"), F.lit("sand"))
        .when(F.col("probe_measure_lc").contains("loam"), F.lit("loam"))
        .when(F.col("probe_measure_lc").contains("silt"), F.lit("silt"))
        .otherwise(F.lit("unknown")),
    )
    .withColumn(
        "measure_type",
        F.when(F.col("probe_measure_lc").contains("moisture") | (F.lower(F.col("unit")) == F.lit("%vwc")), F.lit("moisture"))
        .when(F.col("probe_measure_lc").contains("salinity") | (F.col("unit").isin("µS/cm", "mS/cm")), F.lit("salinity"))
        .when(F.col("probe_measure_lc").contains("temp") | F.col("unit").isin("ºC", "°C"), F.lit("temperature"))
        .otherwise(F.lit("other")),
    )
)

ge_rules = _load_validation_rules(params)
salinity_ms_cm_max = float(ge_rules.get("salinity_ms_cm", {}).get("max", 50))
moisture_vwc_min = float(ge_rules.get("moisture_vwc", {}).get("min", 0))
moisture_vwc_max = float(ge_rules.get("moisture_vwc", {}).get("max", 100))
temp_c_min = float(ge_rules.get("temp_c", {}).get("min", -20))
temp_c_max = float(ge_rules.get("temp_c", {}).get("max", 80))
salinity_us_cm_max = salinity_ms_cm_max * 1000.0

silver_checked_df = (
    silver_base_df.withColumn("dq_missing_site_id", F.col("site_id").isNull() | (F.trim(F.col("site_id")) == ""))
    .withColumn("dq_missing_value", F.col("soil_value_clean").isNull() | (F.col("soil_value_clean") == ""))
    .withColumn(
        "dq_non_numeric_value",
        ~(F.col("soil_value_clean").isNull() | (F.col("soil_value_clean") == "")) & F.col("soil_value_num").isNull(),
    )
    .withColumn("dq_invalid_time", F.col("local_time_ts").isNull())
    .withColumn(
        "dq_invalid_depth",
        F.col("depth_cm").isNotNull() & ((F.col("depth_cm") < F.lit(0)) | (F.col("depth_cm") > F.lit(300))),
    )
    .withColumn(
        "dq_sensor_sentinel",
        (
            F.col("soil_value_num").isin(-9999.0, -9999, -999.0, -999)
            | F.col("soil_value_raw").isin("-9999", "-9999.0", "-999", "-999.0")
        ),
    )
    .withColumn(
        "dq_salinity_out_of_range",
        F.when(
            F.col("unit") == F.lit("mS/cm"),
            (F.col("soil_value_num") < F.lit(0.0)) | (F.col("soil_value_num") > F.lit(salinity_ms_cm_max)),
        )
        .when(
            F.col("unit") == F.lit("µS/cm"),
            (F.col("soil_value_num") < F.lit(0.0)) | (F.col("soil_value_num") > F.lit(salinity_us_cm_max)),
        )
        .otherwise(F.lit(False)),
    )
    .withColumn(
        "dq_moisture_out_of_range",
        (
            (F.col("measure_type") == F.lit("moisture"))
            & ((F.col("soil_value_num") < F.lit(moisture_vwc_min)) | (F.col("soil_value_num") > F.lit(moisture_vwc_max)))
        ),
    )
    .withColumn(
        "dq_temp_out_of_range",
        (
            (F.col("measure_type") == F.lit("temperature"))
            & ((F.col("soil_value_num") < F.lit(temp_c_min)) | (F.col("soil_value_num") > F.lit(temp_c_max)))
        ),
    )
    .withColumn(
        "dq_is_valid",
        ~(
            F.col("dq_missing_site_id")
            | F.col("dq_missing_value")
            | F.col("dq_non_numeric_value")
            | F.col("dq_invalid_time")
            | F.col("dq_invalid_depth")
            | F.col("dq_sensor_sentinel")
            | F.col("dq_salinity_out_of_range")
            | F.col("dq_moisture_out_of_range")
            | F.col("dq_temp_out_of_range")
        ),
    )
    .withColumn(
        "dq_reasons_raw",
        F.array(
            F.when(F.col("dq_missing_site_id"), F.lit("missing_site_id")),
            F.when(F.col("dq_missing_value"), F.lit("missing_value")),
            F.when(F.col("dq_non_numeric_value"), F.lit("non_numeric_value")),
            F.when(F.col("dq_invalid_time"), F.lit("invalid_time")),
            F.when(F.col("dq_invalid_depth"), F.lit("invalid_depth")),
            F.when(F.col("dq_sensor_sentinel"), F.lit("sensor_sentinel")),
            F.when(F.col("dq_salinity_out_of_range"), F.lit("salinity_out_of_range")),
            F.when(F.col("dq_moisture_out_of_range"), F.lit("moisture_out_of_range")),
            F.when(F.col("dq_temp_out_of_range"), F.lit("temp_out_of_range")),
        ),
    )
    .withColumn("dq_reasons", F.expr("filter(dq_reasons_raw, x -> x is not null)"))
    .withColumn("dq_is_valid", F.size(F.col("dq_reasons")) == 0)
)

silver_valid_df = silver_checked_df.filter(F.col("dq_is_valid"))
silver_quarantine_df = silver_checked_df.filter(~F.col("dq_is_valid"))

# Keep output tables clean: remove helper DQ flags and parser internals.
drop_helper_cols_valid = [
    "probe_measure_lc",
    "soil_value_clean",
    "dq_reasons_raw",
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
]
silver_valid_df = silver_valid_df.drop(*drop_helper_cols_valid)

# Keep DQ diagnostics in quarantine for analyst-facing triage.
drop_helper_cols_quarantine = [
    "probe_measure_lc",
    "soil_value_clean",
    "dq_reasons_raw",
]
silver_quarantine_df = silver_quarantine_df.drop(*drop_helper_cols_quarantine)

print(f"[INFO] silver_valid_rows={silver_valid_df.count()}")
print(f"[INFO] silver_quarantine_rows={silver_quarantine_df.count()}")
_run_silver_dq_tests(
    dq_fail_on_error=dq_fail_on_error,
    max_quarantine_ratio=silver_dq_max_quarantine_ratio,
    silver_valid_df=silver_valid_df,
    silver_quarantine_df=silver_quarantine_df,
)


# %% [markdown]
# # Cell 5 - Write Silver Delta tables

silver_records_table = params.get("silver_records_table", "silver_soil_sensor_readings")
silver_quarantine_table = params.get("silver_quarantine_table", "silver_soil_sensor_quarantine")
silver_locations_table = params.get("silver_locations_table", "silver_soil_sensor_locations")
silver_site_file_table = params.get("silver_site_file_table", "silver_site_file_reference")
silver_weather_table = params.get("silver_weather_table", "silver_weather_daily")

silver_valid_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(silver_records_table)
silver_quarantine_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(silver_quarantine_table)

if locations_df is not None:
    locations_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(silver_locations_table)
if site_file_df is not None:
    site_file_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(silver_site_file_table)
if weather_file_df is not None:
    weather_file_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(silver_weather_table)

print(f"[DONE] wrote table: {silver_records_table}")
print(f"[DONE] wrote table: {silver_quarantine_table}")
if locations_df is not None:
    print(f"[DONE] wrote table: {silver_locations_table}")
if site_file_df is not None:
    print(f"[DONE] wrote table: {silver_site_file_table}")
if weather_file_df is not None:
    print(f"[DONE] wrote table: {silver_weather_table}")
