"""Gold layer: star schema for Power BI and clearer storage.

Dimensions: site, depth (10–80 cm), weather (daily climate).
Facts: salinity / moisture / temperature depth-daily tables (site × date × depth; day avg + 30d only),
       irrigation_risk (site × date; **moisture/temperature-led** shallow+deep profile + deltas; salinity tiers secondary),
       soil_depth_peer_daily (site × date × depth with weather bins + moisture-qualified **salinity** peer z-scores),
       site_depth_peer_summary (site × depth rollups for salinity anomaly screening).

Analytical focus aligns with docs/architecture.md *Analytical framing*: compare sites and shallow–deep profiles under
shared regional weather; salinity EC remains context/secondary.
"""

# %% [markdown]
# # Cell 1 - Imports and params

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

NOTEBOOK_VERSION = "v2026-05-04-gold-site-profile-deltas-01"

# Standard sensor depths (cm) — readings are snapped to nearest bucket.
STANDARD_DEPTH_CM: List[int] = [10, 20, 30, 40, 50, 60, 70, 80]

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
                "gold_dim_site_table": "gold_dim_site",
                "gold_dim_depth_table": "gold_dim_depth",
                "gold_dim_weather_daily_table": "gold_dim_weather_daily",
                "gold_fact_salinity_depth_daily_table": "gold_fact_salinity_depth_daily",
                "gold_fact_moisture_depth_daily_table": "gold_fact_moisture_depth_daily",
                "gold_fact_temperature_depth_daily_table": "gold_fact_temperature_depth_daily",
                "gold_fact_irrigation_risk_table": "gold_fact_irrigation_risk",
                "gold_fact_soil_depth_peer_daily_table": "gold_fact_soil_depth_peer_daily",
                "gold_site_depth_peer_summary_table": "gold_site_depth_peer_summary",
                "gold_ec_moisture_min_shallow_vwc": 25.0,
                "gold_ec_moisture_min_deep_vwc": 18.0,
                "gold_ec_moisture_shallow_depth_max_cm": 30,
                "gold_peer_min_sites": 5,
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


def _table_exists(spark: SparkSession, table_name: str) -> bool:
    if not table_name:
        return False
    try:
        spark.table(table_name).limit(1).count()
        return True
    except Exception:
        return False


def _standardize_depth_cm(depth_col):
    """Snap depth to nearest 10 cm in [10, 80]; null if input depth null."""
    rounded = (F.round(F.col(depth_col) / F.lit(10.0)) * F.lit(10)).cast("int")
    bounded = F.least(F.lit(80), F.greatest(F.lit(10), rounded))
    return F.when(F.col(depth_col).isNull(), F.lit(None).cast("int")).otherwise(bounded)


def _dup_pk_depth_fact(df) -> int:
    return df.groupBy("site_id", "as_of_date", "depth_cm").count().filter(F.col("count") > 1).count()


def _bad_depth_rows(df) -> int:
    return df.filter(~F.col("depth_cm").isin(STANDARD_DEPTH_CM)).count()


def _run_gold_dq_tests(
    *,
    dq_fail_on_error: bool,
    fact_salinity_df,
    fact_moisture_df,
    fact_temperature_df,
    fact_irrigation_df,
    dim_weather_df,
    dim_site_df,
) -> None:
    failures = []

    fs_s = fact_salinity_df.count()
    fs_m = fact_moisture_df.count()
    fs_t = fact_temperature_df.count()
    fi_count = fact_irrigation_df.count()

    dup_s = _dup_pk_depth_fact(fact_salinity_df)
    dup_m = _dup_pk_depth_fact(fact_moisture_df)
    dup_tea = _dup_pk_depth_fact(fact_temperature_df)
    dup_irr = (
        fact_irrigation_df.groupBy("site_id", "as_of_date").count().filter(F.col("count") > 1).count()
    )

    bad_s = _bad_depth_rows(fact_salinity_df)
    bad_m = _bad_depth_rows(fact_moisture_df)
    bad_t = _bad_depth_rows(fact_temperature_df)

    invalid_salinity_risk = fact_irrigation_df.filter(
        ~F.col("salinity_risk_tier").isin(
            "unknown",
            "non_saline",
            "slightly_saline",
            "moderately_saline",
            "highly_saline",
        )
    ).count()
    null_keys_irr = fact_irrigation_df.filter(
        F.col("site_id").isNull() | (F.trim(F.col("site_id")) == "") | F.col("as_of_date").isNull()
    ).count()

    print(
        f"[DQ][GOLD] fact_salinity={fs_s} fact_moisture={fs_m} fact_temperature={fs_t} fact_irrigation={fi_count} "
        f"dim_weather={dim_weather_df.count()} dim_site={dim_site_df.count()} "
        f"dup_pk salinity/moisture/temp={dup_s},{dup_m},{dup_tea} dup_irrigation={dup_irr} "
        f"bad_depth s/m/t={bad_s},{bad_m},{bad_t} null_irr_keys={null_keys_irr} "
        f"invalid_salinity_risk_tier={invalid_salinity_risk}"
    )

    if fs_s == 0 and fs_m == 0 and fs_t == 0:
        failures.append("All measure depth facts are empty (salinity, moisture, temperature).")
    if fi_count == 0:
        failures.append("fact_irrigation_risk is empty.")
    if dup_s + dup_m + dup_tea > 0:
        failures.append(
            f"Duplicate (site_id, as_of_date, depth_cm): salinity={dup_s}, moisture={dup_m}, temperature={dup_tea}."
        )
    if dup_irr > 0:
        failures.append(f"Duplicate (site_id, as_of_date) rows in fact_irrigation: {dup_irr}.")
    if bad_s + bad_m + bad_t > 0:
        failures.append(f"depth_cm outside standard set: salinity={bad_s}, moisture={bad_m}, temperature={bad_t}.")
    if null_keys_irr > 0:
        failures.append(f"fact_irrigation has {null_keys_irr} rows with null site_id or as_of_date.")
    if invalid_salinity_risk > 0:
        failures.append(f"fact_irrigation has {invalid_salinity_risk} rows with invalid salinity_risk_tier.")

    if failures:
        for message in failures:
            print(f"[DQ][GOLD][FAIL] {message}")
        if dq_fail_on_error:
            raise RuntimeError("Gold DQ checks failed. See [DQ][GOLD][FAIL] logs above.")
        print("[DQ][GOLD][WARN] dq_fail_on_error=false, continuing despite Gold DQ failures.")
    else:
        print("[DQ][GOLD] all checks passed.")


# %% [markdown]
# # Cell 2 - Load Silver + build dimensions

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
gold_dim_site_table = params.get("gold_dim_site_table", "gold_dim_site")
gold_dim_depth_table = params.get("gold_dim_depth_table", "gold_dim_depth")
gold_dim_weather_daily_table = params.get("gold_dim_weather_daily_table", "gold_dim_weather_daily")
gold_fact_salinity_depth_daily_table = params.get(
    "gold_fact_salinity_depth_daily_table", "gold_fact_salinity_depth_daily"
)
gold_fact_moisture_depth_daily_table = params.get(
    "gold_fact_moisture_depth_daily_table", "gold_fact_moisture_depth_daily"
)
gold_fact_temperature_depth_daily_table = params.get(
    "gold_fact_temperature_depth_daily_table", "gold_fact_temperature_depth_daily"
)
gold_fact_irrigation_risk_table = params.get("gold_fact_irrigation_risk_table", "gold_fact_irrigation_risk")
gold_fact_soil_depth_peer_daily_table = params.get(
    "gold_fact_soil_depth_peer_daily_table", "gold_fact_soil_depth_peer_daily"
)
gold_site_depth_peer_summary_table = params.get(
    "gold_site_depth_peer_summary_table", "gold_site_depth_peer_summary"
)
gold_ec_moisture_min_shallow_vwc = float(params.get("gold_ec_moisture_min_shallow_vwc", 25.0))
gold_ec_moisture_min_deep_vwc = float(params.get("gold_ec_moisture_min_deep_vwc", 18.0))
gold_ec_moisture_shallow_depth_max_cm = int(params.get("gold_ec_moisture_shallow_depth_max_cm", 30))
gold_peer_min_sites = int(params.get("gold_peer_min_sites", 5))
silver_site_table = params.get(
    "silver_site_file_table",
    params.get("silver_site_table", "silver_site_file_reference"),
)
weather_daily_table = params.get("weather_daily_table", "silver_weather_daily")
weather_date_col = params.get("weather_date_col", "as_of_date")
weather_rainfall_col = params.get("weather_rainfall_col", "rainfall_mm")
weather_evap_col = params.get("weather_evap_col", "evap_mm")
snapshot_date = params.get("snapshot_date", "2026-04-27")
dq_fail_on_error = _parse_bool(params.get("dq_fail_on_error", True), True)

silver_df = spark.table(silver_records_table)
print(f"[INFO] silver_records_rows={silver_df.count()}")

# ---- dim_depth (static reference)
depth_rows = [(d, f"{d} cm") for d in STANDARD_DEPTH_CM]
dim_depth_df = spark.createDataFrame(depth_rows, schema="depth_cm int, depth_label string")
dim_depth_df = dim_depth_df.withColumn("snapshot_date", F.lit(snapshot_date))

# ---- dim_site (latest row per site from Silver site reference)
if _table_exists(spark, silver_site_table):
    site_window = Window.partitionBy("site_id").orderBy(F.col("snapshot_date").desc_nulls_last())
    dim_site_df = (
        spark.table(silver_site_table)
        .withColumn("site_id", F.col("site_id").cast("string"))
        .withColumn("site_name", F.col("site_name").cast("string"))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("site_row_num", F.row_number().over(site_window))
        .filter(F.col("site_row_num") == 1)
        .select("site_id", "site_name", "latitude", "longitude", "snapshot_date")
    )
else:
    dim_site_df = spark.createDataFrame([], schema="site_id string, site_name string, latitude double, longitude double, snapshot_date string")

# ---- dim_weather_daily (grain: one row per calendar day; region-wide series)
if _table_exists(spark, weather_daily_table):
    wdf = spark.table(weather_daily_table)
    wc = set(wdf.columns)

    def _pick_date_col() -> str | None:
        for c in (weather_date_col, "as_of_date", "date", "Date"):
            if c in wc:
                return c
        return None

    def _pick_double(*candidates: str):
        for c in candidates:
            if c in wc:
                return F.col(c).cast("double")
        return F.lit(None).cast("double")

    date_col = _pick_date_col()
    if date_col:
        dw = wdf.withColumn("as_of_date", F.to_date(F.col(date_col)))
    else:
        dw = wdf.withColumn("as_of_date", F.lit(None).cast("date"))

    dim_weather_df = (
        dw.withColumn("rainfall_mm", _pick_double(weather_rainfall_col, "rainfall_mm", "rain_mm", "precipitation_mm"))
        .withColumn("evap_mm", _pick_double(weather_evap_col, "evap_mm", "et_mm", "evaporation_mm"))
        .withColumn("humidity_avg_pct", _pick_double("humidity_avg_pct", "humidity_pct"))
        .withColumn("temp_avg_c", _pick_double("temp_avg_c", "temperature_avg_c", "temp_c"))
        .withColumn("temp_min_c", _pick_double("temp_min_c", "temperature_min_c"))
        .withColumn("temp_max_c", _pick_double("temp_max_c", "temperature_max_c"))
        .withColumn("wind_speed_avg_ms", _pick_double("wind_speed_avg_ms", "wind_speed_ms"))
        .withColumn(
            "temp_range_c",
            F.when(
                F.col("temp_max_c").isNotNull() & F.col("temp_min_c").isNotNull(),
                F.col("temp_max_c") - F.col("temp_min_c"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("model_snapshot_date", F.lit(snapshot_date))
        .select(
            "as_of_date",
            "rainfall_mm",
            "evap_mm",
            "humidity_avg_pct",
            "temp_avg_c",
            "temp_min_c",
            "temp_max_c",
            "temp_range_c",
            "wind_speed_avg_ms",
            "model_snapshot_date",
        )
        .dropDuplicates(["as_of_date"])
        .filter(F.col("as_of_date").isNotNull())
    )
else:
    dim_weather_df = spark.createDataFrame(
        [],
        schema="as_of_date date, rainfall_mm double, evap_mm double, humidity_avg_pct double, temp_avg_c double, "
        "temp_min_c double, temp_max_c double, temp_range_c double, wind_speed_avg_ms double, model_snapshot_date string",
    )


# %% [markdown]
# # Cell 3 - Measure facts (site × date × depth; daily avg + 30d only, per-measure reading_count)

base_df = (
    silver_df.withColumn("site_id", F.col("site_id").cast("string"))
    .withColumn("as_of_date", F.col("as_of_date").cast("date"))
    .withColumn("depth_cm", _standardize_depth_cm("depth_cm"))
    # Preserve salinity sensor numeric scale. The source labels salinity as µS/cm,
    # but observed values are mS/cm/dS/m-like; treat as a sensor EC proxy.
    .withColumn("soil_value_for_measure", F.col("soil_value_num"))
    .filter(F.col("depth_cm").isNotNull() & F.col("measure_type").isNotNull())
)

daily_by_measure = (
    base_df.groupBy("site_id", "as_of_date", "depth_cm", "measure_type")
    .agg(
        F.count(F.lit(1)).alias("reading_count"),
        F.avg("soil_value_for_measure").alias("value_avg"),
    )
)

w30_depth = Window.partitionBy("site_id", "depth_cm").orderBy("as_of_date").rowsBetween(-29, 0)


def _measure_depth_fact(measure_type: str, avg_col: str, roll_col: str):
    core = (
        daily_by_measure.filter(F.col("measure_type") == F.lit(measure_type))
        .select(
            "site_id",
            "as_of_date",
            "depth_cm",
            F.col("reading_count").alias("reading_count"),
            F.col("value_avg").alias(avg_col),
        )
    )
    return (
        core.withColumn(roll_col, F.avg(avg_col).over(w30_depth))
        .withColumn("snapshot_date", F.lit(snapshot_date))
    )


fact_salinity_df = _measure_depth_fact("salinity", "salinity_ec_avg", "salinity_ec_30d_avg")
fact_moisture_df = _measure_depth_fact("moisture", "moisture_vwc_avg", "moisture_vwc_30d_avg")
fact_temperature_df = _measure_depth_fact("temperature", "temp_c_avg", "temp_c_30d_avg")


def _shallow_metric(fact_df, metric_col: str, alias: str):
    """Coalesce metric from depths 10 → 20 → 30 cm for shallow profile."""
    d10 = fact_df.filter(F.col("depth_cm") == F.lit(10)).select(
        "site_id", "as_of_date", F.col(metric_col).alias("_v10")
    )
    d20 = fact_df.filter(F.col("depth_cm") == F.lit(20)).select(
        "site_id", "as_of_date", F.col(metric_col).alias("_v20")
    )
    d30 = fact_df.filter(F.col("depth_cm") == F.lit(30)).select(
        "site_id", "as_of_date", F.col(metric_col).alias("_v30")
    )
    return (
        d10.join(d20, on=["site_id", "as_of_date"], how="outer")
        .join(d30, on=["site_id", "as_of_date"], how="outer")
        .withColumn(alias, F.coalesce(F.col("_v10"), F.col("_v20"), F.col("_v30")))
        .select("site_id", "as_of_date", alias)
    )


def _deep_metric(fact_df, metric_col: str, alias: str):
    """Coalesce metric from depths 80 → 70 → 60 → 50 cm for deep profile (vs shallow)."""
    d80 = fact_df.filter(F.col("depth_cm") == F.lit(80)).select(
        "site_id", "as_of_date", F.col(metric_col).alias("_v80")
    )
    d70 = fact_df.filter(F.col("depth_cm") == F.lit(70)).select(
        "site_id", "as_of_date", F.col(metric_col).alias("_v70")
    )
    d60 = fact_df.filter(F.col("depth_cm") == F.lit(60)).select(
        "site_id", "as_of_date", F.col(metric_col).alias("_v60")
    )
    d50 = fact_df.filter(F.col("depth_cm") == F.lit(50)).select(
        "site_id", "as_of_date", F.col(metric_col).alias("_v50")
    )
    return (
        d80.join(d70, on=["site_id", "as_of_date"], how="outer")
        .join(d60, on=["site_id", "as_of_date"], how="outer")
        .join(d50, on=["site_id", "as_of_date"], how="outer")
        .withColumn(alias, F.coalesce(F.col("_v80"), F.col("_v70"), F.col("_v60"), F.col("_v50")))
        .select("site_id", "as_of_date", alias)
    )


# %% [markdown]
# # Cell 4 - Fact irrigation_risk (site × date; shallow + deep profile + salinity secondary)

site_day_readings = base_df.groupBy("site_id", "as_of_date").agg(F.count(F.lit(1)).alias("reading_count"))

m_shallow = _shallow_metric(fact_moisture_df, "moisture_vwc_avg", "moisture_shallow_vwc_avg")
m_deep = _deep_metric(fact_moisture_df, "moisture_vwc_avg", "moisture_deep_vwc_avg")
s_shallow = _shallow_metric(fact_salinity_df, "salinity_ec_avg", "salinity_shallow_ec_avg")
t_shallow = _shallow_metric(fact_temperature_df, "temp_c_avg", "temp_shallow_c_avg")
t_deep = _deep_metric(fact_temperature_df, "temp_c_avg", "temp_deep_c_avg")

irrigation_df = (
    site_day_readings.join(m_shallow, on=["site_id", "as_of_date"], how="left")
    .join(m_deep, on=["site_id", "as_of_date"], how="left")
    .join(s_shallow, on=["site_id", "as_of_date"], how="left")
    .join(t_shallow, on=["site_id", "as_of_date"], how="left")
    .join(t_deep, on=["site_id", "as_of_date"], how="left")
)

if _table_exists(spark, weather_daily_table):
    weather_for_pressure = dim_weather_df.select(
        "as_of_date",
        F.col("rainfall_mm").alias("_rainfall_mm"),
        F.col("evap_mm").alias("_evap_mm"),
    )
    irrigation_df = irrigation_df.join(weather_for_pressure, on="as_of_date", how="left")
else:
    irrigation_df = irrigation_df.withColumn("_rainfall_mm", F.lit(None).cast("double")).withColumn(
        "_evap_mm", F.lit(None).cast("double")
    )

salinity_shallow_30 = Window.partitionBy("site_id").orderBy("as_of_date").rowsBetween(-29, 0)
fact_irrigation_df = (
    irrigation_df.withColumn(
        "moisture_deficit_index",
        F.when(F.col("moisture_shallow_vwc_avg").isNull(), F.lit(None).cast("double")).otherwise(
            F.lit(100.0) - F.col("moisture_shallow_vwc_avg")
        ),
    )
    .withColumn(
        "evap_moisture_pressure",
        F.when(F.col("_evap_mm").isNull() | F.col("_rainfall_mm").isNull(), F.lit(None).cast("double")).otherwise(
            F.col("_evap_mm") - F.col("_rainfall_mm")
        ),
    )
    .withColumn("salinity_shallow_30d_avg", F.avg("salinity_shallow_ec_avg").over(salinity_shallow_30))
    .withColumn(
        "salinity_trend_vs_30d",
        F.when(
            F.col("salinity_shallow_ec_avg").isNull() | F.col("salinity_shallow_30d_avg").isNull(),
            F.lit(None).cast("double"),
        ).otherwise(F.col("salinity_shallow_ec_avg") - F.col("salinity_shallow_30d_avg")),
    )
    .withColumn(
        "moisture_shallow_minus_deep_vwc",
        F.when(
            F.col("moisture_shallow_vwc_avg").isNull() | F.col("moisture_deep_vwc_avg").isNull(),
            F.lit(None).cast("double"),
        ).otherwise(F.col("moisture_shallow_vwc_avg") - F.col("moisture_deep_vwc_avg")),
    )
    .withColumn(
        "temp_shallow_minus_deep_c",
        F.when(
            F.col("temp_shallow_c_avg").isNull() | F.col("temp_deep_c_avg").isNull(),
            F.lit(None).cast("double"),
        ).otherwise(F.col("temp_shallow_c_avg") - F.col("temp_deep_c_avg")),
    )
    .withColumn(
        "salinity_risk_tier",
        F.when(F.col("salinity_shallow_ec_avg").isNull(), F.lit("unknown"))
        .when(F.col("salinity_shallow_ec_avg") < F.lit(2.0), F.lit("non_saline"))
        .when(F.col("salinity_shallow_ec_avg") < F.lit(4.0), F.lit("slightly_saline"))
        .when(F.col("salinity_shallow_ec_avg") < F.lit(8.0), F.lit("moderately_saline"))
        .otherwise(F.lit("highly_saline")),
    )
    .withColumn(
        "salinity_risk_reason",
        F.when(F.col("salinity_shallow_ec_avg").isNull(), F.lit("no shallow salinity reading"))
        .when(F.col("salinity_shallow_ec_avg") < F.lit(2.0), F.lit("0-2 sensor EC proxy"))
        .when(F.col("salinity_shallow_ec_avg") < F.lit(4.0), F.lit("2-4 sensor EC proxy"))
        .when(F.col("salinity_shallow_ec_avg") < F.lit(8.0), F.lit("4-8 sensor EC proxy"))
        .otherwise(F.lit(">8 sensor EC proxy")),
    )
    .withColumn(
        "salinity_observed_band",
        F.when(F.col("salinity_shallow_ec_avg").isNull(), F.lit("unknown"))
        .when(F.col("salinity_shallow_ec_avg") < F.lit(0.3), F.lit("low_observed"))
        .when(F.col("salinity_shallow_ec_avg") < F.lit(0.8), F.lit("medium_observed"))
        .otherwise(F.lit("high_observed")),
    )
    .withColumn(
        "moisture_status",
        F.when(F.col("moisture_shallow_vwc_avg").isNull(), F.lit("unknown"))
        .when(F.col("moisture_shallow_vwc_avg") < F.lit(40.0), F.lit("low_moisture"))
        .when(F.col("moisture_shallow_vwc_avg") <= F.lit(80.0), F.lit("target_zone"))
        .otherwise(F.lit("high_moisture")),
    )
    .withColumn("snapshot_date", F.lit(snapshot_date))
    .drop("_rainfall_mm", "_evap_mm")
    .select(
        "site_id",
        "as_of_date",
        "reading_count",
        "moisture_shallow_vwc_avg",
        "moisture_deep_vwc_avg",
        "moisture_shallow_minus_deep_vwc",
        "salinity_shallow_ec_avg",
        "temp_shallow_c_avg",
        "temp_deep_c_avg",
        "temp_shallow_minus_deep_c",
        "evap_moisture_pressure",
        "moisture_deficit_index",
        "salinity_shallow_30d_avg",
        "salinity_trend_vs_30d",
        "salinity_risk_tier",
        "salinity_risk_reason",
        "salinity_observed_band",
        "moisture_status",
        "snapshot_date",
    )
)


# %% [markdown]
# # Cell 4b - Soil depth × weather bins × peer z-scores (site × date × depth)

weather_binned = (
    dim_weather_df.withColumn(
        "stress_mm",
        F.coalesce(F.col("evap_mm"), F.lit(0.0)) - F.coalesce(F.col("rainfall_mm"), F.lit(0.0)),
    )
    .withColumn(
        "rain_band",
        F.when(F.col("rainfall_mm").isNull(), F.lit("rain_unknown"))
        .when(F.col("rainfall_mm") < F.lit(1.0), F.lit("rain_low"))
        .otherwise(F.lit("rain_high")),
    )
    .withColumn(
        "temp_band",
        F.when(F.col("temp_avg_c").isNull(), F.lit("temp_unknown"))
        .when(F.col("temp_avg_c") < F.lit(16.0), F.lit("temp_cool"))
        .when(F.col("temp_avg_c") > F.lit(24.0), F.lit("temp_warm"))
        .otherwise(F.lit("temp_mild")),
    )
    .withColumn("weather_bin_key", F.concat_ws("|", F.col("rain_band"), F.col("temp_band")))
    .select(
        "as_of_date",
        "stress_mm",
        "rain_band",
        "temp_band",
        "weather_bin_key",
        "rainfall_mm",
        "evap_mm",
        "temp_avg_c",
    )
)

fs2 = fact_salinity_df.select(
    "site_id",
    "as_of_date",
    "depth_cm",
    F.col("reading_count").alias("salinity_reading_count"),
    "salinity_ec_avg",
    "salinity_ec_30d_avg",
)
fm2 = fact_moisture_df.select(
    "site_id",
    "as_of_date",
    "depth_cm",
    F.col("reading_count").alias("moisture_reading_count"),
    "moisture_vwc_avg",
    "moisture_vwc_30d_avg",
)
ft2 = fact_temperature_df.select(
    "site_id",
    "as_of_date",
    "depth_cm",
    F.col("reading_count").alias("temperature_reading_count"),
    "temp_c_avg",
    "temp_c_30d_avg",
)
depth_union = fs2.join(fm2, on=["site_id", "as_of_date", "depth_cm"], how="outer").join(
    ft2, on=["site_id", "as_of_date", "depth_cm"], how="outer"
)

depth_weather = depth_union.join(weather_binned, on="as_of_date", how="left").withColumn(
    "weather_bin_key",
    F.coalesce(F.col("weather_bin_key"), F.lit("missing_weather")),
)

shallow_max = F.lit(gold_ec_moisture_shallow_depth_max_cm)
moist_shallow = F.lit(gold_ec_moisture_min_shallow_vwc)
moist_deep = F.lit(gold_ec_moisture_min_deep_vwc)

ec_reading_reliable = (
    F.col("salinity_ec_avg").isNotNull()
    & F.col("moisture_vwc_avg").isNotNull()
    & F.col("depth_cm").isNotNull()
    & (
        ((F.col("depth_cm") <= shallow_max) & (F.col("moisture_vwc_avg") >= moist_shallow))
        | ((F.col("depth_cm") > shallow_max) & (F.col("moisture_vwc_avg") >= moist_deep))
    )
)

depth_tagged = depth_weather.withColumn("ec_reading_reliable", ec_reading_reliable)

peer_base = depth_tagged.filter(F.col("ec_reading_reliable")).select(
    "site_id",
    "as_of_date",
    "depth_cm",
    "weather_bin_key",
    F.col("salinity_ec_avg").alias("_peer_sal"),
)

peer_stats = peer_base.groupBy("as_of_date", "depth_cm", "weather_bin_key").agg(
    F.avg("_peer_sal").alias("peer_mean_salinity_qualified"),
    F.stddev_pop("_peer_sal").alias("peer_stddev_salinity_qualified"),
    F.count(F.lit(1)).alias("peer_n_qualified_sites"),
)

fact_soil_depth_peer_daily = (
    depth_tagged.join(peer_stats, on=["as_of_date", "depth_cm", "weather_bin_key"], how="left")
    .withColumn(
        "salinity_ec_peer_z",
        F.when(
            ~F.col("ec_reading_reliable")
            | F.col("peer_n_qualified_sites").isNull()
            | (F.col("peer_n_qualified_sites") < F.lit(gold_peer_min_sites))
            | F.col("peer_stddev_salinity_qualified").isNull()
            | (F.col("peer_stddev_salinity_qualified") < F.lit(1.0e-9)),
            F.lit(None).cast("double"),
        ).otherwise(
            (F.col("salinity_ec_avg") - F.col("peer_mean_salinity_qualified"))
            / F.col("peer_stddev_salinity_qualified")
        ),
    )
    .withColumn("snapshot_date", F.lit(snapshot_date))
)

gold_site_depth_peer_summary = fact_soil_depth_peer_daily.groupBy("site_id", "depth_cm").agg(
    F.count(F.lit(1)).alias("n_days_any_measure"),
    F.sum(F.col("ec_reading_reliable").cast("int")).alias("n_days_ec_reliable"),
    F.avg("salinity_ec_peer_z").alias("mean_salinity_peer_z"),
    F.avg(F.abs(F.col("salinity_ec_peer_z"))).alias("mean_abs_salinity_peer_z"),
    F.sum(F.when(F.col("salinity_ec_peer_z") > F.lit(2.0), F.lit(1)).otherwise(F.lit(0))).alias("n_days_peer_z_gt_2"),
    F.sum(F.when(F.col("salinity_ec_peer_z") < F.lit(-2.0), F.lit(1)).otherwise(F.lit(0))).alias("n_days_peer_z_lt_neg2"),
).withColumn(
    "pct_days_ec_reliable",
    F.when(F.col("n_days_any_measure") > 0, F.col("n_days_ec_reliable") / F.col("n_days_any_measure")).otherwise(
        F.lit(None).cast("double")
    ),
).withColumn("snapshot_date", F.lit(snapshot_date))

print(f"[INFO] fact_salinity_rows={fact_salinity_df.count()}")
print(f"[INFO] fact_moisture_rows={fact_moisture_df.count()}")
print(f"[INFO] fact_temperature_rows={fact_temperature_df.count()}")
print(f"[INFO] fact_irrigation_rows={fact_irrigation_df.count()}")
print(f"[INFO] fact_soil_depth_peer_daily_rows={fact_soil_depth_peer_daily.count()}")
print(f"[INFO] gold_site_depth_peer_summary_rows={gold_site_depth_peer_summary.count()}")

_run_gold_dq_tests(
    dq_fail_on_error=dq_fail_on_error,
    fact_salinity_df=fact_salinity_df,
    fact_moisture_df=fact_moisture_df,
    fact_temperature_df=fact_temperature_df,
    fact_irrigation_df=fact_irrigation_df,
    dim_weather_df=dim_weather_df,
    dim_site_df=dim_site_df,
)


# %% [markdown]
# # Cell 5 - Write star schema tables

dim_site_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(gold_dim_site_table)
dim_depth_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(gold_dim_depth_table)
dim_weather_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(gold_dim_weather_daily_table)
fact_salinity_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(
    gold_fact_salinity_depth_daily_table
)
fact_moisture_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(
    gold_fact_moisture_depth_daily_table
)
fact_temperature_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(
    gold_fact_temperature_depth_daily_table
)
fact_irrigation_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(gold_fact_irrigation_risk_table)
fact_soil_depth_peer_daily.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(
    gold_fact_soil_depth_peer_daily_table
)
gold_site_depth_peer_summary.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(
    gold_site_depth_peer_summary_table
)

print(f"[DONE] wrote {gold_dim_site_table}")
print(f"[DONE] wrote {gold_dim_depth_table}")
print(f"[DONE] wrote {gold_dim_weather_daily_table}")
print(f"[DONE] wrote {gold_fact_salinity_depth_daily_table}")
print(f"[DONE] wrote {gold_fact_moisture_depth_daily_table}")
print(f"[DONE] wrote {gold_fact_temperature_depth_daily_table}")
print(f"[DONE] wrote {gold_fact_irrigation_risk_table}")
print(f"[DONE] wrote {gold_fact_soil_depth_peer_daily_table}")
print(f"[DONE] wrote {gold_site_depth_peer_summary_table}")
