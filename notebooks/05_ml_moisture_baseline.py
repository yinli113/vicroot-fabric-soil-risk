"""Fabric ML skeleton: baseline linear model for soil moisture vs lagged weather.

Reads Gold moisture depth fact + Gold weather dimension, engineers simple rolling features,
fits Spark MLlib LinearRegression on a **time-based train split**, writes a Delta prediction table
for Power BI or QA.

Deployment: attach this notebook to a Fabric pipeline (schedule optional). Outputs land in the
**same lakehouse** as other Gold tables unless you add an ADLS shortcut path later.

See docs/architecture.md for table naming; params in notebooks/lib/pipeline_params.py.
"""

# %% [markdown]
# # Cell 1 - Imports and params

from __future__ import annotations

import json
import os
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

NOTEBOOK_VERSION = "v2026-05-06-ml-moisture-baseline-02"

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
                "snapshot_date": "2026-05-06",
                "gold_fact_moisture_depth_daily_table": "gold_fact_moisture_depth_daily",
                "gold_dim_weather_daily_table": "gold_dim_weather_daily",
                "gold_ml_moisture_baseline_table": "gold_ml_moisture_baseline",
                "ml_target_depth_cm": 30,
                "ml_train_end_date": "2023-12-31",
                "ml_rolling_days": 7,
                "ml_min_train_rows": 80,
                "ml_model_id": "linreg_weather_lags_v1",
            }
            if os.environ.get("VICROOT_PARAMS_JSON"):
                params.update(json.loads(os.environ["VICROOT_PARAMS_JSON"]))
            if overrides:
                params.update(overrides)
            return params


def _table_exists(spark: SparkSession, table_name: str) -> bool:
    if not table_name:
        return False
    try:
        spark.table(table_name).limit(1).count()
        return True
    except Exception:
        return False


def _parse_date(s: str) -> date:
    parts = (s or "").strip().split("-")
    if len(parts) != 3:
        raise ValueError(f"Expected YYYY-MM-DD, got {s!r}")
    return date(int(parts[0]), int(parts[1]), int(parts[2]))


# %% [markdown]
# # Cell 2 - Load Gold, join weather, rolling features

spark = SparkSession.builder.getOrCreate()
params = resolve_params({})
print(f"[INFO] notebook_version={NOTEBOOK_VERSION}")
print(
    json.dumps(
        {k: v for k, v in params.items() if "token" not in k.lower() and "key" not in k.lower()},
        indent=2,
    )
)

moisture_table = params.get("gold_fact_moisture_depth_daily_table", "gold_fact_moisture_depth_daily")
weather_table = params.get("gold_dim_weather_daily_table", "gold_dim_weather_daily")
out_table = params.get("gold_ml_moisture_baseline_table", "gold_ml_moisture_baseline")
target_depth = int(params.get("ml_target_depth_cm", 30))
train_end = _parse_date(str(params.get("ml_train_end_date", "2023-12-31")))
rolling_days = int(params.get("ml_rolling_days", 7))
min_train_rows = int(params.get("ml_min_train_rows", 80))
model_id = str(params.get("ml_model_id", "linreg_weather_lags_v1"))
snapshot_date = str(params.get("snapshot_date", "2026-05-06"))

if not _table_exists(spark, moisture_table):
    raise RuntimeError(f"Missing table {moisture_table}. Run 03_gold_features first.")
if not _table_exists(spark, weather_table):
    raise RuntimeError(f"Missing table {weather_table}. Run Gold weather dim / Silver weather first.")

m = spark.table(moisture_table).filter(F.col("depth_cm") == F.lit(target_depth))
w = spark.table(weather_table).select(
    F.col("as_of_date"),
    F.coalesce(F.col("rainfall_mm"), F.lit(0.0)).alias("rainfall_mm"),
    F.coalesce(F.col("evap_mm"), F.lit(0.0)).alias("evap_mm"),
    F.col("temp_avg_c"),
)

joined = (
    m.join(w, on="as_of_date", how="inner")
    .withColumn(
        "stress_mm_daily",
        F.when(F.col("evap_mm").isNull() | F.col("rainfall_mm").isNull(), F.lit(None).cast("double")).otherwise(
            F.col("evap_mm") - F.col("rainfall_mm")
        ),
    )
    .filter(F.col("moisture_vwc_avg").isNotNull())
)

win = Window.partitionBy("site_id", "depth_cm").orderBy("as_of_date").rowsBetween(-(rolling_days - 1), 0)
rolled = (
    joined.withColumn("rain_7d_mm", F.sum("rainfall_mm").over(win))
    .withColumn("evap_7d_mm", F.sum("evap_mm").over(win))
    .withColumn("stress_7d_mm", F.sum("stress_mm_daily").over(win))
    .withColumn(
        "moisture_lag1_vwc",
        F.lag("moisture_vwc_avg", 1).over(Window.partitionBy("site_id", "depth_cm").orderBy("as_of_date")),
    )
)

feature_cols = ["rain_7d_mm", "evap_7d_mm", "stress_7d_mm", "temp_avg_c", "moisture_lag1_vwc"]
scored = rolled.dropna(subset=feature_cols + ["moisture_vwc_avg"])
print(f"[INFO] scored_rows_depth_{target_depth}={scored.count()}")

# %% [markdown]
# # Cell 3 - Fit / score (MLlib) and write Delta

train_cutoff = F.to_date(F.lit(train_end.isoformat()))
train_df = scored.filter(F.col("as_of_date") <= train_cutoff)
test_df = scored.filter(F.col("as_of_date") > train_cutoff)
train_n = train_df.count()
test_n = test_df.count()
print(f"[INFO] train_rows={train_n} test_rows={test_n} train_end={train_end.isoformat()}")

if train_n < min_train_rows:
    print(
        f"[WARN] train_rows={train_n} < ml_min_train_rows={min_train_rows}; "
        "writing scored features with null predictions (skip fit)."
    )
    out_df = (
        scored.withColumn("pred_moisture_vwc_avg", F.lit(None).cast("double"))
        .withColumn("moisture_residual", F.lit(None).cast("double"))
        .withColumn("is_train_row", F.when(F.col("as_of_date") <= train_cutoff, F.lit(True)).otherwise(F.lit(False)))
        .withColumn("model_id", F.lit(model_id))
        .withColumn("snapshot_date", F.lit(snapshot_date))
        .select(
            "site_id",
            "as_of_date",
            "depth_cm",
            "moisture_vwc_avg",
            "moisture_vwc_30d_avg",
            "reading_count",
            *feature_cols,
            "pred_moisture_vwc_avg",
            "moisture_residual",
            "is_train_row",
            "model_id",
            "snapshot_date",
        )
    )
else:
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.regression import LinearRegression

    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="error")
    vec_train = assembler.transform(train_df)
    lr = LinearRegression(
        featuresCol="features",
        labelCol="moisture_vwc_avg",
        predictionCol="pred_moisture_vwc_avg",
        maxIter=100,
        regParam=0.01,
    )
    model = lr.fit(vec_train)
    # Training metrics (R² / RMSE on the fit set — not the same as test generalization)
    _summ = model.summary
    print(
        f"[ML][TRAIN] RMSE={_summ.rootMeanSquaredError:.4f} r2={_summ.r2:.4f} "
        f"numInstances={_summ.numInstances}"
    )
    print(f"[ML][TRAIN] intercept={float(model.intercept):.6f}")
    for _name, _c in zip(feature_cols, model.coefficients.toArray().tolist()):
        print(f"[ML][COEF] {_name}={_c:.6f}")

    from pyspark.ml.evaluation import RegressionEvaluator

    pred_test = model.transform(assembler.transform(test_df))
    _ev_r2 = RegressionEvaluator(
        labelCol="moisture_vwc_avg", predictionCol="pred_moisture_vwc_avg", metricName="r2"
    )
    _ev_rmse = RegressionEvaluator(
        labelCol="moisture_vwc_avg", predictionCol="pred_moisture_vwc_avg", metricName="rmse"
    )
    print(
        f"[ML][TEST]  holdout after {train_end.isoformat()} "
        f"n={test_n} r2={_ev_r2.evaluate(pred_test):.4f} rmse={_ev_rmse.evaluate(pred_test):.4f}"
    )

    vec_all = assembler.transform(scored)
    pred_all = model.transform(vec_all).withColumn(
        "moisture_residual", F.col("moisture_vwc_avg") - F.col("pred_moisture_vwc_avg")
    )
    drop_ml_cols = [c for c in ("features", "rawPrediction") if c in pred_all.columns]
    if drop_ml_cols:
        pred_all = pred_all.drop(*drop_ml_cols)
    pred_all = (
        pred_all.withColumn("is_train_row", F.when(F.col("as_of_date") <= train_cutoff, F.lit(True)).otherwise(F.lit(False)))
        .withColumn("model_id", F.lit(model_id))
        .withColumn("snapshot_date", F.lit(snapshot_date))
    )
    out_df = pred_all.select(
        "site_id",
        "as_of_date",
        "depth_cm",
        "moisture_vwc_avg",
        "moisture_vwc_30d_avg",
        "reading_count",
        *feature_cols,
        "pred_moisture_vwc_avg",
        "moisture_residual",
        "is_train_row",
        "model_id",
        "snapshot_date",
    )

out_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(out_table)

print(f"[DONE] wrote {out_table} rows={out_df.count()} depth_cm={target_depth} model_id={model_id}")
