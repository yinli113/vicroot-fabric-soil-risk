-- Fabric / Spark SQL — adapt database/schema names to your lakehouse

CREATE TABLE IF NOT EXISTS metadata.ingestion_watermarks (
  dataset_name STRING,
  last_success_run_id STRING,
  last_success_utc TIMESTAMP,
  high_watermark STRING,
  snapshot_date STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS metadata.datasets (
  dataset_name STRING,
  api_base STRING,
  enabled BOOLEAN,
  priority INT
) USING DELTA;
