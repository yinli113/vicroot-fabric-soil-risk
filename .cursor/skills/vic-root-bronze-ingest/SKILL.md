---
name: vic-root-bronze-ingest
description: Bronze API ingestion, landing layout, watermark updates for VicRoot
---

# Bronze ingestion — VicRoot

1. **Landing paths:** `{medallion_root}/bronze/{dataset}/year=YYYY/...` (see [docs/architecture.md](../../../docs/architecture.md)). Keep inputs explicit (`soil_csv_input_dir`, `soil_zip_input_path`, etc.) during migration.

2. **Incremental vs full loads:** Some APIs support cursors; **split soil Bronze** (`01_bronze_ingest_soil.py` + `notebooks/lib/bronze_soil_ingest.py`) currently does **full-year API pulls** and replaces `records_api.jsonl` per year when that branch runs — it does **not** read `ingestion_watermarks` back as a resume cursor yet.

3. **Watermarks:**
   - **Soil (`update_watermarks` in `bronze_soil_ingest.py`):** after a successful zip / CSV year / API year segment, **append** Delta rows to `metadata.ingestion_watermarks` (`dataset_name`, `last_success_run_id`, `last_success_utc`, coarse `high_watermark`, `snapshot_date`). This is **audit / lineage**, not CDC high-watermark driving the next fetch.
   - **Ideal pattern elsewhere:** upsert watermark per dataset when you need true incremental semantics; align with pipeline `pipeline_run_id` (`@pipeline().RunId` in Fabric).

4. **`pipeline_run_id`:** Fabric pipelines pass it from parameters; standalone notebook runs may leave it blank — soil Bronze fills with `notebook-{snapshot_date}-{utcstamp}` via `effective_pipeline_run_id`.

5. **Raw writes:** Prefer idempotent merges on reruns where duplicates matter; soil file/API landing uses explicit paths + summaries under `year=.../metadata/ingest_summary.json`.

6. **Log** record counts in notebook output; use Fabric notifications on pipeline failure as needed.
