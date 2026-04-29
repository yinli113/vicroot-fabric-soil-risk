---
name: vic-root-bronze-ingest
description: Bronze API ingestion, landing layout, watermark updates for VicRoot
---

# Bronze ingestion — VicRoot

1. Landing paths: `{medallion_root}/bronze/{dataset}/dt={snapshot_date}/` (adjust to org standard).
2. Before ingest: read `metadata.ingestion_watermarks` for dataset; apply incremental rule if API supports cursors.
3. Write raw JSON/CSV to Files or Bronze Delta as designed; avoid duplicate fact keys on rerun (merge/idempotent write).
4. After success: upsert watermark row with `pipeline_run_id`, `last_success_utc`, `snapshot_date`.
5. Log record counts via `Set variable` + optional notification.
