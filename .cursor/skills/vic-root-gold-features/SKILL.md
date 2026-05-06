---
name: vic-root-gold-features
description: Gold star schema grains, PKs, and Databricks-facing contract for VicRoot
---

# Gold features — VicRoot

1. **Star schema (current):** See `docs/architecture.md` — `gold_dim_site`, `gold_dim_depth` (10–80 cm reference; readings snap to nearest 10 cm bucket), `gold_dim_weather_daily`, **`gold_fact_salinity_depth_daily`**, **`gold_fact_moisture_depth_daily`**, **`gold_fact_temperature_depth_daily`** (grain `site_id` + `as_of_date` + `depth_cm`; **daily avg + 30d rolling only**; **per-measure `reading_count`**). Salinity facts preserve source numeric scale as a **sensor EC proxy** (`salinity_ec_*`); do not divide by 1000 unless source documentation proves values are true `µS/cm`.
2. **Irrigation fact (`gold_fact_irrigation_risk`):** grain `site_id` + `as_of_date`. **Primary:** shallow + deep **moisture/temperature** (including **`moisture_shallow_minus_deep_vwc`**, **`temp_shallow_minus_deep_c`**) and `moisture_status`; keep **salinity** (`salinity_risk_tier`, …) **separate** as **secondary** screening. Do not reintroduce a mixed generic `risk_tier` or “tree suitability” scoring from sensors alone.
3. **Power BI:** Relate measure facts and irrigation fact to `dim_site` and `dim_weather_daily` (and optional `dim_depth` on `depth_cm`).
4. **Future tree grain:** Optional one row per `tree_id` + `as_of_date` when tree inventory is joined; keep Gold as **features** plus optional rule-based columns until Databricks owns ML scoring.
5. Document new columns in `docs/architecture.md` before external consumers rely on them.
