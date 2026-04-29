---
name: vic-root-silver-spatial-join
description: Silver CRS transform, spatial join, GE from config for VicRoot
---

# Silver spatial join — VicRoot

1. Read Bronze tables/Files; project tree coords to `target_crs` (7855).
2. Load soil polygons; ensure consistent CRS before join.
3. Spatial join: point-in-polygon; capture `soil_ph`, salinity fields, join quality flags.
4. Load Great Expectations (or validation) rules from `config/ge_validation_rules.json` or metadata Delta.
5. Write `silver_trees_soil` (name per architecture); quarantine failed expectations to `silver_quarantine` or equivalent.
