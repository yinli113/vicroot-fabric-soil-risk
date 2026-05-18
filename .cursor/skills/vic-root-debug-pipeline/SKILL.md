---
name: vic-root-debug-pipeline
description: Fabric pipeline + notebook failure checklist for VicRoot
---

# Debug pipeline — VicRoot

1. Capture **Run ID**; open activity output / error message verbatim.
2. Check parameter pass-through: notebook `resolve_params()` vs pipeline definitions (typos, missing defaults).
3. **Split soil Bronze:** confirm `notebooks/lib/bronze_soil_ingest.py` is deployed next to `pipeline_params.py` if imports fail (`ModuleNotFoundError: bronze_soil_ingest`).
4. For notebook: stderr first cell import errors (Sedona init, jar attach).
5. For APIs: HTTP status, pagination exhaustion, throttle—log response headers if available.
6. One change at a time; re-run single activity if Fabric supports scoped debug.
