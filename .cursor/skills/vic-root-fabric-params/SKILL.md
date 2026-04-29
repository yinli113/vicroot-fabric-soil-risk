---
name: vic-root-fabric-params
description: Standard Fabric pipeline parameters, variables, and notebook wiring for VicRoot
---

# Fabric parameters — VicRoot

1. Open `docs/architecture.md` at repository root — parameter table.
2. In Fabric pipeline: define matching **pipeline parameters**; defaults for `dev`.
3. Add **variables** for computed paths: `concat` / `formatDateTime` patterns off `snapshot_date`.
4. Notebook activity: pass JSON or individual arguments matching `resolve_params()` keys in `notebooks/lib/pipeline_params.py`.
5. Verify run: check notebook receives `target_crs`, `medallion_root`, `snapshot_date`, API bases.
