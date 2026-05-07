# TL;DR

Use **Azure DevOps Pipelines** with [`azure-pipelines.yml`](../azure-pipelines.yml) to run **offline checks** (Python compile, JSON parse) on every PR. **Fabric** still runs medallion notebooks and owns Spark; **this CI does not touch capacity** or deploy to Fabric automatically.

# Azure DevOps + Fabric (VicRoot)

## What this gives you

| Piece | Role |
|--------|------|
| **Git** (GitHub and/or Azure Repos) | Source of truth for notebooks, `pipeline_params.py`, `fabric/`, `docs/` |
| **`azure-pipelines.yml`** | PR validation: `compileall` on `notebooks/lib` and `src`, JSON checks |
| **Fabric workspace** | Runtime: lakehouse, data pipeline, Spark — **unchanged** by this CI |

Full pipeline executes (Bronze → Silver → Gold → ML) remain **in Fabric** after capacity allows; see [fabric/pipeline_activity_checklist.md](../fabric/pipeline_activity_checklist.md).

## One-time setup (Azure DevOps)

1. **Repo:** Push this repository to **Azure Repos** or use **Azure DevOps GitHub connection** with a service connection (your org’s trial rules apply).
2. **Pipeline:** **Pipelines** → **New pipeline** → select the repo → **Existing Azure Pipelines YAML file** → `/azure-pipelines.yml`.
3. **Branch policy (optional):** **Project settings** → **Repositories** → **Policies** → require the validation build on `main` (and `develop` if you use it).

## What it does *not* do

- Does **not** start Livy or consume **Fabric capacity**.
- Does **not** replace **Fabric Deployment Pipelines** for promoting workspace items (reports, semantic models, etc.).
- Does **not** sync notebooks into a lakehouse; keep using **manual upload**, **Fabric Git integration**, or a release process your tenant allows.

## Related reading

- [docs/power_bi_dashboard_report.md](power_bi_dashboard_report.md) §9 — broader CD mental model (GitHub-focused; same ideas for ADO).
- [docs/architecture.md](architecture.md) — parameters and medallion naming (source of truth for runtime).
