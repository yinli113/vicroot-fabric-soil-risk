# VicRoot Insights — project blueprint (2026)

# TL;DR

Build an **AI-assisted urban forestry data platform** for Greater Melbourne: ingest **~80k City of Melbourne trees**, **Agriculture Victoria soil** profiles, and **BOM** weather, run a **Fabric** medallion pipeline (Bronze → Silver → Gold), expose **Power BI DirectLake**, and leave room for **Databricks** ML on the same Delta/OneLake footprint.

---

## 1. Business goal and context

| | |
|--|--|
| **Vision** | Support Melbourne’s **Urban Forest Strategy** with timely, explainable risk signals—not just a static tree map. |
| **Core problem** | Elevated **tree mortality** driven by **soil stress** (pH, salinity, moisture imbalance). |
| **Solution** | An end-to-end pipeline that scores **which trees are at risk** using **geospatial soil** context and **weather-derived moisture** stress, not species alone. |

---

## 2. Data strategy and sources

### Tree inventory — City of Melbourne Open Data

- **Use:** Canonical urban tree population (~80k) with geometry and attributes.
- **Key fields:** Location (lat/long), **species**, dimensions, **ULE** (useful life expectancy), **health** status.

### Soil — Agriculture Victoria Soils API

- **Use:** Topsoil / subsoil properties tied to spatial units (e.g. polygons or sample frameworks).
- **Key fields:** **pH**, **salinity** proxies, geometry for **spatial join** to trees.

### Weather — BOM Data.vic (and related)

- **Use:** **Rainfall** and **evapotranspiration (ET)** to derive **moisture deficit** and stress windows.

---

## 3. Modern data stack (Fabric-first)

| Layer | Choice |
|--------|--------|
| **Orchestration** | **Microsoft Fabric** Data Factory **pipelines** (scheduled API ingestion). |
| **Storage** | **OneLake** + **Delta** in a **medallion** layout: Bronze → Silver → Gold. |
| **Processing** | **Fabric notebooks** (PySpark). |
| **Geospatial** | **Apache Sedona** (preferred at scale) or **geopandas** for small proofs. |
| **Quality** | **Great Expectations** (or similar) in notebooks; thresholds driven by **config/metadata**. |
| **Governance** | **Fabric Git integration** (e.g. Azure DevOps). |
| **Visualization** | **Power BI DirectLake** on Delta (avoid heavy Import for large facts). |

**Databricks (later):** Training and batch prediction can run on **Databricks** reading the same **Gold/feature Delta** via **OneLake shortcuts**—no second physical copy of curated data.

---

## 4. Implementation phases

### Phase 1 — Bronze (ingestion)

- Fabric pipelines pull **CoM**, **soil**, and **BOM** sources.
- Land **raw JSON/CSV** under the lakehouse **Files** (or raw Delta), partitioned by **`snapshot_date`** / run id.
- **Idempotency:** watermarks / control tables so **reruns do not duplicate** tree or fact rows.

### Phase 2 — Silver (geospatial engineering)

- Treat CoM coords as **EPSG:4326** (WGS84) at ingest; project to **GDA2020 / EPSG:7855** for **local metric** joins (**MGA zone 55** context).
- **Spatial join:** tree **points** ∩ soil **polygons** (or nearest-with-guardrails if required).
- **Validation:** e.g. pH in valid range, salinity bounds, geometry not null—**Pydantic** and/or **Great Expectations**.

### Phase 3 — Gold (insights and ML-ready features)

- **Resilience / risk:** combine **species tolerance** signals with **local soil** and **moisture deficit**.
- **Models:** start with **rule-based** tiers for demos; add **Spark ML** or **Databricks** models for **health-decline** probability as the stack matures.
- **Operational:** **Data Activator** (Reflex) for alerts when **significant** trees enter **high-risk** bands.

---

## 5. Narrative hooks (Melbourne-specific)

- **DirectLake:** “Dashboards track **same-day soil and risk** without a heavyweight Import refresh story.”
- **Zero-copy:** “**Delta on OneLake** lets **Databricks** teams consume **Gold** via **shortcuts**, not a forked warehouse.”
- **Local CRS literacy:** “We don’t only plot WGS84—we **project to GDA2020 / MGA** for defensible distance and overlay work.”

---

## 6. Repo pointers

- **Technical parameters and schemas:** [docs/architecture.md](docs/architecture.md)
- **Session / phase notes:** [memory-bank/activeContext.md](memory-bank/activeContext.md)
- **Local secrets template:** [.env.example](.env.example) (also available as `.env_example` in the repo) — copy to `.env` and fill in values; never commit `.env`.
