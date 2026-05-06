# VicRoot Insights — project blueprint (2026)

# TL;DR

Build an **AI-assisted urban forestry data platform** for Greater Melbourne: ingest **~80k City of Melbourne trees**, **Agriculture Victoria soil** profiles, and **BOM** weather, run a **Fabric** medallion pipeline (Bronze → Silver → Gold), expose **Power BI DirectLake**, and leave room for **Databricks** ML on the same Delta/OneLake footprint.

**Current VicRoot sensor analytics focus (2026):** The **City of Melbourne soil-sensor network** is analysed primarily on **moisture and soil temperature** at **site grain**, including **shallow-vs-deep profiles** (buffering, storage, decoupling from regional weather) when monitoring locations are close enough to treat **daily weather as shared**. **Salinity (sensor EC)** remains in the model for **context and secondary screening**—not as the main story—because values are a **sensor EC proxy** (not lab `ECe`), are **moisture-qualified** for fair peer comparison, and the programme intentionally **avoids “tree suitability” claims** from sensors alone. See [docs/architecture.md](docs/architecture.md) *Analytical framing*.

---

## 1. Business goal and context

| | |
|--|--|
| **Vision** | Support Melbourne’s **Urban Forest Strategy** with timely, explainable **soil monitoring signals**—not just a static tree map. |
| **Core problem** | **Subsurface moisture and thermal behaviour** varies by **site and depth** even under similar regional weather; poor buffering or extreme shallow–deep contrast can indicate **limited soil volume, drainage, or microsite stress**. |
| **Solution** | A medallion pipeline that combines **sensor depth facts**, **daily weather context**, and **peer-normalised salinity** (optional) so analysts can compare **sites** and **profiles**—reserving **species-level suitability** for richer evidence later. |

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

- **Site-centric monitoring:** Gold emphasises **moisture and temperature** by **site × date × depth**, plus **site × date** shallow context, **shallow–deep deltas** (`gold_fact_irrigation_risk`), and **weather-linked peer views** for salinity where EC is interpretable.
- **Salinity:** keep **auditable tiers and z-scores** as **secondary** context; do not fold salinity into a single “suitability” score without agronomic calibration.
- **Models:** start with **rule-based** bands and profile metrics for ops dashboards; add **Spark ML** or **Databricks** models only when outcome labels and features are agreed.
- **Operational:** **Data Activator** (Reflex) can alert on **moisture/temperature profile** thresholds or **salinity peer tails**, scoped to explicit definitions.

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
