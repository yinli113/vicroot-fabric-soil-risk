# VicRoot Insights: Urban Forestry Data Platform
*Professional Portfolio Presentation*

---

## Slide 1: Executive Summary
**Headline:** Empowering Melbourne’s Urban Forest Strategy with Data-Driven Soil Monitoring

* **Project Overview:** Built an AI-assisted urban forestry data platform for Greater Melbourne, integrating ~80k city trees, Agriculture Victoria soil profiles, and BOM weather data.
* **Architecture:** Engineered a scalable Medallion architecture (Bronze ➔ Silver ➔ Gold) on Microsoft Fabric using PySpark and Delta Lake.
* **Impact:** Delivered actionable, site- and depth-aware soil analytics via Power BI DirectLake, enabling analysts to track urban tree health without performance bottlenecks.

---

## Slide 2: The Business Challenge & Solution
**Headline:** Moving Beyond Static Tree Maps

* **The Problem:** Subsurface moisture and thermal behavior vary significantly by site and depth (even under shared weather). Poor buffering or extreme shallow–deep contrast often indicates limited soil volume, drainage issues, or microsite stress.
* **The Solution:** A unified data pipeline that calculates sensor depth facts, daily weather context, and peer-normalised salinity. 
* **The Outcome:** Analysts can now compare sites and soil profiles dynamically, moving from reactive maintenance to proactive, data-informed urban forestry.

---

## Slide 3: Modern Data Architecture & Tech Stack
**Headline:** A Fabric-First, Zero-Copy Architecture

* **Orchestration:** Microsoft Fabric Data Factory pipelines for scheduled API ingestion.
* **Storage:** OneLake + Delta Lake in a Medallion layout for robust data lifecycle management.
* **Processing:** Fabric Notebooks (PySpark) performing complex geospatial transformations (Apache Sedona / Geopandas).
* **Visualization:** Power BI DirectLake, avoiding heavy data imports for large fact tables.
* **Future-Proofing:** Ready for Databricks ML integration via OneLake shortcuts without duplicating the Gold data footprint.

---

## Slide 4: Data Engineering Deep Dive (Medallion Flow)
**Headline:** Building Trust Through Robust Data Pipelines

* **Bronze (Ingestion):** Extracted CoM, soil, and BOM data APIs. Landed raw data with strict idempotency (watermarks/control tables) to prevent duplicate rows across runs.
* **Silver (Geospatial & Cleaning):** Projected geospatial data from WGS84 to local GDA2020 (MGA zone 55) for accurate spatial joins between tree points and soil polygons. Enforced data quality boundaries (e.g., pH/salinity ranges).
* **Gold (Insights & Features):** Designed a high-performance Star Schema. Created multi-dimensional facts (`moisture_depth`, `temperature_depth`, `irrigation_risk`) optimized for site-centric monitoring and machine learning.

---

## Slide 5: Advanced Analytics & Machine Learning
**Headline:** From Descriptive Dashboards to Predictive Baselines

* **Operational Dashboards:** Designed 3-page Power BI reports highlighting shallow extremes, depth-resolved temperature phases, and moisture contrasts along street corridors.
* **ML Baseline (PySpark):** Implemented a baseline Spark MLlib `LinearRegression` model.
    * **Features:** 7-day rolling rain, evapotranspiration, and lag-1 moisture.
    * **Performance:** Achieved high predictive accuracy (R² ≈ 0.99, RMSE ≈ 1.3) to track smooth moisture persistence and flag anomalies via `moisture_residual`.

---

## Slide 6: Engineering Best Practices & CI/CD
**Headline:** Delivering Production-Ready Code

* **Version Control & CI:** Managed codebase via Git/GitHub. Added Azure DevOps YAML CI (`azure-pipelines.yml`) for offline Python compilation checks and JSON template validation; secrets documented via env patterns only — never committed.
* **Parameterization:** Driven entirely by configurable parameters (`pipeline_params.py`), avoiding hard-coded logic and enabling seamless dev-to-prod promotion.
* **Documentation:** Maintained comprehensive data dictionaries, architecture design records, and dashboard interpretation guides for non-technical stakeholders.

---

> **Note to Presenter:** These slides are designed to highlight your skills across **Data Engineering**, **Cloud Architecture (Microsoft Fabric)**, and **Data Analytics**. This combination is highly attractive for roles such as Data Engineer, Analytics Engineer, or BI Developer.
