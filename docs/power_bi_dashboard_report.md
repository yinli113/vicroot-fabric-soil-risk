# VicRoot — Power BI dashboard insights report

# TL;DR

This report summarises **three Power BI report pages** built on VicRoot **Gold** data: **Page 1** (network snapshot: shallow moisture/temperature extremes, depth profiles, low-variance salinity as context), **Page 2** (soil temperature vs weather and **Princess Bridge / Fitzroy Garden / Royal Parade** depth behaviour), **Page 3** (**Bourke Street** moisture: microsite spread and **Bourke South 6** drought-era depth pattern). **Power BI** supports exploration and monitoring well but does not, by itself, quantify **which weather regimes drive** soil response; **the next step is ML** (supervised models and careful evaluation) on the same **OneLake / Gold** footprint, with optional **Databricks**.

---

## 1. Purpose and audience

The dashboards translate the medallion **Gold** star schema into **operational stories**: regional weather as shared context, **site-level** and **depth-level** soil moisture and temperature, and **salinity (sensor EC)** as a **secondary**, low-variance signal. The intended audience is **urban forestry / asset analysts** and **technical stakeholders** who need repeatable visuals tied to `gold_fact_irrigation_risk`, `gold_fact_*_depth_daily`, and `gold_dim_weather_daily`.

---

## 2. Data and interpretation guardrails

| Topic | Note |
|--------|------|
| **Shallow metrics** | Coalesced **10 → 20 → 30 cm** in `gold_fact_irrigation_risk` (`moisture_shallow_vwc_avg`, `temp_shallow_c_avg`, `salinity_shallow_ec_avg`). |
| **Deep profile** | **80 → 70 → 60 → 50 cm** coalesce for moisture/temperature; **deltas** (`moisture_shallow_minus_deep_vwc`, `temp_shallow_minus_deep_c`) describe vertical contrast. |
| **Cross-site vs within-site** | Cross-site comparison is fairest when sites share **similar observation dates** or the same **averaging window**. If coverage differs, **within-site, multi-depth** comparison is the stronger story. |
| **Salinity** | Values are **sensor EC proxy**, not lab **ECe**; they are **moisture-sensitive** and **low-variance** in this network — see **§7**. |

Technical detail remains in [architecture.md](architecture.md) and column definitions in [dic.md](dic.md).

---

## 3. Page 1 — Network snapshot and shallow extremes

![Page 1 — Irrigation risk and shallow soil monitoring](images/page_1.png)

**Role.** **Executive / triage** page: shallow **wettest, driest, warmest, coolest** sites (cards), **30-day** averages (gauges), a **per-site** table (moisture, temperature, salinity), a **map** of site locations (e.g. filtered by `moisture_status`), and **depth** visuals (moisture, temperature, salinity by `depth_cm`, including 30d rolling where used).

**How to read it.**

- **Cards** rank sites by **average shallow moisture or temperature** over the **current slicer period** (verify filters so wettest ≠ driest unintentionally when multiple sites are in scope).
- **Moisture and temperature** carry the main **spatial and temporal signal** for irrigation and heat-risk narratives.
- **Salinity** appears as a **flat, near-zero** profile on many cohorts — consistent with **secondary** use: monitor for drift, not day-to-day prioritisation.

---

## 4. Page 2 — Temperature: weather phase and garden contrasts

![Page 2 — Soil temperature vs weather and depth](images/page_2.png)

**Role.** Show **air temperature** (`gold_dim_weather_daily`: e.g. `temp_avg_c`, `temp_max_c`) alongside **soil temperature** by site and, for selected locations, **by depth** (`gold_fact_temperature_depth_daily`: `temp_c_avg`).

**Observed patterns (analyst narrative).**

1. **Phase with weather** — Shallow soil temperature generally **tracks** air temperature; that shared regional forcing is expected.
2. **Princess Bridge** — **Largest amplitude**: among the **coldest** soil in some cold spells and among the **hottest** in heat. Consistent with a **more exposed, thermally aggressive** microsite (e.g. hardscape / structure / limited canopy buffering).
3. **Fitzroy Garden** — **More buffered**: near peer average when **cold**; when **air is hottest**, shallow soil is **cooler** than many peers. **Depth**: in **heat**, **10 cm** is **warmest**, deeper **cooler** (shading / evaporation). In **cold**, **40 cm then 30 cm** can exceed shallower depths — **thermal inertia** (deeper soil lags surface cooling).
4. **Royal Parade** — **Intermediate** fluctuation, especially noticeable under **high** air temperature.
5. **Hot vs cold** — **Spread across sites increases when weather is hot**; under **cold** conditions, sites often **converge**, dominated by regional cooling.

**Deep-dive sites.** **Princess Bridge** and **FitzroyGarden** illustrate **opposite vertical stories** under heat — useful for teaching **microsite** effects without claiming species suitability from sensors alone.

---

## 5. Page 3 — Moisture: Bourke Street corridor

![Page 3 — Bourke Street moisture by site and depth](images/page_3.png)

**Role.** **High-coverage** cluster along **Bourke Street**: multi-site **shallow** comparison over time and **depth-resolved** moisture (`gold_fact_moisture_depth_daily`: `moisture_vwc_avg`, `moisture_vwc_30d_avg`) for focal sites.

**Observed patterns (analyst narrative).**

1. **Same street, different moisture** — Large spread between nearby sites implies **microsite** differences (paving, tree pits, drainage, compaction, irrigation, install).
2. **Bourke South 4** — tends **driest** among the Bourke cohort in the plotted period.
3. **Bourke South 6** — historically **higher** moisture (e.g. pre-2023), then a **marked decline ~2024–2025** during **hot, dry** regional conditions, followed by **partial recovery** toward peers — coherent with **atmospheric demand and rainfall**.
4. **Vertical structure at South 6** — **10–30 cm** can sit **wetter** than **40–60 cm**; **70–80 cm** behaviour **changes over time** (e.g. **80 cm** **drier early**, later **closer to mid-profile**; **50–60 cm** **driest** in recent window; **70 cm** relatively **high** across years). Suggests **redistribution** and **depth-specific storage** worth studying with explicit hydrologic and ML features rather than visuals alone.

---

## 6. Salinity: why it is not the primary dashboard story

Empirically on Page 1 (and the wider table):

- **EC values are small** and **similar across sites** relative to the moisture/temperature range.
- **Depth profiles** are **flat** compared to moisture/temperature.

Methodologically:

- Gold stores **sensor EC proxy** (`salinity_ec_*`), **not** saturated-paste **ECe**; agronomic thresholds need calibration to instrument documentation.
- **Dry soil** depresses EC — “low EC” can reflect **moisture state**, not absence of salt risk.
- Project **intent** is **moisture- and temperature-led** monitoring and **depth buffering**; **salinity** remains **auditable** (`salinity_risk_tier`, peer tables with `ec_reading_reliable`) for **exception tracking**, not the main operational headline.

---

## 7. Limits of Power BI for “why weather drives soil”

Power BI excels at **filtering, aggregation, ranking, and comparison** over known grains (site × date × depth). It does **not**, without further modelling:

- Quantify **marginal effect** of **rainfall, ET, temperature range, humidity, wind**, or **lags** (e.g. soil response **days after** a dry spell).
- Separate **correlation from mechanism** (e.g. seasonal confounding: hot and dry together).
- Provide **predictive** distributions (“given a forecast week, moisture at depth *d*”).
- Automate **interaction** discovery (e.g. “high PET × low prior rain” regimes).

Those questions motivate **machine learning** (or **statistical models**) on curated features — not replacement of BI, but **the next layer**.

---

## 8. Recommended next step: ML (on the same Gold footprint)

**Platform.** Keep **Gold on OneLake**; train in **Databricks** (or **Fabric notebooks** for smaller jobs); write predictions or diagnostics to **new Delta tables** (e.g. `gold_site_depth_ml_*` or `feature_*`) per [architecture.md](architecture.md) handoff notes.

**Outcome ideas (examples).**

| Target | Features (examples) | Notes |
|--------|----------------------|--------|
| **Moisture** `moisture_vwc_avg` or Δ vs prior day | Lagged weather (`rainfall_mm`, `evap_mm`, `temp_*`), **seasonality**, **site** encoding, **depth**, **prior moisture** | Time-series or panel regression; respect **train/test by time**. |
| **Temperature** `temp_c_avg` | Air temp, **lagged air**, **soil temp lags**, site, depth | Compare **nonlinear** models if linear residuals show regime change. |
| **Regime labels** | Cluster weather bins → predict **moisture quantile** or **stress flags** | Explainable clusters map to operational language. |

**Evaluation.** **Backtest by time** (no random split across dates at same site if leakage is a concern). Report **errors by site and depth** and by **season**. Keep **Power BI** for **monitoring model drift** and **overlaying predictions** on the same pages.

**Optional “deeper” statistics.** **Granger-style** or **distributed-lag** frameworks can complement black-box ML where stakeholders need **interpretable lags** — still data-hungry and not a substitute for domain review.

---

## 9. Figure sources

Screenshots stored in-repo:

- [Page 1](images/page_1.png)
- [Page 2](images/page_2.png)
- [Page 3](images/page_3.png)

---

## 10. Closing

The three pages establish a **coherent monitoring narrative**: **shared weather**, **site contrasts**, and **depth profiles**, with **salinity** in a **supporting** role. **Stopping at Power BI** is reasonable once exploration plateaus; **ML is the natural next step** to ask **conditional** questions — *under which weather sequences do moisture and temperature at each depth deviate?* — while staying on **VicRoot’s** **parameterised, lakehouse-first** architecture.
