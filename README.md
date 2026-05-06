# VicRoot Fabric Soil Risk

VicRoot is a Microsoft Fabric medallion pipeline for Melbourne **urban soil sensor** monitoring, transforming soil/site/weather data from Bronze to Silver to Gold feature tables for **site- and depth-aware** analytics.

**Analytical focus:** Compare **sites** on **moisture and soil temperature** behaviour—including **shallow vs deep** (buffering, storage, decoupling) under **shared regional weather** when sensor locations are close. **Salinity (sensor EC)** is **secondary**: still materialised for context and peer screening, but not the primary narrative—see [docs/architecture.md](docs/architecture.md) *Analytical framing*.

## Map Preview

![VicRoot site risk map](docs/images/site-risk-map.png)

## Gold layer (star schema)

- **Dimensions:** `gold_dim_site`, `gold_dim_depth` (10–80 cm), `gold_dim_weather_daily`
- **Facts:** `gold_fact_salinity_depth_daily`, `gold_fact_moisture_depth_daily`, `gold_fact_temperature_depth_daily` (site × date × depth; day + 30d only), `gold_fact_irrigation_risk` (site × date; shallow + **deep profile** columns, moisture/temperature context, salinity tiers as secondary), `gold_fact_soil_depth_peer_daily` / `gold_site_depth_peer_summary` (weather bins + moisture-qualified **salinity** peer z-scores)
- **GeoJSON map export:** `notebooks/04_export_site_risk_geojson.py` (joins irrigation fact to `gold_dim_site` for lat/lon)

## Repository scope

- Fabric-ready notebooks for Bronze/Silver/Gold
- Parameter-driven runtime via `notebooks/lib/pipeline_params.py`
- Gold outputs for **moisture/temperature-led** monitoring, salinity context, and analyst-facing map visualization

## Documentation

- [Architecture & Fabric parameters](docs/architecture.md)
- [Gold data dictionary (units & grains)](docs/dic.md)
- [Power BI dashboard report — Pages 1–3, salinity rationale, ML next steps](docs/power_bi_dashboard_report.md)
