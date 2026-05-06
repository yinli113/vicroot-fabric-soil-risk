# VicRoot Gold Data Dictionary

# TL;DR

This dictionary explains the current Gold-layer tables, column meanings, grains, and units. Gold uses a star schema for Power BI. **Primary monitoring story:** **site × date × depth** **moisture and soil temperature**, plus **site × date** **shallow–deep profile** columns in `gold_fact_irrigation_risk`. **Salinity** (`salinity_ec_*`, peer z-scores) is **secondary**—sensor EC proxy, moisture-qualified for peers; see [docs/architecture.md](architecture.md) *Analytical framing*. **Peer tables** remain salinity-focused (`salinity_ec_peer_z`).

## Table Grains

| Table | Grain | Primary use |
|-------|-------|-------------|
| `gold_dim_site` | One row per `site_id` | Site lookup and map coordinates. |
| `gold_dim_depth` | One row per `depth_cm` | Standard depth lookup: 10, 20, 30, 40, 50, 60, 70, 80 cm. |
| `gold_dim_weather_daily` | One row per `as_of_date` | Daily weather context. |
| `gold_fact_salinity_depth_daily` | `site_id` + `as_of_date` + `depth_cm` | Salinity by site/day/depth. |
| `gold_fact_moisture_depth_daily` | `site_id` + `as_of_date` + `depth_cm` | Moisture by site/day/depth. |
| `gold_fact_temperature_depth_daily` | `site_id` + `as_of_date` + `depth_cm` | Soil temperature by site/day/depth. |
| `gold_fact_irrigation_risk` | `site_id` + `as_of_date` | **Moisture/temperature-led** site-day profile (shallow + deep + deltas) and weather pressure; **salinity tiers** as secondary context. |
| `gold_fact_soil_depth_peer_daily` | `site_id` + `as_of_date` + `depth_cm` | Depth facts joined to daily weather bins; moisture-qualified EC; peer z-score vs other sites same day/bin/depth. |
| `gold_site_depth_peer_summary` | `site_id` + `depth_cm` | Site–depth rollups: reliability rate, mean / mean-abs peer z, tail-day counts. |

## Shared Columns

| Column | Meaning | Unit / values |
|--------|---------|---------------|
| `site_id` | Stable sensor site identifier. | Text |
| `as_of_date` | Calendar date for the daily aggregate. | Date |
| `depth_cm` | Standardized sensor depth bucket. Source depths are snapped to nearest 10 cm and clamped to 10-80 cm. | cm |
| `reading_count` | Number of readings used for that row's measure and grain. | Count |
| `snapshot_date` | Pipeline/run snapshot label used for traceability. | Text/date-like string |

## `gold_dim_site`

| Column | Meaning | Unit / values |
|--------|---------|---------------|
| `site_id` | Sensor site identifier. | Text |
| `site_name` | Human-readable site name. | Text |
| `latitude` | Site latitude for map visuals. | Decimal degrees, EPSG:4326 |
| `longitude` | Site longitude for map visuals. | Decimal degrees, EPSG:4326 |
| `snapshot_date` | Site reference snapshot used by Gold. | Text/date-like string |

## `gold_dim_depth`

| Column | Meaning | Unit / values |
|--------|---------|---------------|
| `depth_cm` | Standard depth bucket. | cm |
| `depth_label` | Display label for Power BI. | Text, e.g. `10 cm` |
| `snapshot_date` | Build snapshot label. | Text/date-like string |

## `gold_dim_weather_daily`

| Column | Meaning | Unit / values |
|--------|---------|---------------|
| `as_of_date` | Weather date. | Date |
| `rainfall_mm` | Daily precipitation/rainfall. | mm/day |
| `evap_mm` | Daily evapotranspiration/evaporation proxy from weather source. | mm/day |
| `humidity_avg_pct` | Daily average relative humidity. | Percent |
| `temp_avg_c` | Daily average air temperature. | deg C |
| `temp_min_c` | Daily minimum air temperature. | deg C |
| `temp_max_c` | Daily maximum air temperature. | deg C |
| `temp_range_c` | Daily temperature range: `temp_max_c - temp_min_c`. | deg C |
| `wind_speed_avg_ms` | Daily average wind speed. | m/s |
| `model_snapshot_date` | Gold build snapshot label. | Text/date-like string |

## `gold_fact_salinity_depth_daily`

| Column | Meaning | Unit / values |
|--------|---------|---------------|
| `site_id` | Sensor site identifier. | Text |
| `as_of_date` | Date of readings. | Date |
| `depth_cm` | Standardized depth bucket. | cm |
| `reading_count` | Number of salinity readings for site/date/depth. | Count |
| `salinity_ec_avg` | Daily average salinity sensor EC proxy. Preserves source numeric scale. | Sensor EC proxy |
| `salinity_ec_30d_avg` | 30-day rolling average of `salinity_ec_avg` by site/depth. | Sensor EC proxy |
| `snapshot_date` | Gold build snapshot label. | Text/date-like string |

### Salinity Sensor Scale Rule

Gold preserves the salinity sensor numeric value instead of converting it.

The source unit label is `uS/cm` / `µS/cm`, but the observed distribution is approximately `0` to `2.16`, which behaves more like a **mS/cm or dS/m-scale sensor EC proxy** than true microSiemens. Dividing by 1000 made all sites effectively zero and removed practical map signal.

Therefore Gold uses `salinity_ec_*` fields as a **sensor EC proxy**, not a lab-measured saturated paste `ECe`. Risk tiers are monitoring bands that should be calibrated with source documentation and local agronomy evidence.

## `gold_fact_moisture_depth_daily`

| Column | Meaning | Unit / values |
|--------|---------|---------------|
| `site_id` | Sensor site identifier. | Text |
| `as_of_date` | Date of readings. | Date |
| `depth_cm` | Standardized depth bucket. | cm |
| `reading_count` | Number of moisture readings for site/date/depth. | Count |
| `moisture_vwc_avg` | Daily average volumetric water content. | % VWC |
| `moisture_vwc_30d_avg` | 30-day rolling average of `moisture_vwc_avg` by site/depth. | % VWC |
| `snapshot_date` | Gold build snapshot label. | Text/date-like string |

## `gold_fact_temperature_depth_daily`

| Column | Meaning | Unit / values |
|--------|---------|---------------|
| `site_id` | Sensor site identifier. | Text |
| `as_of_date` | Date of readings. | Date |
| `depth_cm` | Standardized depth bucket. | cm |
| `reading_count` | Number of soil temperature readings for site/date/depth. | Count |
| `temp_c_avg` | Daily average soil temperature. | deg C |
| `temp_c_30d_avg` | 30-day rolling average of `temp_c_avg` by site/depth. | deg C |
| `snapshot_date` | Gold build snapshot label. | Text/date-like string |

## `gold_fact_irrigation_risk`

Site × date **profile** table: **moisture and soil temperature** are the **primary** fields for comparing sites under shared regional weather; **salinity** columns are **secondary** (auditable bands, not a combined “suitability” score).

| Column | Meaning | Unit / values |
|--------|---------|---------------|
| `site_id` | Sensor site identifier. | Text |
| `as_of_date` | Date of risk/context row. | Date |
| `reading_count` | Number of valid soil readings for that site/day across measures/depths. | Count |
| `moisture_shallow_vwc_avg` | Shallow moisture from **10 → 20 → 30 cm** coalesce (first non-null). | % VWC |
| `moisture_deep_vwc_avg` | Deep moisture from **80 → 70 → 60 → 50 cm** coalesce—contrasts shallow profile. | % VWC |
| `moisture_shallow_minus_deep_vwc` | `moisture_shallow_vwc_avg − moisture_deep_vwc_avg`; null if either side null. **Positive** → shallow **drier** than deep; **negative** → shallow **wetter**. | % VWC |
| `salinity_shallow_ec_avg` | Shallow salinity sensor EC proxy from **10 → 20 → 30 cm** coalesce. **Secondary** signal. | Sensor EC proxy |
| `temp_shallow_c_avg` | Shallow soil temperature from **10 → 20 → 30 cm** coalesce. | deg C |
| `temp_deep_c_avg` | Deep soil temperature from **80 → 70 → 60 → 50 cm** coalesce. | deg C |
| `temp_shallow_minus_deep_c` | `temp_shallow_c_avg − temp_deep_c_avg`; null if either side null. **Positive** → shallow **warmer** than deep (typical under surface heating). | deg C |
| `evap_moisture_pressure` | Weather pressure context: `evap_mm - rainfall_mm`. Positive values suggest atmospheric water demand exceeded rainfall that day. | mm/day |
| `moisture_deficit_index` | Simple moisture context: `100 - moisture_shallow_vwc_avg`. Higher value means lower shallow moisture. | Index points |
| `salinity_shallow_30d_avg` | 30-day rolling average of `salinity_shallow_ec_avg` by site. | Sensor EC proxy |
| `salinity_trend_vs_30d` | Difference between current shallow salinity and 30-day average: `salinity_shallow_ec_avg - salinity_shallow_30d_avg`. | Sensor EC proxy |
| `salinity_risk_tier` | **Secondary** salinity-only band. | `unknown`, `non_saline`, `slightly_saline`, `moderately_saline`, `highly_saline` |
| `salinity_risk_reason` | Human-readable reason for the salinity tier. | Text |
| `salinity_observed_band` | Relative salinity band for dashboard/map contrast within this dataset. | `unknown`, `low_observed`, `medium_observed`, `high_observed` |
| `moisture_status` | Shallow moisture band; not part of salinity tier. | `unknown`, `low_moisture`, `target_zone`, `high_moisture` |
| `snapshot_date` | Gold build snapshot label. | Text/date-like string |

## Peer analysis parameters (Gold notebook)

| Parameter | Default | Meaning |
|----------|---------|--------|
| `gold_ec_moisture_min_shallow_vwc` | `25.0` | Minimum `%VWC` to treat shallow sensor EC as interpretable for depths ≤ `gold_ec_moisture_shallow_depth_max_cm`. |
| `gold_ec_moisture_min_deep_vwc` | `18.0` | Minimum `%VWC` for deeper buckets (>&nbsp;30&nbsp;cm default). |
| `gold_ec_moisture_shallow_depth_max_cm` | `30` | Depth cutoff (cm) between shallow vs deep moisture floors. |
| `gold_peer_min_sites` | `5` | Minimum number of moisture-qualified sites in a peer group `(as_of_date, depth_cm, weather_bin_key)` before `salinity_ec_peer_z` is computed. |

## `gold_fact_soil_depth_peer_daily`

**Grain:** `site_id`, `as_of_date`, `depth_cm` — one row per site/day/depth where at least one of salinity/moisture/temperature facts exists (outer-joined measures).

**Purpose:** Compare each site’s sensor EC to **peers** under **similar regional weather** (coarse bins from `gold_dim_weather_daily`), while flagging when EC is **not moisture-qualified** for interpretation.

| Column | Meaning | Unit / values |
|--------|---------|---------------|
| `site_id` | Sensor site identifier. | Text |
| `as_of_date` | Calendar date. | Date |
| `depth_cm` | Standard depth bucket (10–80&nbsp;cm). | cm |
| `salinity_reading_count` | Salinity reading count from fact (nullable if no salinity row). | Count |
| `moisture_reading_count` | Moisture reading count. | Count |
| `temperature_reading_count` | Temperature reading count. | Count |
| `salinity_ec_avg` | Daily salinity EC proxy. | Sensor EC proxy |
| `salinity_ec_30d_avg` | 30-day rolling salinity from fact. | Sensor EC proxy |
| `moisture_vwc_avg` | Daily moisture. | % VWC |
| `moisture_vwc_30d_avg` | 30-day rolling moisture. | % VWC |
| `temp_c_avg` | Daily soil temperature. | °C |
| `temp_c_30d_avg` | 30-day rolling soil temperature. | °C |
| `stress_mm` | `evap_mm − rainfall_mm` from weather dim (zeros if nulls coalesced). | mm/day |
| `rain_band` | `rain_low` if rainfall \<&nbsp;1&nbsp;mm else `rain_high`; `rain_unknown` if rainfall null. | Text |
| `temp_band` | `temp_cool` \&lt;16&nbsp;°C, `temp_warm` \&gt;24&nbsp;°C, else `temp_mild`; `temp_unknown` if null. | Text |
| `weather_bin_key` | `rain_band` + `|` + `temp_band`; `missing_weather` if no weather row for date. | Text |
| `rainfall_mm`, `evap_mm`, `temp_avg_c` | Copy of weather context for convenience. | mm, mm, °C |
| `ec_reading_reliable` | True if salinity and moisture present and moisture ≥ shallow/deep floor by depth. | Boolean |
| `peer_mean_salinity_qualified` | Mean `salinity_ec_avg` over sites with `ec_reading_reliable` same day/depth/bin. | Sensor EC proxy |
| `peer_stddev_salinity_qualified` | Population stddev of same peer set. | Sensor EC proxy |
| `peer_n_qualified_sites` | Count of sites in peer set. | Count |
| `salinity_ec_peer_z` | \((\text{salinity\_ec\_avg} - \text{peer\_mean}) / \text{peer\_stddev}\); null if not reliable, too few peers, or zero variance. | Standard deviations |
| `snapshot_date` | Gold build snapshot label. | Text |

**Interpretation:** Positive `salinity_ec_peer_z` means **higher EC than typical peers** that day, depth, and coarse weather bin—useful for spotting sites that diverge under similar climate. Do **not** treat low EC as “safe” when `ec_reading_reliable` is false.

## `gold_site_depth_peer_summary`

**Grain:** `site_id`, `depth_cm`.

| Column | Meaning | Unit / values |
|--------|---------|---------------|
| `site_id` | Site identifier. | Text |
| `depth_cm` | Depth bucket. | cm |
| `n_days_any_measure` | Rows in peer fact for this site×depth. | Count |
| `n_days_ec_reliable` | Days flagged moisture-qualified for EC. | Count |
| `pct_days_ec_reliable` | `n_days_ec_reliable / n_days_any_measure`. | 0–1 |
| `mean_salinity_peer_z` | Average peer z (null-weighted if all null). | z |
| `mean_abs_salinity_peer_z` | Average \|z\| — chronic deviation magnitude vs peers. | z |
| `n_days_peer_z_gt_2` | Days with z \&gt;&nbsp;2. | Count |
| `n_days_peer_z_lt_neg2` | Days with z \&lt;&nbsp;−2. | Count |
| `snapshot_date` | Gold build snapshot label. | Text |

## Risk Settings

### Salinity Risk

| Tier | Rule on `salinity_shallow_ec_avg` | Reason text | Interpretation |
|------|------------------------------------|-------------|----------------|
| `unknown` | Null salinity | `no shallow salinity reading` | No shallow salinity reading was available. |
| `non_saline` | `< 2` | `0-2 sensor EC proxy` | Low salinity concern on current sensor scale. |
| `slightly_saline` | `>= 2 and < 4` | `2-4 sensor EC proxy` | Watch zone; salt-sensitive plants may be affected. |
| `moderately_saline` | `>= 4 and < 8` | `4-8 sensor EC proxy` | Elevated salinity concern. |
| `highly_saline` | `>= 8` | `>8 sensor EC proxy` | High salinity concern. |

### Moisture Status

| Status | Rule on `moisture_shallow_vwc_avg` | Meaning |
|--------|------------------------------------|---------|
| `unknown` | Null moisture | No shallow moisture reading was available. |
| `low_moisture` | `< 40% VWC` | Below the broad target zone. |
| `target_zone` | `>= 40 and <= 80% VWC` | Broad plant-safe moisture range used for early monitoring. |
| `high_moisture` | `> 80% VWC` | Above target zone; may indicate saturated/wet conditions depending on site. |

Temperature and evap/rain pressure remain context fields. They are not combined into the salinity tier. **Profile deltas** (`moisture_shallow_minus_deep_vwc`, `temp_shallow_minus_deep_c`) support **within-site** shallow vs deep comparisons; interpret alongside `gold_dim_weather_daily` for weather phase.

### Observed Salinity Band

Use `salinity_observed_band` for map color when external salinity tiers are all `non_saline`. This is a **relative dashboard band**, not an agronomic risk class.

| Band | Rule on `salinity_shallow_ec_avg` | Suggested use |
|------|------------------------------------|---------------|
| `unknown` | Null salinity | Grey map point. |
| `low_observed` | `< 0.3` | Lower relative salinity in this dataset. |
| `medium_observed` | `>= 0.3 and < 0.8` | Mid-range relative salinity. |
| `high_observed` | `>= 0.8` | Higher relative salinity hotspot for visual attention. |
