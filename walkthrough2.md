# Compound Sustainability Stress Index (SSI) — Complete Project Reference

---

## What This Project Is

A **city-scale environmental stress modeling pipeline** for 10 Indian cities.

It ingests climate data, satellite imagery, air quality readings, urban form data, and socio-economic vulnerability statistics — harmonises everything onto a uniform H3 hexagonal grid — then computes a single **Compound Sustainability Stress Index (SSI)** score per hex per month.

**Final output:** An interactive HTML dashboard + a Parquet dataset with 330,000+ rows showing every hex-cell's monthly stress profile over 10 years (2015–2025).

---

## The Big Picture — How It Works

```
Multi-source raw data
        │
        ▼
  [Phase 1] INGEST         ← Download from APIs (ERA5, Satellite, PM2.5, OSM)
        │
        ▼
  [Phase 2] PROCESS        ← Convert everything to H3 hex grids + monthly panels
        │
        ▼
  [Phase 3] FEATURES       ← Compute 5 stress indicators (heat, water, pollution, vegetation, vulnerability)
        │
        ▼
  [Phase 4] SSI            ← Combine indicators into one composite 0–1 score with PCA weights
        │
        ▼
  [Phase 5] REPORT         ← Generate interactive HTML dashboard with 9 charts + H3 map
```

**H3 at resolution 8** = ~0.74 km² per hexagon. Mumbai has 2,501 hexes × 132 months = 330,132 rows.

---

## Folder Structure

```
Compound-Sustainability-Stress-Modeling-anuj/
│
├── config.yml                  ← Master config (cities, date range, SSI weights)
├── cities_bbox.json            ← Bounding boxes for all 10 cities
├── run_pipeline.py             ← MAIN CLI — runs data ingestion → SSI in one command
├── generate_report.py          ← Report CLI — generates HTML dashboard from output data
├── requirements.txt            ← All Python dependencies
├── .env.example                ← Template for API credentials (copy to .env)
│
├── src/
│   ├── ingest/                 ← Data downloaders
│   │   ├── era5_ingest.py      ← ERA5 climate (CDS API)
│   │   ├── satellite_ingest.py ← NDVI, LST, built-up (Planetary Computer)
│   │   ├── pm25_ingest.py      ← PM2.5 air quality (NASA SEDAC)
│   │   ├── osm_ingest.py       ← Roads, buildings, green space (OpenStreetMap)
│   │   └── vulnerability_ingest.py ← BPL%, slum%, elderly%, literacy%
│   │
│   ├── process/                ← Raw → H3 hex panel
│   │   ├── era5_process.py     ← NetCDF climate → monthly hex values
│   │   ├── raster_to_h3.py     ← GeoTIFFs → H3 zonal statistics
│   │   ├── osm_to_h3.py        ← Vector urban features → H3 aggregation
│   │   └── harmonize.py        ← MASTER MERGE — joins all sources into one panel
│   │
│   ├── features/               ← Stress indicator computation
│   │   ├── heat_stress.py      ← Rothfusz Heat Index + LST anomaly → 0–1 score
│   │   ├── water_stress.py     ← Precip deficit + soil moisture → 0–1 score
│   │   ├── pollution_exposure.py ← PM2.5 vs WHO guideline → 0–1 score
│   │   ├── vegetation_degradation.py ← NDVI anomaly → 0–1 score
│   │   ├── urban_vulnerability.py ← BPL/slum/elderly/literacy → 0–1 score
│   │   ├── normalize.py        ← Z-score, min-max, baseline-aware normalization
│   │   └── ssi.py              ← PCA weights + city adjustments → final SSI
│   │
│   ├── viz/                    ← Visualization module
│   │   ├── maps.py             ← H3 choropleth map (Plotly Mapbox)
│   │   ├── plots.py            ← 9 chart builders (time-series, radar, heatmap, etc.)
│   │   └── report.py           ← HTML dashboard assembler
│   │
│   ├── utils/                  ← Shared utilities
│   │   ├── h3_utils.py         ← H3 grid generation, temporal skeleton
│   │   ├── geo_utils.py        ← Raster zonal stats, vector→H3 aggregation
│   │   ├── config_loader.py    ← Loads config.yml
│   │   └── logger.py           ← UTF-8 safe, Windows-compatible logging
│   │
│   └── quality/                ← Data quality checks (stub — not built yet)
│
├── tests/
│   └── test_pipeline.py        ← 19 integration tests (all passing)
│
├── data/
│   ├── raw/                    ← Downloaded source files (ERA5 .nc, GeoTIFFs, etc.)
│   ├── processed/              ← Intermediate H3 panels (21 cols, no SSI yet)
│   ├── h3_panel/final/         ← FINAL OUTPUT .parquet files (30 cols, with SSI)
│   └── metadata/               ← Checksums, run logs
│
└── reports/
    └── mumbai_ssi_report.html  ← Generated dashboard (1.6 MB, open in any browser)
```

---

## What's Fully Working ✅

### The Pipeline (100% coded, tested with synthetic data)

| Module | What It Does | Status |
|---|---|---|
| H3 grid engine | Generates hexagonal spatial skeletons for any city | ✅ |
| ERA5 ingest | Downloads 8 climate variables from Copernicus CDS API | ✅ |
| Satellite ingest | Downloads NDVI from Sentinel-2, LST from MODIS, GHSL built-up via Planetary Computer | ✅ |
| PM2.5 ingest | Downloads annual NASA SEDAC data + seasonal scaling; synthetic fallback | ✅ |
| OSM ingest | Pulls road density, building density, green space from OpenStreetMap | ✅ |
| Vulnerability ingest | City-level BPL%, slum%, elderly%, literacy% prior data | ✅ |
| Harmonizer | Merges all data sources into one unified H3 × monthly panel | ✅ |
| Heat Stress index | Rothfusz heat index + LST anomaly → normalized 0–1 | ✅ |
| Water Stress index | Precip deficit + soil moisture depletion → normalized 0–1 | ✅ |
| Pollution index | PM2.5 vs WHO 5 µg/m³ guideline + temporal anomaly → normalized 0–1 | ✅ |
| Vegetation index | NDVI anomaly below seasonal baseline → normalized 0–1 | ✅ |
| Urban Vulnerability index | BPL + slum + elderly + (inverse) literacy → normalized 0–1 | ✅ |
| Normalization | Baseline-aware z-score with 3-year rolling window | ✅ |
| SSI composite | PCA-derived weights + city multipliers → final 0–1 score, bands, archetypes, anomaly flags | ✅ |
| CLI runner | `run_pipeline.py` orchestrates all 4 phases | ✅ |
| Test suite | **19/19 tests pass** with synthetic data | ✅ |

### Visualization (100% built)

| Chart | What It Shows |
|---|---|
| H3 SSI Map | Time-averaged SSI per hex on an interactive dark map |
| SSI Time Series | Monthly city-level SSI with ±1 std band |
| Anomaly Timeline | % hexes above the 90th-percentile SSI threshold each month |
| 5-Indicator Trends | All 5 stress components over time (multi-line) |
| SSI Band Donut | Proportion of Low / Moderate / High / Extreme observations |
| Seasonal Heatmap | Year × Month SSI grid — reveals climatological patterns |
| Correlation Matrix | Pearson correlations between all 5 indicators |
| Archetype Radar | K-means stress profiles — spider chart per cluster |
| Vulnerability Scatter | BPL% vs SSI per hex — identifies compounding deprivation |
| Top 20 Hexes | Ranked bar chart of most persistently-stressed locations |

The report is a **single self-contained HTML file** — no internet required to view it after generation.

### Current Output

`data/h3_panel/final/mumbai.parquet` — 59 MB

```
330,132 rows × 30 columns

identity ──────── h3_index, date, city_id
climate ───────── temp_mean_c, dewpoint_mean_c, precip_sum_mm,
                  wind_speed, radiation, soil_moisture
satellite ──────── ndvi, lst_c
air quality ────── pm25
land cover ────── built_up_fraction
urban form ────── road_density_km_km2, building_density,
                  building_fp_fraction, green_space_fraction
vulnerability ─── bpl_pct, slum_pct, elderly_pct, literacy_pct
indicators ────── heat_stress_idx, water_stress_idx, pollution_idx,
                  vegetation_idx, urban_vulnerability_idx
SSI ────────────── ssi_value [0–1], ssi_band [Low/Moderate/High/Extreme],
                  archetype_id [1–6], anomaly_flag [True/False]
```

---

## What's Not Done Yet ❌

| Item | Why It Matters |
|---|---|
| **Real data ingestion not tested** | The entire pipeline has only been validated with synthetic (randomly generated) data. API calls to ERA5, Planetary Computer, and NASA SEDAC have never actually been run |
| **9 remaining cities** | Only Mumbai has been processed. Delhi, Bengaluru, Chennai, Hyderabad, Pune, Ahmedabad, Kolkata, Surat, Indore still need to be run |
| **Quality module** | `src/quality/` is an empty folder — fill-rate reports, outlier detection, and per-city QA checks are not yet built |
| **README** | The repository README is 1 line — needs proper documentation |
| **Multi-city comparison views** | The report works for one city at a time. A cross-city comparison chart doesn't exist yet |

---

## How to Run Everything

### 0. Setup — First Time Only

```bash
# Install dependencies
pip install -r requirements.txt

# Copy the environment template
copy .env.example .env
# Then open .env and add your API keys (see api_keys_guide.md)
```

### 1. Run with Synthetic Data (No API Keys Needed)

This is the fastest way to generate output for any city. Uses randomly generated but statistically realistic data — good for testing and demos.

```bash
# Single city — full 10-year run
python run_pipeline.py --city mumbai --phase all --synthetic

# Single city — short date range (much faster, ~30 seconds)
python run_pipeline.py --city mumbai --phase all --synthetic --start 2022-01 --end 2022-12

# ALL 10 cities at once (~15 minutes)
python run_pipeline.py --city all --phase all --synthetic
```

Output goes to: `data/h3_panel/final/{city}.parquet`

### 2. Run with Real Data (API Keys Required)

Run one phase at a time so you can check intermediate results:

```bash
# Phase 1: Download raw data from all APIs
python run_pipeline.py --city mumbai --phase ingest

# Phase 2: Process raw files → H3 monthly panel
python run_pipeline.py --city mumbai --phase process

# Phase 3: Compute 5 stress indicators
python run_pipeline.py --city mumbai --phase features

# Phase 4: Compute SSI composite score
python run_pipeline.py --city mumbai --phase ssi

# OR — do all 4 phases in one command
python run_pipeline.py --city mumbai --phase all
```

### 3. Generate the HTML Report

```bash
# Generate dashboard for Mumbai (uses existing parquet output)
python generate_report.py --city mumbai

# Generate for all cities that have been processed
python generate_report.py --city all

# Custom output path
python generate_report.py --city mumbai --out my_report.html
```

Open the output file in any browser:
```
reports/mumbai_ssi_report.html
```

### 4. Run the Test Suite

```bash
python -m pytest tests/ -v
# Expected result: 19 passed
```

### 5. Inspect the Output Data

```python
import pandas as pd

df = pd.read_parquet("data/h3_panel/final/mumbai.parquet")

# Basic info
print(df.shape)                           # (330132, 30)
print(df.columns.tolist())

# Most stressed months
print(df.groupby("date")["ssi_value"].mean().sort_values(ascending=False).head(10))

# Most stressed locations
print(df.groupby("h3_index")["ssi_value"].mean().sort_values(ascending=False).head(20))

# Band breakdown
print(df["ssi_band"].value_counts())

# Anomaly hexes for a specific month
anomalies = df[(df["date"] == "2022-06-01") & (df["anomaly_flag"] == True)]
print(f"{len(anomalies)} hexes in anomaly state in Jun 2022")

# Correlation between indicators
indicator_cols = ["heat_stress_idx", "water_stress_idx", "pollution_idx",
                  "vegetation_idx", "urban_vulnerability_idx"]
print(df[indicator_cols].corr())
```

---

## CLI Reference — All Flags

### `run_pipeline.py`

| Flag | Default | Options | Description |
|---|---|---|---|
| `--city` | `mumbai` | any city slug, or `all` | Which city to process |
| `--phase` | `all` | `ingest`, `process`, `features`, `ssi`, `all` | Which pipeline phase to run |
| `--synthetic` | off | (flag, no value) | Use randomly generated data — no API keys needed |
| `--start` | `2015-01` | `YYYY-MM` | Start of study period |
| `--end` | `2025-12` | `YYYY-MM` | End of study period |
| `--resolution` | `8` | `7`, `8`, `9` | H3 hex resolution (8 = ~0.74 km²) |
| `--overwrite` | off | (flag, no value) | Re-download files that already exist |

### `generate_report.py`

| Flag | Default | Options | Description |
|---|---|---|---|
| `--city` | `mumbai` | any city slug, or `all` | Which city to report on |
| `--source` | `final` | `final`, `processed` | Load final panel (with SSI) or intermediate (without) |
| `--out` | `reports/{city}_ssi_report.html` | any path | Custom output file path |

---

## Available Cities

| Slug | City | State | Primary Stresses |
|---|---|---|---|
| `mumbai` | Mumbai | Maharashtra | Heat, Flood, Pollution |
| `delhi` | Delhi NCR | Delhi | Extreme Heat, Pollution, Dust |
| `bengaluru` | Bengaluru | Karnataka | Water Crisis, Urban Heat |
| `chennai` | Chennai | Tamil Nadu | Heat, Flooding, Water Stress |
| `hyderabad` | Hyderabad | Telangana | Heat, Drought, Urban Sprawl |
| `pune` | Pune | Maharashtra | Heat, Water Stress, Rapid Growth |
| `ahmedabad` | Ahmedabad | Gujarat | Extreme Heat |
| `kolkata` | Kolkata | West Bengal | Heat, Flooding, Pollution |
| `surat` | Surat | Gujarat | Flooding, Industrial Pollution, Heat |
| `indore` | Indore | Madhya Pradesh | Heat, Water Stress |

---

## API Keys — What's Needed for Real Data

| API | Used For | How to Get | Required? |
|---|---|---|---|
| **CDS API key** (Copernicus) | ERA5 climate data — temperature, humidity, precipitation, wind | Register at cds.climate.copernicus.eu → My Profile → API key → save to `~/.cdsapirc` | **Yes** for climate |
| **Planetary Computer** (Microsoft) | Sentinel-2 NDVI, MODIS LST, GHSL built-up | No key needed — anonymous tokens auto-signed | No key needed |
| **NASA EarthData** | PM2.5 annual mean from SEDAC | Register at urs.earthdata.nasa.gov → add to `.env` as `EARTHDATA_USER` + `EARTHDATA_PASSWORD` | Optional (pipeline falls back to synthetic PM2.5) |
| **OpenStreetMap / OSMnx** | Road density, buildings, green space | No key needed | No key needed |

Full step-by-step instructions are in `api_keys_guide.md`.

---

## SSI Score — How to Interpret It

| Score | Band | Meaning |
|---|---|---|
| 0.00 – 0.25 | **Low** | Stress within historical norms. No concern. |
| 0.25 – 0.50 | **Moderate** | Elevated stress. Monitor for compounding trends. |
| 0.50 – 0.75 | **High** | Significant compound stress. Multiple indicators are simultaneously elevated. Action required. |
| 0.75 – 1.00 | **Extreme** | Critical compound stress. Multiple severe stressors co-occurring. Priority intervention zone. |

The SSI is a **composite** score — a high score means multiple types of stress are happening at the same time in the same place. A hex could score 0.8 because it has high heat AND high pollution AND low vegetation AND high vulnerability — not just one of these.

---

## Recommended Next Steps

1. **Run all 10 cities with synthetic data** — generates full dataset in ~15 min:
   ```bash
   python run_pipeline.py --city all --phase all --synthetic
   python generate_report.py --city all
   ```

2. **Set up the CDS API key** and run real ERA5 data for one city to validate:
   ```bash
   python run_pipeline.py --city mumbai --phase ingest
   python run_pipeline.py --city mumbai --phase all
   ```

3. **Build the quality module** (`src/quality/`) — fill-rate reports, outlier detection

4. **Write the README** — usage instructions, output schema, methodology summary

5. **Add multi-city comparison charts** — cross-city SSI ranking and trend comparison
