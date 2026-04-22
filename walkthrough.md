# SSI Pipeline — Setup & Bug Fix Walkthrough

## What Was Done

### 1. Dependency Installation

All required packages installed successfully via pip:

| Package | Version |
|---|---|
| geopandas | 1.1.3 |
| rasterio | 1.5.0 |
| h3 | 4.4.2 |
| rasterstats | 0.20.0 |
| xarray | 2026.2.0 |
| fiona | 1.10.1 |
| rioxarray | 0.22.0 |
| netCDF4 | 1.7.4 |
| scikit-learn | 1.7.2 |
| pyproj | 3.7.2 |
| pystac-client | 0.9.0 |
| pytest | 9.0.2 |

---

### 2. Bug Fixes

#### Bug 1 — `KeyError: '_month'` in `apply_baseline_zscore` (normalize.py)

**Root cause**: `compute_monthly_baseline()` returns a `baseline_stats` DataFrame with a column named `"month"`, but `apply_baseline_zscore()` added a temp column `"_month"` to `df` and then tried to merge on `[groupby_col, "_month"]` — which didn't exist in `baseline_stats`.

**Fix** ([normalize.py](file:///c:/Users/Admin/Downloads/Compound-Sustainability-Stress-Modeling-anuj/src/features/normalize.py)):
```python
# Rename "month" -> "_month" in baseline_stats before merging
stats = baseline_stats.copy()
if "month" in stats.columns and "_month" not in stats.columns:
    stats = stats.rename(columns={"month": "_month"})
merged = df.merge(stats, on=[groupby_col, "_month"], how="left")
```

---

#### Bug 2 — NaN propagation in heat_stress_idx / water_stress_idx (normalize.py)

**Root cause**: When baseline std = 0 (constant signal e.g. short date ranges in tests), division by zero produced NaN. `NaN.clip(lower=0)` stays NaN, failing the `>= 0` assertion.

**Fix** ([normalize.py](file:///c:/Users/Admin/Downloads/Compound-Sustainability-Stress-Modeling-anuj/src/features/normalize.py)):
```python
# Fill NaN (e.g. zero-std baseline, or unmatched hex–month) with 0 (= at-baseline)
normalized = normalized.fillna(0.0)
```

---

#### Bug 3 — Spurious `baseline_years` kwarg in `water_stress.py`

**Root cause**: `add_water_stress_idx()` passed `baseline_years=baseline_years` to `compute_soil_moisture_deficit()`, which doesn't accept that parameter.

**Fix** ([water_stress.py](file:///c:/Users/Admin/Downloads/Compound-Sustainability-Stress-Modeling-anuj/src/features/water_stress.py)):
```python
# Before (wrong):
df["soil_moisture_deficit"] = compute_soil_moisture_deficit(df, baseline_years=baseline_years)
# After (correct):
df["soil_moisture_deficit"] = compute_soil_moisture_deficit(df)
```

---

#### Bug 4 — Windows cp1252 Unicode Encoding Errors in Logger

**Root cause**: The loguru console handler wrote to `sys.stdout` which, on Windows, defaults to cp1252 encoding. Log messages containing Unicode box-drawing chars (`→`, `─`, `×`) caused `UnicodeEncodeError`. The original fix (wrapping stdout in `io.TextIOWrapper`) broke pytest's stdout capture, crashing tests.

**Fix** ([logger.py](file:///c:/Users/Admin/Downloads/Compound-Sustainability-Stress-Modeling-anuj/src/utils/logger.py)):
- Use `sys.stdout.reconfigure(encoding="utf-8", errors="replace")` — safe, doesn't break pytest
- Disable `colorize=True` (avoids ANSI passthrough issues on Windows)
- Set `encoding="utf-8"` on the rotating file handler
- Replaced all `→` / `×` / `─` in log *message strings* with ASCII equivalents (`->`, `x`, `-`)

---

### 3. Test Results

```
============================= test session starts =============================
platform win32 -- Python 3.12.4, pytest-9.0.2

19 passed in 4.93s
```

All 19 tests pass:
- **TestH3Utils** (5 tests) — city bbox loading, H3 grid generation, GeoDataFrame, time skeleton
- **TestNormalization** (5 tests) — zscore, minmax, clip_and_floor, normalize_indicator
- **TestHeatStress** (3 tests) — relative humidity, heat index, add_heat_stress_idx
- **TestWaterStress** (1 test) — add_water_stress_idx
- **TestSSI** (4 tests) — ssi_value range, band labels, anomaly flag, archetype id
- **TestSyntheticPipeline** (1 test) — full end-to-end integration test

---

### 4. CLI Verification

```bash
python run_pipeline.py --city mumbai --phase all --synthetic --start 2022-01 --end 2022-03
```

**Pipeline Output (Exit code: 0)**:
- Mumbai: 2,501 H3 cells at resolution 8
- 330,132 rows x 30 columns
- PCA weights: heat=0.355, water=0.053, pollution=0.373, vegetation=0.000, urban=0.219
- SSI anomaly rate: 10.0%
- Saved → `data/h3_panel/final/mumbai.parquet`

---

## How to Run

### Run tests
```bash
python -m pytest tests/ -v
```

### Run full synthetic pipeline (Mumbai, 3 months)
```bash
python run_pipeline.py --city mumbai --phase all --synthetic --start 2022-01 --end 2022-03
```

### Run all 10 cities (synthetic, full 10-year range)
```bash
python run_pipeline.py --city all --phase all --synthetic
```

### Run with real data (requires API keys in .env)
```bash
python run_pipeline.py --city delhi --phase ingest
python run_pipeline.py --city delhi --phase all
```
