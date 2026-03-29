"""
src/features/water_stress.py
─────────────────────────────
Compute water stress index per H3 hex per month.

Inputs: precip_sum_mm, soil_moisture
Output: water_stress_idx (normalized, 0 = baseline, >0 = stress)

Components:
  1. Precipitation deficit: negative anomaly from long-term monthly mean
     (lower-than-normal precip → higher water stress)
  2. Soil moisture deficit: deviation below historical 25th percentile
     (low soil water content → drought stress)
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.features.normalize import (
    compute_monthly_baseline,
    apply_baseline_zscore,
    normalize_indicator,
)
from src.utils.logger import get_logger

log = get_logger(__name__)


def compute_precip_deficit(
    df: pd.DataFrame,
    precip_col: str = "precip_sum_mm",
    baseline_years: int = 3,
) -> pd.DataFrame:
    """
    Compute precipitation deficit as a z-score relative to monthly baseline.

    Negative precip anomaly (rain below normal) → positive deficit score.

    Args:
        df            : Panel DataFrame.
        precip_col    : Column with monthly precipitation.
        baseline_years: Years for baseline.

    Returns:
        DataFrame with added `precip_deficit` column.
    """
    baseline = compute_monthly_baseline(df, precip_col, baseline_years=baseline_years)

    # Invert=True: less rain = higher deficit score
    df = apply_baseline_zscore(
        df,
        precip_col,
        baseline,
        output_col="precip_deficit",
        invert=True,    # Invert so deficit direction is correct
        apply_floor=True,
    )
    return df


def compute_soil_moisture_deficit(
    df: pd.DataFrame,
    sm_col: str = "soil_moisture",
    low_threshold_pct: float = 25.0,
) -> pd.Series:
    """
    Compute soil moisture deficit as deviation below the historical 25th percentile.

    Approach:
      - For each (h3_index, calendar_month), compute p25 of soil moisture
      - Deficit = max(0, p25 - observed) / p25  → fraction below normal
      - This is always non-negative (0 = above p25, >0 = below normal)

    Args:
        df               : Panel DataFrame.
        sm_col           : Soil moisture column.
        low_threshold_pct: Percentile to use as drought threshold.

    Returns:
        pd.Series of soil moisture deficit scores.
    """
    df = df.copy()
    df["_month"] = pd.to_datetime(df["date"]).dt.month

    p25 = (
        df.groupby(["h3_index", "_month"])[sm_col]
        .quantile(low_threshold_pct / 100.0)
        .reset_index()
        .rename(columns={sm_col: "sm_p25"})
    )

    merged = df.merge(p25, on=["h3_index", "_month"], how="left")

    sm_obs = merged[sm_col].fillna(merged["sm_p25"])
    threshold = merged["sm_p25"]

    # Deficit increases as observed drops below threshold
    deficit = (threshold - sm_obs) / (threshold.replace(0, np.nan) + 1e-6)
    deficit = deficit.clip(lower=0.0)

    return pd.Series(deficit.values, index=df.index, name="soil_moisture_deficit")


def add_water_stress_idx(
    df: pd.DataFrame,
    precip_weight: float = 0.6,
    soil_weight: float = 0.4,
    baseline_years: int = 3,
) -> pd.DataFrame:
    """
    Add normalized `water_stress_idx` column to the panel DataFrame.

    Composite:
        water_stress_raw = precip_weight * precip_deficit
                         + soil_weight  * soil_moisture_deficit (if available)

    Both components are floored at 0 before combining.

    Args:
        df            : Panel DataFrame.
        precip_weight : Weight for precipitation deficit.
        soil_weight   : Weight for soil moisture deficit.
        baseline_years: Baseline window.

    Returns:
        DataFrame with added `water_stress_idx` column.
    """
    log.info("Computing water stress index…")
    df = df.copy()

    has_precip = "precip_sum_mm" in df.columns and df["precip_sum_mm"].notna().any()
    has_soil = "soil_moisture" in df.columns and df["soil_moisture"].notna().any()

    if not has_precip and not has_soil:
        log.warning("No precip or soil moisture data; water_stress_idx set to NaN")
        df["water_stress_idx"] = np.nan
        return df

    if has_precip:
        df = compute_precip_deficit(df, baseline_years=baseline_years)
    else:
        df["precip_deficit"] = 0.0
        precip_weight = 0.0
        soil_weight = 1.0

    if has_soil:
        df["soil_moisture_deficit"] = compute_soil_moisture_deficit(df)
    else:
        df["soil_moisture_deficit"] = 0.0
        precip_weight = 1.0
        soil_weight = 0.0

    # Normalise weights
    total_w = precip_weight + soil_weight
    df["water_stress_raw"] = (
        (precip_weight / total_w) * df["precip_deficit"]
        + (soil_weight / total_w) * df["soil_moisture_deficit"]
    )

    # Final z-score normalisation of composite
    df["water_stress_idx"] = normalize_indicator(
        df["water_stress_raw"], method="zscore", apply_floor=True
    )

    df = df.drop(
        columns=["precip_deficit", "soil_moisture_deficit", "water_stress_raw"],
        errors="ignore",
    )

    log.info(
        "Water stress: mean={m:.2f}, max={mx:.2f}",
        m=df["water_stress_idx"].mean(),
        mx=df["water_stress_idx"].max(),
    )
    return df
