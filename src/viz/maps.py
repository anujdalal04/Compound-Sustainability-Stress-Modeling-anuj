"""
src/viz/maps.py
────────────────
H3 hex choropleth map builder using Plotly Mapbox.

No API token required — uses carto-darkmatter basemap (public tile server).

Main function:
    build_h3_map(df, column, agg, title) → plotly Figure
"""

from __future__ import annotations

import json
from typing import Literal

import h3
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


# ─── Colour palettes per column ──────────────────────────────────────────────

_COLOUR_SCALES: dict[str, str] = {
    "ssi_value":              "RdYlGn_r",
    "heat_stress_idx":        "YlOrRd",
    "water_stress_idx":       "Blues_r",
    "pollution_idx":          "YlOrBr",
    "vegetation_idx":         "Greens_r",
    "urban_vulnerability_idx":"Purples",
    "ndvi":                   "Greens",
    "lst_c":                  "RdYlBu_r",
    "pm25":                   "Oranges",
    "temp_mean_c":            "RdYlBu_r",
    "precip_sum_mm":          "Blues",
}

_LABELS: dict[str, str] = {
    "ssi_value":              "SSI Score",
    "heat_stress_idx":        "Heat Stress",
    "water_stress_idx":       "Water Stress",
    "pollution_idx":          "Pollution",
    "vegetation_idx":         "Vegetation Stress",
    "urban_vulnerability_idx":"Urban Vulnerability",
}


# ─── Internal helpers ─────────────────────────────────────────────────────────

def _cells_to_geojson(cells: list[str], values: list[float]) -> dict:
    """Convert H3 cell IDs + scalar values to a GeoJSON FeatureCollection."""
    features = []
    for cell, val in zip(cells, values):
        boundary = h3.cell_to_boundary(cell)          # list of (lat, lon)
        coords   = [[lon, lat] for lat, lon in boundary]
        coords.append(coords[0])                       # close ring
        features.append({
            "type": "Feature",
            "id": cell,
            "geometry": {
                "type": "Polygon",
                "coordinates": [coords],
            },
            "properties": {
                "h3_index": cell,
                "value": float(val) if not (np.isnan(val) if isinstance(val, float) else False) else 0.0,
            },
        })
    return {"type": "FeatureCollection", "features": features}


def _city_center(cells: list[str]) -> tuple[float, float]:
    """Compute (lat, lon) centroid of a set of H3 cells."""
    lats, lons = [], []
    for cell in cells:
        lat, lon = h3.cell_to_latlng(cell)
        lats.append(lat)
        lons.append(lon)
    return float(np.mean(lats)), float(np.mean(lons))


# ─── Public API ───────────────────────────────────────────────────────────────

def build_h3_map(
    df: pd.DataFrame,
    column: str = "ssi_value",
    agg: Literal["mean", "max", "last"] = "mean",
    title: str | None = None,
    height: int = 520,
) -> go.Figure:
    """
    Build an interactive H3 choropleth map with Plotly Mapbox.

    Args:
        df    : Panel DataFrame with h3_index + column.
        column: Variable to colour hexes by.
        agg   : 'mean' / 'max' across time, or 'last' for latest month.
        title : Map title (auto-generated if None).
        height: Figure height in pixels.

    Returns:
        Plotly Figure ready for display or HTML export.
    """
    # ── Aggregate to one value per hex ───────────────────────────────────────
    if agg == "last":
        latest = df["date"].max()
        hex_df = (
            df[df["date"] == latest]
            .groupby("h3_index")[column]
            .mean()
            .reset_index()
        )
        agg_label = f"Latest month ({pd.to_datetime(latest).strftime('%b %Y')})"
    else:
        hex_df = df.groupby("h3_index")[column].agg(agg).reset_index()
        agg_label = f"10-year {agg}"

    hex_df[column] = hex_df[column].fillna(0)

    # ── Build GeoJSON ─────────────────────────────────────────────────────────
    geojson = _cells_to_geojson(hex_df["h3_index"].tolist(), hex_df[column].tolist())

    # ── City centre ──────────────────────────────────────────────────────────
    center_lat, center_lon = _city_center(hex_df["h3_index"].tolist())

    # ── Plot ──────────────────────────────────────────────────────────────────
    label = _LABELS.get(column, column.replace("_", " ").title())
    auto_title = title or f"<b>{label}</b> — {agg_label}"

    colorscale = _COLOUR_SCALES.get(column, "RdYlGn_r")

    fig = px.choropleth_mapbox(
        hex_df,
        geojson=geojson,
        locations="h3_index",
        color=column,
        color_continuous_scale=colorscale,
        mapbox_style="carto-darkmatter",
        center={"lat": center_lat, "lon": center_lon},
        zoom=11,
        opacity=0.78,
        hover_data={"h3_index": True, column: ":.4f"},
        height=height,
    )

    fig.update_layout(
        title={
            "text": auto_title,
            "font": {"size": 15, "color": "#e0e0e0"},
            "x": 0.02,
        },
        paper_bgcolor="rgba(15,17,23,0)",
        plot_bgcolor="rgba(15,17,23,0)",
        font={"color": "#e0e0e0", "family": "Inter, sans-serif"},
        margin={"r": 0, "t": 44, "l": 0, "b": 0},
        coloraxis_colorbar={
            "title": {"text": label, "font": {"color": "#e0e0e0", "size": 11}},
            "tickfont": {"color": "#b0b0b0", "size": 10},
            "thickness": 14,
            "len": 0.7,
        },
    )
    return fig
