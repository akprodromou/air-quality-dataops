# Utility functions utils.py

import streamlit as st
from pathlib import Path
import pandas as pd
import duckdb
from config import (
    EXPECTED_POLLUTANTS,
    LIMITS_DICT,
    VERY_GOOD_HEX,
    GOOD_HEX,
    MEDIUM_HEX,
    POOR_HEX,
    VERY_POOR_HEX,
    EXTREMELY_POOR_HEX,
    VERBAL_LABELS  
)

# Functions
@st.cache_data(ttl=300)
def load_forecast_csv(path: Path):
    """Load forecast CSV if present; normalize column names and types."""
    if not path.exists():
        return None
    df = pd.read_csv(path)
    # try to find a date-like column and normalize to 'forecast_day'
    if "forecast_day" not in df.columns:
        possible_date_cols = [
            c for c in df.columns if ("date" in c.lower() or "day" in c.lower())
        ]
        if possible_date_cols:
            df = df.rename(columns={possible_date_cols[0]: "forecast_day"})
    if "forecast_day" not in df.columns:
        # nothing we can do automatically
        return None

    df["forecast_day"] = pd.to_datetime(df["forecast_day"])
    # coerce expected pollutant columns to numeric if they exist
    for p in EXPECTED_POLLUTANTS:
        if p in df.columns:
            df[p] = pd.to_numeric(df[p], errors="coerce")
    df = df.sort_values("forecast_day").reset_index(drop=True)
    return df


@st.cache_data(ttl=300)
def load_forecast_from_duckdb(db_path: Path):
    """Try to read a forecast/results table from DuckDB. Returns DataFrame or None."""
    if not db_path.exists():
        return None
    try:
        con = duckdb.connect(database=str(db_path), read_only=True)
        # Try common table names that might hold predictions
        candidate_tables = [
            "forecast_results",
            "predicted_pollutants_7day",
            "predictions",
            "predicted_forecast",
        ]
        for t in candidate_tables:
            try:
                df = con.execute(f"SELECT * FROM {t} ORDER BY forecast_day").fetchdf()
                if "forecast_day" in df.columns or "reading_date" in df.columns:
                    # normalize to 'forecast_day'
                    if (
                        "reading_date" in df.columns
                        and "forecast_day" not in df.columns
                    ):
                        df = df.rename(columns={"reading_date": "forecast_day"})
                    df["forecast_day"] = pd.to_datetime(df["forecast_day"])
                    for p in EXPECTED_POLLUTANTS:
                        if p in df.columns:
                            df[p] = pd.to_numeric(df[p], errors="coerce")
                    return df.sort_values("forecast_day").reset_index(drop=True)
            except Exception:
                continue
    except Exception:
        return None
    return None

# Current readings table functions
def get_bin_color(parameter, value):
    # Map parameter name to its limits
    colors = [
        VERY_GOOD_HEX,
        GOOD_HEX,
        MEDIUM_HEX,
        POOR_HEX,
        VERY_POOR_HEX,
        EXTREMELY_POOR_HEX,
    ]

    limits = LIMITS_DICT.get(parameter)
    if limits is None:
        return ""  # fallback

    # Determine which bin the value falls into
    for i in range(len(limits) - 1):
        if limits[i] <= value < limits[i + 1]:
            return colors[i]
    return colors[-1]  # if value >= highest limit


def get_bin_index(parameter, value):
    limits = LIMITS_DICT.get(parameter)
    if limits is None:
        return None

    for i in range(len(limits) - 1):
        if limits[i] <= value < limits[i + 1]:
            return i
    return len(limits) - 2  # last bin if value >= highest limit


def get_verbal_status(parameter, value):
    bin_idx = get_bin_index(parameter, value)
    if bin_idx is None:
        return ""
    return VERBAL_LABELS[bin_idx]

