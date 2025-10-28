"""
Climate data validation.

Sanity checks and validation for climate data processing.
"""
import polars as pl
from typing import Dict, List, Tuple

from .schema import MONTHS, TEMP_SCALE, PPT_MM_SCALE, PPT_IN_SCALE


def validate_climate_ranges(df: pl.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate climate data for reasonable ranges.
    
    Args:
        df: Polars DataFrame with climate data
        
    Returns:
        Tuple of (is_valid, list_of_warnings)
    """
    warnings: List[str] = []
    is_valid = True
    
    # Temperature range checks (quantized values)
    temp_cols = [c for c in df.columns if c.endswith("_f_q")]
    for col in temp_cols:
        if col not in df.columns:
            continue
        
        # Dequantize for range checking
        values = df.select(pl.col(col).cast(pl.Float64) * TEMP_SCALE).to_series()
        min_val = values.min()
        max_val = values.max()
        
        # Reasonable ranges for US temperatures: -70°F to 130°F
        if min_val < -70.0:
            warnings.append(f"{col}: Unusually low temperature {min_val:.1f}°F")
            is_valid = False
        if max_val > 130.0:
            warnings.append(f"{col}: Unusually high temperature {max_val:.1f}°F")
            is_valid = False
    
    # Precipitation range checks (quantized values)
    ppt_mm_cols = [c for c in df.columns if c.startswith("ppt_") and c.endswith("_mm_q")]
    for col in ppt_mm_cols:
        if col not in df.columns:
            continue
        
        # Dequantize for range checking
        values = df.select(pl.col(col).cast(pl.Float64) * PPT_MM_SCALE).to_series()
        min_val = values.min()
        max_val = values.max()
        
        # Reasonable ranges: 0mm to 1000mm per month (extreme monsoon)
        if min_val < 0.0:
            warnings.append(f"{col}: Negative precipitation {min_val:.1f}mm")
            is_valid = False
        if max_val > 1000.0:
            warnings.append(f"{col}: Unusually high precipitation {max_val:.1f}mm")
    
    # Seasonal sanity checks
    if "temp_mean_summer_f_q" in df.columns and "temp_mean_winter_f_q" in df.columns:
        summer = df.select(pl.col("temp_mean_summer_f_q").cast(pl.Float64) * TEMP_SCALE).to_series()
        winter = df.select(pl.col("temp_mean_winter_f_q").cast(pl.Float64) * TEMP_SCALE).to_series()
        
        # Summer should generally be warmer than winter
        diff = (summer - winter)
        min_diff = diff.min()
        if min_diff < -10.0:  # Allow some flexibility for tropical/equatorial regions
            warnings.append(f"Seasonal inversion: Summer colder than winter by {abs(min_diff):.1f}°F in some hexes")
    
    return is_valid, warnings


def validate_climate_completeness(df: pl.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate that all expected climate columns are present.
    
    Args:
        df: Polars DataFrame with climate data
        
    Returns:
        Tuple of (is_complete, list_of_missing_columns)
    """
    missing = []
    
    # Check for monthly temperature columns
    for month in MONTHS:
        col = f"temp_mean_{month}_f_q"
        if col not in df.columns:
            missing.append(col)
    
    # Check for monthly precipitation columns
    for month in MONTHS:
        col_mm = f"ppt_{month}_mm_q"
        col_in = f"ppt_{month}_in_q"
        if col_mm not in df.columns:
            missing.append(col_mm)
        if col_in not in df.columns:
            missing.append(col_in)
    
    # Check for derived columns
    required_derived = [
        "temp_mean_ann_f_q",
        "temp_mean_summer_f_q",
        "temp_mean_winter_f_q",
        "ppt_ann_mm_q",
        "ppt_ann_in_q",
        "climate_label",
    ]
    for col in required_derived:
        if col not in df.columns:
            missing.append(col)
    
    is_complete = len(missing) == 0
    return is_complete, missing
