"""
Shared validation utilities for overlay modules.

This module provides common validation patterns for parquet schema checking,
data quality validation, and output verification across different overlay
processors.
"""
from __future__ import annotations

from typing import Set, Optional
import pandas as pd


def validate_parquet_schema(
    df: pd.DataFrame,
    required_columns: Set[str],
    source_path: Optional[str] = None
) -> None:
    """
    Validate that a DataFrame has all required columns.
    
    Args:
        df: DataFrame to validate
        required_columns: Set of column names that must be present
        source_path: Optional path to the parquet file (for error messages)
        
    Raises:
        ValueError: if any required columns are missing
    """
    missing = required_columns - set(df.columns)
    if missing:
        source = f" from {source_path}" if source_path else ""
        raise ValueError(
            f"Missing required columns{source}: {missing}. "
            f"Found columns: {set(df.columns)}"
        )


def validate_overlay_output(
    df: pd.DataFrame,
    expected_columns: Set[str],
    h3_id_column: str = "h3_id",
    res_column: str = "res"
) -> None:
    """
    Validate standard overlay output schema.
    
    All overlay modules should produce DataFrames with h3_id and res columns,
    plus module-specific data columns.
    
    Args:
        df: DataFrame to validate
        expected_columns: Set of all expected column names (including h3_id, res)
        h3_id_column: Name of the H3 cell ID column (default: 'h3_id')
        res_column: Name of the resolution column (default: 'res')
        
    Raises:
        ValueError: if schema validation fails
    """
    # Check required columns
    validate_parquet_schema(df, expected_columns)
    
    # Validate h3_id column
    if df[h3_id_column].dtype != "uint64":
        raise ValueError(
            f"Column '{h3_id_column}' must be uint64, got {df[h3_id_column].dtype}"
        )
    
    # Validate res column
    if df[res_column].dtype != "int32":
        raise ValueError(
            f"Column '{res_column}' must be int32, got {df[res_column].dtype}"
        )
    
    # Check for null h3_id values
    null_count = df[h3_id_column].isna().sum()
    if null_count > 0:
        raise ValueError(f"Found {null_count} null values in '{h3_id_column}' column")
    
    # Check for duplicate (h3_id, res) pairs
    dup_count = df.duplicated(subset=[h3_id_column, res_column]).sum()
    if dup_count > 0:
        raise ValueError(
            f"Found {dup_count} duplicate ({h3_id_column}, {res_column}) pairs"
        )


def check_parquet_files(
    paths: list[str],
    required_columns: Set[str],
    warn_only: bool = False
) -> list[pd.DataFrame]:
    """
    Load and validate multiple parquet files with consistent schema checking.
    
    Args:
        paths: List of parquet file paths to load
        required_columns: Set of columns that must be present in each file
        warn_only: If True, print warnings instead of raising errors
        
    Returns:
        List of validated DataFrames (only valid ones if warn_only=True)
        
    Raises:
        ValueError: if validation fails and warn_only=False
    """
    valid_frames = []
    
    for path in paths:
        try:
            df = pd.read_parquet(path)
            validate_parquet_schema(df, required_columns, source_path=path)
            valid_frames.append(df)
        except Exception as exc:
            if warn_only:
                print(f"[warn] Failed to load {path}: {exc}")
                continue
            raise
    
    return valid_frames


def enforce_overlay_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enforce standard types for overlay DataFrame columns.
    
    Converts h3_id to uint64 and res to int32 if present, and returns
    a copy with corrected types.
    
    Args:
        df: DataFrame to type-check
        
    Returns:
        DataFrame with corrected types (copy if changes made)
    """
    needs_copy = False
    result = df
    
    if "h3_id" in df.columns and df["h3_id"].dtype != "uint64":
        if not needs_copy:
            result = df.copy()
            needs_copy = True
        result["h3_id"] = result["h3_id"].astype("uint64", copy=False)
    
    if "res" in df.columns and df["res"].dtype != "int32":
        if not needs_copy:
            result = df.copy()
            needs_copy = True
        result["res"] = result["res"].astype("int32", copy=False)
    
    return result

