"""
Shared H3 utilities for overlay modules.

This module provides a consistent interface for H3 operations across different
overlay processors (climate, power corridors, politics, etc.), handling version
compatibility and common operations like cell conversion and polygon filling.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Iterable, Optional, Sequence, Set

import numpy as np
import pandas as pd

# Add src to path to import config
src_path = Path(__file__).parent.parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from config import STATE_BOUNDING_BOXES

# H3 API compatibility layer - handles multiple h3 library versions
try:
    import h3  # type: ignore
    H3_GEO_TO_CELLS = getattr(h3, "geo_to_cells", None)
    H3_POLYFILL_GEOJSON = getattr(h3, "polyfill_geojson", None)
    H3_POLYFILL = getattr(h3, "polyfill", None)
    H3_STRING_TO_INT = next(
        (getattr(h3, attr, None) for attr in ("string_to_h3", "str_to_int", "string_to_int")),
        None,
    )
except ImportError:
    try:
        from h3.api.basic_int import h3 as h3  # type: ignore
        H3_GEO_TO_CELLS = getattr(h3, "geo_to_cells", None)
        H3_POLYFILL_GEOJSON = getattr(h3, "polyfill_geojson", None)
        H3_POLYFILL = getattr(h3, "polyfill", None)
        H3_STRING_TO_INT = getattr(h3, "string_to_h3", None)
    except Exception as exc:  # pragma: no cover - import guard
        raise RuntimeError("h3 library is required") from exc


def cell_to_int(cell) -> int:
    """
    Robustly convert an H3 address (string or int) to its uint64 integer form.
    
    Handles multiple input formats:
    - int/np.integer: returned as-is
    - str: converted via H3 API or base-16 parsing
    - numpy scalars: coerced to int
    
    Args:
        cell: H3 cell in various formats
        
    Returns:
        uint64 integer representation of the H3 cell
        
    Raises:
        TypeError: if cell type is not supported
    """
    if isinstance(cell, (int, np.integer)):
        return int(cell)
    if isinstance(cell, str):
        if callable(H3_STRING_TO_INT):
            return int(H3_STRING_TO_INT(cell))
        # Fall back to base-16 parsing for legacy APIs
        return int(cell, 16)
    # Allow numpy scalars (float64) emitted by some h3 builds
    try:
        return int(cell)
    except Exception as exc:  # pragma: no cover - defensive
        raise TypeError(f"Unsupported H3 cell type: {type(cell)!r}") from exc


def bbox_to_polygon(bbox: dict) -> dict:
    """
    Convert a bounding box to a GeoJSON polygon.
    
    Args:
        bbox: Dictionary with keys 'west', 'east', 'north', 'south' (decimal degrees)
        
    Returns:
        GeoJSON Polygon dictionary with coordinates forming a closed ring
    """
    return {
        "type": "Polygon",
        "coordinates": [[
            [bbox["west"], bbox["south"]],
            [bbox["east"], bbox["south"]],
            [bbox["east"], bbox["north"]],
            [bbox["west"], bbox["north"]],
            [bbox["west"], bbox["south"]],
        ]]
    }


def polygon_to_cells(geom, resolution: int) -> Set[int]:
    """
    Convert a Shapely polygon or multipolygon to H3 cells at given resolution.
    
    Uses the H3 polyfill API to find all cells that intersect the geometry.
    Handles both Polygon and MultiPolygon geometries.
    
    Args:
        geom: Shapely geometry (Polygon or MultiPolygon)
        resolution: H3 resolution level (0-15)
        
    Returns:
        Set of H3 cell IDs as uint64 integers
    """
    if geom is None or geom.is_empty:
        return set()
    
    mapping = geom.__geo_interface__
    polygons: Iterable[dict]
    
    if mapping["type"] == "Polygon":
        polygons = [mapping]
    elif mapping["type"] == "MultiPolygon":
        polygons = (
            {"type": "Polygon", "coordinates": coords}
            for coords in mapping["coordinates"]
        )
    else:
        return set()

    result: Set[int] = set()
    for poly in polygons:
        cells: Iterable = []
        if callable(H3_GEO_TO_CELLS):
            cells = H3_GEO_TO_CELLS(poly, resolution)
        elif callable(H3_POLYFILL_GEOJSON):
            cells = H3_POLYFILL_GEOJSON(poly, resolution)
        elif callable(H3_POLYFILL):
            # Legacy API expects lat/long tuples
            coords_latlon = [
                [(lat, lon) for lon, lat in ring]
                for ring in poly["coordinates"]
            ]
            cells = H3_POLYFILL(coords_latlon, resolution, geo_json_conformant=True)
        else:  # pragma: no cover - defensive
            raise RuntimeError("No suitable H3 polyfill function available.")
        result.update(cell_to_int(cell) for cell in cells)
    return result


def state_hex_universe(state: str, resolutions: Sequence[int]) -> pd.DataFrame:
    """
    Build an H3 grid covering a state's bounding box at specified resolutions.
    
    This creates a complete set of H3 cells that cover the state, useful for
    ensuring all hexes have data even if no features intersect them.
    
    Args:
        state: State slug (e.g., 'massachusetts')
        resolutions: List of H3 resolution levels to generate
        
    Returns:
        DataFrame with columns ['h3_id', 'res'] containing all cells
        
    Raises:
        ValueError: if state not found in STATE_BOUNDING_BOXES
    """
    bbox = STATE_BOUNDING_BOXES.get(state)
    if not bbox:
        raise ValueError(f"No bounding box configured for state '{state}'")
    
    polygon = bbox_to_polygon(bbox)
    records: list[tuple[int, int]] = []
    
    for res in resolutions:
        if callable(H3_GEO_TO_CELLS):
            cells = H3_GEO_TO_CELLS(polygon, res)
        elif callable(H3_POLYFILL_GEOJSON):
            cells = H3_POLYFILL_GEOJSON(polygon, res)
        elif callable(H3_POLYFILL):
            coords_latlon = [
                [(lat, lon) for lon, lat in ring]
                for ring in polygon["coordinates"]
            ]
            cells = H3_POLYFILL(coords_latlon, res, geo_json_conformant=True)
        else:  # pragma: no cover - defensive
            raise RuntimeError("No suitable H3 polyfill function available.")
        for cell in cells:
            records.append((cell_to_int(cell), res))

    if not records:
        return pd.DataFrame(columns=["h3_id", "res"])
    
    df = pd.DataFrame(records, columns=["h3_id", "res"])
    df["h3_id"] = df["h3_id"].astype("uint64", copy=False)
    df["res"] = df["res"].astype("int32", copy=False)
    return df.drop_duplicates(ignore_index=True)

