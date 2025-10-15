"""
Geometry hygiene utilities for Shapely 2.x compatibility.

Fixes ufunc 'create_collection' errors when building GeometryCollection 
or Multi* objects from mixed or invalid entries.
"""
from __future__ import annotations
from typing import List, Optional

import shapely
from shapely.geometry.base import BaseGeometry
import geopandas as gpd


def clean_geoms(
    gdf: gpd.GeoDataFrame, 
    types: Optional[List[str]] = None
) -> gpd.GeoSeries:
    """
    Clean geometries to prevent Shapely 2.x 'create_collection' errors.
    
    Args:
        gdf: GeoDataFrame with potentially problematic geometries
        types: Optional list of allowed geometry types (e.g. ["Polygon", "MultiPolygon"])
    
    Returns:
        Clean GeoSeries with valid, non-empty geometries
        
    Usage:
        polys = clean_geoms(gdf, ["Polygon", "MultiPolygon"])
        lines = clean_geoms(gdf, ["LineString", "MultiLineString"])
    """
    g = gdf.geometry
    
    # Remove null geometries
    g = g[g.notna()]
    
    # Remove empty geometries
    g = g[~g.is_empty]
    
    # Ensure we have actual geometry objects (not numpy arrays)
    g = g[g.apply(lambda x: isinstance(x, BaseGeometry))]
    
    # Filter by geometry type if specified
    if types is not None:
        g = g[g.apply(lambda x: x.geom_type in types)]
    
    # Optionally make valid for polygons
    if types is not None and ("Polygon" in types or "MultiPolygon" in types):
        g = g.apply(lambda x: shapely.make_valid(x))
    
    return g

