"""
Canonical POI Schema Definition

Defines the target schema for all POIs after normalization from various sources.
"""
import geopandas as gpd
import pandas as pd

# Canonical Data Schema
# This defines the target schema for all POIs after normalization.
CANONICAL_POI_SCHEMA = {
    "poi_id": "str",
    "name": "str",
    "brand_id": "str",
    "brand_name": "str",
    "class": "str",
    "category": "str",
    "subcat": "str",
    "trauma_level": "str",
    "lon": "float32",
    "lat": "float32",
    "geometry": "geometry",
    "source": "str",
    "ext_id": "str",
    "h3_r9": "str",
    "provenance": "object",  # list of strings
}


def validate_poi_dataframe(gdf: gpd.GeoDataFrame) -> bool:
    """
    Validate that a GeoDataFrame conforms to the canonical POI schema.
    
    Args:
        gdf: GeoDataFrame to validate
        
    Returns:
        True if valid, raises ValueError otherwise
    """
    missing_cols = set(CANONICAL_POI_SCHEMA.keys()) - set(gdf.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Validate geometry column exists and is valid
    if not hasattr(gdf, 'geometry'):
        raise ValueError("GeoDataFrame must have a geometry column")
    
    # Validate provenance is list-like
    if "provenance" in gdf.columns:
        for val in gdf["provenance"]:
            if val is not None and not isinstance(val, (list, tuple)):
                raise ValueError(f"provenance must be list-like, got {type(val)}")
    
    return True


def create_empty_poi_dataframe() -> gpd.GeoDataFrame:
    """Create an empty GeoDataFrame with the canonical POI schema."""
    return gpd.GeoDataFrame(
        columns=list(CANONICAL_POI_SCHEMA.keys()),
        geometry='geometry',
        crs="EPSG:4326"
    )

