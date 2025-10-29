"""
POI Normalization

Normalizes POIs from various sources (Overture, OSM) into the canonical schema.
"""
import uuid
import sys
from pathlib import Path
import pandas as pd
import geopandas as gpd

# Add data/taxonomy to path to import taxonomy
taxonomy_path = Path(__file__).parent.parent.parent / "data" / "taxonomy"
if str(taxonomy_path) not in sys.path:
    sys.path.insert(0, str(taxonomy_path))

from taxonomy import BRAND_REGISTRY, OVERTURE_CATEGORY_MAP, OSM_TAG_MAP
from .schema import CANONICAL_POI_SCHEMA, create_empty_poi_dataframe


# Invert brand registry for quick lookup of aliases
_brand_alias_to_id = {}
for brand_id, (name, aliases) in BRAND_REGISTRY.items():
    _brand_alias_to_id[name.lower()] = brand_id
    for alias in aliases:
        _brand_alias_to_id[alias.lower()] = brand_id


def normalize_overture_pois(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Normalize Overture POIs to the canonical schema.
    
    Args:
        gdf: GeoDataFrame with raw Overture POI data
        
    Returns:
        GeoDataFrame with normalized POIs in canonical schema
    """
    print("--- Normalizing Overture POIs ---")
    if gdf.empty:
        return create_empty_poi_dataframe()
    
    # Track Costco count at input
    costco_input = sum(1 for _, row in gdf.iterrows() 
                      if (row.get('brand', {}) and 
                          row['brand'].get('names', {}) and 
                          'costco' in str(row['brand']['names'].get('primary', '')).lower()))
    print(f"[COSTCO] Overture input: {costco_input} POIs")
    
    normalized_rows = []
    for _, row in gdf.iterrows():
        # Category mapping
        primary_cat = row['categories']['primary'] if row['categories'] and 'primary' in row['categories'] else None
        ts_class, ts_cat, ts_subcat = OVERTURE_CATEGORY_MAP.get(primary_cat, (None, None, None))
        
        # Handle place_of_worship religion mapping for Overture as well (if present)
        if primary_cat and 'place_of_worship' in str(primary_cat).lower():
            # Try to extract religion from the row data (if available in Overture)
            religion = None
            if 'tags' in row and isinstance(row['tags'], dict):
                religion = row['tags'].get('religion')
            if religion:
                religion_lower = str(religion).lower()
                if religion_lower == 'christian':
                    ts_class, ts_cat, ts_subcat = ('religious', 'place_of_worship_church', 'church')
                elif religion_lower == 'muslim':
                    ts_class, ts_cat, ts_subcat = ('religious', 'place_of_worship_mosque', 'mosque')
                elif religion_lower == 'jewish':
                    ts_class, ts_cat, ts_subcat = ('religious', 'place_of_worship_synagogue', 'synagogue')
                elif religion_lower in ('hindu', 'buddhist', 'jain', 'sikh'):
                    ts_class, ts_cat, ts_subcat = ('religious', 'place_of_worship_temple', 'temple')
                else:
                    # Skip if religion not in mapped set
                    continue

        # Exclude airports from Overture; we will inject airports from CSV only
        if ts_cat == 'airport':
            continue

        # Brand resolution
        brand_id, brand_name = None, None
        primary_brand = row['brand']['names']['primary'] if row['brand'] and row['brand']['names'] and 'primary' in row['brand']['names'] else None
        if primary_brand:
            brand_id = _brand_alias_to_id.get(primary_brand.lower())
            if brand_id:
                brand_name = BRAND_REGISTRY[brand_id][0]
        
        # If no brand found from brand field, try the POI name as a fallback
        if not brand_id:
            poi_name = row['names']['primary'] if row['names'] and 'primary' in row['names'] else None
            if poi_name:
                brand_id = _brand_alias_to_id.get(poi_name.lower())
                if brand_id:
                    brand_name = BRAND_REGISTRY[brand_id][0]

        # ID Generation
        source_id = row['id']
        poi_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"overture|{source_id}"))

        normalized_rows.append({
            "poi_id": poi_id,
            "name": row['names']['primary'] if row['names'] and 'primary' in row['names'] else None,
            "brand_id": brand_id,
            "brand_name": brand_name,
            "class": ts_class,
            "category": ts_cat,
            "subcat": ts_subcat,
            "trauma_level": None,
            "lon": row.geometry.x,
            "lat": row.geometry.y,
            "geometry": row.geometry,
            "source": "overture",
            "ext_id": str(source_id),
            "provenance": ["overture"],
        })

    if not normalized_rows:
        return create_empty_poi_dataframe()

    out_gdf = gpd.GeoDataFrame(normalized_rows, crs="EPSG:4326")
    
    # Track Costco count at output
    costco_output = len(out_gdf[out_gdf['brand_id'] == 'costco']) if len(out_gdf) > 0 and 'brand_id' in out_gdf.columns else 0
    print(f"[COSTCO] Overture output: {costco_output} POIs")

    print(f"[ok] Normalized {len(out_gdf)} POIs from Overture.")
    return out_gdf


def normalize_osm_pois(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Normalize OSM POIs to the canonical schema.
    
    Args:
        gdf: GeoDataFrame with raw OSM POI data
        
    Returns:
        GeoDataFrame with normalized POIs in canonical schema
    """
    print("--- Normalizing OSM POIs ---")
    if gdf.empty:
        return create_empty_poi_dataframe()
    
    # Track Costco count at input
    costco_input = sum(1 for _, row in gdf.iterrows() 
                      if any('costco' in str(row.get(tag, '')).lower() 
                            for tag in ['brand', 'operator', 'name']))
    print(f"[COSTCO] OSM input: {costco_input} POIs")

    normalized_rows = []
    for _, row in gdf.iterrows():
        # Category mapping
        ts_class, ts_cat, ts_subcat = None, None, None
        for tag_key in ['amenity', 'shop', 'leisure', 'tourism', 'aeroway']:
            tag_value = row.get(tag_key)
            if tag_value and (tag_key, tag_value) in OSM_TAG_MAP:
                ts_class, ts_cat, ts_subcat = OSM_TAG_MAP[(tag_key, tag_value)]
                
                # Special handling for place_of_worship: map religion to worship type
                if tag_value == 'place_of_worship':
                    religion = row.get('religion')
                    if religion:
                        religion_lower = str(religion).lower()
                        if religion_lower == 'christian':
                            ts_cat = 'place_of_worship_church'
                            ts_subcat = 'church'
                        elif religion_lower == 'muslim':
                            ts_cat = 'place_of_worship_mosque'
                            ts_subcat = 'mosque'
                        elif religion_lower == 'jewish':
                            ts_cat = 'place_of_worship_synagogue'
                            ts_subcat = 'synagogue'
                        elif religion_lower in ('hindu', 'buddhist', 'jain', 'sikh'):
                            ts_cat = 'place_of_worship_temple'
                            ts_subcat = 'temple'
                        # If religion is not in the mapped set, skip this POI
                        elif religion_lower not in ('christian', 'muslim', 'jewish', 'hindu', 'buddhist', 'jain', 'sikh'):
                            ts_class, ts_cat, ts_subcat = None, None, None
                            break
                break
        
        # Exclude airports from OSM; we will inject airports from CSV only
        if ts_cat == 'airport':
            continue
        
        # Brand resolution
        brand_id, brand_name = None, None
        for tag in ['brand', 'operator', 'name']:
            val = row.get(tag)
            if val and isinstance(val, str):
                brand_id = _brand_alias_to_id.get(val.lower())
                if brand_id:
                    brand_name = BRAND_REGISTRY[brand_id][0]
                    break
        
        # ID Generation
        source_id = row['id']
        poi_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"osm|{source_id}"))

        # Use centroid for polygons, or the point itself for points.
        geom = row.geometry
        point = geom.centroid if hasattr(geom, "centroid") else geom

        normalized_rows.append({
            "poi_id": poi_id,
            "name": row.get('name'),
            "brand_id": brand_id,
            "brand_name": brand_name,
            "class": ts_class,
            "category": ts_cat,
            "subcat": ts_subcat,
            "trauma_level": None,
            "lon": point.x,
            "lat": point.y,
            "geometry": point,  # Store the representative point
            "source": "osm",
            "ext_id": str(source_id),
            "provenance": ["osm"],
        })

    if not normalized_rows:
        return create_empty_poi_dataframe()

    out_gdf = gpd.GeoDataFrame(normalized_rows, crs="EPSG:4326")
    
    # Track Costco count at output
    costco_output = len(out_gdf[out_gdf['brand_id'] == 'costco']) if len(out_gdf) > 0 and 'brand_id' in out_gdf.columns else 0
    print(f"[COSTCO] OSM output: {costco_output} POIs")
    
    print(f"[ok] Normalized {len(out_gdf)} POIs from OSM.")
    return out_gdf
