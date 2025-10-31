"""
Airport curation from CSV source.

Airports are loaded from a curated CSV file (Future/airports_coordinates.csv)
rather than OSM/Overture to ensure data quality and completeness.
"""
import os
import uuid
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from vicinity.poi.schema import create_empty_poi_dataframe
from .schema import AIRPORT_CLASS, AIRPORT_CATEGORY, AIRPORT_SUBCAT


def load_airports_csv(csv_path: str = None) -> gpd.GeoDataFrame:
    """
    Load airport list from CSV and normalize to canonical schema.
    
    This function loads airports from a curated CSV source (CSV-only, ignoring
    OSM/Overture airports) to ensure data quality.
    
    Args:
        csv_path: Path to airports CSV file. If None, uses Future/airports_coordinates.csv
        
    Returns:
        GeoDataFrame with normalized airport POIs
    """
    if csv_path is None:
        csv_path = os.path.join('Future', 'airports_coordinates.csv')
    
    if not os.path.exists(csv_path):
        print(f"[warn] Airports CSV not found at {csv_path}; skipping airports injection.")
        return create_empty_poi_dataframe()
    
    df = pd.read_csv(csv_path)
    # Expect columns: IATA, AIRPORT, CITY, STATE, COUNTRY, LATITUDE, LONGITUDE
    rows = []
    for _, r in df.iterrows():
        try:
            lat = float(r['LATITUDE'])
            lon = float(r['LONGITUDE'])
        except Exception:
            continue
        
        iata = str(r.get('IATA') or '').strip()
        name = str(r.get('AIRPORT') or '').strip() or (iata if iata else None)
        poi_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"csv:airports|{iata}|{lon:.6f}|{lat:.6f}"))
        
        rows.append({
            'poi_id': poi_id,
            'name': name,
            'brand_id': None,
            'brand_name': None,
            'class': AIRPORT_CLASS,
            'category': AIRPORT_CATEGORY,
            'subcat': AIRPORT_SUBCAT,
            'trauma_level': None,
            'lon': lon,
            'lat': lat,
            'geometry': Point(lon, lat),
            'source': 'csv:airports',
            'ext_id': iata if iata else None,
            'provenance': ['csv:airports'],
        })
    
    if not rows:
        return create_empty_poi_dataframe()
    
    gdf = gpd.GeoDataFrame(rows, crs="EPSG:4326")
    print(f"[ok] Loaded {len(gdf)} airports from CSV.")
    return gdf


def snap_to_arterial(
    airports_gdf: gpd.GeoDataFrame,
    road_network: gpd.GeoDataFrame,
    max_distance_m: float = None
) -> gpd.GeoDataFrame:
    """
    Snap airports to arterial roads within specified distance.
    
    Airports have special snapping requirements: they should only snap to
    arterial roads (motorway, trunk, primary, secondary) within 5km.
    
    Args:
        airports_gdf: GeoDataFrame with airport POIs
        road_network: GeoDataFrame with road network edges
        max_distance_m: Maximum snapping distance in meters (default: 5000m)
        
    Returns:
        GeoDataFrame with snapped airport locations
        
    Note:
        This is a placeholder implementation. Full snapping logic will be
        implemented when needed.
    """
    # TODO: Implement arterial road snapping logic
    # For now, just return airports unchanged
    return airports_gdf.copy()

