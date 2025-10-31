"""
Overture POI Ingestion

Loads POI data from Overture Maps parquet files.
"""
import os
import pandas as pd
import geopandas as gpd


def load_overture_pois(state: str, overture_path: str = None) -> gpd.GeoDataFrame:
    """
    Load Overture POIs for a given state.
    
    Args:
        state: State name (e.g., 'massachusetts')
        overture_path: Optional path to Overture parquet file.
                      If not provided, uses data/overture/{state}_places.parquet
        
    Returns:
        GeoDataFrame with Overture POIs
    """
    # For now, we only handle Massachusetts as per the download script.
    # This will need to be generalized.
    if state != "massachusetts":
        print(f"[warn] Overture loading only implemented for 'massachusetts', not '{state}'. Skipping.")
        return gpd.GeoDataFrame(columns=['geometry'], geometry='geometry', crs="EPSG:4326")

    if overture_path is None:
        overture_path = "data/overture/ma_places.parquet"
    
    if not os.path.exists(overture_path):
        print(f"[error] Overture data not found at {overture_path}. Run the download script first.")
        return gpd.GeoDataFrame(columns=['geometry'], geometry='geometry', crs="EPSG:4326")
    
    print(f"--- Loading Overture POIs for {state} from {overture_path} ---")
    
    # Read into a normal pandas DataFrame first
    df = pd.read_parquet(overture_path)
    
    # Manually convert the WKB geometry column to a GeoSeries
    geometries = gpd.GeoSeries.from_wkb(df['geometry'])
    gdf = gpd.GeoDataFrame(df, geometry=geometries)

    gdf = gdf.set_crs("EPSG:4326", allow_override=True)
    print(f"[ok] Loaded {len(gdf)} POIs from Overture for {state}")
    return gdf

