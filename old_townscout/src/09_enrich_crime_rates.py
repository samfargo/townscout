#!/usr/bin/env python3
"""
Enrich H3 hexes with crime rates using centroid-in-polygon matching.
Propagates crime rates from town boundaries to hexes whose centroids fall within.
"""

import logging
import os
import pandas as pd
import geopandas as gpd
import h3
from shapely.geometry import Point
from tqdm import tqdm

from config import (
    STATES, STATE_SLUG_TO_CODE, STATE_FIPS, H3_RES_LOW, H3_RES_HIGH, 
    CRIME_RATE_SOURCE
)
from util_boundaries import (
    build_jurisdiction_layer, load_crime_rate_data, 
    match_crime_rates_to_jurisdictions
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def enrich_h3_with_crime_rates(state_fips: str, resolution: int) -> pd.DataFrame:
    """
    Enrich H3 hexes with crime rates for a specific state and resolution.
    
    Args:
        state_fips: Two-digit state FIPS code
        resolution: H3 resolution (7 or 8)
        
    Returns:
        DataFrame with h3 cell IDs and crime rates
    """
    logger.info(f"Enriching H3 resolution {resolution} hexes for state {state_fips}")
    
    # Load jurisdiction boundaries with crime rates
    jurisdictions_gdf = build_jurisdiction_layer(state_fips)
    if len(jurisdictions_gdf) == 0:
        logger.warning(f"No jurisdictions found for state {state_fips}")
        return pd.DataFrame()
    
    # Load and match crime rate data
    crime_df = load_crime_rate_data(CRIME_RATE_SOURCE)
    jurisdictions_gdf = match_crime_rates_to_jurisdictions(jurisdictions_gdf, crime_df)
    
    # Load existing H3 data for this state and resolution
    h3_file = f"data/minutes/{STATE_FIPS[state_fips].lower()}_r{resolution}.parquet"
    # Handle the case where the file uses full state name instead of abbreviation
    if not os.path.exists(h3_file):
        # Try with full state name (massachusetts instead of ma)
        for slug, abbr in STATE_SLUG_TO_CODE.items():
            if abbr == STATE_FIPS[state_fips]:
                alt_h3_file = f"data/minutes/{slug}_r{resolution}.parquet"
                if os.path.exists(alt_h3_file):
                    h3_file = alt_h3_file
                    break
    
    if not os.path.exists(h3_file):
        logger.warning(f"No H3 data found at {h3_file}")
        return pd.DataFrame()
    
    logger.info(f"Loading existing H3 data from {h3_file}")
    h3_df = pd.read_parquet(h3_file)
    
    if 'h3' not in h3_df.columns:
        logger.error(f"No 'h3' column found in {h3_file}")
        return pd.DataFrame()
    
    # Get H3 cell centroids
    logger.info("Computing H3 cell centroids")
    centroids = []
    h3_cells = []
    
    for h3_cell in tqdm(h3_df['h3'], desc="Getting centroids"):
        try:
            lat, lon = h3.cell_to_latlng(h3_cell)  # Updated function name
            centroids.append(Point(lon, lat))
            h3_cells.append(h3_cell)
        except Exception as e:
            logger.warning(f"Failed to get centroid for H3 cell {h3_cell}: {e}")
            continue
    
    if not centroids:
        logger.error("No valid H3 centroids found")
        return pd.DataFrame()
    
    # Create GeoDataFrame of centroids
    centroids_gdf = gpd.GeoDataFrame(
        {'h3': h3_cells},
        geometry=centroids,
        crs='EPSG:4326'
    )
    
    # Ensure both GeoDataFrames have the same CRS
    if jurisdictions_gdf.crs != centroids_gdf.crs:
        jurisdictions_gdf = jurisdictions_gdf.to_crs(centroids_gdf.crs)
    
    # Spatial join: assign crime rates to hexes based on centroid location
    logger.info("Performing spatial join (centroid-in-polygon)")
    enriched = gpd.sjoin(
        centroids_gdf, 
        jurisdictions_gdf[['geometry', 'juris_name', 'crime_rate']], 
        how='left', 
        predicate='within'
    )
    
    # Handle hexes that don't fall within any jurisdiction
    enriched['crime_rate'] = enriched['crime_rate'].fillna(-1)
    
    # Create result DataFrame
    result = enriched[['h3', 'crime_rate']].copy()
    result['crime_rate'] = result['crime_rate'].astype(int)  # Store as integer like other metrics
    
    logger.info(f"Enriched {len(result)} H3 hexes with crime rates")
    matched_hexes = (result['crime_rate'] > 0).sum()
    no_data_hexes = (result['crime_rate'] == -1).sum()
    unmatched_hexes = len(result) - matched_hexes - no_data_hexes
    
    logger.info(f"  {matched_hexes} hexes matched to jurisdictions with crime data")
    logger.info(f"  {no_data_hexes} hexes in jurisdictions without crime data")
    logger.info(f"  {unmatched_hexes} hexes outside any jurisdiction")
    
    return result

def merge_crime_rates_into_h3_data():
    """Merge crime rates into existing H3 parquet files."""
    logger.info("Starting crime rate enrichment for all states and resolutions")
    
    # Process each state in the pipeline
    for state_slug in STATES:
        if state_slug not in STATE_SLUG_TO_CODE:
            logger.warning(f"Skipping unknown state slug: {state_slug}")
            continue
            
        state_abbr = STATE_SLUG_TO_CODE[state_slug]
        
        # Find FIPS code
        state_fips = None
        for fips, abbr in STATE_FIPS.items():
            if abbr == state_abbr:
                state_fips = fips
                break
        
        if not state_fips:
            logger.warning(f"No FIPS code found for state {state_abbr}")
            continue
        
        logger.info(f"Processing {state_abbr} (FIPS {state_fips})")
        
        # Process both resolutions
        for resolution in [H3_RES_LOW, H3_RES_HIGH]:
            try:
                # Enrich H3 data with crime rates
                crime_enriched = enrich_h3_with_crime_rates(state_fips, resolution)
                
                if len(crime_enriched) == 0:
                    logger.warning(f"No crime enrichment data for {state_abbr} r{resolution}")
                    continue
                
                # Load existing H3 data
                h3_file = f"data/minutes/{state_abbr.lower()}_r{resolution}.parquet"
                # Handle the case where the file uses full state name instead of abbreviation
                if not os.path.exists(h3_file):
                    # Try with full state name (massachusetts instead of ma)
                    for slug, abbr in STATE_SLUG_TO_CODE.items():
                        if abbr == state_abbr:
                            alt_h3_file = f"data/minutes/{slug}_r{resolution}.parquet"
                            if os.path.exists(alt_h3_file):
                                h3_file = alt_h3_file
                                break
                
                if not os.path.exists(h3_file):
                    logger.warning(f"H3 file not found: {h3_file}")
                    continue
                
                h3_df = pd.read_parquet(h3_file)
                
                # Merge crime rates into existing data
                logger.info(f"Merging crime rates into {h3_file}")
                merged = h3_df.merge(
                    crime_enriched[['h3', 'crime_rate']], 
                    on='h3', 
                    how='left'
                )
                
                # Fill missing crime rates with -1 (no data) instead of 0
                # -1 indicates areas without crime data, which should be excluded from filtering
                merged['crime_rate'] = merged['crime_rate'].fillna(-1).astype(int)
                
                # Save updated parquet file
                merged.to_parquet(h3_file, index=False)
                logger.info(f"✓ Updated {h3_file} with crime_rate column")
                
                # Log statistics
                total_hexes = len(merged)
                valid_crime_data = (merged['crime_rate'] > 0).sum()
                no_crime_data = (merged['crime_rate'] == -1).sum()
                logger.info(f"  {valid_crime_data}/{total_hexes} hexes have valid crime data")
                logger.info(f"  {no_crime_data}/{total_hexes} hexes have no crime data (excluded from filtering)")
                
            except Exception as e:
                logger.error(f"Failed to process {state_abbr} r{resolution}: {e}")
                continue
    
    logger.info("Crime rate enrichment complete")

def main():
    """Main entry point."""
    merge_crime_rates_into_h3_data()

if __name__ == "__main__":
    main() 