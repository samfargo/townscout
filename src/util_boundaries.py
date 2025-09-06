"""
Utilities for downloading and processing TIGER/Line municipal boundaries.
Handles the complexity of merging MCDs and Places into a single jurisdiction layer.
"""

import os
import zipfile
import pandas as pd
import geopandas as gpd
from pathlib import Path
import requests
from tqdm import tqdm
from typing import Dict, List, Optional, Tuple
import logging

from src.config import (
    TIGER_PLACES_URL, TIGER_COUSUB_URL, MCD_STATES, STATE_FIPS
)

logger = logging.getLogger(__name__)

def download_tiger_boundaries(state_fips: str, data_dir: str = "data/boundaries") -> Tuple[str, str]:
    """
    Download TIGER/Line boundaries for a state.
    
    Args:
        state_fips: Two-digit state FIPS code (e.g., "25" for MA)
        data_dir: Base directory for boundary data
        
    Returns:
        Tuple of (places_path, cousub_path) to downloaded shapefiles
    """
    os.makedirs(data_dir, exist_ok=True)
    
    places_path = None
    cousub_path = None
    
    # Download Places
    places_url = f"{TIGER_PLACES_URL}/tl_2024_{state_fips}_place.zip"
    places_zip = os.path.join(data_dir, f"tl_2024_{state_fips}_place.zip")
    
    if not os.path.exists(places_zip):
        logger.info(f"Downloading Places for state {state_fips}")
        _download_file(places_url, places_zip)
    
    # Extract if not already extracted
    places_shp = os.path.join(data_dir, f"tl_2024_{state_fips}_place.shp")
    if not os.path.exists(places_shp):
        _extract_zip(places_zip, data_dir)
    
    if os.path.exists(places_shp):
        places_path = places_shp
    
    # Download County Subdivisions
    cousub_url = f"{TIGER_COUSUB_URL}/tl_2024_{state_fips}_cousub.zip"
    cousub_zip = os.path.join(data_dir, f"tl_2024_{state_fips}_cousub.zip")
    
    if not os.path.exists(cousub_zip):
        logger.info(f"Downloading County Subdivisions for state {state_fips}")
        _download_file(cousub_url, cousub_zip)
    
    # Extract if not already extracted
    cousub_shp = os.path.join(data_dir, f"tl_2024_{state_fips}_cousub.shp")
    if not os.path.exists(cousub_shp):
        _extract_zip(cousub_zip, data_dir)
        
    if os.path.exists(cousub_shp):
        cousub_path = cousub_shp
        
    return places_path, cousub_path

def build_jurisdiction_layer(state_fips: str, data_dir: str = "data/boundaries") -> gpd.GeoDataFrame:
    """
    Build jurisdiction layer for a state using MCD/Place strategy.
    
    Args:
        state_fips: Two-digit state FIPS code
        data_dir: Directory containing boundary data
        
    Returns:
        GeoDataFrame with jurisdiction polygons and metadata
    """
    places_path, cousub_path = download_tiger_boundaries(state_fips, data_dir)
    
    jurisdictions = []
    
    if state_fips in MCD_STATES:
        # Use County Subdivisions as primary jurisdictions
        if cousub_path and os.path.exists(cousub_path):
            logger.info(f"Loading County Subdivisions for state {state_fips}")
            cousub_gdf = gpd.read_file(cousub_path)
            
            # Create standardized fields
            cousub_gdf['juris_type'] = 'MCD'
            cousub_gdf['juris_name'] = cousub_gdf['NAME']
            cousub_gdf['juris_geoid'] = cousub_gdf['GEOID']  # STATEFP+COUNTYFP+COUSUBFP
            cousub_gdf['state_fips'] = state_fips
            cousub_gdf['state_abbr'] = STATE_FIPS[state_fips]
            
            # Keep essential columns
            keep_cols = ['juris_type', 'juris_name', 'juris_geoid', 'state_fips', 'state_abbr', 'geometry']
            jurisdictions.append(cousub_gdf[keep_cols])
    
    else:
        # Use Incorporated Places as primary jurisdictions
        if places_path and os.path.exists(places_path):
            logger.info(f"Loading Incorporated Places for state {state_fips}")
            places_gdf = gpd.read_file(places_path)
            
            # Exclude Census Designated Places (CDPs) - they're not legal municipalities
            places_gdf = places_gdf[places_gdf['CLASSFP'] != 'C5']
            
            # Create standardized fields
            places_gdf['juris_type'] = 'PLACE'
            places_gdf['juris_name'] = places_gdf['NAME']
            places_gdf['juris_geoid'] = places_gdf['GEOID']  # STATEFP+PLACEFP
            places_gdf['state_fips'] = state_fips
            places_gdf['state_abbr'] = STATE_FIPS[state_fips]
            
            # Keep essential columns
            keep_cols = ['juris_type', 'juris_name', 'juris_geoid', 'state_fips', 'state_abbr', 'geometry']
            jurisdictions.append(places_gdf[keep_cols])
    
    if not jurisdictions:
        logger.warning(f"No jurisdictions found for state {state_fips}")
        return gpd.GeoDataFrame()
    
    # Combine all jurisdiction types for this state
    result = gpd.GeoDataFrame(pd.concat(jurisdictions, ignore_index=True))
    
    # Ensure consistent CRS (EPSG:4326)
    if result.crs != 'EPSG:4326':
        result = result.to_crs('EPSG:4326')
    
    logger.info(f"Built {len(result)} jurisdictions for state {state_fips}")
    return result

def load_crime_rate_data(crime_file: str) -> pd.DataFrame:
    """
    Load and standardize crime rate data.
    
    Args:
        crime_file: Path to crime rate CSV file
        
    Returns:
        DataFrame with standardized crime rate data
    """
    logger.info(f"Loading crime rate data from {crime_file}")
    crime_df = pd.read_csv(crime_file)
    
    # Standardize column names
    if 'tow' in crime_df.columns:
        crime_df = crime_df.rename(columns={'tow': 'town_name'})
    if 'crime_rate_per_100k' in crime_df.columns:
        crime_df = crime_df.rename(columns={'crime_rate_per_100k': 'crime_rate'})
    
    # Handle missing values and invalid data
    crime_df['crime_rate'] = pd.to_numeric(crime_df['crime_rate'], errors='coerce')
    
    # Count original records for reporting
    original_count = len(crime_df)
    
    # Remove rows with missing town names
    crime_df = crime_df.dropna(subset=['town_name'])
    
    # Remove rows with missing, zero, or very low crime rates (likely incomplete data)
    # 0.0 values in the data represent missing/incomplete data, not actual zero crime
    crime_df = crime_df[
        crime_df['crime_rate'].notna() & 
        (crime_df['crime_rate'] > 0.0)
    ]
    
    # Clean town names for matching
    crime_df['town_clean'] = crime_df['town_name'].str.strip().str.title()
    
    # Report data quality
    valid_count = len(crime_df)
    missing_count = original_count - valid_count
    
    logger.info(f"Loaded {valid_count} valid crime rate records")
    if missing_count > 0:
        logger.info(f"Excluded {missing_count} records with missing/incomplete crime data (0.0 or null values)")
    
    return crime_df

def match_crime_rates_to_jurisdictions(jurisdictions_gdf: gpd.GeoDataFrame, 
                                     crime_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Match crime rates to jurisdiction polygons using fuzzy name matching.
    
    Args:
        jurisdictions_gdf: GeoDataFrame with jurisdiction boundaries
        crime_df: DataFrame with crime rate data
        
    Returns:
        GeoDataFrame with crime rates added to jurisdictions
    """
    logger.info("Matching crime rates to jurisdictions")
    
    # Clean jurisdiction names for matching
    jurisdictions_gdf['name_clean'] = jurisdictions_gdf['juris_name'].str.strip().str.title()
    
    # Create lookup dictionary from crime data
    crime_lookup = dict(zip(crime_df['town_clean'], crime_df['crime_rate']))
    
    # Direct name matching
    jurisdictions_gdf['crime_rate'] = jurisdictions_gdf['name_clean'].map(crime_lookup)
    
    # Report matching statistics
    matched = jurisdictions_gdf['crime_rate'].notna().sum()
    total = len(jurisdictions_gdf)
    logger.info(f"Matched {matched}/{total} jurisdictions to crime data ({matched/total*100:.1f}%)")
    
    # Use a special value (-1) for jurisdictions without crime data
    # This allows us to exclude them from filtering rather than treating them as having 0 crime
    # -1 will be treated as "no data" in the frontend filtering
    jurisdictions_gdf['crime_rate'] = jurisdictions_gdf['crime_rate'].fillna(-1)
    
    # Log which jurisdictions don't have crime data
    no_data_jurisdictions = jurisdictions_gdf[jurisdictions_gdf['crime_rate'] == -1]['juris_name'].tolist()
    if no_data_jurisdictions:
        logger.info(f"Jurisdictions without crime data: {', '.join(no_data_jurisdictions[:5])}" +
                   (f" and {len(no_data_jurisdictions)-5} others" if len(no_data_jurisdictions) > 5 else ""))
    
    return jurisdictions_gdf

def _download_file(url: str, filepath: str) -> None:
    """Download a file with progress bar."""
    import ssl
    import urllib3
    
    # Handle SSL certificate issues with government sites
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    response = requests.get(url, stream=True, verify=False)
    response.raise_for_status()
    
    total_size = int(response.headers.get('content-length', 0))
    
    with open(filepath, 'wb') as f, tqdm(
        desc=os.path.basename(filepath),
        total=total_size,
        unit='iB',
        unit_scale=True,
        unit_divisor=1024,
    ) as progress_bar:
        for chunk in response.iter_content(chunk_size=8192):
            size = f.write(chunk)
            progress_bar.update(size)

def _extract_zip(zip_path: str, extract_dir: str) -> None:
    """Extract a zip file."""
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir) 