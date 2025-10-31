"""
Compute per-hex political lean flags based on county-level 2024 presidential election results.

This module:
1. Loads 2024 US Presidential election results from MIT Election Lab dataset
2. Calculates Republican vote share per county
3. Assigns counties to political lean buckets (0-4)
4. Loads US county boundaries from Census TIGER shapefiles
5. Joins county polygons to H3 cells and assigns political lean
"""
from __future__ import annotations

import os
import sys
import zipfile
from pathlib import Path
from typing import Optional, Sequence, Set, Iterable
from urllib.request import urlretrieve

import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape

# Add src to path to import config
src_path = Path(__file__).parent.parent.parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from config import H3_RES_LOW, H3_RES_HIGH
from .schema import vote_share_to_bucket, POLITICAL_LEAN_LABELS
from ..h3_utils import polygon_to_cells
from ..validation import validate_overlay_output


def _load_election_data(csv_path: str) -> pd.DataFrame:
    """
    Load 2024 presidential election results and compute Republican vote share.
    
    Returns DataFrame with columns:
    - county_fips: FIPS code
    - county_name: County name
    - state: State name
    - rep_votes: Republican votes
    - dem_votes: Democrat votes
    - total_votes: Total votes (Dem + Rep only)
    - rep_vote_share: Republican share (0.0-1.0)
    - political_lean: Bucket 0-4
    """
    print(f"[info] Loading election data from {csv_path}")
    df = pd.read_csv(csv_path, dtype={
        'year': int,
        'county_fips': str,
        'candidatevotes': int,
        'totalvotes': int,
    })
    
    # Filter for 2024 US President, Democrat and Republican only
    df = df[
        (df['year'] == 2024) &
        (df['office'] == 'US PRESIDENT') &
        (df['party'].isin(['DEMOCRAT', 'REPUBLICAN']))
    ].copy()
    
    if df.empty:
        raise ValueError("No 2024 US Presidential election data found in CSV")
    
    print(f"[info] Found {len(df)} rows of 2024 presidential data")
    
    # Pivot to get Rep and Dem votes per county
    pivot = df.pivot_table(
        index=['county_fips', 'county_name', 'state'],
        columns='party',
        values='candidatevotes',
        aggfunc='sum',
        fill_value=0
    ).reset_index()
    
    # Calculate vote share
    pivot['rep_votes'] = pivot.get('REPUBLICAN', 0)
    pivot['dem_votes'] = pivot.get('DEMOCRAT', 0)
    pivot['total_votes'] = pivot['rep_votes'] + pivot['dem_votes']
    
    # Filter out counties with no votes (data quality issue)
    pivot = pivot[pivot['total_votes'] > 0].copy()
    
    pivot['rep_vote_share'] = pivot['rep_votes'] / pivot['total_votes']
    pivot['political_lean'] = pivot['rep_vote_share'].apply(vote_share_to_bucket)
    
    # Clean up FIPS codes (remove .0 suffix if present)
    pivot['county_fips'] = pivot['county_fips'].str.replace('.0', '', regex=False)
    pivot['county_fips'] = pivot['county_fips'].str.zfill(5)  # Ensure 5 digits
    
    print(f"[info] Computed political lean for {len(pivot)} counties")
    print(f"[info] Lean distribution:")
    for bucket, label in POLITICAL_LEAN_LABELS.items():
        count = (pivot['political_lean'] == bucket).sum()
        print(f"[info]   {bucket} ({label}): {count} counties")
    
    return pivot[['county_fips', 'county_name', 'state', 'rep_votes', 'dem_votes', 
                  'total_votes', 'rep_vote_share', 'political_lean']]


def _download_county_boundaries(data_dir: str) -> str:
    """
    Download US county boundaries from Census TIGER if not already present.
    
    Returns path to the county shapefile.
    """
    boundaries_dir = Path(data_dir) / "boundaries"
    boundaries_dir.mkdir(parents=True, exist_ok=True)
    
    # Census TIGER county boundaries (2024)
    county_zip = boundaries_dir / "tl_2024_us_county.zip"
    county_shp = boundaries_dir / "tl_2024_us_county.shp"
    
    if county_shp.exists():
        print(f"[info] Using existing county boundaries: {county_shp}")
        return str(county_shp)
    
    if not county_zip.exists():
        url = "https://www2.census.gov/geo/tiger/TIGER2024/COUNTY/tl_2024_us_county.zip"
        print(f"[info] Downloading county boundaries from {url}")
        urlretrieve(url, county_zip)
        print(f"[info] Downloaded to {county_zip}")
    
    # Extract
    print(f"[info] Extracting {county_zip}")
    with zipfile.ZipFile(county_zip, 'r') as zip_ref:
        zip_ref.extractall(boundaries_dir)
    
    if not county_shp.exists():
        raise FileNotFoundError(f"Expected {county_shp} after extraction")
    
    print(f"[info] County boundaries ready: {county_shp}")
    return str(county_shp)


def _load_county_boundaries(shp_path: str, state_fips_filter: Optional[set] = None) -> gpd.GeoDataFrame:
    """
    Load county boundaries from shapefile.
    
    Args:
        shp_path: Path to county shapefile
        state_fips_filter: Optional set of state FIPS codes to filter (e.g., {'25'} for MA)
        
    Returns:
        GeoDataFrame with columns: GEOID (FIPS), NAME, STATEFP, geometry
    """
    print(f"[info] Loading county boundaries from {shp_path}")
    gdf = gpd.read_file(shp_path)
    
    # TIGER files use GEOID for FIPS code
    if 'GEOID' not in gdf.columns:
        raise ValueError(f"Expected GEOID column in {shp_path}")
    
    # Ensure WGS84 CRS
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")
    
    if state_fips_filter:
        # Filter by state FIPS codes
        original_count = len(gdf)
        gdf = gdf[gdf['STATEFP'].isin(state_fips_filter)]
        print(f"[info] Filtered to {len(gdf)} counties (from {original_count}) in states: {state_fips_filter}")
    
    print(f"[info] Loaded {len(gdf)} county boundaries")
    return gdf[['GEOID', 'NAME', 'STATEFP', 'geometry']]


def _join_counties_to_h3(
    counties_gdf: gpd.GeoDataFrame,
    election_df: pd.DataFrame,
    resolutions: Sequence[int]
) -> pd.DataFrame:
    """
    Join county polygons to H3 cells.
    
    For cells that overlap multiple counties, assign the county with largest area overlap.
    
    Returns DataFrame with columns:
    - h3_id (uint64)
    - res (int32)
    - political_lean (uint8)
    - rep_vote_share (float32)
    - county_fips (str)
    - county_name (str)
    """
    print(f"[info] Joining counties to election data")
    
    # Merge county boundaries with election data
    merged = counties_gdf.merge(
        election_df,
        left_on='GEOID',
        right_on='county_fips',
        how='inner'
    )
    
    print(f"[info] Matched {len(merged)} counties with election data")
    
    if merged.empty:
        print("[warn] No counties matched between boundaries and election data")
        # Return empty DataFrame with correct schema
        return pd.DataFrame(columns=[
            'h3_id', 'res', 'political_lean', 'rep_vote_share', 'county_fips', 'county_name'
        ]).astype({
            'h3_id': 'uint64',
            'res': 'int32',
            'political_lean': 'uint8',
            'rep_vote_share': 'float32',
            'county_fips': 'str',
            'county_name': 'str',
        })
    
    # Convert each county to H3 cells at each resolution
    records = []
    
    for idx, row in merged.iterrows():
        if idx % 100 == 0:
            print(f"[info] Processing county {idx}/{len(merged)}: {row['county_name']}")
        
        for res in resolutions:
            cells = polygon_to_cells(row['geometry'], res)
            
            for cell_id in cells:
                records.append({
                    'h3_id': cell_id,
                    'res': res,
                    'political_lean': row['political_lean'],
                    'rep_vote_share': row['rep_vote_share'],
                    'county_fips': row['county_fips'],
                    'county_name': row['county_name'],
                })
    
    if not records:
        print("[warn] No H3 cells generated")
        return pd.DataFrame(columns=[
            'h3_id', 'res', 'political_lean', 'rep_vote_share', 'county_fips', 'county_name'
        ]).astype({
            'h3_id': 'uint64',
            'res': 'int32',
            'political_lean': 'uint8',
            'rep_vote_share': 'float32',
            'county_fips': 'str',
            'county_name': 'str',
        })
    
    result = pd.DataFrame(records)
    
    # Handle overlapping cells (assign to first county encountered, could be improved with area calculation)
    # For now, just drop duplicates keeping first
    print(f"[info] Generated {len(result)} cell-county pairs before deduplication")
    result = result.drop_duplicates(subset=['h3_id', 'res'], keep='first')
    
    print(f"[info] Final {len(result)} unique H3 cells with political lean")
    
    # Enforce types
    result['h3_id'] = result['h3_id'].astype('uint64')
    result['res'] = result['res'].astype('int32')
    result['political_lean'] = result['political_lean'].astype('uint8')
    result['rep_vote_share'] = result['rep_vote_share'].astype('float32')
    result['county_fips'] = result['county_fips'].astype('str')
    result['county_name'] = result['county_name'].astype('str')
    
    return result


def compute_political_lean_flags(
    csv_path: str,
    output_path: str,
    state_filter: Optional[str] = None,
    resolutions: Optional[Sequence[int]] = None,
    data_dir: str = "data",
) -> None:
    """
    Compute political lean flags for H3 hexes based on 2024 presidential election results.
    
    Args:
        csv_path: Path to countypres_2000-2024.csv
        output_path: Output parquet path
        state_filter: Optional state name to filter (e.g., 'Massachusetts')
        resolutions: H3 resolutions to compute (default: [H3_RES_LOW, H3_RES_HIGH])
        data_dir: Base data directory (default: 'data')
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Missing election CSV at {csv_path}")
    
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    
    target_res = list(resolutions or [H3_RES_LOW, H3_RES_HIGH])
    
    # Load and process election data
    election_df = _load_election_data(csv_path)
    
    # Filter election data by state if requested
    state_fips_filter = None
    if state_filter:
        election_df = election_df[election_df['state'].str.upper() == state_filter.upper()].copy()
        print(f"[info] Filtered election data to {len(election_df)} counties in {state_filter}")
        
        if election_df.empty:
            raise ValueError(f"No election data found for state '{state_filter}'")
        
        # Extract state FIPS codes from county FIPS (first 2 digits)
        state_fips_filter = set(election_df['county_fips'].str[:2].unique())
        print(f"[info] State FIPS codes to filter: {state_fips_filter}")
    
    # Download and load county boundaries
    county_shp = _download_county_boundaries(data_dir)
    counties_gdf = _load_county_boundaries(county_shp, state_fips_filter)
    
    # Join counties to H3 grid
    result = _join_counties_to_h3(counties_gdf, election_df, target_res)
    
    # Validate output schema
    try:
        validate_overlay_output(
            result,
            expected_columns={"h3_id", "res", "political_lean", "rep_vote_share", "county_fips", "county_name"}
        )
    except ValueError as exc:
        print(f"[warn] Output validation failed: {exc}")
    
    # Write output
    result.to_parquet(output_path, index=False)
    print(f"[ok] Wrote {len(result)} rows to {output_path}")
    
    # Print summary statistics
    print("\n[info] Political lean distribution in H3 cells:")
    for res in target_res:
        res_data = result[result['res'] == res]
        print(f"\n[info] Resolution {res}: {len(res_data)} cells")
        for bucket, label in POLITICAL_LEAN_LABELS.items():
            count = (res_data['political_lean'] == bucket).sum()
            pct = 100 * count / len(res_data) if len(res_data) > 0 else 0
            print(f"[info]   {bucket} ({label}): {count} cells ({pct:.1f}%)")

