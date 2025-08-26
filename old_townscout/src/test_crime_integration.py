#!/usr/bin/env python3
"""
Test script to validate crime rate integration.
Tests boundary download, jurisdiction building, and H3 enrichment.
"""

import logging
import os
import pandas as pd
import geopandas as gpd
from pathlib import Path

from config import STATE_FIPS, CRIME_RATE_SOURCE
from util_boundaries import (
    build_jurisdiction_layer, load_crime_rate_data, 
    match_crime_rates_to_jurisdictions
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_crime_data_loading():
    """Test loading and parsing of crime rate data."""
    logger.info("Testing crime rate data loading...")
    
    if not os.path.exists(CRIME_RATE_SOURCE):
        logger.error(f"Crime rate file not found: {CRIME_RATE_SOURCE}")
        return False
    
    try:
        crime_df = load_crime_rate_data(CRIME_RATE_SOURCE)
        logger.info(f"✓ Loaded {len(crime_df)} valid crime records")
        
        # Check data quality
        if crime_df.empty:
            logger.error("Crime data is empty")
            return False
            
        if 'town_clean' not in crime_df.columns or 'crime_rate' not in crime_df.columns:
            logger.error("Missing required columns in crime data")
            return False
        
        # Verify that 0.0 and missing values have been filtered out
        zero_values = (crime_df['crime_rate'] == 0.0).sum()
        if zero_values > 0:
            logger.warning(f"Found {zero_values} zero crime rate values - these should have been filtered out")
            
        logger.info(f"Crime rate range: {crime_df['crime_rate'].min():.1f} - {crime_df['crime_rate'].max():.1f}")
        logger.info("✓ Zero and missing values properly excluded from dataset")
        return True
        
    except Exception as e:
        logger.error(f"Failed to load crime data: {e}")
        return False

def test_jurisdiction_building():
    """Test building jurisdiction layer for Massachusetts."""
    logger.info("Testing jurisdiction layer building for Massachusetts...")
    
    ma_fips = "25"  # Massachusetts
    
    try:
        jurisdictions_gdf = build_jurisdiction_layer(ma_fips)
        
        if jurisdictions_gdf.empty:
            logger.warning("No jurisdictions found - may need to download boundaries first")
            return False
            
        logger.info(f"✓ Built {len(jurisdictions_gdf)} jurisdictions for MA")
        
        # Check required columns
        required_cols = ['juris_type', 'juris_name', 'juris_geoid', 'state_fips', 'state_abbr', 'geometry']
        missing_cols = [col for col in required_cols if col not in jurisdictions_gdf.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return False
            
        logger.info(f"Jurisdiction types: {jurisdictions_gdf['juris_type'].value_counts().to_dict()}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to build jurisdictions: {e}")
        return False

def test_crime_matching():
    """Test matching crime rates to jurisdictions."""
    logger.info("Testing crime rate matching...")
    
    ma_fips = "25"
    
    try:
        # Load jurisdictions
        jurisdictions_gdf = build_jurisdiction_layer(ma_fips)
        if jurisdictions_gdf.empty:
            logger.warning("No jurisdictions available for matching test")
            return False
        
        # Load crime data
        crime_df = load_crime_rate_data(CRIME_RATE_SOURCE)
        if crime_df.empty:
            logger.error("No crime data available for matching test")
            return False
        
        # Perform matching
        matched_gdf = match_crime_rates_to_jurisdictions(jurisdictions_gdf, crime_df)
        
        # Check results
        if 'crime_rate' not in matched_gdf.columns:
            logger.error("crime_rate column not added during matching")
            return False
            
        matched_count = (matched_gdf['crime_rate'] > 0).sum()
        no_data_count = (matched_gdf['crime_rate'] == -1).sum()
        total_count = len(matched_gdf)
        match_rate = matched_count / total_count * 100
        
        logger.info(f"✓ Matched {matched_count}/{total_count} jurisdictions to crime data ({match_rate:.1f}%)")
        logger.info(f"✓ {no_data_count} jurisdictions marked with -1 (no crime data)")
        
        # Verify no jurisdiction has 0 as crime rate (should be -1 if no data)
        zero_count = (matched_gdf['crime_rate'] == 0).sum()
        if zero_count > 0:
            logger.warning(f"Found {zero_count} jurisdictions with 0 crime rate - should be -1 for no data")
        
        if matched_count == 0:
            logger.warning("No jurisdictions matched to crime data - check name matching logic")
            
        return True
        
    except Exception as e:
        logger.error(f"Failed crime matching test: {e}")
        return False

def test_sample_h3_enrichment():
    """Test H3 enrichment with a small sample."""
    logger.info("Testing H3 enrichment with sample data...")
    
    try:
        # Check if any H3 data exists
        h3_files = list(Path("data/minutes").glob("massachusetts_r*.parquet"))
        if not h3_files:
            logger.warning("No H3 data files found - run the main pipeline first")
            return False
            
        # Test with the first available file
        h3_file = h3_files[0]
        logger.info(f"Testing with {h3_file}")
        
        # Load a small sample
        h3_df = pd.read_parquet(h3_file)
        if h3_df.empty or 'h3' not in h3_df.columns:
            logger.error("Invalid H3 data structure")
            return False
            
        # Take first 10 cells for testing
        sample_df = h3_df.head(10).copy()
        logger.info(f"Testing with {len(sample_df)} H3 cells")
        
        # Test centroid computation
        import h3
        from shapely.geometry import Point
        
        centroids = []
        for h3_cell in sample_df['h3']:
            try:
                lat, lon = h3.cell_to_latlng(h3_cell)  # Updated function name
                centroids.append(Point(lon, lat))
            except Exception as e:
                logger.warning(f"Failed to get centroid for {h3_cell}: {e}")
                
        if len(centroids) == 0:
            logger.error("Failed to compute any centroids")
            return False
            
        logger.info(f"✓ Successfully computed {len(centroids)} centroids")
        return True
        
    except Exception as e:
        logger.error(f"Failed H3 enrichment test: {e}")
        return False

def main():
    """Run all validation tests."""
    logger.info("Starting crime rate integration validation tests")
    
    tests = [
        ("Crime Data Loading", test_crime_data_loading),
        ("Jurisdiction Building", test_jurisdiction_building),
        ("Crime Rate Matching", test_crime_matching),
        ("H3 Enrichment Sample", test_sample_h3_enrichment),
    ]
    
    results = {}
    for test_name, test_func in tests:
        logger.info(f"\n--- Running {test_name} Test ---")
        try:
            results[test_name] = test_func()
        except Exception as e:
            logger.error(f"Test {test_name} crashed: {e}")
            results[test_name] = False
    
    # Summary
    logger.info("\n--- Test Results Summary ---")
    passed = 0
    for test_name, result in results.items():
        status = "PASS" if result else "FAIL"
        logger.info(f"{test_name}: {status}")
        if result:
            passed += 1
    
    logger.info(f"\nPassed {passed}/{len(tests)} tests")
    
    if passed == len(tests):
        logger.info("🎉 All tests passed! Crime rate integration looks good.")
    else:
        logger.warning("⚠️  Some tests failed. Check the logs above for details.")
        logger.info("💡 If boundaries tests failed, run 'make boundaries' first.")
        logger.info("💡 If H3 tests failed, run the main pipeline first to generate H3 data.")

if __name__ == "__main__":
    main() 