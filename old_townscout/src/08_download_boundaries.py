#!/usr/bin/env python3
"""
Download TIGER/Line municipal boundaries for states in the pipeline.
Creates jurisdiction layers using the MCD/Place strategy.
"""

import logging
import os
from pathlib import Path

from config import STATES, STATE_SLUG_TO_CODE, STATE_FIPS
from util_boundaries import download_tiger_boundaries

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Download boundaries for Massachusetts only (for now)."""
    logger.info("Starting boundary download for Massachusetts")
    
    # Create boundaries directory
    boundaries_dir = "data/boundaries"
    os.makedirs(boundaries_dir, exist_ok=True)
    
    # For now, only download Massachusetts boundaries
    # This avoids downloading large amounts of data before we need it
    ma_slug = "massachusetts"
    
    if ma_slug not in STATE_SLUG_TO_CODE:
        logger.error(f"Massachusetts not found in STATE_SLUG_TO_CODE mapping")
        return
    
    state_abbr = STATE_SLUG_TO_CODE[ma_slug]
    
    # Find FIPS code for Massachusetts
    fips_code = None
    for fips, abbr in STATE_FIPS.items():
        if abbr == state_abbr:
            fips_code = fips
            break
    
    if not fips_code:
        logger.error(f"No FIPS code found for Massachusetts ({state_abbr})")
        return
    
    logger.info(f"Downloading boundaries for Massachusetts (FIPS {fips_code})")
    
    try:
        places_path, cousub_path = download_tiger_boundaries(fips_code, boundaries_dir)
        
        if places_path:
            logger.info(f"✓ Downloaded Places: {places_path}")
        if cousub_path:
            logger.info(f"✓ Downloaded County Subdivisions: {cousub_path}")
        
        logger.info("Massachusetts boundary download complete")
        logger.info("💡 To download boundaries for other states, add them to this script later")
            
    except Exception as e:
        logger.error(f"Failed to download boundaries for Massachusetts: {e}")
        return

if __name__ == "__main__":
    main() 