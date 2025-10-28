"""
Normalizes POIs from Overture and OSM sources into a single, canonical schema.

Pipeline:
1. Load Overture places for the state.
2. Load OSM POIs for the state.
3. Normalize Overture POIs to the canonical schema.
4. Normalize OSM POIs to the canonical schema.
5. Conflate and deduplicate POIs from both sources.
6. Save the final canonical POI set to a parquet file.

This is now a thin wrapper around the townscout.poi module.
"""
import os
import sys
from pathlib import Path
import pandas as pd
import geopandas as gpd

# Add project root to path to import townscout
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# Add src to path to import config
src_path = Path(__file__).parent
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from config import STATES

# Import from the new townscout.poi module
from townscout.poi import (
    CANONICAL_POI_SCHEMA,
    load_osm_pois,
    load_overture_pois,
    normalize_overture_pois,
    normalize_osm_pois,
    conflate_pois,
)

# Import domain-specific POI handlers
from townscout.domains_poi.airports import load_airports_csv
from townscout.domains_poi.beaches import build_beach_pois_for_state
from townscout.domains_poi.trauma import load_level1_trauma_pois


def main():
    """Main function to run the POI normalization pipeline."""
    os.makedirs("data/poi", exist_ok=True)

    for state in STATES:
        # 1. Load data from sources
        overture_pois = load_overture_pois(state, overture_path=None)
        osm_pois = load_osm_pois(state, pbf_path=None)
        trauma_pois = load_level1_trauma_pois(state, trauma_parquet=None)

        # 2. Normalize each source to the canonical schema
        overture_normalized = normalize_overture_pois(overture_pois)
        osm_normalized = normalize_osm_pois(osm_pois)

        # 2b. Build beach POIs (ocean/lake/river/other) from OSM
        beach_pois = build_beach_pois_for_state(state)
        if not beach_pois.empty:
            print(f"[ok] Built {len(beach_pois)} beach POIs for {state}")

        # 3. Conflate the normalized datasets (+ beaches + airports)
        canonical_pois = conflate_pois(
            ("overture", overture_normalized),
            ("osm", osm_normalized),
            ("acs_trauma", trauma_pois),
        )
        parts = [canonical_pois]
        if not beach_pois.empty:
            parts.append(beach_pois)
        airports_normalized = load_airports_csv()
        if not airports_normalized.empty:
            parts.append(airports_normalized)
        
        # Concatenate all parts
        if len(parts) > 1:
            canonical_pois = pd.concat(parts, ignore_index=True)
            # Ensure it's a GeoDataFrame after concatenation
            canonical_pois = gpd.GeoDataFrame(canonical_pois, geometry="geometry", crs="EPSG:4326")
        
        # 4. Save the result
        output_path = f"data/poi/{state}_canonical.parquet"
        if not canonical_pois.empty:
            # Drop provenance column before saving - PyArrow can't handle list columns
            save_df = canonical_pois.drop(columns=["provenance"], errors="ignore")
            # Remove CRS metadata before saving to avoid read errors with pyproj
            save_df.crs = None
            save_df.to_parquet(output_path)
            print(f"[ok] Saved {len(canonical_pois)} canonical POIs to {output_path}")
        else:
            print("[warn] No canonical POIs were generated.")

if __name__ == "__main__":
    main()
