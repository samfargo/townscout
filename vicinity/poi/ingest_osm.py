"""
OSM POI Ingestion

Loads POI data from OpenStreetMap PBF files using Pyrosm.
"""
import os
import sys
from pathlib import Path
import geopandas as gpd

# Add data/taxonomy to path to import taxonomy
taxonomy_path = Path(__file__).parent.parent.parent / "data" / "taxonomy"
if str(taxonomy_path) not in sys.path:
    sys.path.insert(0, str(taxonomy_path))

from taxonomy import OSM_TAG_MAP
from vicinity.osm.pyrosm_utils import get_osm_data


def load_osm_pois(state: str, pbf_path: str = None) -> gpd.GeoDataFrame:
    """
    Load OSM POIs for a given state using Pyrosm.
    
    Args:
        state: State name (e.g., 'massachusetts')
        pbf_path: Optional path to PBF file. If not provided, uses data/osm/{state}.osm.pbf
        
    Returns:
        GeoDataFrame with OSM POIs
    """
    if pbf_path is None:
        pbf_path = f"data/osm/{state}.osm.pbf"
    
    if not os.path.exists(pbf_path):
        print(f"[error] OSM PBF not found at {pbf_path}. Run the download script first.")
        return gpd.GeoDataFrame(columns=['geometry'], geometry='geometry', crs="EPSG:4326")

    print(f"--- Loading OSM POIs for {state} from {pbf_path} ---")
    
    # Taxonomy-driven filter: only request tags we map into vicinity taxonomy
    wanted: dict[str, set[str]] = {}
    for (k, v), _ts in OSM_TAG_MAP.items():
        wanted.setdefault(k, set()).add(v)
    
    # Fallback to broad filter if mapping is empty
    custom_filter = {k: sorted(list(vals)) for k, vals in wanted.items()} if wanted else {
        "amenity": True, "shop": True, "leisure": True, "tourism": True
    }
    
    # Also request religion tag for place_of_worship classification
    if "amenity" in custom_filter and ("place_of_worship" in custom_filter["amenity"] or True in custom_filter.get("amenity", [])):
        wanted.setdefault("religion", set())
        custom_filter["religion"] = True

    # Keep common tag columns used by normalization.
    tag_cols = ["name", "brand", "operator", "amenity", "shop", "leisure", "tourism", "religion", "emergency", "healthcare"]
    
    gdf = get_osm_data(
        pbf_path,
        custom_filter=custom_filter,
        tags_as_columns=tag_cols,
        keep_nodes=True,
        keep_ways=True,
        keep_relations=False,
    )

    if gdf is None or gdf.empty:
        print(f"[warn] No POIs found in {pbf_path} with the current filter.")
        return gpd.GeoDataFrame(columns=['geometry'], geometry='geometry', crs="EPSG:4326")

    gdf = gdf.to_crs("EPSG:4326")
    print(f"[ok] Loaded {len(gdf)} POIs from OSM for {state}")
    return gdf
