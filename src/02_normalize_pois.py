"""
Normalizes POIs from Overture and OSM sources into a single, canonical schema.

Pipeline:
1. Load Overture places for the state.
2. Load OSM POIs for the state.
3. Normalize Overture POIs to the canonical schema.
4. Normalize OSM POIs to the canonical schema.
5. Conflate and deduplicate POIs from both sources.
6. Save the final canonical POI set to a parquet file.
"""
import os
import uuid
import pandas as pd
import geopandas as gpd
from pyrosm import OSM
from config import STATES
from taxonomy import BRAND_REGISTRY, OVERTURE_CATEGORY_MAP, OSM_TAG_MAP

# Invert brand registry for quick lookup of aliases
_brand_alias_to_id = {}
for brand_id, (name, aliases) in BRAND_REGISTRY.items():
    _brand_alias_to_id[name.lower()] = brand_id
    for alias in aliases:
        _brand_alias_to_id[alias.lower()] = brand_id

# --- Canonical Data Schema (from OVERHAUL.md) ---
# This defines the target schema for all POIs after normalization.
CANONICAL_POI_SCHEMA = {
    "poi_id": "str",
    "name": "str",
    "brand_id": "str",
    "brand_name": "str",
    "class": "str",
    "category": "str",
    "subcat": "str",
    "lon": "float32",
    "lat": "float32",
    "geometry": "geometry",
    "source": "str",
    "ext_id": "str",
    "h3_r9": "str",
    "provenance": "object", # list of strings
}


def load_overture_pois(state: str) -> gpd.GeoDataFrame:
    """Loads Overture POIs for a given state."""
    # For now, we only handle Massachusetts as per the download script.
    # This will need to be generalized.
    if state != "massachusetts":
        print(f"[warn] Overture loading only implemented for 'massachusetts', not '{state}'. Skipping.")
        return gpd.GeoDataFrame(columns=['geometry'], geometry='geometry', crs="EPSG:4326")

    # Corrected path to match the output of the download script
    path = "data/overture/ma_places.parquet"
    if not os.path.exists(path):
        print(f"[error] Overture data not found at {path}. Run the download script first.")
        return gpd.GeoDataFrame(columns=['geometry'], geometry='geometry', crs="EPSG:4326")
    
    print(f"--- Loading Overture POIs for {state} from {path} ---")
    # Read into a normal pandas DataFrame first
    df = pd.read_parquet(path)
    
    # Manually convert the WKB geometry column to a GeoSeries
    geometries = gpd.GeoSeries.from_wkb(df['geometry'])
    gdf = gpd.GeoDataFrame(df, geometry=geometries)

    gdf = gdf.set_crs("EPSG:4326", allow_override=True)
    print(f"[ok] Loaded {len(gdf)} POIs from Overture for {state}")
    return gdf


def load_osm_pois(state: str) -> gpd.GeoDataFrame:
    """Loads OSM POIs for a given state using Pyrosm."""
    pbf_path = f"data/osm/{state}.osm.pbf"
    if not os.path.exists(pbf_path):
        print(f"[error] OSM PBF not found at {pbf_path}. Run the download script first.")
        return gpd.GeoDataFrame(columns=['geometry'], geometry='geometry', crs="EPSG:4326")

    print(f"--- Loading OSM POIs for {state} from {pbf_path} ---")
    osm = OSM(pbf_path)
    # Taxonomy-driven filter: only request tags we map into TownScout taxonomy
    wanted: dict[str, set[str]] = {}
    for (k, v), _ts in OSM_TAG_MAP.items():
        wanted.setdefault(k, set()).add(v)
    # Fallback to broad filter if mapping is empty
    custom_filter = {k: sorted(list(vals)) for k, vals in wanted.items()} if wanted else {"amenity": True, "shop": True, "leisure": True, "tourism": True}

    # Avoid Shapely relation assembly issues by skipping relations on first pass.
    # Keep common tag columns used by normalization.
    tag_cols = ["name", "brand", "operator", "amenity", "shop", "leisure", "tourism"]
    try:
        gdf = osm.get_data_by_custom_criteria(
            custom_filter=custom_filter,
            tags_as_columns=tag_cols,
            keep_nodes=True,
            keep_ways=True,
            keep_relations=False,  # skip relations to avoid multipolygon assembly
        )
    except Exception as e:
        print(f"[warn] get_data_by_custom_criteria failed (nodes+ways). Falling back to nodes-only. Error: {e}")
        gdf = osm.get_data_by_custom_criteria(
            custom_filter=custom_filter,
            tags_as_columns=tag_cols,
            keep_nodes=True,
            keep_ways=False,
            keep_relations=False,
        )
    
    if gdf is None or gdf.empty:
        print(f"[warn] No POIs found in {pbf_path} with the current filter.")
        return gpd.GeoDataFrame(columns=['geometry'], geometry='geometry', crs="EPSG:4326")

    gdf = gdf.to_crs("EPSG:4326")
    print(f"[ok] Loaded {len(gdf)} POIs from OSM for {state}")
    return gdf


def normalize_overture_pois(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Normalizes Overture POIs to the canonical schema."""
    print("--- Normalizing Overture POIs ---")
    if gdf.empty:
        return gpd.GeoDataFrame(columns=list(CANONICAL_POI_SCHEMA.keys()), geometry='geometry', crs="EPSG:4326")
    
    normalized_rows = []
    for _, row in gdf.iterrows():
        # Category mapping
        primary_cat = row['categories']['primary'] if row['categories'] and 'primary' in row['categories'] else None
        ts_class, ts_cat, ts_subcat = OVERTURE_CATEGORY_MAP.get(primary_cat, (None, None, None))

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
            "lon": row.geometry.x,
            "lat": row.geometry.y,
            "geometry": row.geometry,
            "source": "overture",
            "ext_id": str(source_id),
            "provenance": ["overture"],
        })

    if not normalized_rows:
        return gpd.GeoDataFrame(columns=list(CANONICAL_POI_SCHEMA.keys()), geometry='geometry', crs="EPSG:4326")

    out_gdf = gpd.GeoDataFrame(normalized_rows, crs="EPSG:4326")
    print(f"[ok] Normalized {len(out_gdf)} POIs from Overture.")
    return out_gdf


def normalize_osm_pois(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Normalizes OSM POIs to the canonical schema."""
    print("--- Normalizing OSM POIs ---")
    if gdf.empty:
        return gpd.GeoDataFrame(columns=list(CANONICAL_POI_SCHEMA.keys()), geometry='geometry', crs="EPSG:4326")

    normalized_rows = []
    for _, row in gdf.iterrows():
        # Category mapping
        ts_class, ts_cat, ts_subcat = None, None, None
        for tag_key in ['amenity', 'shop', 'leisure', 'tourism']:
            tag_value = row.get(tag_key)
            if tag_value and (tag_key, tag_value) in OSM_TAG_MAP:
                ts_class, ts_cat, ts_subcat = OSM_TAG_MAP[(tag_key, tag_value)]
                break
        
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
            "lon": point.x,
            "lat": point.y,
            "geometry": point, # Store the representative point
            "source": "osm",
            "ext_id": str(source_id),
            "provenance": ["osm"],
        })

    if not normalized_rows:
        return gpd.GeoDataFrame(columns=list(CANONICAL_POI_SCHEMA.keys()), geometry='geometry', crs="EPSG:4326")

    out_gdf = gpd.GeoDataFrame(normalized_rows, crs="EPSG:4326")
    print(f"[ok] Normalized {len(out_gdf)} POIs from OSM.")
    return out_gdf


def conflate_pois(overture_gdf: gpd.GeoDataFrame, osm_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Conflates and deduplicates POIs from Overture and OSM."""
    print("--- Conflating POIs from all sources ---")
    
    # This will fail until normalization is implemented, as schemas won't match.
    # For now, we will return an empty frame.
    combined_gdf = pd.concat([overture_gdf, osm_gdf], ignore_index=True)
    print(f"[ok] Combined POIs: {len(combined_gdf)} total")
    
    # TODO: Implement actual deduplication logic here.
    # For now, just return the combined set.
    
    return combined_gdf


def main():
    """Main function to run the POI normalization pipeline."""
    os.makedirs("data/poi", exist_ok=True)

    for state in STATES:
        # 1. Load data from sources
        overture_pois = load_overture_pois(state)
        osm_pois = load_osm_pois(state)

        # 2. Normalize each source to the canonical schema
        overture_normalized = normalize_overture_pois(overture_pois)
        osm_normalized = normalize_osm_pois(osm_pois)
        
        # 3. Conflate the normalized datasets
        canonical_pois = conflate_pois(overture_normalized, osm_normalized)
        
        # 4. Save the result
        output_path = f"data/poi/{state}_canonical.parquet"
        if not canonical_pois.empty:
            # Remove CRS metadata before saving to avoid read errors with pyproj
            canonical_pois.crs = None
            canonical_pois.to_parquet(output_path)
            print(f"[ok] Saved {len(canonical_pois)} canonical POIs to {output_path}")
        else:
            print("[warn] No canonical POIs were generated.")

if __name__ == "__main__":
    main()
