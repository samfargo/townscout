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
from config import STATES, STATE_BOUNDING_BOXES
from taxonomy import BRAND_REGISTRY, OVERTURE_CATEGORY_MAP, OSM_TAG_MAP
import h3
from shapely.geometry import Point
from osm_beaches import build_beach_pois_for_state

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
    "trauma_level": "str",
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
    
    # Also request religion tag for place_of_worship classification
    if "amenity" in custom_filter and ("place_of_worship" in custom_filter["amenity"] or True in custom_filter.get("amenity", [])):
        wanted.setdefault("religion", set())
        custom_filter["religion"] = True

    # Avoid Shapely relation assembly issues by skipping relations on first pass.
    # Keep common tag columns used by normalization.
    tag_cols = ["name", "brand", "operator", "amenity", "shop", "leisure", "tourism", "religion"]
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


def load_level1_trauma_pois(state: str) -> gpd.GeoDataFrame:
    """
    Load ACS Level 1 trauma centers and filter them to the requested state.
    The ACS export is nationwide; we clip to a coarse bounding box per state.
    """
    path = os.path.join("out", "level1_trauma", "acs_trauma.parquet")
    if not os.path.exists(path):
        print(f"[warn] ACS trauma parquet not found at {path}; skipping.")
        return gpd.GeoDataFrame(columns=list(CANONICAL_POI_SCHEMA.keys()), geometry="geometry", crs="EPSG:4326")

    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        print(f"[warn] Failed to read ACS trauma parquet: {exc}")
        return gpd.GeoDataFrame(columns=list(CANONICAL_POI_SCHEMA.keys()), geometry="geometry", crs="EPSG:4326")

    if df.empty:
        print("[info] ACS trauma parquet is empty.")
        return gpd.GeoDataFrame(columns=list(CANONICAL_POI_SCHEMA.keys()), geometry="geometry", crs="EPSG:4326")

    bbox = STATE_BOUNDING_BOXES.get(state, {})
    west = bbox.get("west", -180.0)
    east = bbox.get("east", 180.0)
    south = bbox.get("south", -90.0)
    north = bbox.get("north", 90.0)
    before = len(df)
    df = df[(df["lon"] >= west) & (df["lon"] <= east) & (df["lat"] >= south) & (df["lat"] <= north)].copy()
    after = len(df)
    print(f"[info] ACS trauma centers clipped to {state}: {after} / {before}")

    if df.empty:
        return gpd.GeoDataFrame(columns=list(CANONICAL_POI_SCHEMA.keys()), geometry="geometry", crs="EPSG:4326")

    try:
        geometry = gpd.points_from_xy(df["lon"], df["lat"])
    except Exception as exc:
        print(f"[warn] Failed to create geometry for ACS trauma centers: {exc}")
        return gpd.GeoDataFrame(columns=list(CANONICAL_POI_SCHEMA.keys()), geometry="geometry", crs="EPSG:4326")

    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

    # Ensure all canonical columns exist
    for col in CANONICAL_POI_SCHEMA.keys():
        if col not in gdf.columns:
            gdf[col] = None

    # Normalize provenance to list-of-str
    if "provenance" in gdf.columns:
        gdf["provenance"] = gdf["provenance"].apply(
            lambda v: list(v) if isinstance(v, (list, tuple)) else ([v] if pd.notna(v) else [])
        )

    gdf = gdf[list(CANONICAL_POI_SCHEMA.keys())]
    trauma_counts = gdf["subcat"].value_counts().to_dict() if "subcat" in gdf.columns else {}
    if trauma_counts:
        summary = ", ".join(f"{k}={v}" for k, v in sorted(trauma_counts.items()))
        print(f"[ok] Loaded {len(gdf)} ACS Level 1 trauma centers for {state}: {summary}")
    else:
        print(f"[ok] Loaded {len(gdf)} ACS Level 1 trauma centers for {state}")
    return gdf


def normalize_overture_pois(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Normalizes Overture POIs to the canonical schema."""
    print("--- Normalizing Overture POIs ---")
    if gdf.empty:
        return gpd.GeoDataFrame(columns=list(CANONICAL_POI_SCHEMA.keys()), geometry='geometry', crs="EPSG:4326")
    
    # Track Costco count at input
    costco_input = sum(1 for _, row in gdf.iterrows() 
                      if (row.get('brand', {}) and 
                          row['brand'].get('names', {}) and 
                          'costco' in str(row['brand']['names'].get('primary', '')).lower()))
    print(f"[COSTCO] Overture input: {costco_input} POIs")
    
    normalized_rows = []
    for _, row in gdf.iterrows():
        # Category mapping
        primary_cat = row['categories']['primary'] if row['categories'] and 'primary' in row['categories'] else None
        ts_class, ts_cat, ts_subcat = OVERTURE_CATEGORY_MAP.get(primary_cat, (None, None, None))
        
        # Handle place_of_worship religion mapping for Overture as well (if present)
        if primary_cat and 'place_of_worship' in str(primary_cat).lower():
            # Try to extract religion from the row data (if available in Overture)
            religion = None
            if 'tags' in row and isinstance(row['tags'], dict):
                religion = row['tags'].get('religion')
            if religion:
                religion_lower = str(religion).lower()
                if religion_lower == 'christian':
                    ts_class, ts_cat, ts_subcat = ('religious', 'place_of_worship_church', 'church')
                elif religion_lower == 'muslim':
                    ts_class, ts_cat, ts_subcat = ('religious', 'place_of_worship_mosque', 'mosque')
                elif religion_lower == 'jewish':
                    ts_class, ts_cat, ts_subcat = ('religious', 'place_of_worship_synagogue', 'synagogue')
                elif religion_lower in ('hindu', 'buddhist', 'jain', 'sikh'):
                    ts_class, ts_cat, ts_subcat = ('religious', 'place_of_worship_temple', 'temple')
                else:
                    # Skip if religion not in mapped set
                    continue

        # Exclude airports from Overture; we will inject airports from CSV only
        if ts_cat == 'airport':
            continue

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
            "trauma_level": None,
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
    
    # Track Costco count at output
    costco_output = len(out_gdf[out_gdf['brand_id'] == 'costco']) if len(out_gdf) > 0 and 'brand_id' in out_gdf.columns else 0
    print(f"[COSTCO] Overture output: {costco_output} POIs")

    print(f"[ok] Normalized {len(out_gdf)} POIs from Overture.")
    return out_gdf


def normalize_osm_pois(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Normalizes OSM POIs to the canonical schema."""
    print("--- Normalizing OSM POIs ---")
    if gdf.empty:
        return gpd.GeoDataFrame(columns=list(CANONICAL_POI_SCHEMA.keys()), geometry='geometry', crs="EPSG:4326")
    
    # Track Costco count at input
    costco_input = sum(1 for _, row in gdf.iterrows() 
                      if any('costco' in str(row.get(tag, '')).lower() 
                            for tag in ['brand', 'operator', 'name']))
    print(f"[COSTCO] OSM input: {costco_input} POIs")

    normalized_rows = []
    for _, row in gdf.iterrows():
        # Category mapping
        ts_class, ts_cat, ts_subcat = None, None, None
        for tag_key in ['amenity', 'shop', 'leisure', 'tourism', 'aeroway']:
            tag_value = row.get(tag_key)
            if tag_value and (tag_key, tag_value) in OSM_TAG_MAP:
                ts_class, ts_cat, ts_subcat = OSM_TAG_MAP[(tag_key, tag_value)]
                
                # Special handling for place_of_worship: map religion to worship type
                if tag_value == 'place_of_worship':
                    religion = row.get('religion')
                    if religion:
                        religion_lower = str(religion).lower()
                        if religion_lower == 'christian':
                            ts_cat = 'place_of_worship_church'
                            ts_subcat = 'church'
                        elif religion_lower == 'muslim':
                            ts_cat = 'place_of_worship_mosque'
                            ts_subcat = 'mosque'
                        elif religion_lower == 'jewish':
                            ts_cat = 'place_of_worship_synagogue'
                            ts_subcat = 'synagogue'
                        elif religion_lower in ('hindu', 'buddhist', 'jain', 'sikh'):
                            ts_cat = 'place_of_worship_temple'
                            ts_subcat = 'temple'
                        # If religion is not in the mapped set, skip this POI
                        elif religion_lower not in ('christian', 'muslim', 'jewish', 'hindu', 'buddhist', 'jain', 'sikh'):
                            ts_class, ts_cat, ts_subcat = None, None, None
                            break
                break
        # Exclude airports from OSM; we will inject airports from CSV only
        if ts_cat == 'airport':
            continue
        
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
            "trauma_level": None,
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
    
    # Track Costco count at output
    costco_output = len(out_gdf[out_gdf['brand_id'] == 'costco']) if len(out_gdf) > 0 and 'brand_id' in out_gdf.columns else 0
    print(f"[COSTCO] OSM output: {costco_output} POIs")
    
    print(f"[ok] Normalized {len(out_gdf)} POIs from OSM.")
    return out_gdf


def conflate_pois(*sources: tuple[str, gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
    """Conflates and deduplicates POIs from heterogeneous sources.

    Simple heuristics:
    - Compute H3 r9 cell for each point
    - Group duplicates by (brand_id if present else lowercase name, category, h3_r9)
    - Prefer Overture over OSM for chains; keep first otherwise
    """
    print("--- Conflating POIs from all sources ---")
    frames = []
    for tag, gdf in sources:
        if gdf is None or gdf.empty:
            continue
        df = gdf.copy()
        # Ensure expected columns exist
        for col in CANONICAL_POI_SCHEMA.keys():
            if col not in df.columns:
                df[col] = None
        # Compute H3 r9 cell id
        try:
            df["h3_r9"] = df.geometry.apply(lambda p: h3.geo_to_h3(p.y, p.x, 9))
        except Exception:
            # Fallback: leave empty
            df["h3_r9"] = None
        frames.append(df)
    if not frames:
        return gpd.GeoDataFrame(columns=list(CANONICAL_POI_SCHEMA.keys()), geometry='geometry', crs="EPSG:4326")

    combined = pd.concat(frames, ignore_index=True)
    print(f"[ok] Combined POIs: {len(combined)} total")
    
    # Track Costco count before deduplication
    costco_before_dedup = len(combined[combined['brand_id'] == 'costco']) if 'brand_id' in combined.columns else 0
    print(f"[COSTCO] Before deduplication: {costco_before_dedup} POIs")

    # Build more precise duplication key - use higher resolution H3 and be more conservative
    def _key_row(r):
        # Use H3 r11 (~20m) for branded POIs, r10 (~60m) for others
        brand = str(r.get("brand_id") or "").strip()
        h3_res = 11 if brand else 10
        
        h3_cell = None
        try:
            if r.get('geometry') and hasattr(r['geometry'], 'y') and hasattr(r['geometry'], 'x'):
                h3_cell = h3.geo_to_h3(r['geometry'].y, r['geometry'].x, h3_res)
        except:
            h3_cell = r.get("h3_r9")  # fallback to coarser resolution
        
        name = str(r.get("name") or "").strip().lower()
        cat = str(r.get("category") or "").strip().lower()
        ext_id = str(r.get("ext_id", "")).strip()
        
        # For branded POIs: be VERY conservative - only deduplicate exact duplicates
        if brand:
            # Include more specificity for branded POIs to avoid false matches
            return (brand, name, cat, h3_cell, ext_id[:8])  # use partial ext_id for additional uniqueness
        else:
            # For non-branded POIs, be conservative but less strict
            return (name, cat, h3_cell, ext_id)

    combined["_dupkey"] = combined.apply(_key_row, axis=1)
    # Sort so that overture comes before osm, but prefer POIs with brand_id
    combined["_brand_priority"] = combined["brand_id"].notna().astype(int)
    combined["_src_rank"] = combined["source"].map({"overture": 0, "osm": 1}).fillna(2)
    combined = combined.sort_values(["_dupkey", "_brand_priority", "_src_rank"], ascending=[True, False, True]).reset_index(drop=True)
    
    dedup = combined.drop_duplicates(subset=["_dupkey"], keep="first").drop(columns=["_dupkey", "_src_rank", "_brand_priority"])
    dedup = gpd.GeoDataFrame(dedup, geometry="geometry", crs="EPSG:4326")
    
    # Track Costco count after deduplication
    costco_after_dedup = len(dedup[dedup['brand_id'] == 'costco']) if 'brand_id' in dedup.columns else 0
    print(f"[COSTCO] After deduplication: {costco_after_dedup} POIs")
    
    print(f"[ok] Deduplicated POIs: {len(dedup)} remaining")
    return dedup


def load_airports_csv() -> gpd.GeoDataFrame:
    """Load airport list from Future/airports_coordinates.csv and normalize to canonical schema (CSV-only source)."""
    path = os.path.join('Future', 'airports_coordinates.csv')
    if not os.path.exists(path):
        print(f"[warn] Airports CSV not found at {path}; skipping airports injection.")
        return gpd.GeoDataFrame(columns=list(CANONICAL_POI_SCHEMA.keys()), geometry='geometry', crs="EPSG:4326")
    df = pd.read_csv(path)
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
            'class': 'transport',
            'category': 'airport',
            'subcat': 'airport',
            'trauma_level': None,
            'lon': lon,
            'lat': lat,
            'geometry': Point(lon, lat),
            'source': 'csv:airports',
            'ext_id': iata if iata else None,
            'provenance': ['csv:airports'],
        })
    if not rows:
        return gpd.GeoDataFrame(columns=list(CANONICAL_POI_SCHEMA.keys()), geometry='geometry', crs="EPSG:4326")
    gdf = gpd.GeoDataFrame(rows, crs="EPSG:4326")
    print(f"[ok] Loaded {len(gdf)} airports from CSV.")
    return gdf


def main():
    """Main function to run the POI normalization pipeline."""
    os.makedirs("data/poi", exist_ok=True)

    for state in STATES:
        # 1. Load data from sources
        overture_pois = load_overture_pois(state)
        osm_pois = load_osm_pois(state)
        trauma_pois = load_level1_trauma_pois(state)

        # 2. Normalize each source to the canonical schema
        overture_normalized = normalize_overture_pois(overture_pois)
        osm_normalized = normalize_osm_pois(osm_pois)

        # 2b. Build beach POIs (ocean/lake/river/other) from OSM
        beach_pois = build_beach_pois_for_state(state)
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
