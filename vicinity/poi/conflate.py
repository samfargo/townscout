"""
POI Conflation and Deduplication

Conflates POIs from multiple sources and removes duplicates using spatial and
attribute-based heuristics.
"""
import pandas as pd
import geopandas as gpd
import h3
from .schema import CANONICAL_POI_SCHEMA, create_empty_poi_dataframe


def conflate_pois(*sources: tuple[str, gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
    """
    Conflate and deduplicate POIs from heterogeneous sources.

    Simple heuristics:
    - Compute H3 r9 cell for each point
    - Group duplicates by (brand_id if present else lowercase name, category, h3_r9)
    - Prefer Overture over OSM for chains; keep first otherwise
    
    Args:
        *sources: Variable number of (tag, GeoDataFrame) tuples
        
    Returns:
        Deduplicated GeoDataFrame with canonical POIs
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
        return create_empty_poi_dataframe()

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

