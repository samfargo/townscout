# src/osm_beaches.py
"""
Beach POI generation using Overture water data for spatial classification.

Strategy (as per issues.md):
- Use Overture water polygons for lakes/shorelines (clean, global coverage)
- Extract beach points from OSM where available
- Classify beaches by proximity to Overture water features
- Fall back to basic OSM tagging when spatial analysis fails

This avoids OSM coastline/water geometry issues that cause Shapely 2.x errors.
"""
import os
import uuid
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from shapely.ops import unary_union
from pyrosm import OSM

from geometry_utils import clean_geoms
import hashlib


def _dissolve_buffer(gdf, types, meters, geom_types=("Polygon", "MultiPolygon", "LineString", "MultiLineString")):
    """Efficiently dissolve geometries before buffering to reduce memory usage."""
    if gdf.empty:
        return None
    
    gm = gdf.to_crs(3857)
    clean = clean_geoms(gm, list(geom_types))
    if len(clean) == 0:
        return None
    
    try:
        # Dissolve before buffer for efficiency
        from shapely import union_all
        union = union_all(clean.geometry.values) if hasattr(__import__("shapely"), "union_all") else unary_union(clean.geometry.values)
        return union.buffer(meters)
    except Exception as e:
        print(f"[warn] Failed to dissolve and buffer {types}: {e}")
        return None


def _stable_uuid(namespace: str, src_id, pt: Point) -> str:
    """Generate stable UUID even when source ID is missing."""
    seed = f"{namespace}|{src_id if src_id is not None else ''}|{pt.x:.6f}|{pt.y:.6f}"
    h = hashlib.sha1(seed.encode()).hexdigest()
    return str(uuid.UUID(h[:32]))


def download_overture_water(state: str, bbox: dict) -> str:
    """
    Download Overture water theme data for a state using DuckDB.
    
    Args:
        state: State name (e.g., 'massachusetts')
        bbox: Dict with xmin, xmax, ymin, ymax keys
    
    Returns:
        Path to downloaded parquet file
    """
    import subprocess
    
    output_dir = "data/overture"
    output_path = os.path.join(output_dir, f"{state}_water.parquet")
    os.makedirs(output_dir, exist_ok=True)
    
    if os.path.exists(output_path):
        print(f"[ok] Overture water data for {state} already exists at {output_path}")
        return output_path
    
    overture_release = os.getenv("OVERTURE_RELEASE", "2025-09-24.0")
    
    # Query Overture water theme with spatial filtering
    duckdb_query = f"""
    INSTALL spatial; LOAD spatial;
    INSTALL httpfs; LOAD httpfs;
    
    SET s3_region='us-west-2';
    SET s3_use_ssl=true;
    -- anonymous public bucket: no keys needed
    
    COPY (
      SELECT
        id, geometry, subtype, class, names
      FROM read_parquet(
        's3://overturemaps-us-west-2/release/{overture_release}/theme=base/type=water/*.parquet',
        hive_partitioning=1
      )
      WHERE subtype IN ('ocean','sea','lake','reservoir','pond','lagoon','river','canal')
        AND ST_Intersects(
          geometry::GEOMETRY,
          ST_MakeEnvelope({bbox['xmin']}, {bbox['ymin']}, {bbox['xmax']}, {bbox['ymax']})
        )
    ) TO '{output_path}' (FORMAT PARQUET);
    """
    
    print(f"[info] Downloading Overture water data for {state}...")
    try:
        subprocess.run(
            ["duckdb", "-c", duckdb_query],
            check=True,
            capture_output=True,
            text=True,
        )
        print(f"[ok] Overture water saved to {output_path}")
        return output_path
    except subprocess.CalledProcessError as e:
        print(f"[error] DuckDB query failed: {e.stderr}")
        return None
    except FileNotFoundError:
        print("[error] DuckDB command not found. Install DuckDB CLI.")
        return None


def load_overture_water(state: str) -> dict:
    """
    Load Overture water features for a state.
    
    Returns dict with keys: ocean, lake, river
    """
    # Massachusetts bbox
    bbox = {
        "xmin": -73.508142,
        "xmax": -69.928393,
        "ymin": 41.186328,
        "ymax": 42.886589,
    }
    
    water_path = download_overture_water(state, bbox)
    if not water_path or not os.path.exists(water_path):
        print("[warn] Overture water data unavailable, returning empty")
        return {"ocean": gpd.GeoDataFrame(), "lake": gpd.GeoDataFrame(), "river": gpd.GeoDataFrame()}
    
    try:
        df = pd.read_parquet(water_path)
        if df.empty:
            print("[warn] Overture water parquet is empty")
            return {"ocean": gpd.GeoDataFrame(), "lake": gpd.GeoDataFrame(), "river": gpd.GeoDataFrame()}
        
        # Convert WKB geometry column safely
        gdf = gpd.GeoDataFrame(df, geometry=gpd.GeoSeries.from_wkb(df["geometry"]), crs="EPSG:4326")
        
        # Lowercase subtype for consistent matching
        if "subtype" in gdf.columns:
            gdf["subtype"] = gdf["subtype"].astype("string").str.lower()
        
        # Classify by subtype (exclude streams to reduce false river hits)
        ocean = gdf[gdf['subtype'].isin(['ocean', 'sea'])].copy() if 'subtype' in gdf.columns else gpd.GeoDataFrame()
        lake = gdf[gdf['subtype'].isin(['lake', 'reservoir', 'pond', 'lagoon'])].copy() if 'subtype' in gdf.columns else gpd.GeoDataFrame()
        river = gdf[gdf['subtype'].isin(['river', 'canal'])].copy() if 'subtype' in gdf.columns else gpd.GeoDataFrame()
        
        print(f"[ok] Loaded Overture water: ocean={len(ocean)}, lake={len(lake)}, river={len(river)}")
        return {"ocean": ocean, "lake": lake, "river": river}
        
    except Exception as e:
        print(f"[error] Failed to load Overture water: {e}")
        return {"ocean": gpd.GeoDataFrame(), "lake": gpd.GeoDataFrame(), "river": gpd.GeoDataFrame()}


def load_osm_beaches(state: str) -> gpd.GeoDataFrame:
    """
    Load beach POINTS from OSM (avoiding problematic polygon assemblies).
    
    Strategy: Only fetch nodes tagged as natural=beach to avoid geometry errors.
    """
    pbf_path = f"data/osm/{state}.osm.pbf"
    if not os.path.exists(pbf_path):
        print(f"[warn] OSM PBF not found at {pbf_path}")
        return gpd.GeoDataFrame(columns=['geometry', 'name', 'id'], geometry='geometry', crs="EPSG:4326")
    
    try:
        osm = OSM(pbf_path)
        # Only load beach NODES (points) to avoid geometry assembly issues
        beach_gdf = osm.get_data_by_custom_criteria(
            custom_filter={"natural": ["beach"]},
            tags_as_columns=["name", "natural"],
            keep_nodes=True,   # Beach points
            keep_ways=False,   # Skip ways/polygons to avoid errors
            keep_relations=False,
        )
        
        if beach_gdf is None or beach_gdf.empty:
            print("[info] No beach nodes found in OSM")
            return gpd.GeoDataFrame(columns=['geometry', 'name', 'id'], geometry='geometry', crs="EPSG:4326")
        
        # Keep only needed columns and ensure WGS84
        cols = [c for c in ("name", "natural", "id", "geometry") if c in beach_gdf.columns]
        beach_gdf = beach_gdf[cols].to_crs("EPSG:4326")
        print(f"[ok] Loaded {len(beach_gdf)} beach points from OSM")
        return beach_gdf
        
    except Exception as e:
        print(f"[warn] Failed to load OSM beaches: {e}")
        return gpd.GeoDataFrame(columns=['geometry', 'name', 'id'], geometry='geometry', crs="EPSG:4326")


def classify_beaches_with_overture(
    beach_gdf: gpd.GeoDataFrame,
    overture_water: dict
) -> gpd.GeoDataFrame:
    """
    Robust classification using spatial index joins—no global union/buffers.
    Priority: ocean > lake > river > other.
    Distances in meters (EPSG:3857).
    """
    if beach_gdf.empty:
        return beach_gdf.assign(beach_type=pd.Series(dtype="string"))

    # Project once
    P = 3857
    pts = beach_gdf.to_crs(P).copy()

    # Dist thresholds
    D_OCEAN, D_LAKE, D_RIVER = 500, 300, 200

    def _prep_polys(gdf):
        if gdf is None or gdf.empty:
            return gpd.GeoDataFrame(geometry=[], crs=beach_gdf.crs).to_crs(P)
        gm = gdf.to_crs(P)
        clean = clean_geoms(gm, ["Polygon", "MultiPolygon"])
        return gpd.GeoDataFrame(geometry=clean, crs=P)

    def _prep_lines_or_polys_for_river(gdf):
        if gdf is None or gdf.empty:
            return gpd.GeoDataFrame(geometry=[], crs=beach_gdf.crs).to_crs(P)
        gm = gdf.to_crs(P)
        clean = clean_geoms(gm, ["LineString","MultiLineString","Polygon","MultiPolygon"])
        return gpd.GeoDataFrame(geometry=clean, crs=P)

    ocean_src = _prep_polys(overture_water.get("ocean"))
    lake_src  = _prep_polys(overture_water.get("lake"))
    # Exclude streams to reduce false positives/load
    river_df  = overture_water.get("river")
    if isinstance(river_df, gpd.GeoDataFrame) and "subtype" in river_df.columns:
        river_df = river_df[ river_df["subtype"].isin(["river","canal"]) ]
    river_src = _prep_lines_or_polys_for_river(river_df)

    def _flag_within(pts_gdf, src_gdf, max_d):
        if src_gdf.empty:
            return pd.Series(False, index=pts_gdf.index)
        # nearest within max_distance; returns NaN if none within range
        j = gpd.sjoin_nearest(
            pts_gdf[["geometry"]],
            src_gdf[["geometry"]],
            how="left",
            max_distance=max_d,
            distance_col="d"
        )
        return j["d"].notna()

    # Priority flags
    is_ocean = _flag_within(pts, ocean_src, D_OCEAN)
    # Mask out already-labeled points before the next joins to save time
    remaining = pts[~is_ocean]
    is_lake = pd.Series(False, index=pts.index)
    if not remaining.empty:
        lake_mask = _flag_within(remaining, lake_src, D_LAKE)
        is_lake.loc[remaining.index] = lake_mask

    remaining = pts[~is_ocean & ~is_lake]
    is_river = pd.Series(False, index=pts.index)
    if not remaining.empty:
        river_mask = _flag_within(remaining, river_src, D_RIVER)
        is_river.loc[remaining.index] = river_mask

    labels = pd.Series("other", index=pts.index, dtype="string")
    labels[is_river] = "river"
    labels[is_lake]  = "lake"
    labels[is_ocean] = "ocean"

    out = beach_gdf.copy()
    out["beach_type"] = labels
    return out


def build_beach_pois_for_state(state: str) -> gpd.GeoDataFrame:
    """
    Build beach POIs for a state using Overture water + OSM beach points.
    
    Returns GeoDataFrame in canonical POI schema.
    """
    # Load Overture water features (clean, global)
    overture_water = load_overture_water(state)
    
    # Load OSM beach points (avoiding polygon errors)
    beach_points = load_osm_beaches(state)
    
    if beach_points.empty:
        print("[info] No beaches found for classification")
        return gpd.GeoDataFrame(
            columns=["poi_id", "name", "brand_id", "brand_name", "class", "category", 
                     "subcat", "lon", "lat", "geometry", "source", "ext_id", "provenance"],
            geometry="geometry", 
            crs="EPSG:4326"
        )
    
    # Classify beaches by water type
    classified = classify_beaches_with_overture(beach_points, overture_water)
    
    # Convert to canonical POI schema
    rows = []
    for _, r in classified.iterrows():
        try:
            name = r.get("name")
            source_id = r.get("id") if "id" in r else None
            pt = r.geometry if isinstance(r.geometry, Point) else r.geometry.representative_point()
            beach_type = r.get("beach_type", "other")
            
            # Generate stable UUID
            poi_id = _stable_uuid("osm_beach", source_id, pt)
            
            # Create category like beach_ocean, beach_lake, etc.
            category = f"beach_{beach_type}"
            
            rows.append({
                "poi_id": poi_id,
                "name": name,
                "brand_id": None,
                "brand_name": None,
                "class": "natural",
                "category": category,
                "subcat": beach_type,
                "lon": float(pt.x),
                "lat": float(pt.y),
                "geometry": pt,
                "source": "osm+overture",
                "ext_id": str(source_id) if source_id is not None else None,
                "provenance": ["osm", "overture"],
            })
        except Exception as e:
            print(f"[warn] Failed to convert beach to POI: {e}")
            continue
    
    if not rows:
        return gpd.GeoDataFrame(
            columns=["poi_id", "name", "brand_id", "brand_name", "class", "category", 
                     "subcat", "lon", "lat", "geometry", "source", "ext_id", "provenance"],
            geometry="geometry", 
            crs="EPSG:4326"
        )
    
    result = gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:4326")
    
    # Print classification summary
    if not result.empty and 'beach_type' in classified.columns:
        counts = classified["beach_type"].value_counts().to_dict()
        print(f"[ok] Classified beaches: {counts}")
    
    # Count by beach type
    type_counts = result['subcat'].value_counts().to_dict() if 'subcat' in result.columns else {}
    type_summary = ", ".join(f"{t}={type_counts.get(t, 0)}" for t in ['ocean', 'lake', 'river', 'other'])
    print(f"[ok] Built {len(result)} beach POIs: {type_summary}")
    
    return result
