import os
import urllib.request
from pyrosm import OSM
import geopandas as gpd
import subprocess
import tempfile
from typing import List


def download_geofabrik(state: str, base: str, out_dir="data/osm"):
    url = f"{base}/{state}-latest.osm.pbf"
    os.makedirs(out_dir, exist_ok=True)
    out = os.path.join(out_dir, f"{state}.osm.pbf")

    if os.path.exists(out) and os.path.getsize(out) > 0:
        return out

    urllib.request.urlretrieve(url, out)
    return out


def _ogr_query_to_gdf(pbf_path: str, layer: str, where_sql: str) -> gpd.GeoDataFrame:
    """Run an ogr2ogr SQL query against a specific OSM layer and return a GeoDataFrame."""
    tmpdir = tempfile.mkdtemp(prefix="ts_ogr_")
    out_geojson = os.path.join(tmpdir, f"{layer}.geojson")
    cmd = [
        "ogr2ogr",
        "-f",
        "GeoJSON",
        out_geojson,
        pbf_path,
        layer,
        "-where",
        where_sql,
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True)
        gdf = gpd.read_file(out_geojson)
        if gdf.crs is None and not gdf.empty:
            gdf = gdf.set_crs("EPSG:4326")
        return gdf
    except Exception as e:
        print(f"Warning: ogr2ogr query failed for layer={layer}: {e}")
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry")


def ogr_find_brand_features(pbf_path: str, brand_keywords: List[str]) -> gpd.GeoDataFrame:
    """
    Use GDAL/OGR to find features in OSM PBF whose name/other_tags contain any of the given keywords.
    Searches points and multipolygons. Returns points (centroids for polygons).
    """
    if not brand_keywords:
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry")

    # Build simple OR expression over name and other_tags. OGR SQL LIKE is case-sensitive,
    # so include common case variants to be pragmatic.
    kws = set()
    for k in brand_keywords:
        if not isinstance(k, str):
            continue
        base = k.strip()
        if not base:
            continue
        kws.update([base, base.lower(), base.upper(), base.title()])

    like_clauses = []
    for k in kws:
        k_escaped = k.replace("'", "''")
        like_clauses.append(f"name LIKE '%{k_escaped}%'")
        like_clauses.append(f"other_tags LIKE '%{k_escaped}%'")
    where_sql = "(" + " OR ".join(like_clauses) + ")"

    points = _ogr_query_to_gdf(pbf_path, "points", where_sql)
    polys = _ogr_query_to_gdf(pbf_path, "multipolygons", where_sql)

    # Convert polygons to representative points (centroids)
    if not polys.empty:
        try:
            polys = polys.set_geometry(polys.geometry.centroid)
        except Exception as e:
            print(f"Warning: centroid conversion failed: {e}")
            polys = gpd.GeoDataFrame(columns=["geometry"], geometry="geometry")

    frames = []
    if points is not None and not points.empty:
        frames.append(points[["geometry"]].copy())
    if polys is not None and not polys.empty:
        frames.append(polys[["geometry"]].copy())

    if not frames:
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry")

    out = gpd.pd.concat(frames, ignore_index=True)
    out = gpd.GeoDataFrame(out, geometry="geometry", crs=points.crs if not points.empty else "EPSG:4326")
    return out


def pois_from_pbf(pbf_path: str, amenity=None, shop=None):
    osm = OSM(pbf_path)
    filt = {}
    if amenity:
        filt["amenity"] = amenity
    if shop:
        filt["shop"] = shop
    
    # Preferred path: use get_pois
    try:
        gdf = osm.get_pois(custom_filter=filt)
        if gdf is None or gdf.empty:
            gdf = gpd.GeoDataFrame(columns=["geometry"], geometry="geometry")
        if gdf.crs is None:
            gdf.set_crs("EPSG:4326", inplace=True)
        else:
            gdf = gdf.to_crs("EPSG:4326")
        return gdf
    except Exception as e:
        print(f"Warning: Error extracting POIs from {pbf_path}: {e}")
    
    # Fallback: broaden criteria and include ways & relations where possible
    try:
        gdf = osm.get_data_by_custom_criteria(
            custom_filter={"shop": True, "amenity": True},
            filter_type="keep",
            osm_keys_to_keep=["amenity", "shop", "name", "brand", "operator"],
            keep_nodes=True,
            keep_ways=True,
            keep_relations=True,
        )
        if gdf is None or gdf.empty:
            gdf = gpd.GeoDataFrame(columns=["geometry"], geometry="geometry")
        if gdf.crs is None:
            gdf.set_crs("EPSG:4326", inplace=True)
        else:
            gdf = gdf.to_crs("EPSG:4326")
        return gdf
    except Exception as e:
        print(f"Warning: Fallback extraction (ways/relations) failed for {pbf_path}: {e}")
    
    # Final fallback: nothing from Pyrosm worked
    return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry")


def find_major_airports(pbf_path: str) -> gpd.GeoDataFrame:
    """
    Return representative points for aerodromes that are major by heuristic:
    - aeroway = 'aerodrome'
    - and has IATA code OR aerodrome:type = international
    Uses OGR SQL against points and multipolygons, converting polygons to centroids.
    """
    where_sql = (
        "aeroway = 'aerodrome' AND ("
        "other_tags LIKE '%\"iata\"=>\"%' OR "
        "other_tags LIKE '%\"aerodrome:type\"=>\"international\"%')"
    )
    pts = _ogr_query_to_gdf(pbf_path, "points", where_sql)
    polys = _ogr_query_to_gdf(pbf_path, "multipolygons", where_sql)
    frames = []
    if pts is not None and not pts.empty:
        frames.append(pts[["geometry"]].copy())
    if polys is not None and not polys.empty:
        try:
            polys = polys.set_geometry(polys.geometry.centroid)
            frames.append(polys[["geometry"]].copy())
        except Exception as e:
            print(f"Warning: centroid conversion failed for airports: {e}")
    if not frames:
        # Fallback: Use Pyrosm to select aeroway=aerodrome, then filter
        try:
            osm = OSM(pbf_path)
            gdf = osm.get_data_by_custom_criteria(
                custom_filter={"aeroway": ["aerodrome"]},
                filter_type="keep",
                osm_keys_to_keep=["aeroway", "name", "iata", "aerodrome:type", "other_tags"],
                keep_nodes=True,
                keep_ways=True,
                keep_relations=True,
            )
            if gdf is None or gdf.empty:
                return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry")
            if gdf.crs is None:
                gdf.set_crs("EPSG:4326", inplace=True)
            else:
                gdf = gdf.to_crs("EPSG:4326")
            # Build mask: IATA present OR aerodrome:type contains 'international'
            cols = gdf.columns
            has_iata = (gdf["iata"].astype(str).str.len() > 0) if "iata" in cols else gdf.assign(_tmp=False)["_tmp"]
            has_international_col = (gdf["aerodrome:type"].astype(str).str.lower().str.contains("international")) if "aerodrome:type" in cols else gdf.assign(_tmp=False)["_tmp"]
            mask = has_iata | has_international_col
            gdf = gdf[mask]
            if gdf.empty:
                return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry")
            # Convert all geometries to representative points
            from shapely.geometry import Point
            pts_list = []
            for geom in gdf.geometry:
                if geom is None:
                    continue
                gt = getattr(geom, "geom_type", None)
                try:
                    if gt == "Point":
                        pts_list.append(geom)
                    elif gt in ("Polygon", "MultiPolygon", "LineString", "MultiLineString"):
                        pts_list.append(geom.representative_point())
                    else:
                        pts_list.append(geom.centroid)
                except Exception:
                    continue
            if not pts_list:
                return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry")
            out = gpd.GeoDataFrame(geometry=gpd.GeoSeries(pts_list, crs="EPSG:4326"))
            return out
        except Exception as e:
            print(f"Warning: Pyrosm fallback failed for airports: {e}")
            return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry")
    out = gpd.pd.concat(frames, ignore_index=True)
    return gpd.GeoDataFrame(out, geometry="geometry", crs=pts.crs if pts is not None and not pts.empty else "EPSG:4326")


def airports_from_csv(csv_path: str, state_code: str) -> gpd.GeoDataFrame:
    """Load airports from a manual CSV, filtered by USPS state code, and return point GeoDataFrame (EPSG:4326)."""
    import pandas as pd
    from shapely.geometry import Point

    if not os.path.exists(csv_path):
        print(f"Warning: airports CSV not found at {csv_path}")
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry")

    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        print(f"Warning: failed to read airports CSV: {e}")
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry")

    # Normalize column names
    cols = {c.lower(): c for c in df.columns}
    lat_col = cols.get("latitude")
    lon_col = cols.get("longitude")
    state_col = cols.get("state")

    if lat_col is None or lon_col is None or state_col is None:
        print("Warning: airports CSV missing required columns: latitude, longitude, state")
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry")

    df = df[df[state_col].astype(str).str.upper() == state_code.upper()].copy()
    if df.empty:
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry")

    # Drop rows with invalid coords
    df = df[pd.to_numeric(df[lat_col], errors="coerce").notna() & pd.to_numeric(df[lon_col], errors="coerce").notna()].copy()
    if df.empty:
        return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry")

    geometry = [Point(float(lon), float(lat)) for lat, lon in zip(df[lat_col], df[lon_col])]
    gdf = gpd.GeoDataFrame(df[[state_col]].copy(), geometry=geometry, crs="EPSG:4326")
    return gdf[["geometry"]] 