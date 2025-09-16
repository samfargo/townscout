import os
import urllib.request
import subprocess
import tempfile
import shutil
from typing import List, Dict

import geopandas as gpd
import networkx as nx
import osmnx as ox
from pyrosm import OSM, get_data
from shapely.geometry import Point

import config


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

    # Build simple OR expression over name and other_tags.
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

    # Final fallback
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
        # Fallback: use Pyrosm to select aeroway=aerodrome, then filter
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


def get_network_type(mode: str) -> str:
    if "walk" in mode:
        return "walk"
    return "drive"


def _ensure_node_coords(G: nx.MultiDiGraph) -> nx.MultiDiGraph:
    """Check for x_orig/y_orig attributes and rename to x/y if needed."""
    if not G.nodes:
        return G

    # Peek at the first node's data to see what we have
    any_id = next(iter(G.nodes))
    d = G.nodes[any_id]

    # Case 1: no 'x', but has 'geometry'. Fill from geometry.
    if "x" not in d:
        for _, nd in G.nodes(data=True):
            geom = nd.get("geometry")
            if geom is not None:
                nd["x"] = float(geom.x)
                nd["y"] = float(geom.y)
    
    # Case 2: no 'x', but has 'x_orig'. Rename.
    if "x" not in d and "x_orig" in d:
        print("[graph] Renaming 'x_orig'/'y_orig' node attributes to 'x'/'y'...")
        for _, nd in G.nodes(data=True):
            if "x_orig" in nd:
                nd["x"] = nd.pop("x_orig")
            if "y_orig" in nd:
                nd["y"] = nd.pop("y_orig")
    return G


def _build_graph_with_osmnx_from_file(pbf_path: str, network_type: str) -> nx.MultiDiGraph:
    """Build a NetworkX MultiDiGraph from a local OSM extract using OSMnx only.

    Strategy:
    1) Try ox.graph_from_file (supports multiple formats in newer OSMnx).
    2) Fallback: convert PBF to OSM XML via 'osmium' CLI if available, then ox.graph_from_xml.
    """
    print(f"[graph] Attempting OSMnx load from file: {os.path.basename(pbf_path)} ({network_type})")
    G = None

    # 1) Try direct graph_from_file if available
    try:
        if hasattr(ox.graph, "graph_from_file"):
            G = ox.graph.graph_from_file(pbf_path, network_type=network_type, retain_all=False, simplify=False)
        elif hasattr(ox, "graph_from_file"):
            # older API style
            G = ox.graph_from_file(pbf_path, network_type=network_type, retain_all=False, simplify=False)
    except Exception as e:
        print(f"[graph] graph_from_file failed: {e}")
        G = None

    # 2) Fallback to converting to XML and using graph_from_xml
    if G is None:
        if shutil.which("osmium") is None:
            raise RuntimeError("'osmium' CLI not found. Install with 'brew install osmium-tool' or provide an OSM XML file.")
        with tempfile.TemporaryDirectory(prefix="ts_osm_") as td:
            xml_path = os.path.join(td, "extract.osm")
            print("[graph] Converting PBF to OSM XML via osmium...")
            subprocess.run(["osmium", "cat", pbf_path, "-o", xml_path, "-O"], check=True)
            print("[graph] Loading graph from XML via OSMnx...")
            if hasattr(ox.graph, "graph_from_xml"):
                G = ox.graph.graph_from_xml(xml_path, retain_all=False, simplify=False)
            else:
                G = ox.graph_from_xml(xml_path, retain_all=False, simplify=False)

    if G is None:
        raise RuntimeError("Failed to build graph with OSMnx from local file.")

    # Ensure lengths/speeds/travel times
    try:
        from osmnx import distance as oxd, speed as oxs
        G = oxd.add_edge_lengths(G)
        G = oxs.add_edge_speeds(G)
        G = oxs.add_edge_travel_times(G)
    except Exception:
        try: G = ox.add_edge_lengths(G)
        except: pass
        try: G = ox.add_edge_speeds(G)
        except: pass
        try: G = ox.add_edge_travel_times(G)
        except: pass

    # Guarantee node coordinates x/y
    G = _ensure_node_coords(G)
    print(f"[graph] OSMnx graph ready: {len(G.nodes)} nodes, {len(G.edges)} edges")
    return G


def load_graph(pbf_path: str, mode: str, force_rebuild: bool = False) -> nx.MultiDiGraph:
    state_name = os.path.basename(pbf_path).split(".")[0]
    network_type = get_network_type(mode)  # "drive" | "walk"
    graph_params = config.GRAPH_CONFIG.get(network_type, {})

    # GraphML cache path
    cache_dir = "data/osm/cache"
    os.makedirs(cache_dir, exist_ok=True)
    graphml_cache_path = os.path.join(cache_dir, f"{state_name}_{network_type}.graphml")

    # Try cache first, but check for corruption
    if not force_rebuild and os.path.exists(graphml_cache_path):
        cache_size = os.path.getsize(graphml_cache_path)
        if cache_size < 1024:  # Less than 1KB suggests corruption
            print(f"[{mode}] Cache file is corrupt (only {cache_size} bytes), removing...")
            os.remove(graphml_cache_path)
        else:
            try:
                print(f"[{mode}] Loading cached graph from {graphml_cache_path} ({cache_size//1024//1024}MB)...")
                G = ox.load_graphml(graphml_cache_path)
                G = _ensure_node_coords(G)
                print(f"[{mode}] Loaded cached graph: {len(G.nodes)} nodes, {len(G.edges)} edges")
                return G
            except Exception as e:
                print(f"[{mode}] Cache load failed: {e}. Removing corrupt cache and rebuilding...")
                try:
                    os.remove(graphml_cache_path)
                except:
                    pass

    # Build from local file using OSMnx only (avoid Pyrosm)
    print(f"[{mode}] Building graph from local extract via OSMnx (this may take a while)...")
    G = _build_graph_with_osmnx_from_file(pbf_path, network_type)

    # Final check on node coordinates before saving
    G = _ensure_node_coords(G)

    # Skip caching for now
    print(f"[{mode}] Skipping graph caching to avoid serialization costs...")

    return G