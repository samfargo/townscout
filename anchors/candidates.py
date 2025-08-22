#candidates.py
from typing import List, Dict
import numpy as np
import networkx as nx
import osmnx as ox
import pandas as pd
from pyrosm import OSM
from .core_utils import normalize_hw, hw_in
from .selection import build_node_kdtree, nearest_node_kdtree

def mk_cand(node_id, data, mode, kind, score, road_class="unclassified", region_fn=None, mandatory=False) -> Dict:
    lat, lon = float(data["y"]), float(data["x"])
    region = region_fn(lat, lon) if region_fn else "rural"
    return {
        "id": f"{mode}_{kind}_{node_id}",
        "node_id": int(node_id),
        "lon": lon, "lat": lat,
        "road_class": road_class,
        "kind": kind,
        "region": region,
        "source": f"{kind}_detection",
        "score": float(score),
        "mandatory": bool(mandatory),
    }

def drive_candidates(G: nx.MultiDiGraph, pbf_path: str, add_rural_cov, add_motorway_chain, region_fn) -> List[Dict]:
    nodes_gdf, edges_gdf = ox.graph_to_gdfs(G, nodes=True, edges=True)
    if 'u' not in edges_gdf.columns and 'v' not in edges_gdf.columns:
        edges_gdf = edges_gdf.reset_index()

    edges_gdf = edges_gdf.copy()
    if "highway" not in edges_gdf.columns:
        edges_gdf["highway"] = None
    edges_gdf["hw_norm"] = edges_gdf["highway"].apply(normalize_hw).astype("category")
    if "access" in edges_gdf.columns:
        # Convert access to string first to handle list values from OSM
        edges_gdf["access"] = edges_gdf["access"].astype(str).astype("category")

    if 'u' not in edges_gdf.columns or 'v' not in edges_gdf.columns:
        return _drive_candidates_fallback(G, add_rural_cov, add_motorway_chain, region_fn)

    svc = edges_gdf["hw_norm"].str.contains(r"\b(?:service|driveway|track)\b", na=False, regex=True)
    prv = edges_gdf["access"].astype(str).isin(["private", "no"]) if "access" in edges_gdf.columns else False
    edges_gdf = edges_gdf[~(svc | prv)]

    is_ramp  = edges_gdf["hw_norm"].str.contains("_link", na=False)
    is_major = edges_gdf["hw_norm"].str.contains(r"\b(?:motorway|trunk)\b", na=False, regex=True)

    ramp_nodes = pd.Index(edges_gdf.loc[is_ramp, "u"]).append(pd.Index(edges_gdf.loc[is_ramp, "v"])).astype("int64").unique()
    major_end_nodes = pd.Index(edges_gdf.loc[is_major, "u"]).append(pd.Index(edges_gdf.loc[is_major, "v"])).astype("int64").unique()

    is_arterial = edges_gdf["hw_norm"].str.contains(r"\b(?:motorway|trunk|primary|secondary|tertiary)\b", na=False, regex=True)
    arterial = edges_gdf.loc[is_arterial, ["u", "v"]]
    deg = arterial.groupby("u").size().add(arterial.groupby("v").size(), fill_value=0)
    major_x_nodes = deg[deg >= 3].index.astype("int64")

    out: List[Dict] = []
    for nid in ramp_nodes:
        if nid in nodes_gdf.index:
            r = nodes_gdf.loc[nid]
            out.append(mk_cand(int(nid), {"x": r["x"], "y": r["y"]}, "drive", "ramp", 10, "ramp", region_fn))
    for nid in np.setdiff1d(major_end_nodes, ramp_nodes):
        if int(nid) in nodes_gdf.index:
            r = nodes_gdf.loc[int(nid)]
            out.append(mk_cand(int(nid), {"x": r["x"], "y": r["y"]}, "drive", "major_end", 9, "motorway", region_fn))
    for nid in major_x_nodes:
        if nid in nodes_gdf.index:
            r = nodes_gdf.loc[nid]
            out.append(mk_cand(int(nid), {"x": r["x"], "y": r["y"]}, "drive", "intersection", 8, "arterial", region_fn))

    add_rural_cov(G, out, mode="drive")
    add_motorway_chain(G, out, spacing_m=2500)
    return out

def _drive_candidates_fallback(G, add_rural_cov, add_motorway_chain, region_fn):
    out: List[Dict] = []
    for nid, data in G.nodes(data=True):
        is_ramp = False
        best_class = "unclassified"
        for _, _, ed in G.edges(nid, data=True):
            hw = ed.get("highway", "")
            if (isinstance(hw, str) and "_link" in hw) or hw_in(hw, {"motorway","trunk"}):
                is_ramp = True
            if hw_in(hw, {"motorway","trunk","primary","secondary","tertiary"}):
                if best_class == "unclassified":
                    best_class = str(hw) if isinstance(hw, str) else "arterial"
        if is_ramp:
            out.append(mk_cand(nid, data, "drive", "ramp", 10, best_class, region_fn))
    for nid, data in G.nodes(data=True):
        if G.degree(nid) < 3:
            continue
        majors = 0
        best_class = "unclassified"
        for _, _, ed in G.edges(nid, data=True):
            hw = ed.get("highway", "")
            if hw_in(hw, {"motorway","trunk","primary","secondary","tertiary"}):
                majors += 1
                if best_class in ("unclassified", "tertiary"):
                    best_class = str(hw) if isinstance(hw, str) else "arterial"
        if majors >= 2:
            out.append(mk_cand(nid, data, "drive", "intersection", 8, best_class, region_fn))
    add_rural_cov(G, out, mode="drive")
    add_motorway_chain(G, out, spacing_m=2500)
    return out

def walk_candidates(G: nx.MultiDiGraph, pbf_path: str, add_ped_hubs_fast, region_fn) -> List[Dict]:
    cands: List[Dict] = []
    cands.extend(_walk_intersections_fast(G, region_fn))

    ids, X, tree = build_node_kdtree(G)  # reused by crossings + hubs

    osm = OSM(pbf_path)
    try:
        crossings = osm.get_pois({"highway": ["crossing"]})
        if crossings is not None and not crossings.empty:
            for _, row in crossings.iterrows():
                # Handle both point and polygon geometries
                geom = row.geometry
                if hasattr(geom, 'x') and hasattr(geom, 'y'):
                    # Point geometry
                    lon, lat = geom.x, geom.y
                else:
                    # Polygon or other geometry - use centroid
                    centroid = geom.centroid
                    lon, lat = centroid.x, centroid.y
                
                nid = nearest_node_kdtree(ids, tree, lon, lat, max_m=25)
                if nid is not None:
                    data = G.nodes[nid]
                    cands.append(mk_cand(nid, data, "walk", "crossing", 5, "walkable", region_fn))
    except Exception as e:
        print(f"[walk] crossings warn: {e}")

    add_ped_hubs_fast(G, cands, (ids, tree), pbf_path=pbf_path, region_fn=region_fn)
    
    # Add rural coverage for walk network too
    add_rural_coverage_fast(G, cands, mode="walk", region_fn=region_fn)
    
    return cands

def _walk_intersections_fast(G: nx.MultiDiGraph, region_fn):
    nodes_gdf, edges_gdf = ox.graph_to_gdfs(G, nodes=True, edges=True)
    if 'u' not in edges_gdf.columns and 'v' not in edges_gdf.columns:
        edges_gdf = edges_gdf.reset_index()
    edges_gdf = edges_gdf.copy()
    if "highway" not in edges_gdf.columns:
        edges_gdf["highway"] = None
    edges_gdf["hw_norm"] = edges_gdf["highway"].apply(normalize_hw).astype('category')
    if 'u' not in edges_gdf.columns or 'v' not in edges_gdf.columns:
        return _walk_intersections_fallback(G, region_fn)

    deg = edges_gdf.groupby("u").size().add(edges_gdf.groupby("v").size(), fill_value=0)
    cand = deg[deg >= 3].index.astype("int64")

    banned = edges_gdf[edges_gdf["hw_norm"].str.contains(r"\b(?:motorway|motorway_link|trunk)\b", na=False, regex=True)]
    bad_nodes = pd.Index(banned["u"]).append(pd.Index(banned["v"])).astype("int64").unique()
    cand = pd.Index(cand).difference(bad_nodes)

    sub = nodes_gdf.loc[cand, ["x", "y"]]
    return [mk_cand(int(nid), {"x": r["x"], "y": r["y"]}, "walk", "intersection", 6, "walkable", region_fn)
            for nid, r in sub.iterrows()]

def _walk_intersections_fallback(G, region_fn):
    out = []
    for nid, data in G.nodes(data=True):
        if G.degree(nid) < 3:
            continue
        if any(hw_in(ed.get("highway", ""), {"motorway","motorway_link","trunk"}) for _,_,ed in G.edges(nid, data=True)):
            continue
        out.append(mk_cand(nid, data, "walk", "intersection", 6, "walkable", region_fn))
    return out

def add_ped_hubs_fast(G, cands, kdtree_data, pbf_path: str, region_fn):
    from pyrosm import OSM
    ids, tree = kdtree_data
    osm = OSM(pbf_path)
    try:
        pois = osm.get_pois({
            "amenity": ["school","hospital","university","bus_station"],
            "railway": ["station"],
            "leisure": ["park"],
        })
    except Exception as e:
        print(f"[walk] ped hub warn: {e}")
        return
    if pois is None or pois.empty:
        return
    snapped = []
    for _, row in pois.iterrows():
        # Handle both point and polygon geometries
        geom = row.geometry
        if hasattr(geom, 'x') and hasattr(geom, 'y'):
            # Point geometry
            lon, lat = geom.x, geom.y
        else:
            # Polygon or other geometry - use centroid
            centroid = geom.centroid
            lon, lat = centroid.x, centroid.y
        
        nid = nearest_node_kdtree(ids, tree, lon, lat, max_m=200)
        if nid is not None:
            snapped.append(nid)
    if not snapped:
        return
    for nid in np.unique(np.array(snapped, dtype=np.int64)):
        if nid in G.nodes:
            data = G.nodes[int(nid)]
            cands.append(mk_cand(int(nid), data, "walk", "poi_hub", 7, "walkable", region_fn))

def add_rural_coverage_fast(G: nx.MultiDiGraph, cands: List[Dict], mode: str, region_fn=None):
    import osmnx as ox, h3, pandas as pd
    nodes_gdf, edges_gdf = ox.graph_to_gdfs(G, nodes=True, edges=True)
    if 'u' not in edges_gdf.columns and 'v' not in edges_gdf.columns:
        edges_gdf = edges_gdf.reset_index()
    edges_gdf = edges_gdf.copy()
    if "highway" not in edges_gdf.columns:
        edges_gdf["highway"] = None
    edges_gdf["hw_norm"] = edges_gdf["highway"].apply(normalize_hw).astype('category')
    if 'u' not in edges_gdf.columns or 'v' not in edges_gdf.columns:
        print("[warning] Rural coverage falling back to slower method")
        return

    nodes = nodes_gdf[["x","y"]].copy()
    
    # Use finer resolution for better coverage
    res = 8 if mode == "walk" else 7
    nodes[f"h3r{res}"] = nodes.apply(lambda r: h3.latlng_to_cell(r["y"], r["x"], res), axis=1)

    cls_score = {"motorway":10,"trunk":10,"primary":6,"secondary":3,"tertiary":2,
                 "residential":0,"unclassified":0}
    ed = edges_gdf[["u","v","hw_norm"]].copy()
    ed["score"] = 0
    for cls, sc in cls_score.items():
        mask = ed["hw_norm"].str.contains(cls, na=False)
        ed.loc[mask, "score"] = ed.loc[mask, "score"] + sc

    deg_score = ed.groupby("u")["score"].sum().add(ed.groupby("v")["score"].sum(), fill_value=0)
    deg_cnt   = ed.groupby("u").size().add(ed.groupby("v").size(), fill_value=0)
    node_score = deg_score.add(deg_cnt, fill_value=0)

    nodes["score"] = nodes.index.map(node_score).fillna(0)
    
    # Instead of just one per cell, take top 3 per cell if available
    top_per_cell = nodes.sort_values("score", ascending=False).groupby(f"h3r{res}").head(3)
    
    # Also add systematic grid coverage for sparse areas
    # Get all H3 cells in the region
    bbox_cells = set()
    lat_min, lat_max = nodes["y"].min(), nodes["y"].max()
    lon_min, lon_max = nodes["x"].min(), nodes["x"].max()
    
    # Create a grid of H3 cells
    step = 0.05 if res == 8 else 0.1  # Finer step for walk mode
    lat_range = np.arange(lat_min, lat_max, step)
    lon_range = np.arange(lon_min, lon_max, step)
    
    for lat in lat_range:
        for lon in lon_range:
            cell = h3.latlng_to_cell(lat, lon, res)
            bbox_cells.add(cell)
    
    # Find cells without any nodes
    occupied_cells = set(nodes[f"h3r{res}"])
    empty_cells = bbox_cells - occupied_cells
    
    # For empty cells, find nearest nodes
    if empty_cells:
        from scipy.spatial import cKDTree
        node_coords = nodes[["x", "y"]].values
        tree = cKDTree(node_coords)
        
        for cell in list(empty_cells)[:500]:  # Limit to avoid too many
            lat, lon = h3.cell_to_latlng(cell)
            distances, indices = tree.query([lon, lat], k=1)
            if distances < 0.02:  # Within ~2km
                nid = nodes.index[indices]
                if nid in nodes.index:
                    r = nodes.loc[nid]
                    top_per_cell = pd.concat([top_per_cell, pd.DataFrame([r])], ignore_index=False)

    # Add all selected candidates
    for nid, r in top_per_cell.iterrows():
        cands.append(mk_cand(int(nid), {"x": r["x"], "y": r["y"]},
                             "drive" if mode == "drive" else "walk",
                             "coverage", (6 if mode == "drive" else 4.5),
                             "coverage", region_fn))

def add_motorway_chain(G: nx.MultiDiGraph, cands: List[Dict], spacing_m=2500, region_fn=None):
    """Add anchors along motorway/trunk/primary to ensure backbone connectivity."""
    import osmnx as ox

    try:
        # --- 1) collect major edges
        major_edges = []
        for u, v, k, d in G.edges(keys=True, data=True):
            if hw_in(d.get("highway"), {"motorway", "trunk", "primary"}):
                major_edges.append((u, v, k, d))
        print(f"[debug] motorway chain: {len(major_edges)} major edges", end="")

        # --- 2) collect motorway junction nodes (nodes carry this!)
        junction_nodes = set()
        for nid, nd in G.nodes(data=True):
            # OSMnx sometimes stores tags in 'tags' dict, or flattened
            hw_node = nd.get("highway") or (nd.get("tags", {}) or {}).get("highway")
            if hw_node == "motorway_junction":
                junction_nodes.add(nid)
        print(f", {len(junction_nodes)} junction nodes")

        # Add explicit junctions (high priority)
        for nid in junction_nodes:
            if nid in G.nodes:
                data = G.nodes[nid]
                cands.append(mk_cand(int(nid), data, "drive", "mw_junction", 9.5, "motorway", region_fn))

        # --- 3) sample along endpoints to cover long stretches
        sampled_nodes = set()
        for u, v, k, d in major_edges:
            highway = d.get("highway")
            if isinstance(highway, list):
                highway = highway[0] if highway else "unknown"

            length = d.get("length") or d.get("edge_length") or 0.0
            if length < spacing_m:
                pick = (u, v)
            else:
                # sample by endpoints alternating + ensure both ends for very long edges
                num = max(2, int(length / spacing_m))
                pick = tuple(u if i % 2 == 0 else v for i in range(num))

            for nid in pick:
                if nid not in sampled_nodes and nid in G.nodes:
                    data = G.nodes[int(nid)]
                    score = 8.5 if highway in ("motorway", "trunk") else 7.5
                    cands.append(mk_cand(int(nid), data, "drive", "mw_chain", score, highway or "unknown", region_fn))
                    sampled_nodes.add(nid)

        print(f"[debug] motorway chain: added {len([c for c in cands if c.get('kind') in ('mw_junction','mw_chain')])} candidates")

    except Exception as e:
        print(f"[warning] Motorway chain failed: {e}")
        import traceback; traceback.print_exc()