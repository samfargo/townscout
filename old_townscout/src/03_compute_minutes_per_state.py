import os, math
import pandas as pd, geopandas as gpd
import networkx as nx
import osmnx as ox
from pyrosm import OSM
import h3
import numpy as np
from scipy.spatial import cKDTree
from src.config import STATES, H3_RES_LOW, H3_RES_HIGH, POI_BRANDS

os.makedirs("data/minutes", exist_ok=True)


def build_graph_from_pbf(pbf_path: str):
    osm = OSM(pbf_path)
    nodes, edges = osm.get_network(network_type="driving", nodes=True)
    # Ensure nodes have required x/y columns and proper index
    if "x" not in nodes.columns or "y" not in nodes.columns:
        nodes = nodes.copy()
        nodes["x"] = nodes.geometry.x
        nodes["y"] = nodes.geometry.y
    if nodes.index.name is None or nodes.index.name != "id":
        if "id" in nodes.columns:
            nodes = nodes.set_index("id", drop=True)
        else:
            nodes.index.name = "id"
    # Ensure nodes have a unique index and consistent dtype
    if nodes.index.has_duplicates:
        nodes = nodes[~nodes.index.duplicated(keep="first")].copy()
    try:
        nodes.index = nodes.index.astype("int64")
    except Exception:
        pass
    # Ensure edges have a key column and MultiIndex (u,v,key)
    if "key" not in edges.columns:
        edges = edges.copy()
        edges["key"] = 0
    if not isinstance(edges.index, pd.MultiIndex) or list(edges.index.names) != ["u", "v", "key"]:
        edges = edges.set_index(["u", "v", "key"], drop=True)
    # Align dtypes and filter edges whose endpoints are present in nodes
    edges = edges.reset_index()
    # Drop rows with null endpoints before casting
    edges = edges[edges["u"].notna() & edges["v"].notna()].copy()
    edges["u"] = pd.to_numeric(edges["u"], errors="coerce")
    edges["v"] = pd.to_numeric(edges["v"], errors="coerce")
    edges = edges[edges["u"].notna() & edges["v"].notna()].copy()
    edges["u"] = edges["u"].astype("int64")
    edges["v"] = edges["v"].astype("int64")
    # Filter to edges where both endpoints exist in the node index
    mask = edges["u"].isin(nodes.index) & edges["v"].isin(nodes.index)
    edges = edges.loc[mask]
    # Recreate MultiIndex and ensure uniqueness
    edges = edges.set_index(["u", "v", "key"], drop=True)
    if edges.index.has_duplicates:
        edges = edges[~edges.index.duplicated(keep="first")].copy()
    G = ox.graph_from_gdfs(nodes, edges)
    G = ox.add_edge_speeds(G)
    G = ox.add_edge_travel_times(G)
    return G


def load_points(state: str, brand: str) -> gpd.GeoDataFrame:
    path = f"data/poi/{state}_{brand}.parquet"
    if not os.path.exists(path):
        return gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs=4326)
    gdf = gpd.read_parquet(path)
    if gdf.crs is None:
        gdf.set_crs(4326, inplace=True)
    return gdf


def make_undirected(G):
    # one-way edges strand sources; use undirected for symmetric travel time
    return G.to_undirected(reciprocal=False)


def component_sizes(Gu):
    comps = list(nx.connected_components(Gu))
    sizes = {n: len(comp) for comp in comps for n in comp}
    return sizes


def build_kdtree(Gu):
    nodes = list(Gu.nodes)
    xs = np.array([Gu.nodes[n]["x"] for n in nodes])
    ys = np.array([Gu.nodes[n]["y"] for n in nodes])
    tree = cKDTree(np.c_[xs, ys])
    return nodes, xs, ys, tree


def smart_snap_points_to_nodes(G_proj, pts_wgs84, comp_size, nodes, xs, ys, tree,
                               k=20, min_comp_size=2000, search_radius_m=1200):
    """
    Pick the nearest node whose connected-component size is big enough.
    Fall back to the absolute nearest if none meet the threshold within radius.
    """
    if pts_wgs84.empty:
        return []
    pts = pts_wgs84.to_crs(G_proj.graph["crs"])
    snapped = []
    # crude meters-per-unit scale (Web Mercator-like) — good enough for snapping
    # If your graph CRS is projected in meters already, radius_deg == radius_m
    radius_deg = search_radius_m  # assume meters if projected
    for geom in pts.geometry:
        qx, qy = geom.x, geom.y
        dists, idxs = tree.query([qx, qy], k=k, distance_upper_bound=radius_deg)
        # cKDTree pads with inf and len(nodes) when out of bounds — filter those
        candidates = [(int(nodes[i]), float(d)) for i, d in zip(idxs, dists)
                      if np.isfinite(d) and i < len(nodes)]
        chosen = None
        # prefer big components first
        for n, _ in candidates:
            if comp_size.get(n, 0) >= min_comp_size:
                chosen = n
                break
        if chosen is None:
            # fall back to the absolute nearest valid node
            if candidates:
                chosen = candidates[0][0]
            else:
                # last resort: nearest node globally
                chosen = ox.nearest_nodes(G_proj, qx, qy)
        snapped.append(int(chosen))
    # de-dup
    return list(np.unique(snapped))


def nodes_to_h3(df_nodes, res: int):
    rows = []
    for _, r in df_nodes.iterrows():
        cell = h3.latlng_to_cell(r["y"], r["x"], res)
        rows.append((cell, r["seconds"]))
    out = pd.DataFrame(rows, columns=["h3", "seconds"])\
        .groupby("h3", as_index=False)["seconds"].min()
    out["minutes"] = (out["seconds"] / 60).apply(lambda s: int(math.ceil(s)))
    return out[["h3", "minutes"]]


def compute_state(state: str):
    pbf = f"data/osm/{state}.osm.pbf"
    print(f"[graph] {state}")
    G = build_graph_from_pbf(pbf)      # your existing builder (adds travel_time)
    Gu = make_undirected(G)
    G_proj = ox.project_graph(Gu)
    comp_size = component_sizes(Gu)
    nodes, xs, ys, tree = build_kdtree(G_proj)

    node_xy = pd.DataFrame({
        "node": list(Gu.nodes),
        "x": [Gu.nodes[n]["x"] for n in Gu.nodes],
        "y": [Gu.nodes[n]["y"] for n in Gu.nodes],
    })

    per_res = {H3_RES_LOW: pd.DataFrame({"h3": []}),
               H3_RES_HIGH: pd.DataFrame({"h3": []})}

    for brand in POI_BRANDS.keys():
        pts = load_points(state, brand)  # EPSG:4326
        sources = smart_snap_points_to_nodes(
            G_proj, pts, comp_size, nodes, xs, ys, tree,
            k=20, min_comp_size=2000, search_radius_m=1200
        )
        print(f"[{brand}] POIs={len(pts)} snapped_sources={len(sources)}")

        node_seconds = nx.multi_source_dijkstra_path_length(
            Gu, sources, weight="travel_time", cutoff=60*60*3  # 3h cap
        )

        if not node_seconds:
            for res in per_res:
                per_res[res][f"{brand}_drive_min"] = pd.Series(dtype="Int64")
            print(f"[{brand}] WARNING: no reachable nodes – NA column")
            continue

        df = (pd.DataFrame({"node": list(node_seconds.keys()),
                            "seconds": list(node_seconds.values())})
              .merge(node_xy, on="node", how="left"))

        for res in per_res:
            h3df = nodes_to_h3(df, res).rename(columns={"minutes": f"{brand}_drive_min"})
            per_res[res] = per_res[res].merge(h3df, on="h3", how="outer")

    for res, df in per_res.items():
        out = f"data/minutes/{state}_r{res}.parquet"
        df.sort_values("h3").reset_index(drop=True).to_parquet(out)
        nn = {c: int(df[c].notna().sum()) for c in df.columns if c.endswith("_drive_min")}
        print(f"[ok] {out} rows={len(df)} non_null={nn}")


if __name__ == "__main__":
    for s in STATES:
        compute_state(s) 