"""
Given a delta file with POI changes, recompute minutes only for affected H3 rings.
Delta CSV schema:
  state,brand,action,lat,lon
action in {add,move,close}
"""
import os, math
import pandas as pd, geopandas as gpd
import networkx as nx
import osmnx as ox
from shapely.geometry import Point
import h3
from pyrosm import OSM
from config import H3_RES_LOW, H3_RES_HIGH

DELTA_PATH = "data/deltas/poi_delta.csv"


def build_graph_from_pbf(pbf_path: str):
    osm = OSM(pbf_path)
    nodes, edges = osm.get_network(network_type="driving", nodes=True)
    G = ox.graph_from_gdfs(nodes, edges)
    G = ox.add_edge_speeds(G)
    G = ox.add_edge_travel_times(G)
    return G


def affected_cells(lat, lon, res, ring_km=15):
    # approximate ring by k neighbors
    base = h3.latlng_to_cell(lat, lon, res)
    k = max(1, int(ring_km / 1.0))  # ~1km cell edge at r8, coarse heuristic
    return set(h3.grid_disk(base, k))


def recompute_for_patch(state, brand, lat, lon):
    pbf = f"data/osm/{state}.osm.pbf"
    G = build_graph_from_pbf(pbf)
    node_xy = pd.DataFrame({
        "node": list(G.nodes),
        "x": [G.nodes[n]["x"] for n in G.nodes],
        "y": [G.nodes[n]["y"] for n in G.nodes],
    })
    # snap the changed POI only
    xs, ys = [lon], [lat]
    src_node = ox.nearest_nodes(G, xs, ys)[0]
    node_seconds = nx.single_source_dijkstra_path_length(G, src_node, weight="travel_time")
    df = pd.DataFrame({"node": list(node_seconds.keys()),
                       "seconds": list(node_seconds.values())}).merge(node_xy, on="node", how="left")

    out = {}
    for res in [H3_RES_LOW, H3_RES_HIGH]:
        # Restrict to affected H3 cells
        patch_cells = affected_cells(lat, lon, res)
        rows = []
        for _, r in df.iterrows():
            cell = h3.latlng_to_cell(r["y"], r["x"], res)
            if cell in patch_cells:
                rows.append((cell, r["seconds"]))
        if not rows:
            continue
        tmp = pd.DataFrame(rows, columns=["h3", "seconds"]).groupby("h3", as_index=False)["seconds"].min()
        tmp["minutes"] = (tmp["seconds"]/60).apply(lambda s: int(math.ceil(s)))
        out[res] = tmp[["h3","minutes"]]
    return out


def apply_patch(state, brand, patch):
    for res, df in patch.items():
        path = f"data/minutes/{state}_r{res}.parquet"
        full = pd.read_parquet(path)
        col = f"{brand}_drive_min"
        df = df.rename(columns={"minutes": col})
        full = full.merge(df, on="h3", how="left", suffixes=("", "_new"))
        # take min with existing for add/move; for close events you'd recompute inversely (omitted for brevity)
        full[col] = full[[col, f"{col}_new"]].min(axis=1, skipna=True)
        full = full.drop(columns=[f"{col}_new"]).sort_values("h3")
        full.to_parquet(path)
        print(f"[patch] updated {path} ({len(df)})")


if __name__ == "__main__":
    if not os.path.exists(DELTA_PATH):
        raise SystemExit("No delta file at data/deltas/poi_delta.csv")
    delta = pd.read_csv(DELTA_PATH)
    for _, r in delta.iterrows():
        state, brand = r.state, r.brand
        lat, lon = float(r.lat), float(r.lon)
        patch = recompute_for_patch(state, brand, lat, lon)
        apply_patch(state, brand, patch) 
