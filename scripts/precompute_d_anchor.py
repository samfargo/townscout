#!/usr/bin/env python3
# scripts/precompute_d_anchor.py
"""
Precompute Anchor → Category travel times (D_anchor).

What this emits:
  Parquet with columns:
    anchor_int_id (int32)   # matches precompute_t_hex anchor index
    category_id   (int32)
    mode          (string)  # "drive" | "walk"
    seconds_u16   (uint16)  # travel time to nearest POI in category (capped)
    snapshot_ts   (string, YYYY-MM-DD)

Design choices:
- Driving uses the REVERSED directed graph and runs multi-source Dijkstra
  from POIs. That equals anchor→POI on the original graph and respects one-ways.
- Walking uses an undirected graph (directionless).
- Values are in SECONDS (uint16) with UNREACH_U16 sentinel=65535, consistent with T_hex.
- Optionally consumes an anchor index parquet (anchor_int_id ↔ anchor_stable_id)
  to guarantee IDs match T_hex tiles.

Inputs expected:
- --pbf:          PBF extract for the state/region
- --anchors:      anchors parquet with at least columns: id (stable), node_id (int64), [mode]
- --anchor-index: parquet with columns: anchor_int_id(int32), anchor_stable_id(string) [optional but recommended]
- POIs:           GeoParquet per category at data/poi/{state}_{category}.parquet (geometry in WGS84)

"""

import argparse
import os
import time
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import geopandas as gpd
import networkx as nx
import osmnx as ox
from pyrosm import OSM
from scipy.spatial import cKDTree

from src.categories import get_category  # your category registry

SNAPSHOT_TS = time.strftime("%Y-%m-%d")

# Sentinels (keep consistent with T_hex)
UNREACH_U16 = np.uint16(65535)

# -----------------------------
# Graph build
# -----------------------------
def build_graph(pbf: str, mode: str) -> nx.MultiDiGraph:
    """
    Build routable graph with 'travel_time' seconds per edge.
    Drive: osmnx speeds + pruning private; Walk: fixed 4.8 kph.
    Finally simplify graph to speed up Dijkstra.
    """
    osm = OSM(pbf)
    net = "driving" if mode == "drive" else "walking"

    nodes, edges = osm.get_network(
        network_type=net, nodes=True, extra_attributes=["highway", "access", "length"]
    )

    # ---- Nodes: ensure unique int index and x/y columns ----
    if "x" not in nodes:
        nodes = nodes.copy()
        nodes["x"] = nodes.geometry.x
        nodes["y"] = nodes.geometry.y

    # normalize node id/index
    if nodes.index.name != "id":
        nodes = nodes.set_index("id", drop=True)
    # make sure they are integers (pyrosm can deliver ints already, but be strict)
    nodes.index = pd.to_numeric(nodes.index, errors="coerce").astype("Int64")
    nodes = nodes.dropna().copy()
    nodes.index = nodes.index.astype("int64")
    nodes = nodes[~nodes.index.duplicated(keep="first")].copy()

    # ---- Edges: normalize u/v, force unique (u,v,key) ----
    edges = edges.reset_index(drop=True)
    edges = edges.dropna(subset=["u", "v"]).copy()
    edges["u"] = pd.to_numeric(edges["u"], errors="coerce").astype("Int64")
    edges["v"] = pd.to_numeric(edges["v"], errors="coerce").astype("Int64")
    edges = edges.dropna(subset=["u", "v"]).copy()
    edges["u"] = edges["u"].astype("int64")
    edges["v"] = edges["v"].astype("int64")

    # keep only edges whose endpoints exist
    edges = edges[edges["u"].isin(nodes.index) & edges["v"].isin(nodes.index)].copy()

    # If no 'key', or if keys collide, assign unique keys per (u,v)
    if "key" not in edges.columns:
        edges["key"] = edges.groupby(["u", "v"]).cumcount().astype("int64")
    else:
        # Coerce to int where possible; fill NaNs; then fix collisions anyway
        edges["key"] = pd.to_numeric(edges["key"], errors="coerce").fillna(0).astype("int64")
        # Detect collisions and rebuild keys per (u,v) when needed
        dup_mask = edges.duplicated(subset=["u", "v", "key"], keep=False)
        if dup_mask.any():
            # rebuild a fresh unique key that includes cumcount
            edges["key"] = edges.groupby(["u", "v"]).cumcount().astype("int64")

    # Finalize MultiIndex
    edges = edges.set_index(["u", "v", "key"], drop=True)

    # ---- Sanity checks (fail fast with clear message) ----
    if nodes.index.has_duplicates:
        dups = nodes.index[nodes.index.duplicated()].unique().tolist()[:5]
        raise ValueError(f"nodes index not unique; examples: {dups}")
    if edges.index.duplicated().any():
        # Surface a few offenders to help debugging
        ex = edges.index[edges.index.duplicated()].unique().tolist()[:5]
        raise ValueError(f"edges (u,v,key) not unique; examples: {ex}")

    # ---- Build graph ----
    G = ox.graph_from_gdfs(nodes, edges)

    if mode == "walk":
        # Constant walk speed 4.8 kph
        for _, _, k, d in G.edges(keys=True, data=True):
            length_m = float(d.get("length", 0.0))
            d["speed_kph"] = 4.8
            d["travel_time"] = (length_m / 1000.0) / 4.8 * 3600.0
    else:
        G = ox.add_edge_speeds(G)
        G = ox.add_edge_travel_times(G)

        # Prune private/no-access after TT assignment
        rm = [(u, v, k) for u, v, k, d in G.edges(keys=True, data=True)
              if str(d.get("access")) in ("private", "no")]
        if rm:
            G.remove_edges_from(rm)

    print(f"[{mode}] Simplifying graph…")
    G = ox.simplify_graph(G)
    return G


# -----------------------------
# KD-tree snapping (lon/lat → nearest node within meters)
# -----------------------------
def build_node_kdtree(G: nx.MultiDiGraph) -> Tuple[np.ndarray, cKDTree, float]:
    ids = np.fromiter(G.nodes, dtype=np.int64)
    xs = np.array([G.nodes[n]["x"] for n in ids])  # lon
    ys = np.array([G.nodes[n]["y"] for n in ids])  # lat
    lat0 = float(np.deg2rad(np.mean(ys)))
    X = np.c_[(xs * np.cos(lat0)) * 111000.0, ys * 111000.0]  # meters
    tree = cKDTree(X)
    return ids, tree, lat0

def snap_points_to_nodes(G: nx.MultiDiGraph, pts: gpd.GeoDataFrame, max_m: float, mode: str = "drive") -> List[int]:
    if pts is None or pts.empty:
        return []
    ids, tree, lat0 = build_node_kdtree(G)
    out = []
    for geom in pts.geometry:
        if geom is None:
            continue
        # Handle both Point and Polygon geometries
        if hasattr(geom, 'x'):  # Point geometry
            qx, qy = float(geom.x), float(geom.y)
        else:  # Polygon or other geometry - use centroid
            centroid = geom.centroid
            qx, qy = float(centroid.x), float(centroid.y)
        
        # Find candidate nodes within radius
        Xq = np.array([(qx * np.cos(lat0)) * 111000.0, qy * 111000.0])
        
        # For driving mode, try to find publicly accessible nodes
        if mode == "drive":
            # Search for multiple candidates within radius and pick the best accessible one
            distances, indices = tree.query(Xq, k=min(10, len(ids)), distance_upper_bound=max_m)
            
            best_node = None
            for dist, idx in zip(distances, indices):
                if not np.isfinite(dist) or dist > max_m:
                    continue
                    
                candidate_node = int(ids[idx])
                
                # Check if this node has any publicly accessible edges
                has_public_access = False
                for _, _, d in G.edges(candidate_node, data=True):
                    if str(d.get("access")) not in ("private", "no"):
                        has_public_access = True
                        break
                
                # If no outgoing edges checked, check incoming edges
                if not has_public_access:
                    for _, _, d in G.in_edges(candidate_node, data=True):
                        if str(d.get("access")) not in ("private", "no"):
                            has_public_access = True
                            break
                
                # Use the first publicly accessible node we find
                if has_public_access:
                    best_node = candidate_node
                    break
            
            # If no publicly accessible node found, fall back to closest node
            if best_node is None:
                d, idx = tree.query(Xq, k=1)
                if float(d) <= float(max_m):
                    best_node = int(ids[int(idx)])
            
            if best_node is not None:
                out.append(best_node)
        else:
            # For walking mode, use simple nearest node (access restrictions less relevant)
            d, idx = tree.query(Xq, k=1)
            if float(d) <= float(max_m):
                out.append(int(ids[int(idx)]))
    
    # unique & stable
    return list(pd.Index(out).unique().astype("int64"))


# -----------------------------
# POI loading (one category file)
# -----------------------------
def load_pois_for_category(state_slug: str, cat_slug: str) -> gpd.GeoDataFrame:
    path = f"data/poi/{state_slug}_{cat_slug}.parquet"
    if not os.path.exists(path):
        print(f"[warn] POI parquet missing: {path}")
        return gpd.GeoDataFrame({"geometry": []}, geometry="geometry", crs="EPSG:4326")
    gdf = gpd.read_parquet(path)
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")
    return gdf[["geometry"]]


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Precompute Anchor→Category seconds (D_anchor)")
    ap.add_argument("--pbf", required=True)
    ap.add_argument("--anchors", required=True, help="anchors parquet (id, node_id, [mode])")
    ap.add_argument("--anchor-index", default="", help="anchor index parquet (anchor_int_id, anchor_stable_id)")
    ap.add_argument("--mode", required=True, choices=["drive", "walk"])
    ap.add_argument("--state", required=True, help="state slug, e.g., 'massachusetts'")
    ap.add_argument("--categories", nargs="+", required=True, help="category slugs")
    ap.add_argument("--out", required=True)
    ap.add_argument("--drive-cutoff-min", type=int, default=240, help="max minutes for drive leg (cap to fit uint16 seconds)")
    ap.add_argument("--walk-cutoff-min", type=int, default=60, help="max minutes for walk leg")
    ap.add_argument("--snap-max-m", type=int, default=1200, help="drive snap radius (m); walking will override smaller")
    args = ap.parse_args()

    # Load anchors
    anc = pd.read_parquet(args.anchors)
    if "mode" in anc.columns:
        anc = anc[anc["mode"] == args.mode]
    if not {"id", "node_id"}.issubset(anc.columns):
        raise SystemExit("anchors parquet must include columns: id, node_id")
    anc = anc[["id", "node_id"]].copy()
    anc["node_id"] = pd.to_numeric(anc["node_id"], errors="coerce").astype("Int64")
    anc = anc.dropna(subset=["node_id"])
    anc["node_id"] = anc["node_id"].astype("int64")
    anc["id"] = anc["id"].astype("string")

    # Anchor ID mapping (stable -> int32) to match T_hex
    if args.anchor_index and os.path.exists(args.anchor_index):
        idx = pd.read_parquet(args.anchor_index)
        if not {"anchor_int_id", "anchor_stable_id"}.issubset(idx.columns):
            raise SystemExit("--anchor-index parquet must have columns: anchor_int_id, anchor_stable_id")
        idx["anchor_stable_id"] = idx["anchor_stable_id"].astype("string")
        # Join to get anchor_int_id
        anc = anc.merge(idx, left_on="id", right_on="anchor_stable_id", how="left")
        if anc["anchor_int_id"].isna().any():
            missing = anc[anc["anchor_int_id"].isna()]["id"].unique().tolist()
            raise SystemExit(f"Some anchors missing in anchor-index: {missing[:10]} …")
        anc["anchor_int_id"] = anc["anchor_int_id"].astype("int32")
        print(f"[info] Anchors mapped via index: {len(anc)} rows, "
              f"{anc['anchor_int_id'].nunique()} unique anchor_int_id")
    else:
        # Build local mapping (OK for one-off runs; WARN for production)
        print("[warn] --anchor-index not provided; building a local mapping. "
              "Ensure this matches the mapping used by T_hex!")
        unique_ids = pd.Index(anc["id"].unique()).astype("string").tolist()
        stable_to_int = {sid: i for i, sid in enumerate(unique_ids)}
        anc["anchor_int_id"] = anc["id"].map(stable_to_int).astype("int32")

    anchor_nodes = anc[["anchor_int_id", "node_id"]].copy()

    # Build graph & pick query graph per mode
    G = build_graph(args.pbf, args.mode)
    if args.mode == "drive":
        # Direction-correct: POIs → anchors on REVERSED graph equals anchor→POI on forward
        Gq = G.reverse(copy=False)
        snap_max_m = int(args.snap_max_m)
        cutoff_sec = int(min(args.drive_cutoff_min, 1092 * 24 * 60) * 60)  # guard absurd values
    else:
        Gq = G.to_undirected(reciprocal=False)
        snap_max_m = 400  # tighter for walks
        cutoff_sec = int(args.walk_cutoff_min * 60)

    out_rows = []

    for slug in args.categories:
        cat = get_category(slug)  # expect .id and optional default_cutoff_min
        cat_cutoff_min = getattr(cat, "default_cutoff", None)
        if cat_cutoff_min is not None:
            cutoff_sec_eff = int(min(cat_cutoff_min, cutoff_sec / 60) * 60)
        else:
            cutoff_sec_eff = cutoff_sec

        print(f"[cat] {slug} (category_id={int(cat.id)}) cutoff={cutoff_sec_eff//60} min")
        pois = load_pois_for_category(args.state, slug)
        src_nodes = snap_points_to_nodes(Gq, pois, max_m=snap_max_m, mode=args.mode)
        if not src_nodes:
            print(f"[cat] {slug}: 0 POIs snapped; skipping.")
            continue

        # Multi-source Dijkstra from POIs on query graph
        node_sec: Dict[int, float] = nx.multi_source_dijkstra_path_length(
            Gq, src_nodes, weight="travel_time", cutoff=cutoff_sec_eff
        )

        # Map anchor nodes to seconds
        tmp = pd.DataFrame({"node_id": list(node_sec.keys()),
                            "seconds": list(node_sec.values())})
        merged = anchor_nodes.merge(tmp, on="node_id", how="left")

        # seconds → uint16 with UNREACH sentinel
        def to_u16(s) -> np.uint16:
            if pd.isna(s) or not np.isfinite(s):
                return UNREACH_U16
            v = int(round(float(s)))
            if v < 0:
                v = 0
            if v > 65534:
                # Cap at 65534; 65535 reserved as UNREACH
                v = 65534
            return np.uint16(v)

        merged["seconds_u16"] = merged["seconds"].apply(to_u16).astype("uint16")
        merged["category_id"] = int(cat.id)
        merged["mode"] = args.mode
        merged["snapshot_ts"] = SNAPSHOT_TS

        out_rows.append(merged[["anchor_int_id", "category_id", "mode", "seconds_u16", "snapshot_ts"]])

        # QA print
        secs = merged.loc[merged["seconds_u16"] < UNREACH_U16, "seconds_u16"].astype("int")
        if len(secs):
            med = int(np.median(secs))
            p95 = int(np.percentile(secs, 95))
            cover = 100.0 * len(secs) / max(1, len(merged))
            print(f"[QA] {slug}: median={med}s p95={p95}s coverage={cover:.1f}%")

    if not out_rows:
        raise SystemExit("No categories produced output.")

    out_df = pd.concat(out_rows, ignore_index=True)

    # Write Parquet
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    out_df.to_parquet(args.out, index=False)
    print(f"[ok] wrote {args.out} rows={len(out_df)} "
          f"(anchors={out_df['anchor_int_id'].nunique()}, categories={out_df['category_id'].nunique()})")


if __name__ == "__main__":
    main()