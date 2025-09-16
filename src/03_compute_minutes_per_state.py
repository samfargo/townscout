#!/usr/bin/env python3
# scripts/precompute_t_hex.py
"""
Precompute Hex → Anchor travel times (T_hex) with top-K anchors per hex.

What this does (refined):
- Builds a routable graph with 'travel_time' seconds per edge.
- Runs a multi-source, multi-label Dijkstra from all anchors (in batches) to
  compute the K-best anchors per node (node→anchor leg) using the road graph.
- Aggregates node labels up to H3 hexes at res 8/9 with a true GLOBAL top-K
  across all batches (no duplicates).
- Optionally "borrows" neighbors to fill sparse hexes and marks which slots
  were borrowed via a small bitfield (per-hex provenance: bit k = borrowed a{k}).
- Emits compact Parquet with uint16 seconds and int32 anchor IDs.
- Optionally writes a sidecar mapping of anchor_int_id → anchor_stable_id.

Sentinels:
- UNREACH_U16 (65535): reachable status unknown or >= cutoff
- NODATA_U16  (65534): no road node for hex even after borrowing
"""

import argparse
import gc
import os
import time
from collections import defaultdict
from heapq import heappop, heappush
from typing import Dict, Iterable, List, Tuple

import h3
import networkx as nx
import numpy as np
import osmnx as ox
import pandas as pd
import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
from pyrosm import OSM
from scipy.spatial import cKDTree
from tqdm import tqdm
import uuid
import polars as pl

from graph.csr_export import graph_to_csr
from t_hex import kbest_multisource_csr
import util_h3
import util_osm
import config

SNAPSHOT_TS = time.strftime("%Y-%m-%d")


def build_anchor_sites(
    canonical_pois: gpd.GeoDataFrame, G: nx.MultiDiGraph, mode: str
) -> pd.DataFrame:
    """
    Builds anchor sites from canonical POIs by snapping them to the nearest graph nodes.

    Args:
        canonical_pois: GeoDataFrame of canonical POIs.
        G: The road network graph.
        mode: The travel mode ('drive' or 'walk').

    Returns:
        A DataFrame of anchor sites with schema from OVERHALL.md.
    """
    print(f"--- Building anchor sites for {mode} mode ---")
    if canonical_pois.empty or not G.nodes:
        print("[warn] Canonical POIs or graph is empty. No anchor sites will be built.")
        return pd.DataFrame()
    
    # 1. Build a KD-tree from the graph nodes.
    print(f"[info] Building KD-tree from {len(G.nodes)} graph nodes...")
    node_ids, tree, lat0, m_per_deg = build_node_kdtree(G)
    
    # 2. For each POI, find the nearest node_id.
    print(f"[info] Snapping {len(canonical_pois)} POIs to nearest graph nodes...")
    poi_coords = np.c_[
        (canonical_pois.geometry.x.to_numpy() * np.cos(lat0)) * m_per_deg,
        canonical_pois.geometry.y.to_numpy() * m_per_deg,
    ]
    
    # Query the tree for nearest neighbors. dists are in meters.
    dists, indices = tree.query(poi_coords, k=1)
    
    pois_with_nodes = canonical_pois.copy()
    pois_with_nodes['node_id'] = node_ids[indices]
    pois_with_nodes['snap_dist_m'] = dists
    
    # Filter out POIs that are too far from the graph
    MAX_SNAP_DISTANCE_M = 250 if mode == 'drive' else 75
    pois_with_nodes = pois_with_nodes[pois_with_nodes['snap_dist_m'] <= MAX_SNAP_DISTANCE_M]
    print(f"[info] {len(pois_with_nodes)} POIs snapped within {MAX_SNAP_DISTANCE_M}m of the graph.")

    # 3. Group POIs by node_id to create sites.
    print("[info] Grouping POIs into anchor sites...")
    
    # Define aggregations
    aggs = {
        'poi_id': lambda x: list(x),
        'brand_id': lambda x: list(x.dropna().unique()),
        'category': lambda x: list(x.dropna().unique()),
    }
    
    sites = pois_with_nodes.groupby('node_id').agg(aggs).reset_index()
    
    # 4. Add node coordinates and generate a stable site_id.
    node_coords = pd.DataFrame(
        [ (nid, data['x'], data['y']) for nid, data in G.nodes(data=True) ],
        columns=['node_id', 'lon', 'lat']
    ).set_index('node_id')

    sites = sites.join(node_coords, on='node_id')
    
    # Generate site_id
    sites['site_id'] = sites.apply(
        lambda row: str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{mode}|{row['node_id']}")),
        axis=1
    )
    
    # Rename columns to match the old 'anchors_df' structure for now.
    # The rest of the script expects 'id' for stable id. This can be cleaned up later.
    # The OVERHAUL.md schema for sites is also more complex, this is a starting point.
    sites = sites.rename(columns={
        'poi_id': 'poi_ids',
        'brand_id': 'brands',
        'category': 'categories',
    })
    
    # Reorder columns and select the ones needed for the rest of the pipeline
    final_cols = ['site_id', 'node_id', 'lon', 'lat', 'poi_ids', 'brands', 'categories']
    sites = sites[final_cols]
    
    print(f"[ok] Built {len(sites)} anchor sites from {len(pois_with_nodes)} POIs.")

    return sites


# -----------------------------
# Sentinels & provenance flags
# -----------------------------
UNREACH_U16 = config.UNREACH_U16
NODATA_U16 = config.NODATA_U16

# provenance byte (uint8): bit k set => the a{k} entry was borrowed from neighbor hexes
def set_bit(u8: int, k: int) -> int:
    return int(u8 | (1 << k))


# -----------------------------
# KD-tree snapping (lon/lat → nearest node within meters)
# -----------------------------
def build_node_kdtree(G: nx.MultiDiGraph) -> Tuple[np.ndarray, cKDTree, float, float]:
    """Builds a KD-tree from graph nodes for fast spatial lookups."""
    ids = np.array(list(G.nodes), dtype=object)
    xs = np.array([G.nodes[n]["x"] for n in ids], dtype="float64")  # lon
    ys = np.array([G.nodes[n]["y"] for n in ids], dtype="float64")  # lat
    lat0 = float(np.deg2rad(np.mean(ys)))
    m_per_deg = 111000.0
    X = np.c_[(xs * np.cos(lat0)) * m_per_deg, ys * m_per_deg]
    tree = cKDTree(X)
    return ids, tree, lat0, m_per_deg


# -----------------------------
# H3 compatibility shim (v3 & v4)
# -----------------------------
_HAS_LATLNG_TO_CELL = hasattr(h3, "latlng_to_cell")     # v4
_HAS_STRING_TO_H3   = hasattr(h3, "string_to_h3")       # v4
_HAS_H3_TO_INT      = hasattr(h3, "h3_to_int")          # v3
_HAS_CELL_TO_LATLNG = hasattr(h3, "cell_to_latlng")     # v4

def to_cell(lat: float, lon: float, res: int):
    """Return an H3 cell (v4: int, v3: string) from lat/lon/res."""
    if _HAS_LATLNG_TO_CELL:
        return h3.latlng_to_cell(lat, lon, res)   # v4
    return h3.geo_to_h3(lat, lon, res)            # v3

def cell_to_uint64(cell) -> np.uint64:
    """
    Normalize an H3 index to uint64 regardless of API/version.
    - v4: ints already
    - v3: strings → int via h3_to_int
    - v4 alt: strings (if ever) → int via string_to_h3
    """
    if isinstance(cell, (int, np.integer)):
        return np.uint64(cell)
    if _HAS_STRING_TO_H3:
        return np.uint64(h3.string_to_h3(cell))   # v4 helper
    if _HAS_H3_TO_INT:
        return np.uint64(h3.h3_to_int(cell))      # v3 helper
    # Fallback: parse as hexadecimal string
    return np.uint64(int(cell, 16))


# -----------------------------
# Multi-source K-best Dijkstra
# -----------------------------
def multi_source_kbest(
    G: nx.MultiDiGraph,
    source_nodes: List[int],
    weight: str,
    cutoff_s: int,
    k_best: int,
) -> Dict[int, List[Tuple[float, int]]]:
    """
    One pass, multi-source, multi-label Dijkstra:
      returns node -> [(secs, src_node_id), …] sorted asc by secs, len<=k_best
    """
    node_best: Dict[int, List[Tuple[float, int]]] = defaultdict(list)
    pq: List[Tuple[float, int, int]] = []

    for src in source_nodes:
        heappush(pq, (0.0, int(src), int(src)))
        # (optionally seed node_best[src] with (0,src); harmless either way)

    best = {}  # (u,src) -> best_dist
    while pq:
        dist, u, src = heappop(pq)
        # guard against any stray IDs not present (e.g., after simplification)
        if u not in G:
            continue
        if dist > cutoff_s:
            continue
        key = (u, src)
        prev = best.get(key)
        if prev is not None and dist >= prev:
            continue
        best[key] = dist

        L = node_best[u]
        # Optimization: if we already have K and this is not better than the worst,
        # we can't improve on this node, so skip insertion but still relax neighbors.
        if not (len(L) >= k_best and dist >= L[-1][0]):
            # insert keeping list sorted; k_best small so O(K) is fine
            inserted = False
            for i, (old, _) in enumerate(L):
                if dist < old:
                    L.insert(i, (dist, src))
                    inserted = True
                    break
            if not inserted and len(L) < k_best:
                L.append((dist, src))
            if len(L) > k_best:
                L.pop()

        # relax neighbors
        if hasattr(G, 'out_edges'):
            # Directed graph
            edges_iter = G.out_edges(u, keys=True, data=True)
        else:
            # Undirected graph
            edges_iter = G.edges(u, keys=True, data=True)
        
        for _, v, k, d in edges_iter:
            tt = d.get(weight)
            if tt is None:
                continue
            nd = dist + float(tt)
            if nd <= cutoff_s:
                heappush(pq, (nd, int(v), int(src)))
    # ensure sorted
    for n in node_best:
        node_best[n].sort(key=lambda t: t[0])
    return node_best


# -----------------------------
# Collect node→anchor labels into hex buckets (no borrowing yet)
# -----------------------------
def collect_hex_pairs(
    G: nx.MultiDiGraph,
    node_best: Dict[int, List[Tuple[float, int]]],
    node_to_anchor_int: Dict[int, int],
    res: int,
) -> Dict[str, List[Tuple[np.uint16, int]]]:
    """
    Returns dict: h3_hex -> list of (secs_u16, anchor_int_id)
    """
    buckets: Dict[str, List[Tuple[np.uint16, int]]] = defaultdict(list)
    for n, labels in node_best.items():
        if n not in G:
            continue
        lat, lon = float(G.nodes[n]["y"]), float(G.nodes[n]["x"])
        h = to_cell(lat, lon, res)
        L = buckets[h]
        for secs, src_node in labels:
            aid_int = node_to_anchor_int.get(int(src_node))
            if aid_int is None:
                continue
            s = int(round(float(secs)))
            if s < 0:
                s = 0
            if s > 65534:
                s = 65534
            L.append((np.uint16(s), int(aid_int)))
    return buckets


# -----------------------------
# Borrow neighbors & reduce to top-K (with provenance bits)
# -----------------------------
def reduce_with_borrowing(
    hex_pairs: Dict[str, List[Tuple[np.uint16, int]]],
    K: int,
    borrow_neighbors: bool,
) -> pd.DataFrame:
    """
    From raw pairs, produce one row per hex with columns:
      h3_id(uint64), k(u8), prov(u8), a{i}_id(i32), a{i}_s(u16), a{i}_flags(u8)
    prov bit k is 1 if a{k} was borrowed (not from the hex's own nodes).
    a{i}_flags bit 0 is 1 if a{i} was borrowed.
    """
    print(f"[reduce] Processing {len(hex_pairs)} hexes with borrowing={borrow_neighbors}")
    
    # Don't pre-populate neighbor hex buckets globally - borrow on the fly instead

    rows = []
    for h, L in hex_pairs.items():
        prov = 0  # legacy uint8 bitfield
        slot_flags = [0] * K  # uint8 per slot

        # Candidate list holds (seconds, anchor_id, is_borrowed_flag)
        # Start with candidates from the hex itself.
        candidates_with_provenance: List[Tuple[np.uint16, int, bool]] = [
            (secs, aid, False) for secs, aid in L
        ]

        if borrow_neighbors:
            # Collect candidates from neighbors (on-the-fly borrowing)
            try:
                nbrs = h3.grid_disk(h, 1)
            except AttributeError:
                nbrs = set(h3.k_ring(h, 1))
            for nb in nbrs:
                if nb == h: 
                    continue
                neighbor_pairs = hex_pairs.get(nb)
                if neighbor_pairs:
                    for secs, aid in neighbor_pairs:
                        candidates_with_provenance.append((secs, aid, True))

        # pick top-K with per-anchor min, respecting provenance
        best = _dedupe_sort_topk_with_provenance(candidates_with_provenance, K)

        row = {"h3_id": cell_to_uint64(h)}
        slots_used = 0
        for i in range(K):
            if i < len(best):
                secs, aid_int, is_borrowed = best[i]
                row[f"a{i}_id"] = np.int32(aid_int)
                row[f"a{i}_s"]  = np.uint16(secs)
                slot_flags[i] = 1 if is_borrowed else 0
                slots_used += 1
            else:
                row[f"a{i}_id"] = np.int32(-1)
                row[f"a{i}_s"]  = UNREACH_U16  # unreachable within cutoff
                slot_flags[i] = 0

        # legacy byte kept for backward-compat (bit k == borrowed)
        for i, b in enumerate(slot_flags):
            if b:
                prov = set_bit(prov, i)
        
        row["prov"] = np.uint8(prov)
        row["k"] = np.uint8(slots_used)
        for i, b in enumerate(slot_flags):
            row[f"a{i}_flags"] = np.uint8(b)  # bit 0 = borrowed

        rows.append(row)

    if not rows:
        return pd.DataFrame(
            {"h3_id": pd.Series([], dtype="uint64"),
             "k": pd.Series([], dtype="uint8"),
             "prov": pd.Series([], dtype="uint8")}
        )

    cols = ["h3_id", "k", "prov"]
    for i in range(K):
        cols += [f"a{i}_id", f"a{i}_s", f"a{i}_flags"]
    out = pd.DataFrame(rows)[cols]
    return out


def _dedupe_sort_topk_with_provenance(
    pairs: List[Tuple[np.uint16, int, bool]], K: int
) -> List[Tuple[np.uint16, int, bool]]:
    """Per-anchor min, then sort by secs asc and return top-K with provenance."""
    if not pairs:
        return []
    
    # Store: anchor_id -> (seconds, is_borrowed)
    best_by_anchor: Dict[int, Tuple[np.uint16, bool]] = {}
    
    for s, aid, is_borrowed in pairs:
        prev_s, prev_borrowed = best_by_anchor.get(aid, (None, None))
        
        # Always prefer a better time
        if prev_s is None or int(s) < int(prev_s):
            best_by_anchor[aid] = (s, is_borrowed)
        # Tie-breaking rule: if times are identical, prefer non-borrowed over borrowed
        elif int(s) == int(prev_s) and prev_borrowed and not is_borrowed:
            best_by_anchor[aid] = (s, is_borrowed)

    # Convert dict to list for sorting by (time, anchor_id)
    # item is (anchor_id, (seconds, is_borrowed))
    ordered = sorted(best_by_anchor.items(), key=lambda item: (int(item[1][0]), int(item[0])))
    
    # Format for output
    top = [(s, aid, is_borrowed) for aid, (s, is_borrowed) in ordered[:K]]
    return top

def _dedupe_sort_topk(pairs: List[Tuple[np.uint16, int]], K: int) -> List[Tuple[np.uint16, int]]:
    """Per-anchor min, then sort by secs asc and return top-K."""
    if not pairs:
        return []
    best_by_anchor: Dict[int, np.uint16] = {}
    for s, aid in pairs:
        prev = best_by_anchor.get(aid)
        if (prev is None) or (int(s) < int(prev)):
            best_by_anchor[aid] = s
    ordered = sorted(best_by_anchor.items(), key=lambda t: (int(t[1]), int(t[0])))
    top = [(np.uint16(s), int(aid)) for (aid, s) in ordered[:K]]
    return top


def single_source_dijkstra_multi_target(
    G: nx.MultiDiGraph,
    source_nodes: List[int],
    weight: str,
    cutoff_s: int,
) -> Dict[int, Tuple[float, int]]:
    """
    Single-label multi-source Dijkstra: returns node -> (seconds, src_node_id) for best path only.
    Much more memory efficient than multi-label version.
    """
    node_best: Dict[int, Tuple[float, int]] = {}
    pq: List[Tuple[float, int, int]] = []

    for src in source_nodes:
        heappush(pq, (0.0, int(src), int(src)))

    visited = set()
    while pq:
        dist, u, src = heappop(pq)
        
        if u not in G or dist > cutoff_s:
            continue
        if u in visited:
            continue
        visited.add(u)
        
        # Record best path to this node
        current_best = node_best.get(u)
        if current_best is None or dist < current_best[0]:
            node_best[u] = (dist, src)

        # Relax neighbors
        if hasattr(G, 'out_edges'):
            edges_iter = G.out_edges(u, keys=True, data=True)
        else:
            edges_iter = G.edges(u, keys=True, data=True)
        
        for _, v, k, d in edges_iter:
            tt = d.get(weight)
            if tt is None:
                continue
            nd = dist + float(tt)
            if nd <= cutoff_s and v not in visited:
                heappush(pq, (nd, int(v), int(src)))
    
    return node_best


def k_pass_dijkstra(
    G: nx.MultiDiGraph,
    source_nodes: List[int],
    weight: str,
    cutoff_s: int,
    k_best: int,
) -> Dict[int, List[Tuple[float, int]]]:
    """
    K-pass single-label Dijkstra for ultimate memory efficiency.
    Each pass finds the globally best remaining anchor per node.
    """
    node_results: Dict[int, List[Tuple[float, int]]] = defaultdict(list)
    remaining_sources = set(source_nodes)
    
    for pass_num in range(k_best):
        if not remaining_sources:
            break
            
        print(f"[k-pass] Pass {pass_num + 1}/{k_best}: {len(remaining_sources)} remaining anchors")
        
        # Run single-label Dijkstra
        pass_results = single_source_dijkstra_multi_target(
            G, list(remaining_sources), weight, cutoff_s
        )
        
        # Track which sources won in this pass
        pass_winners = set()
        for node_id, (secs, src) in pass_results.items():
            node_results[node_id].append((secs, src))
            pass_winners.add(src)
        
        # Remove winners from remaining sources for next pass
        remaining_sources -= pass_winners
        print(f"[k-pass] Pass {pass_num + 1} complete: {len(pass_winners)} anchors won paths")
        
        # Memory cleanup
        del pass_results
        gc.collect()
    
    return dict(node_results)


# -----------------------------
# Reduce to long-format travel times
# -----------------------------
def reduce_to_long_format(
    hex_pairs: Dict[str, List[Tuple[np.uint16, int]]], K: int
) -> pd.DataFrame:
    """
    From raw hex->[(secs, anchor_id)] pairs, produce a long-format DataFrame:
    h3_id (uint64), site_id (int32), time_s (uint16)
    
    This version simplifies the output to be a pure long-format table,
    which is easier to process in the merge step.
    """
    print(f"[reduce] Processing {len(hex_pairs)} hexes into long format...")
    rows = []
    for h, pairs in hex_pairs.items():
        # Deduplicate and sort to get the best time for each anchor, then take top K overall.
        # This ensures we don't have multiple entries for the same hex-anchor pair
        # while still limiting the total number of connections per hex to K.
        best_pairs = _dedupe_sort_topk(pairs, K)
        
        h3_uint64 = cell_to_uint64(h)
        for secs, anchor_int_id in best_pairs:
            rows.append({
                "h3_id": h3_uint64,
                "site_id": np.int32(anchor_int_id),
                "time_s": secs,
            })

    if not rows:
        return pd.DataFrame({
            "h3_id": pd.Series([], dtype="uint64"),
            "site_id": pd.Series([], dtype="int32"),
            "time_s": pd.Series([], dtype="uint16"),
        })

    return pd.DataFrame(rows)


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Precompute travel times from POIs to H3 hexes.")
    ap.add_argument("--pbf", required=True, help="Path to .pbf extract")
    ap.add_argument("--pois", required=True,
                    help="Parquet with canonical POIs for the state.")
    ap.add_argument("--mode", required=True, choices=["drive", "walk"])
    ap.add_argument("--res", nargs="+", type=int, default=[8], help="H3 resolutions (e.g., 8 9)")
    ap.add_argument("--cutoff", type=int, default=30, help="Cutoff MINUTES for node→anchor leg")
    ap.add_argument("--k-best", type=int, default=5, help="Max anchors per hex to keep")
    ap.add_argument("--out-times", required=True, help="Output Parquet path for long-format travel times (t_hex)")
    ap.add_argument("--out-sites", required=True, help="Output Parquet path for the generated anchor sites")
    ap.add_argument("--simplify-graph", action="store_true", help="Simplify graph before routing")
    ap.add_argument("--batch-size", type=int, default=500, help="Batch size for anchor processing (default: 500)")
    ap.add_argument("--k-pass-mode", action="store_true", help="Use K-pass single-label Dijkstra for ultimate memory efficiency")
    args = ap.parse_args()

    # Load canonical POIs
    print("[info] Loading canonical POI data...")
    if not os.path.exists(args.pois):
        raise FileNotFoundError(f"Canonical POI file not found at {args.pois}")
    
    # Robustly load GeoParquet file, but without setting CRS to avoid pyproj error
    df = pd.read_parquet(args.pois)
    canonical_pois_gdf = gpd.GeoDataFrame(
        df.drop(columns=['geometry']), 
        geometry=gpd.GeoSeries.from_wkb(df['geometry'])
    )

    # Load graph using the centralized utility function
    G = util_osm.load_graph(args.pbf, args.mode)
    
    # Build Anchor Sites from POIs
    # This replaces the old logic of loading pre-made anchors.
    anchors_df = build_anchor_sites(canonical_pois_gdf, G, args.mode)
    
    if anchors_df.empty:
        raise SystemExit("No anchor sites could be built. Aborting.")

    # Save the generated anchor sites for the merge step
    os.makedirs(os.path.dirname(args.out_sites) or ".", exist_ok=True)
    anchors_df.to_parquet(args.out_sites, index=False)
    print(f"[ok] Saved {len(anchors_df)} anchor sites to {args.out_sites}")

    # --- Start of new native implementation ---

    # 1. Export graph to CSR format
    print("[info] Exporting graph to CSR format...")
    nodes, indptr, indices, w_sec, node_lats, node_lons, nid_to_idx = graph_to_csr(G, "travel_time")
    del G  # Free up memory
    gc.collect()

    # 2. Build anchor mappings aligned to CSR indices
    print("[info] Mapping anchor sites to CSR node indices...")
    anchors_df = anchors_df.sort_values("site_id").reset_index(drop=True)
    anchors_df["anchor_int_id"] = anchors_df.index.astype(np.int32)

    anchor_idx = np.full(len(nodes), -1, dtype=np.int32)
    for node_id, aint in anchors_df[["node_id","anchor_int_id"]].itertuples(index=False):
        j = nid_to_idx.get(node_id)
        if j is not None:
            anchor_idx[j] = aint

    source_idxs = np.flatnonzero(anchor_idx >= 0).astype(np.int32)
    print(f"[info] Found {len(source_idxs)} valid anchor source nodes in the graph.")

    if not source_idxs.any():
        raise SystemExit("No valid anchor nodes found in the graph. Aborting.")

    # 3. Call the native kernel
    print(f"[info] Calling native kernel for k-best search (k={args.k_best}, cutoff={args.cutoff} min)...")
    cutoff_s = int(args.cutoff) * 60
    K = int(args.k_best)
    
    best_src_idx, time_s = kbest_multisource_csr(
        indptr, indices, w_sec, source_idxs, K, cutoff_s, os.cpu_count()
    )

    # Map source node indices back to the stable anchor_int_id
    print("[info] Mapping results back to anchor IDs...")
    best_anchor_int = np.where(best_src_idx >= 0, anchor_idx[best_src_idx], -1).astype(np.int32)

    # 4. Vectorized node->H3 reduction using Polars
    print("[info] Aggregating results into H3 hexes using Polars...")

    # Precompute node->hex per res once
    hex_by_res: Dict[int, np.ndarray] = {}
    for r in args.res:
        hex_by_res[r] = np.array([
            h3.latlng_to_cell(float(lat), float(lon), r)
            for lat, lon in zip(node_lats, node_lons)
        ], dtype=np.uint64)

    out_parts = []
    for r in args.res:
        print(f"[info] Processing resolution {r}...")
        Hn = hex_by_res[r]  # [N]

        # Build long form without huge global repeats if possible
        # Fall back to simple approach for now; optimize if memory hits
        H = np.repeat(Hn, K)
        A = best_anchor_int.ravel()
        T = time_s.ravel()

        M = (A >= 0) & (T < UNREACH_U16)
        df = (
            pl.DataFrame({"h3_id": H[M], "site_id": A[M], "time_s": T[M]})
              .with_columns(pl.col("time_s").cast(pl.UInt16))
        )

        # Min per (hex, site), then per-hex top-K using group-wise sort
        df_min = (
            df.group_by(["h3_id", "site_id"])  # dedup
              .agg(pl.min("time_s").alias("time_s"))
              .with_columns(pl.col("time_s").cast(pl.UInt16))
        )

        df_topk = (
            df_min.group_by("h3_id").agg([
                pl.col("site_id").sort_by("time_s").head(K).alias("site_id"),
                pl.col("time_s").sort_by("time_s").head(K).alias("time_s"),
            ]).explode(["site_id", "time_s"]).with_columns([
                pl.lit(args.mode).alias("mode"),
                pl.lit(np.int32(r)).alias("res"),
                pl.lit(SNAPSHOT_TS).alias("snapshot_ts"),
            ])
        )
        out_parts.append(df_topk)

    out_df = pl.concat(out_parts)
    print(f"[info] Aggregation complete. Total rows: {len(out_df)}")

    # 5. Save final output
    os.makedirs(os.path.dirname(args.out_times) or ".", exist_ok=True)

    print(f"[info] Writing final output to {args.out_times}...")
    table = out_df.to_arrow()

    metadata = {
        "source_pbf": os.path.basename(args.pbf),
        "mode": args.mode,
        "k_best": str(args.k_best),
        "cutoff_minutes": str(args.cutoff),
        "creation_date": SNAPSHOT_TS,
        "dataset_version": config.DATASET_VERSION,
    }
    metadata_bytes = {k: v.encode('utf-8') for k, v in metadata.items()}
    table = table.replace_schema_metadata(metadata_bytes)

    pq.write_table(
        table,
        args.out_times,
        compression="zstd",
        use_dictionary=True
    )
    print(f"[ok] wrote {args.out_times}  rows={len(out_df)}  (long format)")


if __name__ == "__main__":
    main()