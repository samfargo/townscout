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
from pyrosm import OSM
from tqdm import tqdm

SNAPSHOT_TS = time.strftime("%Y-%m-%d")

# -----------------------------
# Sentinels & provenance flags
# -----------------------------
UNREACH_U16 = np.uint16(65535)  # ≥ cutoff or unknown reachability
NODATA_U16  = np.uint16(65534)  # no road node for hex even after borrowing

# provenance byte (uint8): bit k set => the a{k} entry was borrowed from neighbor hexes
def set_bit(u8: int, k: int) -> int:
    return int(u8 | (1 << k))


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
# Graph build
# -----------------------------
def build_graph(pbf: str, mode: str) -> Tuple[nx.MultiDiGraph, pd.DataFrame]:
    """
    Build a routable MultiDiGraph with consistent 'travel_time' per edge (seconds).
    Driving: osmnx speeds + travel_time; prunes private access; simplifies.
    Walking: fixed 4.8 kph travel_time; simplifies.
    Returns the simplified graph AND the original nodes (id,x,y) for optional remap.
    Caches processed nodes/edges as parquet for faster re-runs.
    """
    cache_dir = os.path.join(os.path.dirname(pbf), "cache")
    nodes_path = os.path.join(cache_dir, f"{os.path.basename(pbf)}_{mode}_nodes.parquet")
    edges_path = os.path.join(cache_dir, f"{os.path.basename(pbf)}_{mode}_edges.parquet")
    os.makedirs(cache_dir, exist_ok=True)

    if os.path.exists(nodes_path) and os.path.exists(edges_path):
        print(f"[{mode}] Loading cached graph from {cache_dir}...")
        nodes = pd.read_parquet(nodes_path)
        edges = pd.read_parquet(edges_path)
        if "key" not in edges.columns:
            edges["key"] = 0
        nodes = nodes.set_index("id")
        edges = edges.set_index(["u", "v", "key"])
        G = ox.graph_from_gdfs(nodes, edges)
        # return G, nodes.reset_index()[["id", "x", "y"]] # bug fix for cached version
        return G, nodes.reset_index()[["id", "x", "y"]].rename(columns={"x": "x_orig", "y": "y_orig"})


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
    nodes.index = pd.to_numeric(nodes.index, errors="coerce")
    nodes = nodes[nodes.index.notna()]
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

    # ---- Travel time ----
    if mode == "walk":
        for _, _, k, d in G.edges(keys=True, data=True):
            length_m = float(d.get("length", 0.0))
            d["speed_kph"] = 4.8
            d["travel_time"] = (length_m / 1000.0) / 4.8 * 3600.0
    else:
        # ensure length exists for speeds/travel times; pyrosm normally provides it
        if not all("length" in d for _, _, _, d in G.edges(keys=True, data=True)):
            G = ox.add_edge_lengths(G)
        G = ox.add_edge_speeds(G)
        G = ox.add_edge_travel_times(G)

    # ---- Guardrail: check for valid travel_time ----
    invalid_tt = 0
    for _, _, d in G.edges(data=True):
        tt = d.get("travel_time")
        if tt is None or not isinstance(tt, (int, float)) or tt <= 0:
            invalid_tt += 1
            # patch with small positive value to avoid Dijkstra errors
            d["travel_time"] = 0.1
    if invalid_tt > 0:
        pct = 100 * invalid_tt / max(1, G.number_of_edges())
        print(f"[warning] Patched {invalid_tt} ({pct:.2f}%) edges with invalid/missing 'travel_time' to 0.1s.")

    # ---- Prune private for driving ----
    if mode == "drive":
        rm = []
        for u, v, k, d in G.edges(keys=True, data=True):
            access = d.get("access")
            # pyrosm can deliver list/tuple; normalize to str token(s)
            if isinstance(access, (list, tuple, set)):
                tokens = {str(a).lower() for a in access if a is not None}
                if "private" in tokens or "no" in tokens:
                    rm.append((u, v, k))
            else:
                tok = str(access).lower()
                if tok in ("private", "no"):
                    rm.append((u, v, k))
        if rm:
            G.remove_edges_from(rm)

    # ---- Simplify (merge degree-2 chains) ----
    print(f"[{mode}] Simplifying graph…")
    G = ox.simplify_graph(G)

    # ---- Cache to Parquet ----
    # Deconstruct back to GDFs for saving
    nodes_gdf, edges_gdf = ox.graph_to_gdfs(G)
    nodes_gdf.reset_index().to_parquet(nodes_path)
    edges_gdf.reset_index().to_parquet(edges_path)
    print(f"[{mode}] Cached simplified graph to {cache_dir}")

    # Return simplified graph and original node coordinates for optional remap
    return G, nodes.reset_index()[["id", "x", "y"]]


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

    seen = set()
    while pq:
        dist, u, src = heappop(pq)
        # guard against any stray IDs not present (e.g., after simplification)
        if u not in G:
            continue
        if dist > cutoff_s:
            continue
        key = (u, src)
        if key in seen:
            continue
        seen.add(key)

        L = node_best[u]
        # If we already have K and this is not better than worst, skip insert
        if len(L) < k_best or dist < L[-1][0]:
            # insert keeping list sorted; k_best small so O(K) is fine
            inserted = False
            for i, (old, _) in enumerate(L):
                if dist < old:
                    L.insert(i, (dist, src))
                    inserted = True
                    break
            if not inserted:
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
            if s > 65535:
                s = 65535
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
      h3_id(uint64), prov(uint8), a0_id(int32), a0_s(uint16), a1_id, a1_s, ...
    prov bit k is 1 if a{k} was borrowed (not from the hex's own nodes).
    """
    # Optionally identify empty hexes and plan borrowing
    if borrow_neighbors:
        missing = [h for h, L in hex_pairs.items() if not L]
        print(f"[debug] empty hex buckets after collect: {len(missing)} (sample {missing[:10]})")
        # also hexes not present at all—collect neighborhood from existing keys
        all_hexes = set(hex_pairs.keys())
        for h in list(all_hexes):
            # include immediate neighbors in the universe
            try:
                nbrs = h3.grid_disk(h, 1)
            except AttributeError:
                nbrs = set(h3.k_ring(h, 1))
            for nb in nbrs:
                if nb not in hex_pairs:
                    hex_pairs[nb] = []  # create placeholder so we can fill

    rows = []
    for h, L in hex_pairs.items():
        prov = 0  # uint8 bitfield
        if not L and borrow_neighbors:
            # collect candidates from neighbors
            cands: List[Tuple[np.uint16, int]] = []
            try:
                nbrs = h3.grid_disk(h, 1)
            except AttributeError:
                nbrs = set(h3.k_ring(h, 1))
            for nb in nbrs:
                pairs = hex_pairs.get(nb)
                if pairs:
                    # take up to K best from neighbor to avoid explosion
                    best_nb = _dedupe_sort_topk(pairs, K)
                    cands.extend(best_nb)
            if cands:
                L = cands
        # pick top-K with per-anchor min
        best = _dedupe_sort_topk(L, K)

        row = {"h3_id": cell_to_uint64(h)}
        for i in range(K):
            if i < len(best):
                secs, aid_int = best[i]
                row[f"a{i}_id"] = np.int32(aid_int)
                row[f"a{i}_s"]  = np.uint16(secs)
                # borrowed if original hex had no pairs
                if not hex_pairs.get(h):
                    prov = set_bit(prov, i)
            else:
                row[f"a{i}_id"] = np.int32(-1)
                row[f"a{i}_s"]  = UNREACH_U16  # unreachable within cutoff
        row["prov"] = np.uint8(prov)
        rows.append(row)

    if not rows:
        return pd.DataFrame(
            {"h3_id": pd.Series([], dtype="uint64"),
             "prov": pd.Series([], dtype="uint8")}
        )

    cols = ["h3_id", "prov"]
    for i in range(K):
        cols += [f"a{i}_id", f"a{i}_s"]
    out = pd.DataFrame(rows)[cols]
    return out


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


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Precompute Hex→Anchor seconds (T_hex) with true global top-K")
    ap.add_argument("--pbf", required=True, help="Path to .pbf extract")
    ap.add_argument("--anchors", required=True,
                    help="Parquet with anchors; must have columns: id (stable), node_id (int), [mode]")
    ap.add_argument("--mode", required=True, choices=["drive", "walk"])
    ap.add_argument("--res", nargs="+", type=int, default=[8], help="H3 resolutions (e.g., 8 9)")
    ap.add_argument("--cutoff", type=int, default=90, help="Cutoff MINUTES for node→anchor leg")
    ap.add_argument("--batch", type=int, default=400, help="Anchors per batch for multi-source pass")
    ap.add_argument("--k-best", type=int, default=2, help="K anchors per hex to keep")
    ap.add_argument("--borrow-neighbors", action="store_true", help="Borrow neighbors for sparse/empty hexes")
    ap.add_argument("--out", required=True, help="Output Parquet path for T_hex")
    ap.add_argument("--anchor-index-out", default="", help="Optional: write anchor_int_id index parquet here")
    ap.add_argument("--remap-missing-anchors", action="store_true",
                    help="Snap anchors dropped by simplify to nearest surviving node")
    args = ap.parse_args()

    # Load anchors and filter by mode if present
    anchors_df = pd.read_parquet(args.anchors)
    if "mode" in anchors_df.columns:
        anchors_df = anchors_df[anchors_df["mode"] == args.mode]
    if not {"id", "node_id"}.issubset(anchors_df.columns):
        raise SystemExit("anchors parquet must include columns: id, node_id")

    anchors_df = anchors_df[["id", "node_id"]].copy()
    anchors_df["node_id"] = pd.to_numeric(anchors_df["node_id"], errors="coerce").astype("Int64")
    anchors_df = anchors_df.dropna(subset=["node_id"])
    anchors_df["node_id"] = anchors_df["node_id"].astype("int64")

    # Numeric anchor IDs for compact tiles
    # Build a stable map from stable id (any type) -> int32
    unique_stable_ids = pd.Index(anchors_df["id"].astype("string").unique()).tolist()
    stable_to_int = {sid: i for i, sid in enumerate(unique_stable_ids)}
    int_to_stable = pd.DataFrame({
        "anchor_int_id": np.arange(len(unique_stable_ids), dtype=np.int32),
        "anchor_stable_id": unique_stable_ids,
    })
    # node_id -> anchor_int_id for fast lookup
    node_to_anchor_int = dict(
        zip(anchors_df["node_id"].tolist(),
            [stable_to_int[str(sid)] for sid in anchors_df["id"].astype("string").tolist()])
    )

    print(f"[info] mode={args.mode} anchors={len(anchors_df)} "
          f"cutoff={args.cutoff} min K={args.k_best} batches≈{(len(anchors_df)+args.batch-1)//args.batch}")

    # Build graph
    G, nodes_xy = build_graph(args.pbf, args.mode)
    # Direction-correct query graph (node -> anchor)
    if args.mode == "drive":
        Gq = G.reverse(copy=False)
    else:
        Gq = G.to_undirected(reciprocal=False)

    # Optional: remap anchors whose node_id vanished after simplify
    if args.remap_missing_anchors:
        present = set(Gq.nodes())
        # Join original coordinates for all anchors
        nodes_xy = nodes_xy.rename(columns={"id": "node_id"})
        anchors_xy = anchors_df.merge(nodes_xy, on="node_id", how="left")
        missing = anchors_xy[~anchors_xy["node_id"].isin(present)].dropna(subset=["x", "y"])
        if not missing.empty:
            xs = missing["x"].to_numpy()
            ys = missing["y"].to_numpy()
            # nearest_nodes supports vectorized arrays
            nearest = ox.distance.nearest_nodes(G, xs, ys)
            remap = pd.DataFrame({
                "node_id": missing["node_id"].to_numpy(dtype=np.int64),
                "node_id_new": np.asarray(nearest, dtype=np.int64),
            })
            # Apply remap
            anchors_df = anchors_df.merge(remap, on="node_id", how="left")
            anchors_df["node_id"] = anchors_df["node_id_new"].fillna(anchors_df["node_id"]).astype("int64")
            anchors_df = anchors_df.drop(columns=["node_id_new"])
            kept = anchors_df["node_id"].isin(Gq.nodes()).mean()
            print(f"[remap] anchors present after remap: {kept:.1%}")
            # Rebuild node → anchor_int map (ids may have changed)
            node_to_anchor_int = dict(
                zip(anchors_df["node_id"].tolist(),
                    [stable_to_int[str(sid)] for sid in anchors_df["id"].astype("string").tolist()])
            )
        else:
            print("[remap] No coords for missing anchors or none missing; skipping remap.")

    cutoff_s = int(args.cutoff * 60)

    # ---- Guardrail: check anchor presence ----
    all_anchor_nodes = anchors_df["node_id"].unique()
    present_nodes = [n for n in all_anchor_nodes if n in Gq]
    present_pct = len(present_nodes) / len(all_anchor_nodes) * 100 if len(all_anchor_nodes) > 0 else 0
    print(f"[guardrail] Anchor presence in graph: {present_pct:.1f}% ({len(present_nodes)} / {len(all_anchor_nodes)})")
    if present_pct < 80.0:
        print("[warning] Low anchor coverage. Many sources will be dropped. Consider --remap-missing-anchors.")

    # Prepare global collectors per res
    global_hex_pairs_by_res: Dict[int, Dict[str, List[Tuple[np.uint16, int]]]] = {
        r: defaultdict(list) for r in args.res
    }

    # Process in batches
    batches = [anchors_df.iloc[i:i + args.batch] for i in range(0, len(anchors_df), args.batch)]
    for bi, batch_df in enumerate(tqdm(batches, desc="Batches", unit="batch")):
        # Filter sources to nodes present in the query graph to avoid NX errors
        all_sources = batch_df["node_id"].astype(int).tolist()
        source_nodes = [n for n in all_sources if n in Gq]
        dropped = len(all_sources) - len(source_nodes)
        kept_pct = (len(source_nodes) / max(1, len(all_sources))) * 100.0
        msg = f"[batch {bi+1}/{len(batches)}] sources={len(source_nodes)}"
        if dropped:
            msg += f" (dropped {dropped} not-in-graph, kept {kept_pct:.1f}%)"
        print(msg)
        if not source_nodes:
            continue
        node_best = multi_source_kbest(Gq, source_nodes, "travel_time", cutoff_s, k_best=max(2, args.k_best))

        if not node_best:
            print(f"[batch {bi+1}] no coverage within cutoff")
            continue

        for r in args.res:
            bucket = collect_hex_pairs(G, node_best, node_to_anchor_int, res=r)
            # merge into global (append)
            glob = global_hex_pairs_by_res[r]
            for h, pairs in bucket.items():
                glob[h].extend(pairs)

        # Quick per-batch QA (a0 only, approximate from nodes→hex)
        secs0 = []
        for labels in node_best.values():
            if labels:
                secs0.append(labels[0][0])
        if secs0:
            med = float(np.median(secs0))
            p95 = float(np.percentile(secs0, 95))
            print(f"[QA] node→nearest anchor: median={med:.0f}s p95={p95:.0f}s")

    # Build final per-res DataFrames with borrowing + global top-K
    out_parts = []
    for r in args.res:
        print(f"[reduce] res={r} borrowing={args.borrow_neighbors}")
        T_hex = reduce_with_borrowing(global_hex_pairs_by_res[r], K=args.k_best,
                                      borrow_neighbors=args.borrow_neighbors)
        if not len(T_hex):
            continue
        T_hex["mode"] = args.mode
        T_hex["res"] = np.int32(r)
        T_hex["snapshot_ts"] = SNAPSHOT_TS
        out_parts.append(T_hex)

        # QA per res from hex table
        a0 = T_hex["a0_s"].replace({NODATA_U16: np.nan, UNREACH_U16: np.nan}).astype("float")
        med = float(np.nanmedian(a0)) if np.isfinite(a0).any() else float("nan")
        p95 = float(np.nanpercentile(a0, 95)) if np.isfinite(a0).any() else float("nan")
        have_k = ~(T_hex[f"a{args.k_best-1}_s"] == UNREACH_U16)
        pct_k = 100.0 * float(have_k.mean()) if len(have_k) else 0.0
        print(f"[QA] res={r} a0_s median={med:.0f}s p95={p95:.0f}s  hexes with ≥{args.k_best} anchors={pct_k:.1f}%")

    if not out_parts:
        raise SystemExit("No output produced; check anchors/graph/cutoff.")

    out_df = pd.concat(out_parts, ignore_index=True)

    # Final column order
    cols = ["h3_id", "prov"]
    for k in range(args.k_best):
        cols += [f"a{k}_id", f"a{k}_s"]
    cols += ["mode", "res", "snapshot_ts"]
    out_df = out_df[cols]

    # Write T_hex parquet
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    out_df.to_parquet(args.out, index=False)
    print(f"[ok] wrote {args.out}  rows={len(out_df)}  (one row per hex per res)")

    # Optional: write anchor index parquet (int32 → stable id)
    if args.anchor_index_out:
        os.makedirs(os.path.dirname(args.anchor_index_out) or ".", exist_ok=True)
        int_to_stable.to_parquet(args.anchor_index_out, index=False)
        print(f"[ok] wrote anchor index → {args.anchor_index_out} "
              f"(rows={len(int_to_stable)})")


if __name__ == "__main__":
    main()