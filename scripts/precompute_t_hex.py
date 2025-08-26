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
import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
from pyrosm import OSM
from scipy.spatial import cKDTree
from tqdm import tqdm

from src import util_h3, util_osm, config

SNAPSHOT_TS = time.strftime("%Y-%m-%d")

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
    ids = np.fromiter(G.nodes, dtype=np.int64)
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
        # Optimization: if we already have K and this is not better than the worst,
        # we can't improve on this node, so no need to insert or relax neighbors.
        if len(L) >= k_best and dist >= L[-1][0]:
            continue

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
    all_hexes_in_play = set(hex_pairs.keys())
    if borrow_neighbors:
        # Pre-create empty lists for any hexes that are neighbors but not in the keys
        # to ensure they are processed.
        for h in list(all_hexes_in_play):
            try:
                nbrs = h3.grid_disk(h, 1)
            except AttributeError:
                nbrs = set(h3.k_ring(h, 1))
            for nb in nbrs:
                if nb not in hex_pairs:
                    hex_pairs[nb] = []

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
            # Collect candidates from neighbors
            try:
                nbrs = h3.grid_disk(h, 1)
            except AttributeError:
                nbrs = set(h3.k_ring(h, 1))
            for nb in nbrs:
                if nb == h: continue # don't borrow from self
                
                neighbor_pairs = hex_pairs.get(nb)
                if neighbor_pairs:
                    # Mark all candidates from neighbors as borrowed
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

    # Load and validate anchors
    print("[info] Loading anchor data...")
    anchors_df = pd.read_parquet(args.anchors)
    if "node_id" not in anchors_df.columns:
        raise ValueError("Anchors parquet must contain 'node_id' column")
    if "id" not in anchors_df.columns:
        raise ValueError("Anchors parquet must contain 'id' column for stable IDs")

    # Create anchor mappings
    # stable id (string) -> integer id (for compact parquet)
    # node_id -> integer_id
    anchors_df = anchors_df.sort_values("id").reset_index(drop=True)
    anchors_df["anchor_int_id"] = anchors_df.index.astype(np.int32)
    node_to_anchor_int = anchors_df.set_index("node_id")["anchor_int_id"].to_dict()
    int_to_stable = anchors_df[["anchor_int_id", "id"]].rename(columns={"id": "stable_id"})


    # Load graph using the centralized utility function
    G = util_osm.load_graph(args.pbf, args.mode)

    # Prepare metadata to embed in output
    metadata = {
        "source_pbf": os.path.basename(args.pbf),
        "mode": args.mode,
        "k_best": str(args.k_best),
        "cutoff_minutes": str(args.cutoff),
        "borrow_neighbors": str(args.borrow_neighbors),
        "graph_config": str(config.GRAPH_CONFIG.get(args.mode, {})),
        "creation_date": SNAPSHOT_TS,
        "dataset_version": config.DATASET_VERSION,
        "id_space": "anchor_int_id",
    }

    # Guardrail: Check that most anchors are actually in the graph
    anchor_nodes = anchors_df["node_id"].unique()
    present_nodes_mask = anchors_df["node_id"].isin(G.nodes)
    present_nodes = anchors_df.loc[present_nodes_mask, "node_id"].unique()
    present_pct = len(present_nodes) / len(anchor_nodes) * 100 if len(anchor_nodes) > 0 else 0
    print(f"[guardrail] Anchor presence in graph: {present_pct:.1f}% ({len(present_nodes)} / {len(anchor_nodes)})")
    if present_pct < 80.0:
        print("[warning] Low anchor coverage. Many sources will be dropped. Consider --remap-missing-anchors.")

    if args.remap_missing_anchors:
        missing = anchors_df[~present_nodes_mask]
        if not missing.empty:
            if 'lon' not in missing.columns or 'lat' not in missing.columns:
                 print("[warning] --remap-missing-anchors needs 'lon' and 'lat' in anchors file. Skipping remap.")
            else:
                print(f"[info] Remapping {len(missing)} anchors not found in graph...")
                ids, tree, lat0, m_per_deg = build_node_kdtree(G)
                xy = np.c_[(missing["lon"].to_numpy()*np.cos(lat0))*m_per_deg,
                           (missing["lat"].to_numpy())*m_per_deg]
                d, idx = tree.query(xy, k=1)
                anchors_df.loc[missing.index, "node_id"] = ids[idx].astype("int64")
                # after remapping, need to update node_to_anchor_int
                node_to_anchor_int = anchors_df.set_index("node_id")["anchor_int_id"].to_dict()
                print(f"[info] Remapped {len(missing)} anchors.")


    # Prepare global collectors per res
    global_hex_pairs_by_res: Dict[int, Dict[str, List[Tuple[np.uint16, int]]]] = {
        r: defaultdict(list) for r in args.res
    }

    # Process in batches
    batches = [anchors_df.iloc[i:i + args.batch] for i in range(0, len(anchors_df), args.batch)]
    for bi, batch_df in enumerate(tqdm(batches, desc="Batches", unit="batch")):
        # Filter sources to nodes present in the query graph to avoid NX errors
        all_sources = batch_df["node_id"].astype(int).tolist()
        source_nodes = [n for n in all_sources if n in G]
        dropped = len(all_sources) - len(source_nodes)
        kept_pct = (len(source_nodes) / max(1, len(all_sources))) * 100.0
        msg = f"[batch {bi+1}/{len(batches)}] sources={len(source_nodes)}"
        if dropped:
            msg += f" (dropped {dropped} not-in-graph, kept {kept_pct:.1f}%)"
        print(msg)
        if not source_nodes:
            continue
        node_best = multi_source_kbest(G, source_nodes, "travel_time", cutoff_s=args.cutoff * 60, k_best=max(2, args.k_best))

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
        pct_k = 100.0 * float((T_hex["k"] >= args.k_best).mean()) if len(T_hex) else 0.0
        print(f"[QA] res={r} a0_s median={med:.0f}s p95={p95:.0f}s  hexes with ≥{args.k_best} anchors={pct_k:.1f}%")

    if not out_parts:
        raise SystemExit("No output produced; check anchors/graph/cutoff.")

    out_df = pd.concat(out_parts, ignore_index=True)

    # Final column order
    cols = ["h3_id", "k", "prov"]
    for k in range(args.k_best):
        cols += [f"a{k}_id", f"a{k}_s", f"a{k}_flags"]
    cols += ["mode", "res", "snapshot_ts"]
    out_df = out_df[cols]

    # Schema validation
    for i in range(args.k_best):
        assert out_df.dtypes[f"a{i}_s"] == "uint16"
        assert out_df.dtypes[f"a{i}_id"] == "int32"
        assert out_df.dtypes[f"a{i}_flags"] == "uint8"
    assert out_df.dtypes["k"] == "uint8"

    # Write T_hex parquet
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    
    table = pa.Table.from_pandas(out_df, preserve_index=False)
    
    # Add metadata
    metadata_bytes = {k: v.encode('utf-8') for k, v in metadata.items()}
    table = table.replace_schema_metadata(metadata_bytes)

    pq.write_table(table, args.out)
    print(f"[ok] wrote {args.out}  rows={len(out_df)}  (one row per hex per res)")

    # Optional: write anchor index parquet (int32 → stable id)
    if not args.anchor_index_out:
        args.anchor_index_out = os.path.splitext(args.out)[0] + ".anchor_index.parquet"

    if args.anchor_index_out:
        os.makedirs(os.path.dirname(args.anchor_index_out) or ".", exist_ok=True)
        int_to_stable.to_parquet(args.anchor_index_out, index=False)
        print(f"[ok] wrote anchor index → {args.anchor_index_out} "
              f"(rows={len(int_to_stable)})")


if __name__ == "__main__":
    main()