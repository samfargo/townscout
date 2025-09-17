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
from typing import Dict, Iterable, List, Tuple

import h3
import numpy as np
import pandas as pd
import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
from scipy.spatial import cKDTree
from tqdm import tqdm
import uuid
import polars as pl

from graph.pyrosm_csr import load_or_build_csr
from t_hex import kbest_multisource_bucket_csr, aggregate_h3_topk_precached
import util_h3
import config

SNAPSHOT_TS = time.strftime("%Y-%m-%d")


def build_anchor_sites_from_nodes(
    canonical_pois: gpd.GeoDataFrame,
    node_ids: np.ndarray,
    node_lats: np.ndarray,
    node_lons: np.ndarray,
    mode: str,
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
    if canonical_pois.empty or node_ids.size == 0:
        print("[warn] Canonical POIs or graph is empty. No anchor sites will be built.")
        return pd.DataFrame()
    
    # 1. Build a KD-tree from the graph nodes.
    print(f"[info] Building KD-tree from {len(node_ids)} graph nodes...")
    # Build KD-tree from node arrays
    lat0 = float(np.deg2rad(float(np.mean(node_lats))))
    m_per_deg = 111000.0
    X = np.c_[ (node_lons.astype(np.float64) * np.cos(lat0)) * m_per_deg, node_lats.astype(np.float64) * m_per_deg ]
    tree = cKDTree(X)
    
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
    node_coords = pd.DataFrame({
        'node_id': node_ids,
        'lon': node_lons.astype(np.float64),
        'lat': node_lats.astype(np.float64),
    }).set_index('node_id')
    sites = sites.join(node_coords, on='node_id', how='left')
    
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
    ap.add_argument("--simplify-graph", action="store_true", help="(no-op) kept for CLI compatibility")
    ap.add_argument("--batch-size", type=int, default=500, help="Batch size for anchor processing (default: 500)")
    ap.add_argument("--k-pass-mode", action="store_true", help="(no-op) K-pass kept for compatibility; kernel handles K-pass internally")
    ap.add_argument("--progress", action="store_true", help="Show progress bars/logs during heavy stages")
    ap.add_argument("--overflow-cutoff", type=int, default=90, help="Overflow cutoff MINUTES for nodes missing K labels (default: 90; set equal to --cutoff to disable overflow)")
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

    # Load or build CSR graph directly from Pyrosm (cached)
    print("[info] Loading/building CSR graph from Pyrosm cache...")
    # Only track k-best stage; disable progress elsewhere
    node_ids, indptr, indices, w_sec, node_lats, node_lons, node_h3_by_res, res_used = load_or_build_csr(args.pbf, args.mode, args.res, False)

    # Build Anchor Sites from POIs using node arrays
    anchors_df = build_anchor_sites_from_nodes(canonical_pois_gdf, node_ids, node_lats, node_lons, args.mode)
    
    if anchors_df.empty:
        raise SystemExit("No anchor sites could be built. Aborting.")

    # Save the generated anchor sites for the merge step
    os.makedirs(os.path.dirname(args.out_sites) or ".", exist_ok=True)
    anchors_df.to_parquet(args.out_sites, index=False)
    print(f"[ok] Saved {len(anchors_df)} anchor sites to {args.out_sites}")
    

    # --- Start of new native implementation ---

    # 1. Build anchor mappings aligned to CSR indices
    print("[info] Mapping anchor sites to CSR node indices...")
    anchors_df = anchors_df.sort_values("site_id").reset_index(drop=True)
    anchors_df["anchor_int_id"] = anchors_df.index.astype(np.int32)

    # Build mapping from OSM node id -> CSR index
    nid_to_idx = {int(n): i for i, n in enumerate(node_ids.tolist())}
    anchor_idx = np.full(len(node_ids), -1, dtype=np.int32)
    for node_id, aint in anchors_df[["node_id","anchor_int_id"]].itertuples(index=False):
        j = nid_to_idx.get(int(node_id))
        if j is not None:
            anchor_idx[j] = int(aint)

    source_idxs = np.flatnonzero(anchor_idx >= 0).astype(np.int32)
    print(f"[info] Found {len(source_idxs)} valid anchor source nodes in the graph.")

    if not source_idxs.any():
        raise SystemExit("No valid anchor nodes found in the graph. Aborting.")

    # 3. Call the native kernel
    print(f"[info] Calling native kernel (bucket K-pass) for k-best search (k={args.k_best}, cutoff={args.cutoff} min)...")
    cutoff_primary_s = int(args.cutoff) * 60
    # Allow tuning overflow cutoff to trade accuracy for speed
    cutoff_overflow_s = int(args.overflow_cutoff) * 60
    K = int(args.k_best)

    # Live progress bar for k-best only
    kb_pbar = None
    def _kb_cb(done: int, total: int):
        nonlocal kb_pbar
        if kb_pbar is None:
            kb_pbar = tqdm(total=total, desc="k-best", disable=not args.progress)
        elif kb_pbar.total != total:
            try:
                kb_pbar.reset(total=total)
            except Exception:
                kb_pbar.total = total
                kb_pbar.refresh()
        kb_pbar.update(1)

    best_src_idx, time_s = kbest_multisource_bucket_csr(
        indptr, indices, w_sec, source_idxs, K, cutoff_primary_s, cutoff_overflow_s, os.cpu_count(), bool(args.progress), (_kb_cb if args.progress else None)
    )
    if kb_pbar is not None:
        kb_pbar.close()

    # Map source node indices back to the stable anchor_int_id
    print("[info] Mapping results back to anchor IDs...")
    best_anchor_int = np.where(best_src_idx >= 0, anchor_idx[best_src_idx], -1).astype(np.int32)

    # 4. Node->H3 aggregation + per-hex top-K in native Rust
    print("[info] Aggregating results into H3 hexes (precomputed H3) using native kernel...")
    h3_id_arr, site_id_arr, time_arr, res_arr = aggregate_h3_topk_precached(
        node_h3_by_res, best_anchor_int, time_s, np.array(res_used, dtype=np.int32), K, int(UNREACH_U16), os.cpu_count(), False
    )

    out_df = (
        pl.DataFrame({
            "h3_id": np.asarray(h3_id_arr, dtype=np.uint64),
            "site_id": np.asarray(site_id_arr, dtype=np.int32),
            "time_s": np.asarray(time_arr, dtype=np.uint16),
            "res": np.asarray(res_arr, dtype=np.int32),
        })
        .with_columns([
            pl.lit(args.mode).alias("mode"),
            pl.lit(SNAPSHOT_TS).alias("snapshot_ts"),
        ])
    )
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
