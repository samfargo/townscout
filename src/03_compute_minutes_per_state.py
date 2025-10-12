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
from tqdm import tqdm
import polars as pl

from graph.pyrosm_csr import load_or_build_csr
from graph.csr_utils import build_rev_csr
from t_hex import kbest_multisource_bucket_csr, aggregate_h3_topk_precached
import config

# Import the shared anchor site builder from 03_build_anchor_sites.py to avoid duplication
import importlib.util, os
_THIS_DIR = os.path.dirname(__file__)
_ANCHOR_BUILDER_PATH = os.path.join(_THIS_DIR, "03_build_anchor_sites.py")
spec = importlib.util.spec_from_file_location("_build_anchor_sites", _ANCHOR_BUILDER_PATH)
_mod = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(_mod)
build_anchor_sites_from_nodes = _mod.build_anchor_sites_from_nodes

SNAPSHOT_TS = time.strftime("%Y-%m-%d")


# build_anchor_sites_from_nodes is now imported from 03_build_anchor_sites.py


# -----------------------------
# Sentinels & provenance flags
# -----------------------------
UNREACH_U16 = config.UNREACH_U16
NODATA_U16 = config.NODATA_U16

# (Removed unused top-K helpers and provenance utilities.)



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
    ap.add_argument("--out-sites", required=False, help="Output Parquet path for the generated anchor sites (if building inline)")
    ap.add_argument("--anchors", required=False, help="Optional: path to prebuilt anchor sites parquet (preferred)")
    ap.add_argument("--simplify-graph", action="store_true", help="(no-op) kept for CLI compatibility")
    ap.add_argument("--batch-size", type=int, default=500, help="Batch size for anchor processing (default: 500)")
    ap.add_argument("--k-pass-mode", action="store_true", help="(no-op) K-pass kept for compatibility; kernel handles K-pass internally")
    ap.add_argument("--progress", action="store_true", help="Show progress bars/logs during heavy stages")
    ap.add_argument("--overflow-cutoff", type=int, default=90, help="Overflow cutoff MINUTES for nodes missing K labels (default: 90; set equal to --cutoff to disable overflow)")
    ap.add_argument("--threads", type=int, default=1, help="Threads for k-best compute. Use 1 to compute all sources in a single pass (fastest, avoids repeated per-chunk traversals).")
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

    # Load prebuilt anchors or build inline
    if args.anchors and os.path.exists(args.anchors):
        print(f"[info] Loading prebuilt anchors from {args.anchors} ...")
        anchors_df = pd.read_parquet(args.anchors)
    else:
        print("[info] Building anchors inline (consider precomputing via anchors step)...")
        anchors_df = build_anchor_sites_from_nodes(canonical_pois_gdf, node_ids, node_lats, node_lons, args.mode, indptr)
        if anchors_df.empty:
            raise SystemExit("No anchor sites could be built. Aborting.")
        if args.out_sites:
            os.makedirs(os.path.dirname(args.out_sites) or ".", exist_ok=True)
            anchors_df.to_parquet(args.out_sites, index=False)
            print(f"[ok] Saved {len(anchors_df)} anchor sites to {args.out_sites}")
    

    # --- Start of new native implementation ---

    # 1. Build anchor mappings aligned to CSR indices
    print("[info] Mapping anchor sites to CSR node indices...")
    # Use existing anchor_int_id if present; else assign deterministically by site_id
    if "anchor_int_id" not in anchors_df.columns:
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
    print(f"[info] Preparing adjacency (transpose for node→anchor times)...")
    # Use shared CSR transpose utility
    indptr_rev, indices_rev, w_rev = build_rev_csr(indptr, indices, w_sec)

    print(f"[info] Calling native kernel (bucket K-pass) for k-best search (k={args.k_best}, cutoff={args.cutoff} min, overflow={args.overflow_cutoff} min, threads={args.threads})...")
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

    # Important: pass threads=1 by default so the kernel computes all sources in a single pass.
    # The parallel path partitions sources and repeats graph traversals per chunk, which can be
    # substantially slower overall despite parallelism.
    best_src_idx, time_s = kbest_multisource_bucket_csr(
        indptr_rev, indices_rev, w_rev, source_idxs, K, cutoff_primary_s, cutoff_overflow_s, int(max(1, args.threads)), bool(args.progress), (_kb_cb if args.progress else None)
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
            "anchor_int_id": np.asarray(site_id_arr, dtype=np.int32),
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
