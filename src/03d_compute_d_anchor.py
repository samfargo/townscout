"""
Compute D_anchor for brands: anchor_int_id -> seconds to nearest anchor containing that brand.

This enables brand-level filtering in anchor-mode without needing per-brand
columns in the tiles. Layout is Hive-partitioned Parquet:

  data/d_anchor_brand/mode=<0|2>/brand_id=<brand_id>/part-000.parquet

Columns:
  - anchor_int_id: int32
  - seconds: uint16 (65535 sentinel for unreachable)
  - snapshot_ts: str (YYYY-MM-DD)

Usage:
  PY=PYTHONPATH=src .venv/bin/python src/03d_compute_d_anchor.py \
    --pbf data/osm/massachusetts.osm.pbf \
    --anchors data/anchors/massachusetts_drive_sites.parquet \
    --mode drive --brands-threshold 5

Or target a single brand:
  PY=PYTHONPATH=src .venv/bin/python src/03d_compute_d_anchor.py \
    --pbf data/osm/massachusetts.osm.pbf \
    --anchors data/anchors/massachusetts_drive_sites.parquet \
    --mode drive --brand starbucks
"""
from __future__ import annotations
import argparse
import os
import time
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import polars as pl

from graph.pyrosm_csr import load_or_build_csr
from t_hex import kbest_multisource_bucket_csr
import config

SNAPSHOT_TS = time.strftime("%Y-%m-%d")


def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def _build_anchor_mappings(anchors_df: pd.DataFrame, node_ids: np.ndarray) -> Tuple[np.ndarray, Dict[int,int]]:
    if "anchor_int_id" not in anchors_df.columns:
        anchors_df = anchors_df.sort_values("site_id").reset_index(drop=True)
        anchors_df["anchor_int_id"] = anchors_df.index.astype(np.int32)
    nid_to_idx = {int(n): i for i, n in enumerate(node_ids.tolist())}
    anchor_idx = np.full(len(node_ids), -1, dtype=np.int32)
    for node_id, aint in anchors_df[["node_id","anchor_int_id"]].itertuples(index=False):
        j = nid_to_idx.get(int(node_id))
        if j is not None:
            anchor_idx[j] = int(aint)
    return anchor_idx, nid_to_idx


def _collect_anchor_brand_lists(anchors_df: pd.DataFrame) -> Dict[int, List[str]]:
    """Return mapping anchor_int_id -> list of canonical brand ids."""
    out: Dict[int, List[str]] = {}
    for aint, brands in anchors_df[["anchor_int_id", "brands"]].itertuples(index=False):
        lst: List[str] = []
        if isinstance(brands, (list, np.ndarray)):
            if isinstance(brands, np.ndarray):
                brands = brands.tolist()
            lst = [str(b) for b in brands]
        out[int(aint)] = lst
    return out


def _build_rev_csr(indptr: np.ndarray, indices: np.ndarray, w_sec: np.ndarray) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    N = int(indptr.shape[0] - 1)
    M = int(indices.shape[0])
    indptr_rev = np.zeros(N + 1, dtype=np.int64)
    for u in range(N):
        lo, hi = int(indptr[u]), int(indptr[u+1])
        for v in indices[lo:hi]:
            indptr_rev[int(v) + 1] += 1
    np.cumsum(indptr_rev, out=indptr_rev)
    indices_rev = np.empty(M, dtype=np.int32)
    w_rev = np.empty(M, dtype=np.uint16)
    cursor = indptr_rev.copy()
    for u in range(N):
        lo, hi = int(indptr[u]), int(indptr[u+1])
        for i in range(lo, hi):
            v = int(indices[i])
            pos = cursor[v]
            indices_rev[pos] = np.int32(u)
            w_rev[pos] = w_sec[i]
            cursor[v] = pos + 1
    return indptr_rev, indices_rev, w_rev


def main():
    ap = argparse.ArgumentParser(description="Compute D_anchor brand tables (anchor->brand seconds)")
    ap.add_argument("--pbf", required=True)
    ap.add_argument("--anchors", required=True)
    ap.add_argument("--mode", required=True, choices=["drive", "walk"])
    ap.add_argument("--brand", action="append", default=[], help="Brand id (canonical or alias) to compute; can repeat")
    ap.add_argument("--brands-threshold", type=int, default=0, help="If >0, compute for all brands with >= this many anchors")
    ap.add_argument("--allowlist", default="data/brands/allowlist.txt", help="Optional path to brand allowlist (one canonical brand_id per line)")
    ap.add_argument("--cutoff", type=int, default=30)
    ap.add_argument("--overflow-cutoff", type=int, default=90)
    ap.add_argument("--threads", type=int, default=1)
    ap.add_argument("--out-dir", default="data/d_anchor_brand")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    anchors_df = pd.read_parquet(args.anchors)
    if "anchor_int_id" not in anchors_df.columns:
        anchors_df = anchors_df.sort_values("site_id").reset_index(drop=True)
        anchors_df["anchor_int_id"] = anchors_df.index.astype(np.int32)

    # Build brand frequency table at anchor sites
    brand_counts: Dict[str, int] = {}
    for brands in anchors_df["brands"].dropna().values:
        if not isinstance(brands, (list, np.ndarray)):
            continue
        if isinstance(brands, np.ndarray):
            brands = brands.tolist()
        for b in brands:
            bid = str(b)
            brand_counts[bid] = brand_counts.get(bid, 0) + 1

    # Resolve brand targets
    targets: List[str] = list(dict.fromkeys(map(str, args.brand)))
    # Optional allowlist file
    if (not targets) and args.allowlist and os.path.isfile(args.allowlist):
        try:
            with open(args.allowlist, "r") as f:
                allowed = [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
            targets = [b for b in allowed]
            print(f"[info] Loaded {len(targets)} brands from allowlist {args.allowlist}")
        except Exception as e:
            print(f"[warn] Failed to read brand allowlist {args.allowlist}: {e}")
    if args.brands_threshold > 0 and not targets:
        targets += [b for b, cnt in brand_counts.items() if cnt >= args.brands_threshold]
    targets = sorted(set(targets))
    if not targets:
        print("[warn] No brand targets specified; nothing to compute.")
        return

    # Build/load CSR and mappings
    node_ids, indptr, indices, w_sec, node_lats, node_lons, node_h3_by_res, res_used = load_or_build_csr(args.pbf, args.mode, [8], False)
    anchor_idx, _ = _build_anchor_mappings(anchors_df, node_ids)
    anchor_to_brands = _collect_anchor_brand_lists(anchors_df)

    # Build brand -> list of node indices serving as sources for multi-source search
    from collections import defaultdict
    brand_to_source_idxs: Dict[str, np.ndarray] = {}
    temp = defaultdict(list)
    for j, aint in enumerate(anchor_idx.tolist()):
        if aint < 0:
            continue
        for b in anchor_to_brands.get(int(aint), []):
            temp[str(b)].append(j)
    for b, lst in temp.items():
        brand_to_source_idxs[b] = np.asarray(lst, dtype=np.int32)

    indptr_rev, indices_rev, w_rev = _build_rev_csr(indptr, indices, w_sec)
    cutoff_primary_s = int(args.cutoff) * 60
    cutoff_overflow_s = int(args.overflow_cutoff) * 60

    out_base = os.path.join(args.out_dir, f"mode={0 if args.mode=='drive' else 2}")
    _ensure_dir(out_base)

    for raw in targets:
        canon = str(raw)
        src = brand_to_source_idxs.get(canon, np.array([], dtype=np.int32))
        anchors_cnt = brand_counts.get(canon, 0)
        print(f"[info] Brand '{raw}' → '{canon}': anchors={anchors_cnt}, source_nodes={src.size}")
        out_dir = os.path.join(out_base, f"brand_id={canon}")
        _ensure_dir(out_dir)
        out_path = os.path.join(out_dir, "part-000.parquet")
        # Skip if up-to-date unless forced
        try:
            if (not args.force) and os.path.exists(out_path):
                out_m = os.path.getmtime(out_path)
                dep_m = max(os.path.getmtime(args.anchors), os.path.getmtime(args.pbf))
                if out_m >= dep_m:
                    print(f"[skip] Up-to-date D_anchor brand for {canon}: {out_path}")
                    continue
        except Exception:
            pass

        if src.size == 0:
            print(f"[warn] No source nodes for brand={canon}; writing empty.")
            pl.DataFrame({"anchor_int_id": [], "seconds": [], "snapshot_ts": []}).write_parquet(out_path, compression="zstd")
            continue

        best_src_idx, time_s = kbest_multisource_bucket_csr(
            indptr_rev, indices_rev, w_rev, src, 1, cutoff_primary_s, cutoff_overflow_s, int(max(1, args.threads)), False, None
        )
        # For each anchor (node index j where anchor_idx[j] >= 0), pick time_s[j,0]
        records = []
        ts = np.asarray(time_s)
        if ts.ndim == 1:
            ts = ts.reshape(-1, 1)
        for j, aint in enumerate(anchor_idx.tolist()):
            if aint < 0:
                continue
            t = int(ts[j, 0])
            if t < 0:
                t = int(config.UNREACH_U16)
            records.append((int(aint), np.uint16(t), SNAPSHOT_TS))
        df = pl.DataFrame(records, schema=[("anchor_int_id", pl.Int32), ("seconds", pl.UInt16), ("snapshot_ts", pl.Utf8)], orient="row")
        df.write_parquet(out_path, compression="zstd")
        print(f"[ok] Wrote D_anchor brand: {out_path} rows={df.height}")


if __name__ == "__main__":
    main()
