"""
Compute per-hex nearest-time overlays for selected brands or categories.

Goal: Guarantee correctness for UI filters by producing one column per
overlay (e.g., starbucks_drive_min), computed via a restricted multi-source
search (sources limited to that brand/category), K=1.

Outputs (Hive-style):
  data/overlays/mode=<mode>/brand_id=<brand_id>/part-*.parquet
  Columns: h3_id:uint64, res:int32, seconds_u16:uint16

Usage examples:
  PY=PYTHONPATH=src .venv/bin/python src/03c_compute_overlays.py \
    --pbf data/osm/massachusetts.osm.pbf \
    --anchors data/anchors/massachusetts_drive_sites.parquet \
    --mode drive --res 8 --brands-threshold 20

  # Single brand
  PY=PYTHONPATH=src .venv/bin/python src/03c_compute_overlays.py \
    --pbf data/osm/massachusetts.osm.pbf \
    --anchors data/anchors/massachusetts_drive_sites.parquet \
    --mode drive --res 7 8 --brand starbucks
"""
from __future__ import annotations
import argparse
import os
from typing import Dict, Iterable, List, Set, Tuple

import numpy as np
import pandas as pd
import polars as pl

from graph.pyrosm_csr import load_or_build_csr
from graph.csr_utils import build_rev_csr
from graph.anchors import build_anchor_mappings
from t_hex import kbest_multisource_bucket_csr, aggregate_h3_topk_precached
import config
import re
try:
    from taxonomy import BRAND_REGISTRY as _BR
except Exception:
    _BR = {}


def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def _noop():
    pass


def _collect_brand_sources_anchor_ids(anchors_df: pd.DataFrame) -> Dict[str, Set[int]]:
    """Return mapping canonical_brand_id -> set(anchor_int_id)."""
    out: Dict[str, Set[int]] = {}
    for aint, brands in anchors_df[["anchor_int_id", "brands"]].itertuples(index=False):
        if not isinstance(brands, (list, np.ndarray)):
            continue
        if isinstance(brands, np.ndarray):
            brands = brands.tolist()
        for b in brands:
            try:
                out.setdefault(str(b), set()).add(int(aint))
            except Exception:
                continue
    return out


def _norm(s: str) -> str:
    s = ("" if s is None else str(s)).lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _build_alias_to_canon() -> Dict[str, str]:
    alias_to_id: Dict[str, str] = {}
    for bid, (name, aliases) in _BR.items():
        alias_to_id[_norm(bid)] = bid
        if name:
            alias_to_id[_norm(name)] = bid
        for a in (aliases or []):
            alias_to_id[_norm(a)] = bid
    return alias_to_id


def compute_brand_overlay_from_sources(
    indptr: np.ndarray,
    indices: np.ndarray,
    w_sec: np.ndarray,
    node_h3_by_res: np.ndarray,
    res_used: List[int],
    anchor_idx: np.ndarray,
    source_idxs: np.ndarray,
    cutoff_min: int,
    overflow_min: int,
    threads: int,
) -> pl.DataFrame:
    if source_idxs.size == 0:
        return pl.DataFrame({"h3_id": [], "res": [], "seconds_u16": []})
    K = 1
    cutoff_primary_s = int(cutoff_min) * 60
    cutoff_overflow_s = int(overflow_min) * 60
    # Use CSR transpose so that multi-source search from brand sources
    # yields node→brand times respecting one-ways.
    indptr_rev, indices_rev, w_rev = build_rev_csr(indptr, indices, w_sec)

    best_src_idx, time_s = kbest_multisource_bucket_csr(
        indptr_rev, indices_rev, w_rev, source_idxs, K, cutoff_primary_s, cutoff_overflow_s, int(max(1, threads)), False, None
    )
    best_anchor_int = np.where(best_src_idx >= 0, anchor_idx[best_src_idx], -1).astype(np.int32)
    h3_id_arr, site_id_arr, time_arr, res_arr = aggregate_h3_topk_precached(
        node_h3_by_res, best_anchor_int, time_s, np.array(res_used, dtype=np.int32), K, int(config.UNREACH_U16), os.cpu_count(), False
    )
    return pl.DataFrame({
        "h3_id": np.asarray(h3_id_arr, dtype=np.uint64),
        "res": np.asarray(res_arr, dtype=np.int32),
        "seconds_u16": np.asarray(time_arr, dtype=np.uint16),
    })


def _is_up_to_date(out_path: str, deps: list[str]) -> bool:
    try:
        if not os.path.exists(out_path):
            return False
        out_m = os.path.getmtime(out_path)
        dep_m = max(os.path.getmtime(p) for p in deps if p and os.path.exists(p))
        return out_m >= dep_m
    except Exception:
        return False


def main():
    ap = argparse.ArgumentParser(description="Compute brand/category overlays (nearest times)")
    ap.add_argument("--pbf", required=True)
    ap.add_argument("--anchors", required=True)
    ap.add_argument("--mode", required=True, choices=["drive", "walk"])
    ap.add_argument("--res", nargs="+", type=int, default=[8])
    ap.add_argument("--brand", action="append", default=[], help="Brand id to compute (can repeat)")
    ap.add_argument("--brands-threshold", type=int, default=0, help="If >0, compute overlays for all brands with >= this many sites")
    ap.add_argument("--cutoff", type=int, default=30)
    ap.add_argument("--overflow-cutoff", type=int, default=90)
    ap.add_argument("--threads", type=int, default=1)
    ap.add_argument("--out-dir", default="data/overlays")
    ap.add_argument("--force", action="store_true", help="Recompute overlays even if outputs appear up-to-date")
    args = ap.parse_args()

    anchors_df = pd.read_parquet(args.anchors)
    if "anchor_int_id" not in anchors_df.columns:
        anchors_df = anchors_df.sort_values("site_id").reset_index(drop=True)
        anchors_df["anchor_int_id"] = anchors_df.index.astype(np.int32)

    # Build frequency table for canonical brands (as stored in anchors_df)
    brand_counts: Dict[str, int] = {}
    for brands in anchors_df["brands"].dropna().values:
        if not isinstance(brands, (list, np.ndarray)):
            continue
        if isinstance(brands, np.ndarray):
            brands = brands.tolist()
        for b in brands:
            b = str(b)
            brand_counts[b] = brand_counts.get(b, 0) + 1

    targets: List[str] = list(args.brand)
    if args.brands_threshold > 0:
        targets += [b for b, cnt in brand_counts.items() if cnt >= args.brands_threshold]
    targets = sorted(set(targets))

    if not targets:
        print("[warn] No overlay targets specified; nothing to compute.")
        return

    # Ensure out dir exists
    base = os.path.join(args.out_dir, f"mode={0 if args.mode=='drive' else 2}")
    _ensure_dir(base)

    # Load CSR ONCE
    node_ids, indptr, indices, w_sec, node_lats, node_lons, node_h3_by_res, res_used = load_or_build_csr(args.pbf, args.mode, args.res, False)
    anchor_idx, _ = build_anchor_mappings(anchors_df, node_ids)

    # Precompute brand -> set(anchor_int_id)
    brand_to_anchor_ids = _collect_brand_sources_anchor_ids(anchors_df)
    # Map canonical id -> array of node source idxs
    brand_to_source_idxs: Dict[str, np.ndarray] = {}
    # Build reverse mapping: anchor_id -> list of canonical brands
    anchor_to_brands: Dict[int, List[str]] = {}
    for aint, brands in anchors_df[["anchor_int_id", "brands"]].itertuples(index=False):
        brand_list = []
        if isinstance(brands, (list, np.ndarray)):
            if isinstance(brands, np.ndarray):
                brands = brands.tolist()
            brand_list = [str(b) for b in brands]
        anchor_to_brands[int(aint)] = brand_list
    # Iterate nodes once and assign to brand lists
    from collections import defaultdict
    temp_map = defaultdict(list)  # canonical brand -> list of node indices
    for j, aint in enumerate(anchor_idx.tolist()):
        if aint < 0:
            continue
        for b in anchor_to_brands.get(int(aint), []):
            temp_map[str(b)].append(j)
    for b, lst in temp_map.items():
        brand_to_source_idxs[b] = np.asarray(lst, dtype=np.int32)

    alias_to_canon = _build_alias_to_canon()

    deps = [args.anchors, args.pbf]
    for raw in targets:
        # Resolve brand id: accept canonical id directly, else map aliases to canonical
        canon = raw
        norm = _norm(raw)
        if norm in alias_to_canon:
            canon = alias_to_canon[norm]
        # Stats/logging
        anchors_cnt = brand_counts.get(canon, 0)
        source_cnt = int(brand_to_source_idxs.get(canon, np.array([], dtype=np.int32)).size)
        if anchors_cnt == 0:
            print(f"[warn] Brand '{raw}' resolved as '{canon}' has 0 anchors in anchors_df. Check normalization/aliases.")
        else:
            print(f"[info] Brand '{raw}' → '{canon}': anchors={anchors_cnt}, source_nodes={source_cnt}")

        out_dir = os.path.join(base, f"brand_id={canon}")
        _ensure_dir(out_dir)
        out_path = os.path.join(out_dir, "part-000.parquet")
        # Skip if up-to-date unless forced
        if not args.force and _is_up_to_date(out_path, deps):
            print(f"[skip] Up-to-date overlay for brand={canon}: {out_path}")
            continue
        print(f"[info] Computing overlay for brand={canon} → {out_path}")
        src = brand_to_source_idxs.get(canon, np.array([], dtype=np.int32))
        df = compute_brand_overlay_from_sources(
            indptr, indices, w_sec, node_h3_by_res, res_used, anchor_idx, src, args.cutoff, args.overflow_cutoff, args.threads
        )
        if df.height == 0:
            print(f"[warn] No overlay results for brand={canon} (no sources?)")
            continue
        df.write_parquet(out_path, compression="zstd")
        print(f"[ok] Wrote overlay: {out_path} rows={df.height}")


if __name__ == "__main__":
    main()
