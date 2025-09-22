"""
Compute D_anchor for categories: anchor_int_id -> seconds to nearest anchor that contains a POI in that category.

This mirrors the brand variant (03d_compute_d_anchor.py), but partitions by
numeric category_id under a unified directory:

  data/d_anchor_category/mode=<0|2>/category_id=<id>/part-000.parquet

Columns:
  - anchor_int_id: int32
  - seconds: uint16 (65535 sentinel for unreachable)
  - snapshot_ts: str (YYYY-MM-DD)

Also writes a convenience label map at data/taxonomy/category_labels.json
mapping string ids to human-friendly labels, if possible.

Usage:
  PY=PYTHONPATH=src .venv/bin/python src/03e_compute_d_anchor_category.py \
    --pbf data/osm/massachusetts.osm.pbf \
    --anchors data/anchors/massachusetts_drive_sites.parquet \
    --mode drive
"""
from __future__ import annotations
import argparse
import json
import os
import time
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import polars as pl

from graph.pyrosm_csr import load_or_build_csr
from graph.csr_utils import build_rev_csr
from graph.anchors import build_anchor_mappings
from t_hex import kbest_multisource_bucket_csr
import config

SNAPSHOT_TS = time.strftime("%Y-%m-%d")


def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)




def _normalize_label(s: str) -> str:
    # Minimal prettifier for labels
    return (str(s) if s is not None else "").strip().replace("_", " ").title()


def main():
    ap = argparse.ArgumentParser(description="Compute D_anchor category tables (anchor->category seconds)")
    ap.add_argument("--pbf", required=True)
    ap.add_argument("--anchors", required=True)
    ap.add_argument("--mode", required=True, choices=["drive", "walk"])
    ap.add_argument("--category", action="append", default=[], help="Category label to compute; can repeat. Default = all in anchors")
    ap.add_argument("--min-sites", type=int, default=0, help="If >0, compute for categories with at least this many sites")
    ap.add_argument("--cutoff", type=int, default=30)
    ap.add_argument("--overflow-cutoff", type=int, default=90)
    ap.add_argument("--threads", type=int, default=1)
    ap.add_argument("--out-dir", default="data/d_anchor_category")
    ap.add_argument("--allowlist", default="data/taxonomy/category_allowlist.txt", help="Optional path to category allowlist (one category label per line)")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--prune", action="store_true", help="Remove existing category partitions not in current targets")
    args = ap.parse_args()

    anchors_df = pd.read_parquet(args.anchors)
    if "anchor_int_id" not in anchors_df.columns:
        anchors_df = anchors_df.sort_values("site_id").reset_index(drop=True)
        anchors_df["anchor_int_id"] = anchors_df.index.astype(np.int32)

    # Build category frequencies from anchors
    cat_counts: Dict[str, int] = {}
    cat_values: List[str] = []
    for cats in anchors_df.get("categories", pd.Series([], dtype=object)).dropna().values:
        if isinstance(cats, (list, np.ndarray)):
            iterable = cats.tolist() if isinstance(cats, np.ndarray) else cats
            for c in iterable:
                if c is None:
                    continue
                s = str(c).strip()
                if not s:
                    continue
                cat_counts[s] = cat_counts.get(s, 0) + 1
                cat_values.append(s)

    # Resolve targets
    targets: List[str] = list(dict.fromkeys(map(str, args.category)))
    # Optional allowlist file
    if (not targets) and args.allowlist and os.path.isfile(args.allowlist):
        try:
            with open(args.allowlist, "r") as f:
                allowed = [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
            targets = [c for c in allowed]
            print(f"[info] Loaded {len(targets)} categories from allowlist {args.allowlist}")
        except Exception as e:
            print(f"[warn] Failed to read allowlist {args.allowlist}: {e}")
    if args.min_sites > 0 and not targets:
        targets += [c for c, n in cat_counts.items() if n >= args.min_sites]
    if not targets:
        # Default: all categories seen in anchors
        targets = sorted(set(cat_values))
    else:
        # Keep only those present in anchors (avoid empty outputs)
        present = set(cat_counts.keys())
        targets = sorted([t for t in set(targets) if t in present])
    if not targets:
        print("[warn] No categories to compute; exiting.")
        return

    # Deterministic category_id mapping
    sorted_labels = sorted(set(targets))
    label_to_id: Dict[str, int] = {lab: i + 1 for i, lab in enumerate(sorted_labels)}

    # Persist a label map for the API (optional); keys must be strings
    labels_dir = os.path.join("data", "taxonomy")
    _ensure_dir(labels_dir)
    labels_path = os.path.join(labels_dir, "category_labels.json")
    try:
        with open(labels_path, "w") as f:
            json.dump({str(label_to_id[k]): _normalize_label(k) for k in sorted_labels}, f)
        print(f"[ok] Wrote labels to {labels_path}")
    except Exception as e:
        print(f"[warn] Failed to write labels: {e}")

    # CSR + mappings
    node_ids, indptr, indices, w_sec, node_lats, node_lons, node_h3_by_res, res_used = load_or_build_csr(
        args.pbf, args.mode, [8], False
    )
    anchor_idx, _ = build_anchor_mappings(anchors_df, node_ids)

    # Build category -> list of node indices serving as sources
    from collections import defaultdict
    cat_to_source_idxs: Dict[str, np.ndarray] = {}
    # Build anchor -> categories list for quick lookup
    anchor_to_cats: Dict[int, List[str]] = {}
    for aint, cats in anchors_df[["anchor_int_id", "categories"]].itertuples(index=False):
        lst: List[str] = []
        if isinstance(cats, (list, np.ndarray)):
            lst = [str(c) for c in (cats.tolist() if isinstance(cats, np.ndarray) else cats) if c is not None]
        anchor_to_cats[int(aint)] = lst

    tmp = defaultdict(list)
    for j, aint in enumerate(anchor_idx.tolist()):
        if aint < 0:
            continue
        for c in anchor_to_cats.get(int(aint), []):
            tmp[str(c)].append(j)
    for c, lst in tmp.items():
        cat_to_source_idxs[c] = np.asarray(lst, dtype=np.int32)

    indptr_rev, indices_rev, w_rev = build_rev_csr(indptr, indices, w_sec)
    cutoff_primary_s = int(args.cutoff) * 60
    cutoff_overflow_s = int(args.overflow_cutoff) * 60

    out_base = os.path.join(args.out_dir, f"mode={0 if args.mode=='drive' else 2}")
    _ensure_dir(out_base)

    # Optionally prune any existing category partitions not targeted
    if args.prune:
        try:
            wanted_cids = set(label_to_id.values())
            for name in os.listdir(out_base):
                if not name.startswith("category_id="):
                    continue
                try:
                    cid = int(name.split("=", 1)[1])
                except Exception:
                    continue
                if cid not in wanted_cids:
                    import shutil
                    shutil.rmtree(os.path.join(out_base, name), ignore_errors=True)
                    print(f"[prune] removed {os.path.join(out_base, name)}")
        except Exception as e:
            print(f"[warn] prune step failed: {e}")

    for label in targets:
        cid = label_to_id[label]
        src = cat_to_source_idxs.get(label, np.array([], dtype=np.int32))
        anchors_cnt = cat_counts.get(label, 0)
        print(f"[info] Category '{label}': id={cid}, anchors={anchors_cnt}, source_nodes={src.size}")

        out_dir = os.path.join(out_base, f"category_id={cid}")
        _ensure_dir(out_dir)
        out_path = os.path.join(out_dir, "part-000.parquet")
        # Skip if up-to-date unless forced
        try:
            if (not args.force) and os.path.exists(out_path):
                out_m = os.path.getmtime(out_path)
                dep_m = max(os.path.getmtime(args.anchors), os.path.getmtime(args.pbf))
                if out_m >= dep_m:
                    print(f"[skip] Up-to-date D_anchor category for id={cid}: {out_path}")
                    continue
        except Exception:
            pass

        if src.size == 0:
            print(f"[warn] No source nodes for category id={cid}; writing empty.")
            pl.DataFrame({"anchor_int_id": [], "seconds": [], "snapshot_ts": []}).write_parquet(out_path, compression="zstd")
            continue

        best_src_idx, time_s = kbest_multisource_bucket_csr(
            indptr_rev, indices_rev, w_rev, src, 1, cutoff_primary_s, cutoff_overflow_s, int(max(1, args.threads)), False, None
        )
        # For each anchor (node index j where anchor_idx[j] >= 0), pick time_s[j,0]
        records = []
        # Ensure time_s is a 2D array [N, K]
        ts = np.asarray(time_s)
        if ts.ndim == 1:
            ts = ts.reshape(-1, 1)
        for j, aint in enumerate(anchor_idx.tolist()):
            if aint < 0:
                continue
            # K=1, safe to take column 0
            t = int(ts[j, 0])
            if t < 0:
                t = int(config.UNREACH_U16)
            records.append((int(aint), np.uint16(t), SNAPSHOT_TS))
        df = pl.DataFrame(records, schema=[("anchor_int_id", pl.Int32), ("seconds", pl.UInt16), ("snapshot_ts", pl.Utf8)], orient="row")
        df.write_parquet(out_path, compression="zstd")
        print(f"[ok] Wrote D_anchor category id={cid}: {out_path} rows={df.height}")


if __name__ == "__main__":
    main()
