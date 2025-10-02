"""
Compute D_anchor for brands: anchor_id -> seconds to nearest anchor containing that brand.

This enables brand-level filtering in anchor-mode without needing per-brand
columns in the tiles. Layout is Hive-partitioned Parquet:

  data/d_anchor_brand/mode=<0|2>/brand_id=<brand_id>/part-000.parquet

Columns:
  - anchor_id: uint32
  - brand_id: str
  - mode: uint8 (0=drive, 2=walk)
  - seconds_u16: uint16 (nullable; NULL = unreachable or overflow)
  - snapshot_ts: date

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
from collections import defaultdict
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import polars as pl

from d_anchor_common import (
    compute_target_nodes,
    compute_times,
    ensure_dir,
    build_graph_context,
    execute_tasks,
    write_empty_shard,
    write_shard,
)

SNAPSHOT_TS = time.strftime("%Y-%m-%d")

_BRAND_SCHEMA: Dict[str, pl.DataType] = {
    "anchor_id": pl.UInt32,
    "brand_id": pl.Utf8,
    "mode": pl.UInt8,
    "seconds_u16": pl.UInt16,
    "snapshot_ts": pl.Date,
}


def _vectorized_write(
    out_path: str,
    brand_id: str,
    mode_code: int,
    time_s: np.ndarray,
    snapshot_ts: str,
) -> int:
    return write_shard(
        out_path=out_path,
        time_s=time_s,
        snapshot_ts=snapshot_ts,
        schema=_BRAND_SCHEMA,
        dedupe_keys=["anchor_id", "brand_id", "mode", "snapshot_ts"],
        extra_builder=lambda size: {
            "brand_id": [brand_id] * size,
            "mode": np.full(size, mode_code, dtype=np.uint8),
        },
    )


def _write_empty_brand_shard(out_path: str) -> None:
    write_empty_shard(out_path, _BRAND_SCHEMA)


def _compute_one_brand(task: Tuple[str, int, np.ndarray, np.ndarray, int, int, str]) -> Tuple[str, str]:
    brand_id, mode_code, src, targets_idx, cutoff_primary_s, cutoff_overflow_s, out_path = task

    task_start = time.perf_counter()
    sssp_start = time.perf_counter()
    time_s = compute_times(src, targets_idx, cutoff_primary_s, cutoff_overflow_s)
    sssp_elapsed = time.perf_counter() - sssp_start
    write_start = time.perf_counter()
    rows = _vectorized_write(out_path, brand_id, mode_code, time_s, SNAPSHOT_TS)
    write_elapsed = time.perf_counter() - write_start
    total_elapsed = time.perf_counter() - task_start
    print(
        f"[ok] Wrote D_anchor brand '{brand_id}': {out_path} rows={rows} "
        f"sssp={sssp_elapsed:.2f}s write={write_elapsed:.2f}s total={total_elapsed:.2f}s"
    )
    return brand_id, out_path


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
    ap.add_argument("--workers", type=int, default=min(8, os.cpu_count() or 1), help="Parallel brand workers (processes)")
    ap.add_argument("--out-dir", default="data/d_anchor_brand")
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    max_workers = max(1, int(args.workers))
    kernel_threads = max(1, int(args.threads))
    if max_workers > 1 and kernel_threads > 1:
        print(
            f"[debug] Reducing kernel threads from {kernel_threads} to 1 to avoid oversubscription with {max_workers} workers"
        )
        kernel_threads = 1
    args.threads = kernel_threads
    for env_var in ("OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        os.environ.setdefault(env_var, str(kernel_threads))

    load_start = time.perf_counter()
    anchors_df = pd.read_parquet(args.anchors)
    if "anchor_int_id" not in anchors_df.columns:
        anchors_df = anchors_df.sort_values("site_id").reset_index(drop=True)
        anchors_df["anchor_int_id"] = anchors_df.index.astype(np.int32)
    print(f"[debug] Loaded anchors: rows={len(anchors_df)} took={time.perf_counter() - load_start:.2f}s")

    csr_start = time.perf_counter()
    graph_ctx = build_graph_context(args.pbf, args.mode, anchors_df)
    print(
        f"[debug] Loaded CSR + anchor mappings: nodes={graph_ctx.node_count} anchors={graph_ctx.anchor_nodes.size} "
        f"components={len(graph_ctx.comp_to_anchor_nodes)} took={time.perf_counter() - csr_start:.2f}s"
    )

    # Build brand frequency table at anchor sites
    brand_counts: Dict[str, int] = {}
    for brands in anchors_df.get("brands", pd.Series([], dtype=object)).dropna().values:
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

    anchor_idx = graph_ctx.anchor_idx
    anchor_nodes = graph_ctx.anchor_nodes
    anchor_int_ids = graph_ctx.anchor_int_ids
    anchor_to_brands = _collect_anchor_brand_lists(anchors_df)

    # Build brand -> list of node indices serving as sources for multi-source search
    brand_to_source_idxs: Dict[str, np.ndarray] = {}
    temp = defaultdict(list)
    for node_idx, aint in zip(anchor_nodes, anchor_int_ids):
        brands_for_anchor = anchor_to_brands.get(int(aint), [])
        if not brands_for_anchor:
            continue
        for b in brands_for_anchor:
            temp[str(b)].append(int(node_idx))
    for b, lst in temp.items():
        brand_to_source_idxs[b] = np.asarray(lst, dtype=np.int32)
    print(
        f"[debug] Built brand→source map for {len(brand_to_source_idxs)} brands "
        f"in {time.perf_counter() - csr_start:.2f}s total CSR pipeline"
    )

    comp_id = graph_ctx.comp_id
    comp_to_anchor_nodes = graph_ctx.comp_to_anchor_nodes
    cutoff_primary_s = int(args.cutoff) * 60
    cutoff_overflow_s = int(args.overflow_cutoff) * 60

    mode_code = 0 if args.mode == "drive" else 2
    out_base = os.path.join(args.out_dir, f"mode={mode_code}")
    ensure_dir(out_base)

    work: List[Tuple[str, int, np.ndarray, np.ndarray, int, int, str]] = []
    for raw in targets:
        canon = str(raw)
        src = brand_to_source_idxs.get(canon, np.array([], dtype=np.int32))
        anchors_cnt = brand_counts.get(canon, 0)
        print(f"[info] Brand '{raw}' → '{canon}': anchors={anchors_cnt}, source_nodes={src.size}")
        out_dir = os.path.join(out_base, f"brand_id={canon}")
        ensure_dir(out_dir)
        out_path = os.path.join(out_dir, "part-000.parquet")
        if src.size == 0:
            print(f"[warn] No source nodes for brand={canon}; writing empty.")
            _write_empty_brand_shard(out_path)
            continue

        build_targets_start = time.perf_counter()
        src_comp, targets_idx, fallback_used = compute_target_nodes(
            src, comp_id, comp_to_anchor_nodes, anchor_idx, anchor_nodes
        )
        build_targets_elapsed = time.perf_counter() - build_targets_start
        print(
            f"[debug] Brand '{canon}' comps={src_comp.size} target_nodes={targets_idx.size} "
            f"build={build_targets_elapsed:.2f}s fallback={fallback_used}"
        )
        if targets_idx.size == 0:
            print(f"[warn] No target nodes for brand={canon}; writing empty.")
            _write_empty_brand_shard(out_path)
            continue

        # Skip if up-to-date unless forced (only once we know there is actual work)
        try:
            if (not args.force) and os.path.exists(out_path):
                out_m = os.path.getmtime(out_path)
                dep_m = max(os.path.getmtime(args.anchors), os.path.getmtime(args.pbf))
                if out_m >= dep_m:
                    print(f"[skip] Up-to-date D_anchor brand for {canon}: {out_path}")
                    continue
        except Exception:
            pass

        work.append((canon, mode_code, src, targets_idx, cutoff_primary_s, cutoff_overflow_s, out_path))

    execute_tasks(
        work,
        graph_ctx,
        kernel_threads,
        max_workers,
        _compute_one_brand,
        describe=lambda task: f"Brand '{task[0]}'",
    )


if __name__ == "__main__":
    main()
