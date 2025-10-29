"""
Compute D_anchor for categories: anchor_id -> seconds to nearest anchor that contains a POI in that category.

This mirrors the brand variant (05_compute_d_anchor.py), but partitions by
numeric category_id under a unified directory:

  data/d_anchor_category/mode=<0|2>/category_id=<id>/part-000.parquet

Columns:
  - anchor_id: uint32
  - category_id: uint32
  - mode: uint8 (0=drive, 2=walk)
  - seconds_u16: uint16 (nullable; NULL = unreachable or overflow)
  - snapshot_ts: date

Also writes a convenience label map at data/taxonomy/category_labels.json
mapping string ids to human-friendly labels, if possible.

Usage:
  PY=PYTHONPATH=src .venv/bin/python src/06_compute_d_anchor_category.py \
    --pbf data/osm/massachusetts.osm.pbf \
    --anchors data/anchors/massachusetts_drive_sites.parquet \
    --mode drive
"""
from __future__ import annotations
import argparse
import json
import os
import time
from collections import defaultdict
from typing import Dict, List, Tuple

SNAPSHOT_TS = time.strftime("%Y-%m-%d")

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
    get_entity_limits,
)
def _normalize_label(s: str) -> str:
    # Minimal prettifier for labels
    return (str(s) if s is not None else "").strip().replace("_", " ").title()


_CATEGORY_SCHEMA: Dict[str, pl.DataType] = {
    "anchor_id": pl.UInt32,
    "category_id": pl.UInt32,
    "mode": pl.UInt8,
    "seconds_u16": pl.UInt16,
    "snapshot_ts": pl.Date,
}


def _vectorized_write(
    out_path: str,
    category_id: int,
    category_label: str,
    mode_code: int,
    time_s: np.ndarray,
    snapshot_ts: str,
) -> int:
    # Get limits for this category
    limits = get_entity_limits("category", category_label)
    max_seconds = limits["max_minutes"] * 60
    top_k = limits["top_k"]
    
    return write_shard(
        out_path=out_path,
        time_s=time_s,
        snapshot_ts=snapshot_ts,
        schema=_CATEGORY_SCHEMA,
        dedupe_keys=["anchor_id", "category_id", "mode", "snapshot_ts"],
        extra_builder=lambda size: {
            "category_id": np.full(size, category_id, dtype=np.uint32),
            "mode": np.full(size, mode_code, dtype=np.uint8),
        },
        top_k=top_k,
        max_seconds=max_seconds,
    )


def _write_empty_category_shard(out_path: str) -> None:
    write_empty_shard(out_path, _CATEGORY_SCHEMA)


def _compute_one_category(
    task: Tuple[int, str, int, np.ndarray, np.ndarray, str]
) -> Tuple[int, str]:
    """Worker entrypoint for one category.
    Args tuple = (cid, label, mode_code, src, targets_idx, out_path)
    Returns (cid, out_path)
    """
    cid, label, mode_code, src, targets_idx, out_path = task

    # Get limits for this category
    limits = get_entity_limits("category", label)
    cutoff_primary_s = limits["max_minutes"] * 60
    # Use same cutoff for overflow (no two-pass strategy yet)
    cutoff_overflow_s = cutoff_primary_s

    task_start = time.perf_counter()
    # Compute SSSP for this category's sources
    sssp_start = time.perf_counter()
    time_s = compute_times(src, targets_idx, cutoff_primary_s, cutoff_overflow_s)
    sssp_elapsed = time.perf_counter() - sssp_start
    # Write parquet using vectorized path
    write_start = time.perf_counter()
    rows = _vectorized_write(out_path, cid, label, mode_code, time_s, SNAPSHOT_TS)
    write_elapsed = time.perf_counter() - write_start
    total_elapsed = time.perf_counter() - task_start
    print(
        f"[ok] Wrote D_anchor category id={cid}: {out_path} rows={rows} "
        f"sssp={sssp_elapsed:.2f}s write={write_elapsed:.2f}s total={total_elapsed:.2f}s "
        f"max_minutes={limits['max_minutes']} top_k={limits['top_k']}"
    )
    return cid, out_path


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
    ap.add_argument("--workers", type=int, default=min(8, os.cpu_count() or 1), help="Parallel category workers (processes)")
    ap.add_argument("--out-dir", default="data/d_anchor_category")
    ap.add_argument("--categories-csv", default="data/taxonomy/POI_category_registry.csv", help="Path to categories CSV with explicit numeric IDs")
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--prune", action="store_true", help="Remove existing category partitions not in current targets")
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

    # Load categories from CSV with explicit numeric IDs (anti-drift design)
    from taxonomy import get_categories
    try:
        all_categories = get_categories(args.categories_csv)
        print(f"[info] Loaded {len(all_categories)} categories from {args.categories_csv}")
    except ValueError as e:
        print(f"[error] Invalid categories CSV: {e}")
        return
    except Exception as e:
        print(f"[error] Failed to load categories CSV: {e}")
        return
    
    if not all_categories:
        print(f"[error] No categories found in {args.categories_csv}")
        return

    # Resolve targets
    targets: List[str] = list(dict.fromkeys(map(str, args.category)))
    if not targets:
        # Use all categories from CSV
        targets = sorted(all_categories.keys())
        print(f"[info] Using all {len(targets)} categories from CSV")
    
    if args.min_sites > 0:
        # Filter by minimum site count
        targets = [c for c in targets if cat_counts.get(c, 0) >= args.min_sites]
        print(f"[info] Filtered to {len(targets)} categories with >= {args.min_sites} sites")
    
    # Keep only those present in anchors (avoid empty outputs)
    present = set(cat_counts.keys())
    targets = sorted([t for t in set(targets) if t in present])
    
    if not targets:
        print("[warn] No categories to compute; exiting.")
        return
    
    # Build label_to_id mapping from CSV (explicit IDs, no drift)
    label_to_id: Dict[str, int] = {cat_id: numeric_id for cat_id, (numeric_id, _) in all_categories.items() if cat_id in targets}
    
    print(f"[info] Computing {len(targets)} categories: {', '.join(targets)}")

    # CSR + mappings
    csr_start = time.perf_counter()
    graph_ctx = build_graph_context(args.pbf, args.mode, anchors_df)
    anchor_idx = graph_ctx.anchor_idx
    anchor_nodes = graph_ctx.anchor_nodes
    anchor_int_ids = graph_ctx.anchor_int_ids
    print(
        f"[debug] Loaded CSR + anchor mappings: nodes={graph_ctx.node_count} anchors={anchor_nodes.size} "
        f"components={len(graph_ctx.comp_to_anchor_nodes)} took={time.perf_counter() - csr_start:.2f}s"
    )

    # Build category -> list of node indices serving as sources
    build_sources_start = time.perf_counter()
    cat_to_source_idxs: Dict[str, np.ndarray] = {}
    # Build anchor -> categories list for quick lookup
    anchor_to_cats: Dict[int, List[str]] = {}
    for aint, cats in anchors_df[["anchor_int_id", "categories"]].itertuples(index=False):
        lst: List[str] = []
        if isinstance(cats, (list, np.ndarray)):
            lst = [str(c) for c in (cats.tolist() if isinstance(cats, np.ndarray) else cats) if c is not None]
        anchor_to_cats[int(aint)] = lst
    tmp = defaultdict(list)
    for node_idx, aint in zip(anchor_nodes, anchor_int_ids):
        cats = anchor_to_cats.get(int(aint), [])
        if not cats:
            continue
        for c in cats:
            tmp[str(c)].append(int(node_idx))
    for c, lst in tmp.items():
        cat_to_source_idxs[c] = np.asarray(lst, dtype=np.int32)
    print(
        f"[debug] Built category→source map for {len(cat_to_source_idxs)} categories "
        f"in {time.perf_counter() - build_sources_start:.2f}s"
    )

    comp_id = graph_ctx.comp_id
    comp_to_anchor_nodes = graph_ctx.comp_to_anchor_nodes

    mode_code = 0 if args.mode == "drive" else 2
    out_base = os.path.join(args.out_dir, f"mode={mode_code}")
    ensure_dir(out_base)

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

    # Queue work items, but handle up-to-date and empty-src in parent
    work: List[Tuple[int, str, int, np.ndarray, np.ndarray, str]] = []
    for label in targets:
        cid = label_to_id[label]
        src = cat_to_source_idxs.get(label, np.array([], dtype=np.int32))
        anchors_cnt = cat_counts.get(label, 0)
        
        # Get limits for this category for display
        limits = get_entity_limits("category", label)
        print(f"[info] Category '{label}': id={cid}, anchors={anchors_cnt}, source_nodes={src.size}, "
              f"max_minutes={limits['max_minutes']}, top_k={limits['top_k']}")

        out_dir = os.path.join(out_base, f"category_id={cid}")
        ensure_dir(out_dir)
        out_path = os.path.join(out_dir, "part-000.parquet")

        # Skip if exists unless forced (simple existence check, no timestamp comparison)
        if (not args.force) and os.path.exists(out_path):
            print(f"[skip] D_anchor category for id={cid} already exists: {out_path}")
            continue

        if src.size == 0:
            print(f"[warn] No source nodes for category id={cid}; writing empty.")
            _write_empty_category_shard(out_path)
            continue

        # Build target set as anchors in components that contain at least one source
        build_targets_start = time.perf_counter()
        src_comp, targets_idx, fallback_used = compute_target_nodes(
            src, comp_id, comp_to_anchor_nodes, anchor_idx, anchor_nodes
        )
        build_targets_elapsed = time.perf_counter() - build_targets_start
        print(
            f"[debug] Category '{label}' comps={src_comp.size} target_nodes={targets_idx.size} "
            f"build={build_targets_elapsed:.2f}s fallback={fallback_used}"
        )
        if targets_idx.size == 0:
            print(f"[warn] No target nodes for category id={cid}; writing empty.")
            _write_empty_category_shard(out_path)
            continue

        work.append((cid, label, mode_code, src, targets_idx, out_path))

    execute_tasks(
        work,
        graph_ctx,
        kernel_threads,
        max_workers,
        _compute_one_category,
        describe=lambda task: f"Category id={task[0]} label='{task[1]}'",
    )


if __name__ == "__main__":
    main()
