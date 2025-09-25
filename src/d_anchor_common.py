"""Shared utilities for computing D_anchor tables for brands and categories."""
from __future__ import annotations

import os
import tempfile
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, List, Tuple

import numpy as np
import polars as pl

from graph.pyrosm_csr import load_or_build_csr
from graph.csr_utils import build_rev_csr
from graph.anchors import build_anchor_mappings
from t_hex import kbest_multisource_bucket_csr, weakly_connected_components

_G: Dict[str, Any] = {}


@dataclass
class GraphContext:
    """Precomputed graph data shared across workers and entity-specific code."""

    anchor_idx: np.ndarray
    anchor_nodes: np.ndarray
    anchor_int_ids: np.ndarray
    comp_id: np.ndarray
    comp_to_anchor_nodes: Dict[int, np.ndarray]
    indptr_rev: np.ndarray
    indices_rev: np.ndarray
    w_rev: np.ndarray
    node_count: int

    def worker_arrays(self) -> Dict[str, np.ndarray]:
        return {
            "indptr_rev": self.indptr_rev,
            "indices_rev": self.indices_rev,
            "w_rev": self.w_rev,
            "anchor_idx": self.anchor_idx,
            "anchor_nodes": self.anchor_nodes,
            "anchor_int_ids": self.anchor_int_ids,
        }


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def empty_frame(schema: Dict[str, pl.DataType]) -> pl.DataFrame:
    return pl.DataFrame({name: pl.Series([], dtype=dtype) for name, dtype in schema.items()})


def init_graph_worker(csr_pack: Dict[str, Any]) -> None:
    global _G
    data: Dict[str, Any] = {}
    paths: Dict[str, str] = csr_pack.get("paths", {})
    for key, path in paths.items():
        data[key] = np.load(path, mmap_mode="r")
    data["threads"] = int(csr_pack.get("threads", 1))
    _G = data


def _anchor_projection() -> Tuple[np.ndarray, np.ndarray]:
    anchor_nodes: np.ndarray | None = _G.get("anchor_nodes")
    anchor_int_ids: np.ndarray | None = _G.get("anchor_int_ids")
    if anchor_nodes is not None and anchor_int_ids is not None:
        target_node_idx = anchor_nodes
        anchor_ids = anchor_int_ids.astype(np.int32, copy=False)
    else:
        anchor_idx: np.ndarray = _G["anchor_idx"]
        target_node_idx = np.flatnonzero(anchor_idx >= 0)
        anchor_ids = anchor_idx[target_node_idx].astype(np.int32, copy=False)
    return target_node_idx, anchor_ids


def build_graph_context(pbf_path: str, mode: str, anchors_df) -> GraphContext:
    node_ids, indptr, indices, w_sec, *_ = load_or_build_csr(pbf_path, mode, [8], False)
    anchor_idx, _ = build_anchor_mappings(anchors_df, node_ids)
    anchor_nodes = np.flatnonzero(anchor_idx >= 0).astype(np.int32, copy=False)
    anchor_int_ids = anchor_idx[anchor_nodes].astype(np.int32, copy=False)

    indptr_rev, indices_rev, w_rev = build_rev_csr(indptr, indices, w_sec)
    comp_id = weakly_connected_components(indptr, indices, indptr_rev, indices_rev)
    anchor_comp_ids = comp_id[anchor_nodes]

    comp_lists: Dict[int, List[int]] = defaultdict(list)
    for comp_val, node_idx in zip(anchor_comp_ids, anchor_nodes):
        comp_lists[int(comp_val)].append(int(node_idx))
    comp_to_anchor_nodes = {k: np.asarray(v, dtype=np.int32) for k, v in comp_lists.items()}

    return GraphContext(
        anchor_idx=anchor_idx,
        anchor_nodes=anchor_nodes,
        anchor_int_ids=anchor_int_ids,
        comp_id=comp_id,
        comp_to_anchor_nodes=comp_to_anchor_nodes,
        indptr_rev=indptr_rev,
        indices_rev=indices_rev,
        w_rev=w_rev,
        node_count=len(node_ids),
    )


def compute_times(
    src: np.ndarray,
    targets_idx: np.ndarray,
    cutoff_primary_s: int,
    cutoff_overflow_s: int,
) -> np.ndarray:
    indptr_rev = _G["indptr_rev"]
    indices_rev = _G["indices_rev"]
    w_rev = _G["w_rev"]
    threads = max(1, int(_G.get("threads", 1)))
    _best_src_idx, time_s = kbest_multisource_bucket_csr(
        indptr_rev,
        indices_rev,
        w_rev,
        src,
        1,
        cutoff_primary_s,
        cutoff_overflow_s,
        threads,
        False,
        None,
        targets_idx,
    )
    return time_s


def write_empty_shard(out_path: str, schema: Dict[str, pl.DataType]) -> int:
    df = empty_frame(schema)
    tmp_path = out_path + ".tmp"
    df.write_parquet(tmp_path, compression="zstd", statistics=True, row_group_size=128_000)
    os.replace(tmp_path, out_path)
    return 0


def write_shard(
    out_path: str,
    time_s: np.ndarray,
    snapshot_ts: str,
    schema: Dict[str, pl.DataType],
    dedupe_keys: Iterable[str],
    extra_builder: Callable[[int], Dict[str, Any]],
) -> int:
    target_node_idx, anchor_ids = _anchor_projection()
    if time_s.size == 0 or target_node_idx.size == 0:
        return write_empty_shard(out_path, schema)

    ts = np.asarray(time_s)
    if ts.ndim == 1:
        ts = ts.reshape(-1, 1)
    t = ts[target_node_idx, 0].astype(np.int64, copy=False)

    seconds = pl.Series("seconds_u16", np.where(t < 0, None, t)).cast(pl.UInt16)
    anchor_series = pl.Series("anchor_id", anchor_ids.astype(np.uint32, copy=False), dtype=pl.UInt32)
    size = anchor_series.len()

    columns: Dict[str, Any] = {
        "anchor_id": anchor_series,
        "seconds_u16": seconds,
        "snapshot_ts": pl.Series([snapshot_ts] * size, dtype=pl.Utf8).str.to_date(),
    }
    extra_cols = extra_builder(size)
    if extra_cols:
        columns.update(extra_cols)

    df = pl.DataFrame(columns)
    df = df.with_columns([pl.col(name).cast(dtype) for name, dtype in schema.items() if name in df.columns])
    dedupe_keys = list(dedupe_keys)
    if dedupe_keys:
        df = (
            df.group_by(dedupe_keys)
            .agg(pl.col("seconds_u16").min())
            .select(list(schema.keys()))
        )
    else:
        df = df.select(list(schema.keys()))

    tmp_path = out_path + ".tmp"
    df.write_parquet(tmp_path, compression="zstd", statistics=True, row_group_size=128_000)
    os.replace(tmp_path, out_path)
    return df.height


def compute_target_nodes(
    src: np.ndarray,
    comp_id: np.ndarray,
    comp_to_anchor_nodes: Dict[int, np.ndarray],
    anchor_idx: np.ndarray,
    anchor_nodes: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray, bool]:
    if src.size == 0:
        return np.empty(0, dtype=np.int32), np.empty(0, dtype=np.int32), False

    src_comp = np.unique(comp_id[src])
    targets_idx = np.empty(0, dtype=np.int32)
    if src_comp.size > 0:
        target_arrays = [comp_to_anchor_nodes.get(int(comp_val)) for comp_val in src_comp]
        target_arrays = [arr for arr in target_arrays if arr is not None and arr.size > 0]
        if target_arrays:
            targets_idx = target_arrays[0].copy() if len(target_arrays) == 1 else np.concatenate(target_arrays)

    fallback_used = False
    if src_comp.size > 0 and targets_idx.size == 0:
        fallback_used = True
        try:
            anchor_mask = (anchor_idx >= 0) & np.isin(comp_id, src_comp)
            targets_idx = np.nonzero(anchor_mask)[0].astype(np.int32, copy=False)
        except Exception:
            targets_idx = anchor_nodes.copy()

    if targets_idx.size > 1:
        targets_idx = np.unique(targets_idx)

    return src_comp, targets_idx, fallback_used


def persist_graph_arrays(tmpdir: str, arrays: Dict[str, np.ndarray]) -> Dict[str, str]:
    paths: Dict[str, str] = {}
    for name, arr in arrays.items():
        if arr is None:
            continue
        path = os.path.join(tmpdir, f"{name}.npy")
        np.save(path, arr, allow_pickle=False)
        paths[name] = path
    return paths


def execute_tasks(
    work: List[Any],
    graph_ctx: GraphContext,
    kernel_threads: int,
    max_workers: int,
    worker_fn: Callable[[Any], Any],
    describe: Callable[[Any], str],
) -> None:
    if not work:
        return

    import multiprocessing as mp
    from concurrent.futures import ProcessPoolExecutor, as_completed

    effective_workers = min(max_workers, len(work))
    print(
        f"[debug] Launching ProcessPool with max_workers={effective_workers} pending_tasks={len(work)}"
    )

    try:
        ctx = mp.get_context("spawn")
    except ValueError:
        ctx = mp.get_context()

    with tempfile.TemporaryDirectory() as tmpdir:
        persist_start = time.perf_counter()
        paths = persist_graph_arrays(tmpdir, graph_ctx.worker_arrays())
        print(
            f"[debug] Materialized memmap views for workers in {time.perf_counter() - persist_start:.2f}s"
        )
        csr_pack = {
            "paths": paths,
            "threads": kernel_threads,
        }

        with ProcessPoolExecutor(
            max_workers=effective_workers,
            mp_context=ctx,
            initializer=init_graph_worker,
            initargs=(csr_pack,),
        ) as ex:
            futures: Dict[Any, Tuple[float, str]] = {}
            for task in work:
                start = time.perf_counter()
                desc = describe(task)
                fut = ex.submit(worker_fn, task)
                futures[fut] = (start, desc)
            for fut in as_completed(futures):
                start, desc = futures.pop(fut)
                try:
                    fut.result()
                    elapsed = time.perf_counter() - start
                    if desc:
                        print(f"[debug] {desc} finished in {elapsed:.2f}s")
                except Exception as exc:
                    print(f"[error] {desc} failed: {exc}")
