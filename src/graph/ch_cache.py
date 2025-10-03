"""Helpers for caching Contraction Hierarchies (CH) prepared graphs on disk."""

from __future__ import annotations

import os
from typing import Optional

import numpy as np

try:
    from t_hex import CHGraph, ch_build_from_csr, ch_from_bytes  # type: ignore
except ImportError as exc:  # pragma: no cover - native module missing in docs builds
    raise RuntimeError("t_hex (native module) must be built before using CH helpers") from exc


def _ch_cache_path(cache_dir: str, suffix: str = "") -> str:
    suffix = suffix.strip()
    name = "ch_graph" + (suffix if suffix else "") + ".bin"
    return os.path.join(cache_dir, name)


def load_cached_ch(cache_dir: str, suffix: str = "") -> Optional[CHGraph]:
    path = _ch_cache_path(cache_dir, suffix)
    if not os.path.exists(path):
        return None
    with open(path, "rb") as f:
        data = f.read()
    return ch_from_bytes(data)


def _as_c_contiguous(arr: np.ndarray, dtype) -> np.ndarray:
    return np.ascontiguousarray(arr, dtype=dtype)


def build_and_cache_ch(
    cache_dir: str,
    indptr: np.ndarray,
    indices: np.ndarray,
    w_sec: np.ndarray,
    *,
    suffix: str = "",
) -> CHGraph:
    os.makedirs(cache_dir, exist_ok=True)
    ch_graph = ch_build_from_csr(
        _as_c_contiguous(indptr, np.int64),
        _as_c_contiguous(indices, np.int32),
        _as_c_contiguous(w_sec, np.uint16),
    )
    path = _ch_cache_path(cache_dir, suffix)
    with open(path, "wb") as f:
        f.write(ch_graph.to_bytes())
    return ch_graph


def load_or_build_ch(
    cache_dir: str,
    indptr: np.ndarray,
    indices: np.ndarray,
    w_sec: np.ndarray,
    *,
    suffix: str = "",
) -> CHGraph:
    cached = load_cached_ch(cache_dir, suffix)
    if cached is not None:
        return cached
    return build_and_cache_ch(cache_dir, indptr, indices, w_sec, suffix=suffix)
