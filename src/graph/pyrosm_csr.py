import os
from typing import Tuple

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.ipc as pa_ipc
from pyrosm import OSM
from t_hex import build_csr_from_arrays, compute_h3_for_nodes


def _default_drive_speed_kmh_for_highway(hwy: str) -> float:
    # Conservative defaults; favor smaller values to avoid underestimating travel time
    if not isinstance(hwy, str):
        return 40.0
    h = hwy.lower()
    if "motorway" in h:
        return 100.0
    if "trunk" in h:
        return 80.0
    if "primary" in h:
        return 65.0
    if "secondary" in h:
        return 55.0
    if "tertiary" in h:
        return 45.0
    if "residential" in h or "living_street" in h:
        return 25.0
    if "service" in h or "unclassified" in h:
        return 15.0
    return 40.0


def _parse_maxspeed_kmh(ms: pd.Series) -> pd.Series:
    """Parse OSM maxspeed values to km/h robustly.
    Handles examples like '50', '50 km/h', '35 mph', '30;45', '20; 30 mph', 'signals', None.
    Strategy: extract numeric tokens; if 'mph' appears in the string, convert the min token to km/h.
    Otherwise, treat tokens as km/h. Unknowns remain NaN.
    """
    if ms is None:
        return pd.Series([], dtype="float64")

    s = ms.astype(str).str.lower()

    # Identify mph rows (anywhere in the string)
    mask_mph = s.str.contains("mph", na=False)

    # Extract all numeric tokens per row
    # This yields a Series indexed by a MultiIndex (row, match_idx)
    nums = s.str.extractall(r"(\d+(?:\.\d+)?)")[0]
    # Convert tokens to float, coerce failures to NaN and drop them
    nums = pd.to_numeric(nums, errors="coerce").dropna()

    # Take the minimum numeric value per original row (conservative speed)
    mins = nums.groupby(level=0).min()

    out = pd.Series(np.nan, index=s.index, dtype="float64")
    out.loc[mins.index] = mins.values

    # Convert mph rows to km/h
    # Intersect mph mask with rows where we have numeric mins
    mph_idx = out.index[mask_mph.reindex(out.index, fill_value=False) & out.notna()]
    out.loc[mph_idx] = out.loc[mph_idx] * 1.60934

    # Clean up obviously invalid zeros
    out.replace(0.0, np.nan, inplace=True)
    return out


def _default_drive_speed_kmh_for_highway_vec(hw: pd.Series) -> pd.Series:
    """Vectorized default speeds by highway type in km/h."""
    if hw is None:
        return pd.Series([], dtype="float64")
    s = hw.astype(str).str.lower()
    out = pd.Series(40.0, index=s.index, dtype="float64")
    # Order matters: assign from fastest to slowest where matches
    out[s.str.contains("motorway", na=False)] = 100.0
    out[s.str.contains("trunk", na=False)] = 80.0
    out[s.str.contains("primary", na=False)] = 65.0
    out[s.str.contains("secondary", na=False)] = 55.0
    out[s.str.contains("tertiary", na=False)] = 45.0
    mask_res = s.str.contains("residential|living_street", na=False)
    out[mask_res] = 25.0
    mask_svc = s.str.contains("service|unclassified", na=False)
    out[mask_svc] = 15.0
    return out


def _compute_travel_time_seconds_drive(edges: pd.DataFrame) -> np.ndarray:
    # Prefer maxspeed; fall back to highway-based defaults
    kmh = _parse_maxspeed_kmh(edges.get("maxspeed"))
    # Fill NaNs via highway mapping
    if "highway" in edges.columns:
        defaults = _default_drive_speed_kmh_for_highway_vec(edges["highway"]).astype(float)
        kmh = kmh.fillna(defaults)
    else:
        kmh = kmh.fillna(40.0)

    # Convert to m/s, avoid zeros
    mps = (kmh.clip(lower=1.0) * (1000.0 / 3600.0)).to_numpy(dtype="float64", copy=False)
    length_m = edges.get("length")
    if length_m is None:
        # If pyrosm didn't compute length, assume 20m segments as a pessimistic fallback
        length_m = pd.Series(20.0, index=edges.index)
    L = length_m.to_numpy(dtype="float64", copy=False)

    secs = np.ceil(L / np.maximum(mps, 0.1))
    secs = np.nan_to_num(secs, nan=1e12, posinf=1e12, neginf=0.0)
    secs = np.minimum(np.maximum(secs, 0.0), 65534.0).astype(np.uint16)
    return secs


def _compute_travel_time_seconds_walk(edges: pd.DataFrame) -> np.ndarray:
    # 5 km/h walking speed
    mps = 5_000.0 / 3600.0
    length_m = edges.get("length")
    if length_m is None:
        length_m = pd.Series(15.0, index=edges.index)
    L = length_m.to_numpy(dtype="float64", copy=False)
    secs = np.ceil(L / mps)
    secs = np.minimum(np.maximum(secs, 0.0), 65534.0).astype(np.uint16)
    return secs


def build_csr_from_pbf(pbf_path: str, mode: str):
    """
    Build CSR graph directly from a local .pbf using Pyrosm.
    Returns: (node_ids:int64[N], indptr:int64[N+1], indices:int32[M], w_sec:uint16[M], lats:float32[N], lons:float32[N])
    """
    osm = OSM(pbf_path)
    network_type = "driving" if "drive" in mode else "walking"
    nodes, edges = osm.get_network(network_type=network_type, nodes=True)

    if nodes is None or edges is None or nodes.empty or edges.empty:
        raise RuntimeError("Pyrosm returned empty nodes/edges. Check the PBF or network_type.")

    # Ensure consistent columns
    # nodes: expect id (int), x (lon), y (lat)
    if "id" not in nodes.columns:
        # In some pyrosm versions, the index is the id
        nodes = nodes.reset_index().rename(columns={nodes.index.name or "index": "id"})
    if "x" not in nodes.columns or "y" not in nodes.columns:
        # Project if needed (rare), but assume EPSG:4326 provided by Pyrosm
        if "lon" in nodes.columns and "lat" in nodes.columns:
            nodes = nodes.rename(columns={"lon": "x", "lat": "y"})
        else:
            raise RuntimeError("Nodes are missing x/y coordinates.")

    # edges: expect u, v (node ids) and direction info; Pyrosm provides 'oneway' bool or string
    if not {"u", "v"}.issubset(edges.columns):
        raise RuntimeError("Edges are missing 'u'/'v' columns.")

    # Travel times per edge (in seconds)
    if network_type == "driving":
        w = _compute_travel_time_seconds_drive(edges)
    else:
        w = _compute_travel_time_seconds_walk(edges)

    # Extract arrays for Rust CSR builder
    u = edges["u"].to_numpy(dtype=np.int64, copy=False)
    v = edges["v"].to_numpy(dtype=np.int64, copy=False)
    # Normalize oneway to uint8 array (1=yes, 0=no)
    oneway_col = edges.get("oneway")
    if oneway_col is None:
        oneway_u8 = np.zeros(len(edges), dtype=np.uint8)
    else:
        if oneway_col.dtype == bool:
            oneway_u8 = oneway_col.to_numpy(dtype=np.uint8, copy=False)
        else:
            sv = oneway_col.astype(str).str.lower()
            oneway_u8 = sv.isin(["yes", "true", "1"]).to_numpy(dtype=np.uint8, copy=False)

    node_ids = nodes["id"].to_numpy(dtype=np.int64, copy=False)
    lons = nodes["x"].to_numpy(dtype=np.float32, copy=False)
    lats = nodes["y"].to_numpy(dtype=np.float32, copy=False)

    # Delegate CSR construction to Rust
    node_ids_out, indptr, indices, w_sec, lats_out, lons_out = build_csr_from_arrays(
        node_ids, lats, lons, u, v, oneway_u8, w.astype(np.uint16, copy=False)
    )

    return (
        np.asarray(node_ids_out, dtype=np.int64),
        np.asarray(indptr, dtype=np.int64),
        np.asarray(indices, dtype=np.int32),
        np.asarray(w_sec, dtype=np.uint16),
        np.asarray(lats_out, dtype=np.float32),
        np.asarray(lons_out, dtype=np.float32),
    )


def _csr_cache_dir(pbf_path: str, mode: str) -> str:
    base = os.path.basename(pbf_path).split(".")[0]
    out_dir = os.path.join("data", "osm", "cache_csr")
    os.makedirs(out_dir, exist_ok=True)
    return os.path.join(out_dir, f"{base}_{mode}.npycache")


def _save_npy(arr: np.ndarray, path: str):
    np.save(path, arr)


def _load_npy(path: str, mmap: bool = True) -> np.ndarray:
    return np.load(path, mmap_mode="r" if mmap else None, allow_pickle=False)


def save_csr_npy(cache_dir: str,
                 node_ids: np.ndarray,
                 indptr: np.ndarray,
                 indices: np.ndarray,
                 w_sec: np.ndarray,
                 lats: np.ndarray,
                 lons: np.ndarray,
                 h3_by_res: dict[int, np.ndarray],
                 meta: dict | None = None):
    os.makedirs(cache_dir, exist_ok=True)
    _save_npy(node_ids, os.path.join(cache_dir, "node_ids.npy"))
    _save_npy(indptr, os.path.join(cache_dir, "indptr.npy"))
    _save_npy(indices, os.path.join(cache_dir, "indices.npy"))
    _save_npy(w_sec, os.path.join(cache_dir, "w_sec.npy"))
    _save_npy(lats, os.path.join(cache_dir, "lats.npy"))
    _save_npy(lons, os.path.join(cache_dir, "lons.npy"))
    for r, arr in h3_by_res.items():
        _save_npy(arr, os.path.join(cache_dir, f"h3_r{int(r)}.npy"))
    if meta:
        import json
        with open(os.path.join(cache_dir, "meta.json"), "w") as f:
            json.dump(meta, f)


def load_csr_npy(cache_dir: str, res: list[int]) -> tuple:
    node_ids = _load_npy(os.path.join(cache_dir, "node_ids.npy"))
    indptr = _load_npy(os.path.join(cache_dir, "indptr.npy"))
    indices = _load_npy(os.path.join(cache_dir, "indices.npy"))
    w_sec = _load_npy(os.path.join(cache_dir, "w_sec.npy"))
    lats = _load_npy(os.path.join(cache_dir, "lats.npy"))
    lons = _load_npy(os.path.join(cache_dir, "lons.npy"))
    h3_list = []
    for r in res:
        p = os.path.join(cache_dir, f"h3_r{int(r)}.npy")
        if os.path.exists(p):
            h3_list.append(_load_npy(p))
        else:
            h3_list.append(None)
    return node_ids, indptr, indices, w_sec, lats, lons, h3_list


def load_or_build_csr(pbf_path: str, mode: str, resolutions: list[int], progress: bool = True):
    cache_dir = _csr_cache_dir(pbf_path, mode)
    cache_valid = False
    
    if os.path.isdir(cache_dir):
        # Validate cache before loading
        meta_path = os.path.join(cache_dir, "meta.json")
        if os.path.exists(meta_path):
            try:
                import json
                with open(meta_path, "r") as f:
                    meta = json.load(f)
                
                # Check if PBF file has been modified since cache was created
                pbf_mtime = os.path.getmtime(pbf_path)
                cache_pbf_mtime = meta.get("pbf_mtime")
                
                if cache_pbf_mtime is not None:
                    if pbf_mtime <= cache_pbf_mtime:
                        cache_valid = True
                    else:
                        print(f"[graph cache] PBF modified ({pbf_mtime} > {cache_pbf_mtime}), invalidating cache for {mode}")
                else:
                    # Old cache format without pbf_mtime - treat as potentially stale
                    print(f"[graph cache] WARNING: Cache missing pbf_mtime metadata, treating as stale. Rebuild recommended.")
                    cache_valid = False
                if meta.get("hierarchical_h3") is not True:
                    print("[graph cache] Cache missing hierarchical_h3 flag; rebuilding to ensure consistent mapping.")
                    cache_valid = False
            except Exception as e:
                print(f"[graph cache] Failed to read/validate metadata: {e}, rebuilding cache")
                cache_valid = False
        else:
            print(f"[graph cache] No metadata found, cannot validate cache age. Rebuilding.")
            cache_valid = False
    
    if cache_valid:
        try:
            node_ids, indptr, indices, w_sec, lats, lons, h3_list = load_csr_npy(cache_dir, resolutions)
            # If any requested res missing, compute and save it
            missing = [i for i, a in enumerate(h3_list) if a is None]
            if missing:
                print(f"[graph cache] Missing H3 columns for requested resolutions; recomputing all {resolutions}.")
                h3_ids = compute_h3_for_nodes(lats.astype(np.float32, copy=False),
                                              lons.astype(np.float32, copy=False),
                                              np.array(resolutions, dtype=np.int32),
                                              os.cpu_count(),
                                              bool(progress))
                h3_ids = np.asarray(h3_ids, dtype=np.uint64)
                new_h3_list = []
                for j, r in enumerate(resolutions):
                    arr = h3_ids[:, j]
                    _save_npy(arr, os.path.join(cache_dir, f"h3_r{int(r)}.npy"))
                    new_h3_list.append(arr)
                h3_list = new_h3_list
                # Update metadata to mark hierarchical H3 mapping
                import json
                meta_path = os.path.join(cache_dir, "meta.json")
                try:
                    with open(meta_path, "r") as f:
                        meta = json.load(f)
                    meta["hierarchical_h3"] = True
                    with open(meta_path, "w") as f:
                        json.dump(meta, f)
                except Exception as e:
                    print(f"[graph cache] Warning: Failed to update metadata: {e}")
            # Stack h3_by_res into [N,R]
            h3_by_res = np.column_stack(h3_list) if h3_list else np.empty((len(lats), 0), dtype=np.uint64)
            print(f"[graph cache] Loaded validated cache for {mode}")
            return node_ids, indptr, indices, w_sec, lats, lons, h3_by_res, resolutions
        except Exception as e:
            print(f"[graph cache] Failed to load cache: {e}, rebuilding")
            pass

    # Build and cache anew
    print(f"[graph cache] Building fresh CSR graph for {mode} from {os.path.basename(pbf_path)}")
    node_ids, indptr, indices, w_sec, lats, lons = build_csr_from_pbf(pbf_path, mode)
    # Precompute H3 for requested resolutions and save
    h3_ids = compute_h3_for_nodes(lats, lons, np.array(resolutions, dtype=np.int32), os.cpu_count(), bool(progress))
    h3_ids = np.asarray(h3_ids, dtype=np.uint64)
    h3_by_res = {int(r): h3_ids[:, i] for i, r in enumerate(resolutions)}
    
    # Save with enhanced metadata including PBF modification time
    pbf_mtime = os.path.getmtime(pbf_path)
    save_csr_npy(
        cache_dir,
        node_ids, indptr, indices, w_sec, lats, lons, h3_by_res,
        meta={
            "pbf": os.path.basename(pbf_path),
            "mode": mode,
            "pbf_mtime": pbf_mtime,
            "cache_created": os.path.getmtime(cache_dir) if os.path.exists(cache_dir) else pbf_mtime,
            "hierarchical_h3": True,
        }
    )
    # Return stacked [N,R]
    h3_mat = np.column_stack([h3_by_res[int(r)] for r in resolutions]) if resolutions else np.empty((len(lats), 0), dtype=np.uint64)
    return node_ids, indptr, indices, w_sec, lats, lons, h3_mat, resolutions
