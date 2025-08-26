#!/usr/bin/env python3
# server/runtime_tiles.py

import os
import math
from functools import lru_cache
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import h3
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn

from src.categories import get_category

APP_NAME = "TownScout Tiles (seconds-based)"

# ---------- Config ----------
# Paths
DATA_DIR = os.environ.get("TS_DATA_DIR", "out")          # where T_hex & D_anchor parquet live
STATE = os.environ.get("TS_STATE", "massachusetts")
DEFAULT_RES = int(os.environ.get("TS_H3_RES", "8"))

# Column & sentinel conventions (must match your precompute scripts)
UNREACH_U16 = np.uint16(65535)  # reserved 'unreachable' sentinel

# ---------- Helpers ----------
def tile_to_bbox(z: int, x: int, y: int) -> Tuple[float, float, float, float]:
    """Web Mercator tile -> lon/lat bbox (min_lon, min_lat, max_lon, max_lat)."""
    n = 2.0 ** z
    lon1 = x / n * 360.0 - 180.0
    lat1 = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * y / n))))
    lon2 = (x + 1) / n * 360.0 - 180.0
    lat2 = math.degrees(math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))))
    return (min(lon1, lon2), min(lat1, lat2), max(lon1, lon2), max(lat1, lat2))

def bbox_h3_cells(bbox: Tuple[float, float, float, float], res: int) -> List[str]:
    """Quickly cover a bbox with H3 cells (approx by sampling)."""
    min_lon, min_lat, max_lon, max_lat = bbox
    cells = set()
    # adaptive step: ~30 probes per dimension
    step_lat = max(0.02, (max_lat - min_lat) / 30.0)
    step_lon = max(0.02, (max_lon - min_lon) / 30.0)
    lat = min_lat
    while lat <= max_lat + 1e-9:
        lon = min_lon
        while lon <= max_lon + 1e-9:
            cells.add(h3.latlng_to_cell(lat, lon, res))
            lon += step_lon
        lat += step_lat
    return list(cells)

def cell_to_uint64(h: str) -> np.uint64:
    """H3 cell string → uint64 (consistent with precompute scripts)."""
    return np.uint64(int(h, 16))

# ---------- Data loading (cached) ----------
@lru_cache(maxsize=8)
def load_T_hex(mode: str, res: int) -> pd.DataFrame:
    """
    Load seconds-based hex→anchor table produced by precompute_t_hex.py.
    Expected columns: h3_id(uint64), prov(uint8), a0_id(int32), a0_s(uint16), a1_id, a1_s, ..., mode, res, snapshot_ts
    """
    # Example filename pattern; adjust to your actual output name if different:
    # out/T_hex_{mode}.parquet with multi-res rows (filter by res)
    path = os.path.join(DATA_DIR, f"T_hex_{mode}.parquet")
    if not os.path.exists(path):
        raise RuntimeError(f"T_hex parquet missing at {path}")
    cols_needed = {"h3_id", "prov", "mode", "res"}
    df = pd.read_parquet(path)
    missing = cols_needed - set(df.columns)
    if missing:
        raise RuntimeError(f"T_hex missing required columns: {missing}")

    df = df[df["res"].astype(int) == int(res)]
    df = df[df["mode"] == mode]

    # Identify how many anchor slots (K) exist by scanning columns
    k_slots = 0
    while f"a{k_slots}_id" in df.columns and f"a{k_slots}_s" in df.columns:
        k_slots += 1
    if k_slots == 0:
        raise RuntimeError("T_hex has no a{k}_id/a{k}_s columns")

    keep = ["h3_id", "prov"] + sum(([f"a{i}_id", f"a{i}_s"] for i in range(k_slots)), [])
    df = df[keep].copy()
    # enforce dtypes
    df["h3_id"] = df["h3_id"].astype("uint64")
    df["prov"] = df["prov"].astype("uint8")
    for i in range(k_slots):
        df[f"a{i}_id"] = df[f"a{i}_id"].astype("int32")
        df[f"a{i}_s"] = df[f"a{i}_s"].astype("uint16")
    df.attrs["k_slots"] = k_slots
    return df

@lru_cache(maxsize=8)
def load_D_anchor(mode: str) -> pd.DataFrame:
    """
    Load seconds-based anchor→category table produced by precompute_d_anchor.py.
    Expected columns: anchor_int_id(int32), category_id(int32), mode(str), seconds_u16(uint16), snapshot_ts
    """
    # Example filename: out/D_anchor_{mode}.parquet
    path = os.path.join(DATA_DIR, f"D_anchor_{mode}.parquet")
    if not os.path.exists(path):
        raise RuntimeError(f"D_anchor parquet missing at {path}")
    df = pd.read_parquet(path)
    need = {"anchor_int_id", "category_id", "mode", "seconds_u16"}
    missing = need - set(df.columns)
    if missing:
        raise RuntimeError(f"D_anchor missing required columns: {missing}")
    df = df[df["mode"] == mode][["anchor_int_id", "category_id", "seconds_u16"]].copy()
    df["anchor_int_id"] = df["anchor_int_id"].astype("int32")
    df["category_id"] = df["category_id"].astype("int32")
    df["seconds_u16"] = df["seconds_u16"].astype("uint16")
    return df

def build_D_lookup(mode: str, category_ids: List[int]) -> Dict[int, np.ndarray]:
    """
    Build a lookup: category_id -> vector 'd' where d[anchor_int_id] = seconds_u16 (or UNREACH_U16 if missing).
    We pack into a dense vector sized to max anchor_int_id+1 to allow O(1) indexing in the inner loop.
    """
    D = load_D_anchor(mode)
    # Find universe size
    max_aid = int(D["anchor_int_id"].max()) if len(D) else -1
    if max_aid < 0:
        return {cid: np.zeros(0, dtype=np.uint16) for cid in category_ids}
    out: Dict[int, np.ndarray] = {}
    for cid in category_ids:
        d = np.full(max_aid + 1, UNREACH_U16, dtype=np.uint16)
        sub = D[D["category_id"] == int(cid)]
        if len(sub):
            d[sub["anchor_int_id"].to_numpy()] = sub["seconds_u16"].to_numpy()
        out[int(cid)] = d
    return out

# ---------- FastAPI ----------
app = FastAPI(title=APP_NAME)

# Serve static files - mount before routes
app.mount("/tiles", StaticFiles(directory="tiles"), name="tiles")

@app.get("/health")
def health():
    return {"ok": True, "app": APP_NAME}

@app.post("/tiles/criteria")
def tiles_criteria(
    z: int = Query(...),
    x: int = Query(...),
    y: int = Query(...),
    res: int = Query(DEFAULT_RES),
    unit: str = Query("minutes", description='threshold unit: "minutes" (default) or "seconds"'),
    criteria: str = Query(..., description='JSON string of criteria: [{"category","mode","threshold","op"="AND"|"OR"}]'),
):
    """
    Return a GeoJSON FeatureCollection of H3 hex polygons at the requested tile
    that satisfy the boolean criteria, evaluated in **seconds** using:
        ETA_hex,cat = min_k( a{k}_s_hex + D_anchor[cat][ a{k}_id ] )
    where a{k} are the precomputed K-best anchors for that hex.

    Example 'criteria':
      [
        {"category":"costco","mode":"drive","threshold":15,"op":"AND"},
        {"category":"chipotle","mode":"drive","threshold":30,"op":"AND"},
        {"category":"major_airport","mode":"drive","threshold":240,"op":"AND"}
      ]
    """
    try:
        import json
        # Parse the JSON string into a list of dicts
        criteria = json.loads(criteria) if isinstance(criteria, str) else criteria
        
        # Parse criteria → (category_id, mode, threshold_seconds, op)
        parsed: List[Dict] = []
        for c in criteria:
            if "category" not in c or "threshold" not in c:
                raise ValueError("Each criterion must have 'category' and 'threshold'")
            cat = get_category(c["category"])
            mode = str(c.get("mode", getattr(cat, "default_mode", "drive")))
            thr = float(c["threshold"])
            thr_s = int(round(thr * 60.0)) if unit.lower().startswith("min") else int(round(thr))
            op = str(c.get("op", "AND")).upper()
            if op not in ("AND", "OR"):
                raise ValueError("op must be 'AND' or 'OR'")
            parsed.append({"cid": int(cat.id), "mode": mode, "thr_s": thr_s, "op": op})

        # Group by mode (we evaluate drive and walk independently, then combine)
        by_mode: Dict[str, List[Dict]] = {}
        for it in parsed:
            by_mode.setdefault(it["mode"], []).append(it)

        bbox = tile_to_bbox(z, x, y)
        h3_cells = bbox_h3_cells(bbox, res)
        if not h3_cells:
            return JSONResponse({"type": "FeatureCollection", "features": []})

        # We'll build a global boolean mask across all criteria
        global_mask: Dict[str, bool] = {h: True for h in h3_cells}

        for mode, items in by_mode.items():
            # Load T_hex for mode+res and reduce to the H3 cells in the tile
            T = load_T_hex(mode, res)
            k_slots = T.attrs["k_slots"]
            wanted_ids = np.array([cell_to_uint64(h) for h in h3_cells], dtype=np.uint64)
            Tsub = T[T["h3_id"].isin(wanted_ids)].copy()
            if not len(Tsub):
                # No precomputed coverage in this tile for this mode; AND keeps them false, OR unchanged
                for h in h3_cells:
                    # If any constraint in this mode is AND, it will fail
                    for it in items:
                        if it["op"] == "AND":
                            global_mask[h] = False and global_mask[h]
                continue

            # Build a fast map h3_id_uint64 -> string h3
            u64_to_h3: Dict[int, str] = {int(h, 16): h for h in h3_cells}

            # Build D lookups for all categories in this mode
            cat_ids = [it["cid"] for it in items]
            D_lookup = build_D_lookup(mode, cat_ids)

            # Evaluate each criterion independently, then combine with op
            # We'll compute a local pass mask per hex for each item.
            # Data layout: for each hex row, we have a{i}_id (int32) and a{i}_s (uint16).
            # For each item, ETA = min_i( a{i}_s + D[ cid ][ a{i}_id ] )
            # Handle missing anchors (id == -1) or unreachable seconds with sentinels.
            # Vectorization by slots K to keep it fast without dense [H×A].
            # Convert Tsub into arrays per slot
            slot_ids = [Tsub[f"a{i}_id"].to_numpy(dtype=np.int32, copy=False) for i in range(k_slots)]
            slot_secs = [Tsub[f"a{i}_s"].to_numpy(dtype=np.uint16, copy=False) for i in range(k_slots)]

            # Index by hex
            hex_ids = Tsub["h3_id"].to_numpy(dtype=np.uint64, copy=False)

            for it in items:
                cid = it["cid"]
                thr_s = int(it["thr_s"])
                op = it["op"]

                dvec = D_lookup.get(cid)
                if dvec is None or dvec.size == 0:
                    # No data for this category; AND fails, OR makes no change
                    for h in h3_cells:
                        if op == "AND":
                            global_mask[h] = False and global_mask[h]
                    continue

                # Compute ETA per hex using precomputed K slots
                # Start with all UNREACH
                eta = np.full(hex_ids.shape[0], UNREACH_U16, dtype=np.uint16)

                for i in range(k_slots):
                    aid = slot_ids[i]
                    hs = slot_secs[i]

                    # valid anchors are aid >= 0 and hs < UNREACH
                    valid = (aid >= 0) & (hs < UNREACH_U16)
                    if not np.any(valid):
                        continue

                    # Map anchor_int_id -> seconds_u16 for this category
                    # For invalid aid (>= len(dvec)), treat as UNREACH
                    # Clamp indices to vector size to avoid index error
                    aid_valid = aid[valid]
                    aid_valid_clamped = np.minimum(aid_valid, dvec.size - 1, dtype=np.int32)
                    d_secs = dvec[aid_valid_clamped]  # uint16

                    # Sum seconds with saturation at UNREACH-1
                    sum_secs = hs[valid].astype(np.int32) + d_secs.astype(np.int32)
                    sum_secs = np.where(
                        (hs[valid] >= UNREACH_U16) | (d_secs >= UNREACH_U16), 65534, np.minimum(sum_secs, 65534)
                    ).astype(np.uint16)

                    # Take per-hex min across slots
                    # Initialize eta for valid indices if greater than sum
                    eta_valid = eta[valid]
                    eta[valid] = np.minimum(eta_valid, sum_secs)

                # Build pass mask for these hexes
                local_pass = (eta.astype(np.int32) <= thr_s)

                # Combine with global mask using op, placing by hex_id
                for idx, h64 in enumerate(hex_ids):
                    h = u64_to_h3.get(int(h64))
                    if h is None:
                        continue
                    if op == "OR":
                        global_mask[h] = bool(local_pass[idx]) or global_mask[h]
                    else:
                        global_mask[h] = bool(local_pass[idx]) and global_mask[h]

        # Emit GeoJSON of passing hexes
        features = []
        for h, ok in global_mask.items():
            if not ok:
                continue
            boundary = h3.cell_to_boundary(h)
            # H3 returns (lat, lon); GeoJSON needs [lon, lat]
            ring = [[lon, lat] for lat, lon in boundary]
            # close polygon
            ring.append([ring[0][0], ring[0][1]])
            features.append({
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [ring]},
                "properties": {"h3": h}
            })

        return JSONResponse({"type": "FeatureCollection", "features": features})

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))