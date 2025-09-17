#!/usr/bin/env python3
# api/app/main.py

import os
import sys
import math
import glob
import pyarrow.dataset as ds
from typing import Dict, List, Tuple, Optional
import json

import numpy as np
import pandas as pd
import h3
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, FileResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from typing import Any
# Ensure we can import modules from the repo's src/ when running via uvicorn
_SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, 'src'))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

try:
    from taxonomy import BRAND_REGISTRY
except Exception:
    BRAND_REGISTRY = {}

APP_NAME = "TownScout D_anchor API"

# ---------- Config ----------
# Paths
DATA_DIR = os.environ.get("TS_DATA_DIR", "data/minutes")
STATE = os.environ.get("TS_STATE", "massachusetts")

# Column & sentinel conventions
UNREACH_U16 = np.uint16(65535)

# ---------- Data loading (cached) ----------
def load_D_anchor(mode: str) -> pd.DataFrame:
    """
    Load seconds-based anchor→category table produced by precompute_d_anchor.py.
    Expected columns: anchor_int_id(int32), category_id(int32), seconds_u16(uint16)
    """
    # Prefer partitioned dataset if present (mode={0,2}/category_id=*)
    mode_map = {"drive": 0, "walk": 2}
    mode_id = mode_map.get(mode, 0)
    part_dir = os.path.join(DATA_DIR, f"mode={mode_id}")
    if os.path.isdir(part_dir):
        # Read the Hive-partitioned dataset so partition columns (category_id) are materialized
        dataset = ds.dataset(part_dir, format="parquet", partitioning="hive")
        table = dataset.to_table(columns=["anchor_int_id", "category_id", "seconds"])  # seconds written by pipeline
        df = table.to_pandas()
        # Normalize dtype/name to API contract
        df = df.rename(columns={"seconds": "seconds_u16"})
        df["seconds_u16"] = df["seconds_u16"].astype("uint16", errors="ignore")
        return df[["anchor_int_id", "category_id", "seconds_u16"]]

    # Fallback: legacy unified file
    path = os.path.join(DATA_DIR, f"{STATE}_anchor_to_category_{mode}.parquet")
    if not os.path.exists(path):
        raise RuntimeError(f"D_anchor parquet missing at {path}")
    df = pd.read_parquet(path)
    need = {"anchor_int_id", "category_id", "seconds_u16"}
    missing = need - set(df.columns)
    if missing:
        raise RuntimeError(f"D_anchor missing required columns: {missing}")
    return df[["anchor_int_id", "category_id", "seconds_u16"]].copy()

# ---------- Category resolution ----------

def _mode_to_partition(mode: str) -> int:
    return {"drive": 0, "walk": 2}.get(mode, 0)

def list_available_categories(mode: str) -> list[int]:
    """Return sorted list of available category_id from Hive partitions for given mode, if present."""
    base = os.path.join(DATA_DIR, f"mode={_mode_to_partition(mode)}")
    ids: list[int] = []
    if os.path.isdir(base):
        for name in os.listdir(base):
            if name.startswith("category_id="):
                try:
                    ids.append(int(name.split("=", 1)[1]))
                except Exception:
                    pass
    return sorted(set(ids))

def _load_category_labels() -> Dict[str, str]:
    """Load optional category id -> label mapping.
    Supports JSON (object {"1":"Supermarket",...}) or CSV with headers category_id,label.
    Returns mapping with string keys.
    """
    base = os.path.join("data", "taxonomy")
    # JSON first
    json_path = os.path.join(base, "category_labels.json")
    if os.path.isfile(json_path):
        try:
            with open(json_path, "r") as f:
                obj = json.load(f)
                # Normalize keys to str
                return {str(k): str(v) for k, v in obj.items()}
        except Exception:
            pass
    # CSV fallback
    csv_path = os.path.join(base, "category_labels.csv")
    if os.path.isfile(csv_path):
        try:
            import csv
            out: Dict[str, str] = {}
            with open(csv_path, newline="") as f:
                rdr = csv.DictReader(f)
                for row in rdr:
                    k = str(row.get("category_id"))
                    v = str(row.get("label"))
                    if k and v and k != "None":
                        out[k] = v
            return out
        except Exception:
            pass
    return {}

def resolve_category_id(category: str, mode: str) -> int:
    """Resolve a category input to numeric id.
    Accepts a numeric string directly. If a taxonomy file is present in data/taxonomy, it can be wired later.
    """
    # Numeric id is always allowed
    try:
        return int(category)
    except Exception:
        # No taxonomy wired yet — fall back to available ids and instruct caller
        raise HTTPException(status_code=404, detail=f"Unknown category '{category}'. Use numeric category_id from /api/categories?mode={mode}.")

# ---------- FastAPI ----------
app = FastAPI(title=APP_NAME)

# Mount static files for the web demo assets
# Note: We serve .pmtiles via a custom route below to ensure HTTP Range (byte serving).
app.mount("/static", StaticFiles(directory="tiles/web"), name="static")
app.mount("/tiles/web", StaticFiles(directory="tiles/web"), name="tiles-web")

# Basic CORS for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, restrict this to your frontend's domain
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

@app.get("/")
async def serve_frontend():
    """Serve the main frontend page"""
    return FileResponse("tiles/web/index.html")

@app.get("/health")
def health():
    return {"ok": True, "app": APP_NAME}

@app.get("/api/categories")
def categories(mode: str = Query("drive", description="Travel mode, e.g. 'drive' or 'walk'")):
    """List available category_id values and optional labels for current dataset/partitions."""
    ids = list_available_categories(mode)
    labels = _load_category_labels()
    # Build label mapping for returned ids; fallback to generic if missing
    label_map: Dict[str, str] = {}
    for cid in ids:
        scid = str(cid)
        label_map[scid] = labels.get(scid, f"Category {scid}")
    return {"mode": mode, "category_id": ids, "labels": label_map}

@app.get("/api/catalog")
def catalog(mode: str = Query("drive", description="Travel mode, e.g. 'drive' or 'walk'")):
    """Return categories (ids + labels), available brands (ids + labels),
    and a mapping from category_id -> brand_ids (best-effort based on canonical POIs + labels).
    """
    # Categories
    ids = list_available_categories(mode)
    labels_map = _load_category_labels()
    categories = [{"id": cid, "label": labels_map.get(str(cid), f"Category {cid}")} for cid in ids]

    # Brands present in state_tiles (drive/mins)
    brands: list[dict[str, str]] = []
    present: set[str] = set()
    try:
        st_path = os.path.join("state_tiles", "us_r8.parquet")
        if os.path.exists(st_path):
            cols = list(pd.read_parquet(st_path, columns=None).columns)
            present = {c[:-10] for c in cols if c.endswith("_drive_min")}
    except Exception:
        present = set()

    for bid in sorted(present):
        name = BRAND_REGISTRY.get(bid, (None, []))[0] or bid.replace("_", " ").title()
        brands.append({"id": bid, "label": name})

    # Build category -> brands mapping via canonical POIs if possible
    cat_to_brands: dict[str, list[str]] = {str(c['id']): [] for c in categories}
    try:
        state = STATE
        canon_path = os.path.join("data", "poi", f"{state}_canonical.parquet")
        if os.path.exists(canon_path):
            cdf = pd.read_parquet(canon_path, columns=["brand_id", "category"])  # best-effort
            cdf = cdf.dropna(subset=["brand_id", "category"])
            # Reverse label mapping to id by lowercase label
            label_to_id = {v.lower(): k for k, v in labels_map.items()}
            # Accumulate mapping
            tmp: dict[str, set[str]] = {}
            for _, r in cdf.iterrows():
                cat_label = str(r["category"]).lower()
                bid = str(r["brand_id"]).strip()
                if bid not in present:
                    continue
                cid = label_to_id.get(cat_label)
                if cid is None:
                    continue
                tmp.setdefault(cid, set()).add(bid)
            # Convert to lists
            for cid, s in tmp.items():
                cat_to_brands.setdefault(str(cid), [])
                cat_to_brands[str(cid)] = sorted(s)
    except Exception:
        pass

    return {
        "mode": mode,
        "categories": categories,
        "brands": brands,
        "cat_to_brands": cat_to_brands,
    }

@app.get("/api/d_anchor")
def get_d_anchor_slice(
    category: str = Query(..., description="Category id (numeric) or taxonomy key"),
    mode: str = Query("drive", description="Travel mode, e.g. 'drive' or 'walk'")
):
    """
    Returns a JSON object mapping anchor_int_id to travel time in seconds
    for a given category and travel mode.
    """
    # Resolve category id (raises HTTPException 404 if invalid)
    cid = resolve_category_id(category, mode)

    try:
        D = load_D_anchor(mode)
        sub = D[D["category_id"] == cid]

        if sub.empty:
            return {}

        # Convert to a dictionary: { anchor_id: seconds }
        # The client will use this to map anchor IDs from the T_hex tiles
        # to the travel times for the selected category.
        result = pd.Series(
            sub.seconds_u16.values,
            index=sub.anchor_int_id
        ).to_dict()

        # Ensure keys are strings for JSON compatibility, as JS objects have string keys.
        return {str(k): int(v) for k, v in result.items()}

    except RuntimeError as e:
        # This occurs if the D_anchor file for the mode is missing.
        print(f"ERROR in get_d_anchor_slice for category={category} mode={mode}: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        import traceback
        print(f"ERROR in get_d_anchor_slice: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


# --------- PMTiles byte-serving (HTTP Range) ---------

def _etag_for_path(path: str) -> str:
    try:
        stat = os.stat(path)
        # Weak ETag based on mtime + size
        return f'W/"{int(stat.st_mtime)}-{stat.st_size}"'
    except FileNotFoundError:
        return '"0-0"'


@app.api_route("/tiles/{file_path:path}", methods=["GET", "HEAD"])
async def serve_pmtiles(file_path: str, request: Request):
    """Serve .pmtiles with proper HTTP Range support for pmtiles.js.

    Also serves other files in the tiles directory (non-pmtiles) for convenience,
    but .pmtiles must support Range to work with the PMTiles protocol.
    """
    full_path = os.path.join("tiles", file_path)

    if not os.path.isfile(full_path):
        raise HTTPException(status_code=404, detail="File not found")

    # If not a .pmtiles file, fall back to a simple static response
    if not full_path.endswith(".pmtiles"):
        # Let Starlette handle conditional headers, etag, etc.
        return FileResponse(full_path)

    file_size = os.path.getsize(full_path)
    etag = _etag_for_path(full_path)

    # Handle If-None-Match for simple client-side caching
    inm = request.headers.get("if-none-match")
    if inm and inm == etag:
        return Response(status_code=304)

    range_header = request.headers.get("range")
    headers = {
        "Accept-Ranges": "bytes",
        "ETag": etag,
        "Cache-Control": "public, max-age=0",
    }

    # If no Range header, return the entire file (200 OK)
    if not range_header:
        headers["Content-Length"] = str(file_size)
        return FileResponse(full_path, headers=headers, media_type="application/octet-stream")

    # Parse a simple single-range request: bytes=start-end
    try:
        units, rng = range_header.split("=", 1)
        if units.strip().lower() != "bytes":
            raise ValueError("Only 'bytes' range is supported")
        start_s, end_s = rng.split("-", 1)
        if start_s.strip() == "":
            # suffix-byte-range-spec: last N bytes
            suffix = int(end_s)
            if suffix <= 0:
                raise ValueError("Invalid suffix length")
            start = max(file_size - suffix, 0)
            end = file_size - 1
        else:
            start = int(start_s)
            end = int(end_s) if end_s.strip() != "" else file_size - 1
        if start < 0 or end < start or end >= file_size:
            raise ValueError("Invalid range")
    except Exception:
        # Unsatisfiable range
        headers["Content-Range"] = f"bytes */{file_size}"
        return Response(status_code=416, headers=headers)

    chunk_size = end - start + 1
    headers.update({
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Content-Length": str(chunk_size),
        "Content-Type": "application/octet-stream",
    })

    if request.method == "HEAD":
        return Response(status_code=206, headers=headers)

    def file_iterator(path: str, offset: int, length: int, block_size: int = 1024 * 1024):
        with open(path, "rb") as f:
            f.seek(offset)
            remaining = length
            while remaining > 0:
                read_size = min(block_size, remaining)
                data = f.read(read_size)
                if not data:
                    break
                remaining -= len(data)
                yield data

    return StreamingResponse(file_iterator(full_path, start, chunk_size), status_code=206, headers=headers)


if __name__ == "__main__":
    # For local dev, allow overriding the port
    port = int(os.environ.get("PORT", 5174)) # Default to 5174 to avoid conflict with frontend
    print(f"Starting TownScout D_anchor server on http://0.0.0.0:{port}")
    print(f"Using data from STATE={STATE} in DATA_DIR={DATA_DIR}")
    uvicorn.run("api.main:app", host="0.0.0.0", port=port, reload=True)
