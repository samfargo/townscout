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

# ---------- Brand D_anchor loading ----------

def _mode_to_partition(mode: str) -> int:
    return {"drive": 0, "walk": 2}.get(mode, 0)

def load_D_anchor_brand(mode: str, brand_id: str) -> pd.DataFrame:
    """
    Load seconds-based anchor→brand table produced by 03d_compute_d_anchor.py.
    Layout: data/d_anchor_brand/mode={0|2}/brand_id=<brand_id>/part-*.parquet
    Columns: anchor_int_id:int32, seconds:uint16 (or seconds_u16), optional snapshot_ts
    """
    base = os.path.join("data", "d_anchor_brand", f"mode={_mode_to_partition(mode)}", f"brand_id={brand_id}")
    if os.path.isdir(base):
        dataset = ds.dataset(base, format="parquet", partitioning="hive")
        # accept either seconds or seconds_u16
        cols = set()
        try:
            cols = set(dataset.schema.names)
        except Exception:
            pass
        pick = [c for c in ["anchor_int_id", "seconds_u16", "seconds"] if c in cols]
        if not pick:
            table = dataset.to_table()
        else:
            table = dataset.to_table(columns=pick)
        df = table.to_pandas()
        if "seconds" in df.columns and "seconds_u16" not in df.columns:
            df = df.rename(columns={"seconds": "seconds_u16"})
        if "seconds_u16" not in df.columns:
            raise RuntimeError("Brand D_anchor missing 'seconds' column")
        df["seconds_u16"] = df["seconds_u16"].astype("uint16", errors="ignore")
        need = {"anchor_int_id", "seconds_u16"}
        missing = need - set(df.columns)
        if missing:
            raise RuntimeError(f"Brand D_anchor missing required columns: {missing}")
        return df[["anchor_int_id", "seconds_u16"]]
    # No consolidated fallback; require per-brand Hive partitions
    raise RuntimeError(f"Brand D_anchor parquet missing at {base}")

# ---------- Category resolution ----------

def _resolve_brand_id(raw: str) -> str:
    """Resolve a brand input to canonical id using BRAND_REGISTRY aliases."""
    s = str(raw or "").strip().lower()
    if not s:
        return s
    # direct hit
    if s in BRAND_REGISTRY:
        return s
    # alias or name
    for bid, (name, aliases) in BRAND_REGISTRY.items():
        if name and s == str(name).strip().lower():
            return bid
        for a in (aliases or []):
            if s == str(a).strip().lower():
                return bid
    # return as-is; caller may still have produced canonical ids we don't know
    return s

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
    # Derive labels from existing data if no explicit mapping is present
    try:
        inferred = _infer_category_labels_from_data("drive")
        if inferred:
            return inferred
    except Exception:
        pass
    return {}


# ---------- Derived category labels (fallback) ----------
_CACHED_CAT_LABELS: dict[str, dict[str, str]] = {}

def _find_sites_parquet(mode: str) -> Optional[str]:
    """Locate an anchors/sites parquet for the given mode.
    Preference order:
      data/anchors/*_{mode}_sites.parquet -> data/minutes/*_{mode}_sites.parquet
    """
    pat1 = os.path.join("data", "anchors", f"*_{mode}_sites.parquet")
    pat2 = os.path.join("data", "minutes", f"*_{mode}_sites.parquet")
    cands = sorted(glob.glob(pat1)) or sorted(glob.glob(pat2))
    return cands[0] if cands else None

def _prettify(label: str) -> str:
    s = str(label).strip()
    s = s.replace("_", " ")
    # Title case but keep common acronyms upper (simple heuristics)
    out = s.title()
    for ac in ("OSM", "US", "USA", "UK"):
        out = out.replace(ac.title(), ac)
    return out

def _infer_category_labels_from_data(mode: str) -> Dict[str, str]:
    """Best-effort inference of {category_id:str -> label:str}.
    Strategy: join D_anchor (anchor_int_id, category_id) to anchor sites' categories list,
    tally the most frequent category string per numeric id, and prettify the label.
    Results are cached per mode.
    """
    if mode in _CACHED_CAT_LABELS:
        return _CACHED_CAT_LABELS[mode]

    try:
        D = load_D_anchor(mode)
    except Exception:
        return {}

    sites_path = _find_sites_parquet(mode)
    if not sites_path or D.empty:
        return {}

    try:
        import pandas as pd  # type: ignore
        sites = pd.read_parquet(sites_path, columns=["anchor_int_id", "categories"])  # categories is list[str]
    except Exception:
        return {}

    if "anchor_int_id" not in sites.columns or "categories" not in sites.columns:
        return {}

    # Explode categories for each anchor
    try:
        exploded = sites.dropna(subset=["categories"]).explode("categories")
    except Exception:
        # If explode fails due to dtype, coerce to list then explode
        sites = sites.copy()
        sites["categories"] = sites["categories"].apply(lambda v: v if isinstance(v, list) else ([] if pd.isna(v) else [v]))
        exploded = sites.explode("categories")

    exploded = exploded.dropna(subset=["categories"]).rename(columns={"categories": "category_label"})

    # Join with D_anchor to associate each (anchor, numeric category_id) pair to textual labels observed at the site
    merged = D.merge(exploded, how="left", on="anchor_int_id")
    # Count labels per category_id
    vc = merged.dropna(subset=["category_label"]).groupby(["category_id", "category_label"]).size().reset_index(name="n")
    if vc.empty:
        return {}

    # Pick the most frequent label per id
    vc = vc.sort_values(["category_id", "n"], ascending=[True, False])
    best = vc.groupby("category_id").first().reset_index()

    out: Dict[str, str] = {str(int(r["category_id"])): _prettify(str(r["category_label"])) for _, r in best.iterrows()}
    _CACHED_CAT_LABELS[mode] = out
    return out

def resolve_category_id(category: str, mode: str) -> int:
    """Resolve a category input to numeric id.
    Accepts a numeric string directly. If a taxonomy file is present in data/taxonomy, it can be wired later.
    """
    # Numeric id is always allowed
    try:
        return int(str(category))
    except Exception:
        # Try label mapping (derived or explicit)
        labels = _load_category_labels()
        if labels:
            inv = {str(v).lower(): int(k) for k, v in labels.items()}
            cid = inv.get(str(category).lower())
            if cid is not None:
                return cid
        # Fall back to instructive error
        raise HTTPException(status_code=404, detail=f"Unknown category '{category}'. Use numeric category_id from /api/categories?mode={mode} or a known label.")

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
        label_map[scid] = labels.get(scid, labels.get(str(cid), f"Category {scid}"))
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

    # Brands: list all canonical brand_ids present in canonical POIs/anchors
    brands: list[dict[str, str]] = []
    present: set[str] = set()
    try:
        canon_path = os.path.join("data", "poi", f"{STATE}_canonical.parquet")
        if os.path.exists(canon_path):
            cdf = pd.read_parquet(canon_path, columns=["brand_id"])  # type: ignore
            present = set(str(b) for b in cdf["brand_id"].dropna().unique().tolist())
    except Exception:
        present = set()

    # No tile-column fallback; catalog is driven by canonical POIs

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

# ---------- POI Pins (GeoJSON) ----------
_CANON_POI_CACHE: Optional[pd.DataFrame] = None

def _load_canonical_pois() -> pd.DataFrame:
    global _CANON_POI_CACHE
    if _CANON_POI_CACHE is not None:
        return _CANON_POI_CACHE
    path = os.path.join("data", "poi", f"{STATE}_canonical.parquet")
    if not os.path.exists(path):
        # Empty dataframe with expected columns
        _CANON_POI_CACHE = pd.DataFrame(columns=["brand_id", "lon", "lat", "name"])  # type: ignore
        return _CANON_POI_CACHE
    try:
        df = pd.read_parquet(path, columns=["brand_id", "lon", "lat", "name"])  # type: ignore
        # Ensure correct dtypes
        if "brand_id" in df.columns:
            df["brand_id"] = df["brand_id"].astype(str)
        if "lon" in df.columns:
            df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
        if "lat" in df.columns:
            df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
        # Drop invalid rows
        df = df.dropna(subset=["lon", "lat", "brand_id"]).copy()
        _CANON_POI_CACHE = df
        return df
    except Exception as e:
        print(f"[warn] Failed to read canonical POIs at {path}: {e}")
        _CANON_POI_CACHE = pd.DataFrame(columns=["brand_id", "lon", "lat", "name"])  # type: ignore
        return _CANON_POI_CACHE


@app.get("/api/poi_points")
def poi_points(
    brands: str = Query(..., description="Comma-separated brand_ids to include"),
    bbox: Optional[str] = Query(None, description="Optional bbox lonmin,latmin,lonmax,latmax")
):
    """Return GeoJSON FeatureCollection of POI points for the given brands.

    Example: /api/poi_points?brands=chipotle,costco
    Optional bbox filters points server-side to reduce payload.
    """
    brand_list = [b.strip() for b in str(brands).split(",") if b.strip()]
    if not brand_list:
        raise HTTPException(status_code=400, detail="No brand ids provided")

    df = _load_canonical_pois()
    if df.empty:
        return {"type": "FeatureCollection", "features": []}

    sub = df[df["brand_id"].isin(brand_list)][["lon", "lat", "brand_id", "name"]]

    # Optional bbox filter
    if bbox:
        try:
            parts = [float(x) for x in bbox.split(",")]
            if len(parts) == 4:
                x0, y0, x1, y1 = parts
                xmin, xmax = (min(x0, x1), max(x0, x1))
                ymin, ymax = (min(y0, y1), max(y0, y1))
                sub = sub[(sub["lon"] >= xmin) & (sub["lon"] <= xmax) & (sub["lat"] >= ymin) & (sub["lat"] <= ymax)]
        except Exception:
            pass

    # Build GeoJSON
    feats = []
    for _, r in sub.iterrows():
        lon = float(r["lon"]) if pd.notna(r["lon"]) else None
        lat = float(r["lat"]) if pd.notna(r["lat"]) else None
        if lon is None or lat is None:
            continue
        props: Dict[str, Any] = {
            "brand_id": str(r["brand_id"]) if pd.notna(r["brand_id"]) else None,
        }
        name = r.get("name") if isinstance(r.get("name"), str) else None
        if name:
            props["name"] = name
        feats.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": props,
        })

    return {"type": "FeatureCollection", "features": feats}

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


@app.get("/api/d_anchor_brand")
def get_d_anchor_brand(
    brand: str = Query(..., description="Brand id or alias"),
    mode: str = Query("drive", description="Travel mode, e.g. 'drive' or 'walk'")
):
    """
    Returns a JSON object mapping anchor_int_id to travel time in seconds
    for a given brand and travel mode.
    """
    bid = _resolve_brand_id(brand)
    try:
        D = load_D_anchor_brand(mode, bid)
        if D.empty:
            return {}
        result = pd.Series(D.seconds_u16.values, index=D.anchor_int_id).to_dict()
        return {str(k): int(v) for k, v in result.items()}
    except RuntimeError as e:
        print(f"ERROR in get_d_anchor_brand for brand={brand} mode={mode}: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        import traceback
        print(f"ERROR in get_d_anchor_brand: {e}\n{traceback.format_exc()}")
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
