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
import requests
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, FileResponse, RedirectResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from typing import Any
from urllib.parse import quote
# Ensure we can import modules from the repo's src/ when running via uvicorn
_SRC_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, 'src'))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

try:
    from taxonomy import BRAND_REGISTRY
except Exception:
    BRAND_REGISTRY = {}
try:
    from graph.csr_utils import build_rev_csr
except Exception:
    build_rev_csr = None  # type: ignore
try:
    from graph.ch_cache import load_or_build_ch
except Exception:
    load_or_build_ch = None  # type: ignore

APP_NAME = "TownScout D_anchor API"

_TRAUMA_CATEGORY_LABELS = {
    "Level 1 Trauma (Adults)",
    "Level 1 Trauma (Pediatric)",
}

# ---------- Config ----------
# Paths
# Unify d_anchor locations under data/d_anchor_{category|brand}
STATE = os.environ.get("TS_STATE", "massachusetts")
_DANCHOR_CATEGORY_DIR = os.environ.get("TS_DANCHOR_CATEGORY_DIR", os.path.join("data", "d_anchor_category"))
_DANCHOR_BRAND_DIR = os.environ.get("TS_DANCHOR_BRAND_DIR", os.path.join("data", "d_anchor_brand"))

_FRONTEND_ENV = (os.environ.get("TS_FRONTEND_ORIGIN") or os.environ.get("TOWNSCOUT_FRONTEND_ORIGIN") or "").strip()
_DEFAULT_FRONTEND_ORIGIN = os.environ.get("TS_DEFAULT_FRONTEND_ORIGIN", "http://localhost:3000").strip() or None
_FRONTEND_ORIGIN = _FRONTEND_ENV or _DEFAULT_FRONTEND_ORIGIN

GOOGLE_PLACES_API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY")
GOOGLE_PLACES_AUTOCOMPLETE_URL = "https://places.googleapis.com/v1/places:autocomplete"
GOOGLE_PLACES_DETAILS_URL = "https://places.googleapis.com/v1/places"
GOOGLE_PLACES_TIMEOUT = float(os.environ.get("GOOGLE_PLACES_TIMEOUT", "7"))

_ADDRESS_TYPE_HINTS = {
    "street_address",
    "street_number",
    "premise",
    "subpremise",
    "route",
    "intersection",
    "plus_code",
    "postal_code",
    "postal_code_prefix",
    "postal_town",
    "locality",
    "sublocality",
    "neighborhood",
}


def _coalesce_text(*values: Optional[str]) -> Optional[str]:
    for value in values:
        if value:
            stripped = str(value).strip()
            if stripped:
                return stripped
    return None


def _extract_bbox(viewport: Optional[dict]) -> Optional[dict]:
    if not isinstance(viewport, dict):
        return None
    low = viewport.get("low") or {}
    high = viewport.get("high") or {}
    try:
        west = float(low["longitude"])
        south = float(low["latitude"])
        east = float(high["longitude"])
        north = float(high["latitude"])
    except (KeyError, TypeError, ValueError):
        return None
    return {
        "west": west,
        "south": south,
        "east": east,
        "north": north,
    }


def _classify_place(types: Optional[List[str]]) -> str:
    iterable = types or []
    for t in iterable:
        if not isinstance(t, str):
            continue
        t_lower = t.lower()
        if t_lower.startswith("administrative_area_level_"):
            return "address"
        if t_lower.startswith("sublocality_level_"):
            return "address"
        if t_lower in _ADDRESS_TYPE_HINTS:
            return "address"
    return "place"


def _parse_location_bias(location_bias: Optional[str]) -> Optional[dict]:
    if not location_bias:
        return None
    parts = [p.strip() for p in location_bias.split(",") if p.strip()]
    if len(parts) == 2:
        try:
            lon, lat = (float(parts[0]), float(parts[1]))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid locationBias; expected lon,lat") from exc
        if not (-180.0 <= lon <= 180.0 and -90.0 <= lat <= 90.0):
            raise HTTPException(status_code=400, detail="locationBias lon/lat out of range")
        # Use a modest radius to bias around current map center (~30km)
        return {
            "circle": {
                "center": {"latitude": lat, "longitude": lon},
                "radius": 30000,
            }
        }
    if len(parts) == 4:
        try:
            west, south, east, north = map(float, parts)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid locationBias bbox; expected west,south,east,north") from exc
        if not (-180.0 <= west <= 180.0 and -180.0 <= east <= 180.0 and -90.0 <= south <= 90.0 and -90.0 <= north <= 90.0):
            raise HTTPException(status_code=400, detail="locationBias bbox out of range")
        return {
            "rectangle": {
                "low": {"latitude": min(south, north), "longitude": min(west, east)},
                "high": {"latitude": max(south, north), "longitude": max(west, east)},
            }
        }
    raise HTTPException(status_code=400, detail="Unsupported locationBias format; use lon,lat or west,south,east,north")


def _normalize_autocomplete_prediction(pred: dict) -> dict:
    place_types = pred.get("types") or []
    text_blob = pred.get("text")
    structured = pred.get("structuredFormat") or {}
    place_blob = pred.get("place") or {}
    location = place_blob.get("location") or {}

    label = _coalesce_text(
        text_blob.get("text") if isinstance(text_blob, dict) else text_blob if isinstance(text_blob, str) else None,
        structured.get("mainText", {}).get("text") if isinstance(structured.get("mainText"), dict) else structured.get("mainText"),
        place_blob.get("displayName", {}).get("text") if isinstance(place_blob.get("displayName"), dict) else None,
    )

    secondary = structured.get("secondaryText") if isinstance(structured, dict) else None
    if isinstance(secondary, dict):
        secondary_text = secondary.get("text")
    else:
        secondary_text = secondary
    sublabel = _coalesce_text(place_blob.get("formattedAddress"), secondary_text)

    try:
        lat_raw = location.get("latitude")
        lon_raw = location.get("longitude")
        lat_f = float(lat_raw) if lat_raw is not None else None
        lon_f = float(lon_raw) if lon_raw is not None else None
    except (TypeError, ValueError):
        lat_f = None
        lon_f = None

    normalized: dict[str, Any] = {
        "id": pred.get("placeId"),
        "type": _classify_place(place_types),
        "label": label or sublabel or pred.get("placeId"),
    }
    if sublabel is not None:
        normalized["sublabel"] = sublabel
    if lat_f is not None:
        normalized["lat"] = lat_f
    if lon_f is not None:
        normalized["lon"] = lon_f
    bbox = _extract_bbox(place_blob.get("viewport"))
    if bbox is not None:
        normalized["bbox"] = bbox
    return normalized


def _normalize_place_detail(place: dict) -> dict:
    types = place.get("types") if isinstance(place, dict) else []
    display_name = place.get("displayName")
    formatted = place.get("formattedAddress")
    short_formatted = place.get("shortFormattedAddress")
    location = place.get("location") or {}

    try:
        lat_f = float(location.get("latitude")) if location.get("latitude") is not None else None
        lon_f = float(location.get("longitude")) if location.get("longitude") is not None else None
    except (TypeError, ValueError):
        lat_f = None
        lon_f = None

    display_text = None
    if isinstance(display_name, dict):
        display_text = display_name.get("text")
    elif isinstance(display_name, str):
        display_text = display_name

    label = _coalesce_text(display_text, formatted, short_formatted)
    sublabel = _coalesce_text(formatted, short_formatted)
    return {
        "id": place.get("id") or place.get("name"),
        "type": _classify_place(types),
        "label": label,
        "sublabel": sublabel,
        "lat": lat_f,
        "lon": lon_f,
        "bbox": _extract_bbox(place.get("viewport")),
    }


def _place_path_segment(place_id: str) -> str:
    if place_id is None:
        raise HTTPException(status_code=400, detail="Place id is required")
    pid = str(place_id).strip()
    if not pid:
        raise HTTPException(status_code=400, detail="Place id is required")
    if pid.startswith("places/"):
        pid = pid.split("places/", 1)[1]
    return pid

# Column & sentinel conventions
UNREACH_U16 = np.uint16(65535)

# Lazily loaded CSR graph + anchors cache for custom D_anchor
_GRAPH_CACHE: dict[str, dict[str, object]] = {}
_ANCHOR_CACHE: dict[str, dict[str, object]] = {}
def _mode_to_partition(mode: str) -> int:
    return {"drive": 0, "walk": 2}.get(mode, 0)

_DANCHOR_BRAND_DTYPES = {
    "anchor_id": pd.UInt32Dtype(),
    "brand_id": pd.StringDtype(),
    "mode": pd.UInt8Dtype(),
    "seconds_u16": pd.UInt16Dtype(),
    "snapshot_ts": "datetime64[ns]",
    "is_reachable": pd.BooleanDtype(),
    "seconds_clamped": pd.UInt16Dtype(),
}

_DANCHOR_CATEGORY_DTYPES = {
    "anchor_id": pd.UInt32Dtype(),
    "category_id": pd.UInt32Dtype(),
    "mode": pd.UInt8Dtype(),
    "seconds_u16": pd.UInt16Dtype(),
    "snapshot_ts": "datetime64[ns]",
    "is_reachable": pd.BooleanDtype(),
    "seconds_clamped": pd.UInt16Dtype(),
}


def _empty_df(dtypes: Dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame({name: pd.Series(dtype=dtype) for name, dtype in dtypes.items()})


def _ensure_uint(values: Any, dtype: object) -> pd.Series:
    return pd.Series(pd.to_numeric(values, errors="coerce"), dtype=dtype)


def _ensure_seconds(values: Any) -> pd.Series:
    sec = pd.to_numeric(values, errors="coerce")
    sec = sec.mask(sec == int(UNREACH_U16))
    return pd.Series(sec, dtype=pd.UInt16Dtype())


def _ensure_mode(df: pd.DataFrame, mode_code: int) -> pd.Series:
    if "mode" in df.columns:
        series = pd.Series(pd.to_numeric(df["mode"], errors="coerce"), dtype=pd.UInt8Dtype())
        return series.fillna(mode_code)
    if df.empty:
        return pd.Series(dtype=pd.UInt8Dtype())
    return pd.Series([mode_code] * len(df), dtype=pd.UInt8Dtype())


def _read_hive_dataset(base: str, requested: List[str]) -> Optional[pd.DataFrame]:
    if not os.path.isdir(base):
        return None
    dataset = ds.dataset(base, format="parquet", partitioning="hive")
    try:
        schema_names = set(dataset.schema.names)
    except Exception:
        schema_names = set()
    columns = [c for c in requested if c in schema_names]
    table = dataset.to_table(columns=columns or None)
    return table.to_pandas()

# ---------- Data loading (cached) ----------
def load_D_anchor(mode: str) -> pd.DataFrame:
    """
    Load seconds-based anchor→category table produced by precompute_d_anchor.py.
    Expected columns: anchor_id(uint32), category_id(uint32), mode(uint8), seconds_u16(uint16, nullable), snapshot_ts(date)
    """
    mode_code = _mode_to_partition(mode)
    requested_cols = [
        "anchor_id",
        "anchor_int_id",
        "category_id",
        "mode",
        "seconds_u16",
        "seconds",
        "snapshot_ts",
    ]

    part_dir = os.path.join(_DANCHOR_CATEGORY_DIR, f"mode={mode_code}")
    legacy_dir = os.path.join("data", "minutes", f"mode={mode_code}")

    for base in (part_dir, legacy_dir):
        df = _read_hive_dataset(base, requested_cols)
        if df is not None:
            return _finalize_category_df(df, mode_code)

    path = os.path.join("data", "minutes", f"{STATE}_anchor_to_category_{mode}.parquet")
    if os.path.exists(path):
        df = pd.read_parquet(path)
        return _finalize_category_df(df, mode_code)

    # Gracefully handle missing data (e.g., walk mode not yet computed)
    print(f"WARNING: Category D_anchor parquet not found for mode={mode}. Returning empty DataFrame.")
    return _empty_df(_DANCHOR_CATEGORY_DTYPES)


def _finalize_category_df(df: pd.DataFrame, mode_code: int) -> pd.DataFrame:
    df = df.copy()
    if "anchor_int_id" in df.columns and "anchor_id" not in df.columns:
        df = df.rename(columns={"anchor_int_id": "anchor_id"})
    if "seconds" in df.columns and "seconds_u16" not in df.columns:
        df = df.rename(columns={"seconds": "seconds_u16"})
    if "snapshot_ts" not in df.columns:
        df["snapshot_ts"] = pd.NaT
    if "mode" not in df.columns:
        df["mode"] = mode_code

    required = {"anchor_id", "category_id", "seconds_u16"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"Category D_anchor missing required columns: {missing}")

    if df.empty:
        return _empty_df(_DANCHOR_CATEGORY_DTYPES)

    out = pd.DataFrame(
        {
            "anchor_id": _ensure_uint(df["anchor_id"], pd.UInt32Dtype()),
            "category_id": _ensure_uint(df["category_id"], pd.UInt32Dtype()),
        }
    )
    out["mode"] = _ensure_mode(df, mode_code)
    out["seconds_u16"] = _ensure_seconds(df["seconds_u16"])
    out["snapshot_ts"] = pd.to_datetime(df["snapshot_ts"], errors="coerce")
    out["is_reachable"] = out["seconds_u16"].notna()
    out["seconds_clamped"] = out["seconds_u16"].to_numpy(dtype=np.uint16, na_value=UNREACH_U16)
    return out[list(_DANCHOR_CATEGORY_DTYPES.keys())]

def load_D_anchor_category(mode: str, category_id: int) -> pd.DataFrame:
    """
    Load seconds-based anchor→category table for a specific category.
    Layout: data/d_anchor_category/mode={0|2}/category_id=<id>/part-*.parquet
    Columns: anchor_id:uint32, category_id:uint32, mode:uint8, seconds_u16:uint16(nullable), snapshot_ts:date
    """
    mode_code = _mode_to_partition(mode)
    base = os.path.join(_DANCHOR_CATEGORY_DIR, f"mode={mode_code}", f"category_id={category_id}")
    print(f"[DEBUG] Loading category from: {base}")
    df = _read_hive_dataset(
        base,
        [
            "anchor_id",
            "anchor_int_id",
            "category_id",
            "mode",
            "seconds_u16",
            "seconds",
            "snapshot_ts",
        ],
    )
    if df is None:
        print(f"WARNING: Category D_anchor parquet missing at {base} for mode={mode}. Returning empty DataFrame.")
        return _empty_df(_DANCHOR_CATEGORY_DTYPES)
    print(f"[DEBUG] Loaded {len(df)} rows for category {category_id}")
    return _finalize_category_df(df, mode_code)

# ---------- Brand D_anchor loading ----------

def load_D_anchor_brand(mode: str, brand_id: str) -> pd.DataFrame:
    """
    Load seconds-based anchor→brand table produced by 05_compute_d_anchor.py.
    Layout: data/d_anchor_brand/mode={0|2}/brand_id=<brand_id>/part-*.parquet
    Columns: anchor_id:uint32, brand_id:str, mode:uint8, seconds_u16:uint16(nullable), snapshot_ts:date
    """
    mode_code = _mode_to_partition(mode)
    base = os.path.join(_DANCHOR_BRAND_DIR, f"mode={mode_code}", f"brand_id={brand_id}")
    print(f"[DEBUG] Loading from: {base}")
    df = _read_hive_dataset(
        base,
        [
            "anchor_id",
            "anchor_int_id",
            "brand_id",
            "mode",
            "seconds_u16",
            "seconds",
            "snapshot_ts",
        ],
    )
    if df is None:
        print(f"WARNING: Brand D_anchor parquet missing at {base} for mode={mode}. Returning empty DataFrame.")
        return _empty_df(_DANCHOR_BRAND_DTYPES)
    print(f"[DEBUG] Loaded {len(df)} rows, sample anchor 13279: {df[df['anchor_id'] == 13279]['seconds_u16'].values if 'anchor_id' in df.columns and 13279 in df['anchor_id'].values else 'NOT FOUND'}")
    result = _finalize_brand_df(df, brand_id, mode_code)
    print(f"[DEBUG] After finalize, anchor 13279: {result[result['anchor_id'] == 13279]['seconds_clamped'].values if 13279 in result['anchor_id'].values else 'NOT FOUND'}")
    return result


def _finalize_brand_df(df: pd.DataFrame, brand_id: str, mode_code: int) -> pd.DataFrame:
    df = df.copy()
    if "anchor_int_id" in df.columns and "anchor_id" not in df.columns:
        df = df.rename(columns={"anchor_int_id": "anchor_id"})
    if "seconds" in df.columns and "seconds_u16" not in df.columns:
        df = df.rename(columns={"seconds": "seconds_u16"})
    if "snapshot_ts" not in df.columns:
        df["snapshot_ts"] = pd.NaT
    if "brand_id" not in df.columns:
        df["brand_id"] = brand_id

    if "seconds_u16" not in df.columns:
        raise RuntimeError("Brand D_anchor missing 'seconds_u16' column")
    if "anchor_id" not in df.columns:
        raise RuntimeError("Brand D_anchor missing 'anchor_id' column")

    if df.empty:
        return _empty_df(_DANCHOR_BRAND_DTYPES)

    out = pd.DataFrame(
        {
            "anchor_id": _ensure_uint(df["anchor_id"], pd.UInt32Dtype()),
            "brand_id": df["brand_id"].astype(pd.StringDtype()).fillna(brand_id),
        }
    )
    out["mode"] = _ensure_mode(df, mode_code)
    out["seconds_u16"] = _ensure_seconds(df["seconds_u16"])
    out["snapshot_ts"] = pd.to_datetime(df["snapshot_ts"], errors="coerce")
    out["is_reachable"] = out["seconds_u16"].notna()
    out["seconds_clamped"] = out["seconds_u16"].to_numpy(dtype=np.uint16, na_value=UNREACH_U16)
    return out[list(_DANCHOR_BRAND_DTYPES.keys())]

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
    base = os.path.join(_DANCHOR_CATEGORY_DIR, f"mode={_mode_to_partition(mode)}")
    ids: list[int] = []
    if os.path.isdir(base):
        for name in os.listdir(base):
            if name.startswith("category_id="):
                try:
                    ids.append(int(name.split("=", 1)[1]))
                except Exception:
                    pass
    return sorted(set(ids))

def _load_category_label_to_id() -> Dict[str, int]:
    """Load category label -> id mapping from POI_category_registry.csv.
    Returns mapping from label strings (e.g. 'fast_food') to integer category IDs.
    
    Falls back to legacy category_label_to_id.json if CSV doesn't exist.
    """
    base = os.path.join("data", "taxonomy")
    csv_path = os.path.join(base, "POI_category_registry.csv")
    
    # Try CSV first (new approach with explicit IDs)
    if os.path.isfile(csv_path):
        try:
            import csv
            label_to_id = {}
            with open(csv_path, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    cat_id = str(row.get("category_id", "")).strip()
                    numeric_id = int(row.get("numeric_id", 0))
                    if cat_id and numeric_id:
                        label_to_id[cat_id] = numeric_id
            return label_to_id
        except Exception as e:
            print(f"[warn] Failed to load POI_category_registry.csv: {e}")
    
    # Fallback to legacy JSON
    json_path = os.path.join(base, "category_label_to_id.json")
    if os.path.isfile(json_path):
        try:
            with open(json_path, "r") as f:
                obj = json.load(f)
                return {str(k): int(v) for k, v in obj.items()}
        except Exception:
            pass
    return {}

def _load_category_labels() -> Dict[str, str]:
    """Load category id -> display name mapping from POI_category_registry.csv.
    Returns mapping with string numeric_id keys (e.g. {"1": "Airport"}).
    
    Falls back to legacy category_labels.json if CSV doesn't exist.
    """
    base = os.path.join("data", "taxonomy")
    csv_path = os.path.join(base, "POI_category_registry.csv")
    
    # Try CSV first (new approach)
    if os.path.isfile(csv_path):
        try:
            import csv
            id_to_label = {}
            with open(csv_path, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    numeric_id = str(row.get("numeric_id", "")).strip()
                    display_name = str(row.get("display_name", "")).strip()
                    if numeric_id and display_name:
                        id_to_label[numeric_id] = display_name
            return id_to_label
        except Exception as e:
            print(f"[warn] Failed to load POI_category_registry.csv: {e}")
    
    # Fallback to legacy JSON
    json_path = os.path.join(base, "category_labels.json")
    if os.path.isfile(json_path):
        try:
            with open(json_path, "r") as f:
                obj = json.load(f)
                # Normalize keys to str
                return {str(k): str(v) for k, v in obj.items()}
        except Exception:
            pass
    
    # Final fallback: CSV without display_name column
    csv_fallback_path = os.path.join(base, "category_labels.csv")
    if os.path.isfile(csv_fallback_path):
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
    Strategy: join D_anchor (anchor_id, category_id) to anchor sites' categories list,
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

    sites = sites.rename(columns={"anchor_int_id": "anchor_id"})

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
    merged = D.merge(exploded, how="left", on="anchor_id")
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
    """Redirect to the Next.js frontend if configured; otherwise emit API status."""
    if _FRONTEND_ORIGIN:
        return RedirectResponse(_FRONTEND_ORIGIN, status_code=307)
    return JSONResponse(
        {
            "app": APP_NAME,
            "frontend": "Next.js frontend is served separately. Set TS_FRONTEND_ORIGIN (or TS_DEFAULT_FRONTEND_ORIGIN='') to disable redirect.",
        }
    )

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
    categories: list[dict[str, object]] = []
    for cid in ids:
        label = labels_map.get(str(cid), f"Category {cid}")
        group = None
        if label == "Hospital":
            group = "hospital"
        elif label in _TRAUMA_CATEGORY_LABELS:
            group = "hospital_trauma"
        payload = {"id": cid, "label": label}
        if group:
            payload["group"] = group
        categories.append(payload)

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
            # Load the label -> id mapping (e.g., "fast_food" -> 5)
            label_to_id = _load_category_label_to_id()
            # Accumulate mapping
            tmp: dict[str, set[str]] = {}
            for _, r in cdf.iterrows():
                cat_label = str(r["category"]).strip()
                bid = str(r["brand_id"]).strip()
                if bid not in present:
                    continue
                cid = label_to_id.get(cat_label)
                if cid is None:
                    continue
                tmp.setdefault(str(cid), set()).add(bid)
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


def _ensure_places_key():
    if not GOOGLE_PLACES_API_KEY:
        raise HTTPException(status_code=500, detail="Places API key not configured")


def _places_error_detail(payload: Optional[dict]) -> Optional[str]:
    if not isinstance(payload, dict):
        return None
    error = payload.get("error")
    if isinstance(error, dict):
        message = error.get("message")
        if message:
            return str(message)
    return None


@app.get("/api/places/autocomplete")
def places_autocomplete(
    q: str = Query(..., min_length=1, alias="input", description="Search text"),
    session: str = Query(..., min_length=1, description="Google Places session token"),
    location_bias: Optional[str] = Query(None, alias="locationBias", description="Bias as lon,lat or west,south,east,north"),
    limit: int = Query(8, ge=1, le=10, description="Maximum number of suggestions to return"),
):
    _ensure_places_key()
    query = q.strip()
    if len(query) < 2:
        return {"suggestions": [], "has_more": False}
    payload: dict[str, Any] = {
        "input": query,
        "sessionToken": session,
    }
    try:
        bias = _parse_location_bias(location_bias) if location_bias else None
    except HTTPException:
        # Re-raise with same detail for clarity
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid locationBias") from exc
    if bias:
        payload["locationBias"] = bias

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": (
            "suggestions.placePrediction.placeId,"
            "suggestions.placePrediction.text,"
            "suggestions.placePrediction.structuredFormat,"
            "suggestions.placePrediction.types"
        ),
    }

    try:
        resp = requests.post(
            GOOGLE_PLACES_AUTOCOMPLETE_URL,
            headers=headers,
            json=payload,
            timeout=GOOGLE_PLACES_TIMEOUT,
        )
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail="Places Autocomplete unavailable") from exc

    print("[places/autocomplete] status", resp.status_code)
    print("[places/autocomplete] headers", dict(resp.headers))
    try:
        raw_body = resp.json()
    except ValueError:
        raw_body = resp.text
    print("[places/autocomplete] body", raw_body)

    if resp.status_code == 429:
        detail = _places_error_detail(raw_body) or "Places Autocomplete quota exceeded"
        raise HTTPException(status_code=429, detail=detail)
    if resp.status_code >= 400:
        detail = _places_error_detail(raw_body) or "Places Autocomplete error"
        raise HTTPException(status_code=resp.status_code, detail=detail)
    if raw_body is None or not isinstance(raw_body, dict):
        raise HTTPException(status_code=502, detail="Invalid response from Places Autocomplete")

    predictions = []
    for suggestion in raw_body.get("suggestions", []):
        if not isinstance(suggestion, dict):
            continue
        pred = suggestion.get("placePrediction")
        if isinstance(pred, dict):
            predictions.append(pred)
    normalized: list[dict[str, Any]] = []
    for pred in predictions:
        if not isinstance(pred, dict):
            continue
        norm = _normalize_autocomplete_prediction(pred)
        if norm.get("id"):
            normalized.append(norm)

    # Truncate client-side expectations while signaling availability of more predictions
    limited = normalized[:limit]
    return {
        "suggestions": limited,
        "has_more": len(normalized) > len(limited),
    }


@app.get("/api/places/details")
def places_details(
    place_id: str = Query(..., alias="place_id", description="Place identifier"),
    session: str = Query(..., min_length=1, description="Google Places session token"),
):
    _ensure_places_key()
    try:
        path_segment = _place_path_segment(place_id)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid place id") from exc

    headers = {
        "Accept": "application/json",
        "X-Goog-Api-Key": GOOGLE_PLACES_API_KEY,
        "X-Goog-FieldMask": ",".join(
            [
                "id",
                "displayName",
                "formattedAddress",
                "shortFormattedAddress",
                "types",
                "location",
                "viewport",
            ]
        ),
    }
    params = {
        "sessionToken": session,
    }
    url = f"{GOOGLE_PLACES_DETAILS_URL}/{quote(path_segment, safe='')}"

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=GOOGLE_PLACES_TIMEOUT)
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail="Places Details unavailable") from exc

    print("[places/details] status", resp.status_code)
    print("[places/details] headers", dict(resp.headers))
    try:
        raw_body = resp.json()
    except ValueError:
        raw_body = resp.text
    print("[places/details] body", raw_body)

    if resp.status_code == 429:
        detail = _places_error_detail(raw_body) or "Places Details quota exceeded"
        raise HTTPException(status_code=429, detail=detail)
    if resp.status_code >= 400:
        detail = _places_error_detail(raw_body) or "Places Details error"
        raise HTTPException(status_code=resp.status_code, detail=detail)
    if raw_body is None or not isinstance(raw_body, dict):
        raise HTTPException(status_code=502, detail="Invalid response from Places Details")

    normalized = _normalize_place_detail(raw_body)
    return {"result": normalized}

# ---------- Custom D_anchor (one-off for a user-picked point) ----------
# Reuse graph + anchors and compute anchor->custom seconds via a single-source run on the CSR transpose.

def _load_graph_and_anchors(mode: str):
    key = mode
    if key not in _GRAPH_CACHE:
        print(f"[_load_graph_and_anchors] Loading graph for mode={mode} (first-time load, may take 30-60 seconds)...")
        # Locate PBF by STATE name
        state = os.environ.get("TS_STATE", "massachusetts")
        pbf = os.path.join("data", "osm", f"{state}.osm.pbf")
        cache_dir = os.path.join("data", "osm", "cache_csr", f"{state}_{mode}.npycache")
        if not os.path.isfile(pbf) and not os.path.isdir(cache_dir):
            raise RuntimeError(f"OSM PBF not found and no CSR cache available: {pbf}")
        # Deferred import to avoid hard dependency at import time
        from graph.pyrosm_csr import load_or_build_csr  # type: ignore
        import time
        start = time.time()
        node_ids, indptr, indices, w_sec, node_lats, node_lons, node_h3_by_res, res_used = load_or_build_csr(pbf, mode, [8], False)
        elapsed = time.time() - start
        print(f"[_load_graph_and_anchors] Graph loaded in {elapsed:.1f}s: {len(node_ids)} nodes, {len(indices)} edges")
        if load_or_build_ch is None or build_rev_csr is None:
            raise RuntimeError("CH helpers unavailable; native module not built")
        print(f"[_load_graph_and_anchors] Building CSR transpose for CH (one-time)...")
        rev_start = time.time()
        indptr_rev, indices_rev, w_rev = build_rev_csr(indptr, indices, w_sec)
        rev_elapsed = time.time() - rev_start
        print(f"[_load_graph_and_anchors] CSR transpose built in {rev_elapsed:.1f}s")
        print(f"[_load_graph_and_anchors] Preparing CH graph (cached, reverse edges) for mode={mode}...")
        ch_graph = load_or_build_ch(cache_dir, indptr_rev, indices_rev, w_rev, suffix="_rev")
        try:
            ch_nodes = getattr(ch_graph, "num_nodes", None)
        except Exception:
            ch_nodes = None
        if isinstance(ch_nodes, int):
            print(f"[_load_graph_and_anchors] CH ready with {ch_nodes:,} nodes")
        _GRAPH_CACHE[key] = {
            "node_ids": node_ids,
            "indptr": indptr,
            "indices": indices,
            "w_sec": w_sec,
            "indptr_rev": indptr_rev,
            "indices_rev": indices_rev,
            "w_rev": w_rev,
            "lats": node_lats,
            "lons": node_lons,
            "ch_rev": ch_graph,
        }
    if key not in _ANCHOR_CACHE:
        print(f"[_load_graph_and_anchors] Loading anchor sites for mode={mode}...")
        sites_path = _find_sites_parquet(mode)
        if not sites_path:
            print(f"WARNING: No anchor sites parquet found for mode={mode}. Creating empty anchor cache.")
            # Create empty anchor cache to allow graceful degradation
            _ANCHOR_CACHE[key] = {
                "anchors_df": pd.DataFrame(),
                "anchor_idx": np.array([], dtype=np.int32),
                "anchor_nodes": np.array([], dtype=np.int32),
                "anchor_ids": np.array([], dtype=np.int32),
                "anchor_lats": np.array([], dtype=np.float32),
                "anchor_lons": np.array([], dtype=np.float32),
            }
            return _GRAPH_CACHE[key], _ANCHOR_CACHE[key]
        anchors_df = pd.read_parquet(sites_path)
        if "anchor_int_id" not in anchors_df.columns:
            anchors_df = anchors_df.sort_values("site_id").reset_index(drop=True)
            anchors_df["anchor_int_id"] = anchors_df.index.astype("int32")
        # Build mapping from node id -> anchor_int_id aligned to CSR node order
        node_ids = _GRAPH_CACHE[key]["node_ids"]  # type: ignore
        nid_to_idx = {int(n): i for i, n in enumerate(node_ids.tolist())}
        anchor_idx = np.full(len(node_ids), -1, dtype=np.int32)
        for node_id, aint in anchors_df[["node_id", "anchor_int_id"]].itertuples(index=False):
            j = nid_to_idx.get(int(node_id))
            if j is not None:
                anchor_idx[j] = int(aint)
        anchor_nodes = np.flatnonzero(anchor_idx >= 0).astype(np.int32, copy=False)
        anchor_ids = anchor_idx[anchor_nodes].astype(np.int32, copy=False)
        lats = _GRAPH_CACHE[key]["lats"]  # type: ignore
        lons = _GRAPH_CACHE[key]["lons"]  # type: ignore
        anchor_lats = lats[anchor_nodes].astype(np.float32, copy=False)
        anchor_lons = lons[anchor_nodes].astype(np.float32, copy=False)
        print(f"[_load_graph_and_anchors] Loaded {len(anchors_df)} anchor sites")
        _ANCHOR_CACHE[key] = {
            "anchors_df": anchors_df,
            "anchor_idx": anchor_idx,
            "anchor_nodes": anchor_nodes,
            "anchor_ids": anchor_ids,
            "anchor_lats": anchor_lats,
            "anchor_lons": anchor_lons,
        }
    return _GRAPH_CACHE[key], _ANCHOR_CACHE[key]


    


def _nearest_node_index(lons: np.ndarray, lats: np.ndarray, lon: float, lat: float) -> int:
    # Equirectangular projection distance (fast, good enough for nearest neighbor)
    lat0 = float(np.deg2rad(np.mean(lats))) if lats.size else 0.0
    m_per_deg = 111000.0
    xs = (lons.astype(np.float64) * np.cos(lat0)) * m_per_deg
    ys = (lats.astype(np.float64)) * m_per_deg
    x = (float(lon) * np.cos(lat0)) * m_per_deg
    y = float(lat) * m_per_deg
    j = int(np.argmin((xs - x) ** 2 + (ys - y) ** 2))
    return j


def _approx_anchor_mask(
    anchor_lats: np.ndarray,
    anchor_lons: np.ndarray,
    lat: float,
    lon: float,
    minutes: float,
    speed_m_per_min: float = 1500.0,
    pad_factor: float = 1.4,
) -> np.ndarray:
    """Return boolean mask of anchors within a generous straight-line radius for the requested minutes.

    Uses a fast equirectangular projection to approximate great-circle distance. The radius is
    minutes * speed_m_per_min with an additional pad_factor safety margin to avoid false negatives.
    """
    if anchor_lats.size == 0:
        return np.zeros(0, dtype=bool)
    minutes = max(float(minutes), 0.0)
    if minutes <= 0:
        return np.zeros(anchor_lats.shape, dtype=bool)
    radius_m = minutes * float(speed_m_per_min) * float(pad_factor)
    if radius_m <= 0:
        return np.zeros(anchor_lats.shape, dtype=bool)
    m_per_deg = 111000.0
    lat_rad = float(np.deg2rad(lat))
    cos_lat = max(np.cos(lat_rad), 1e-4)
    dx = (anchor_lons.astype(np.float64) - float(lon)) * cos_lat * m_per_deg
    dy = (anchor_lats.astype(np.float64) - float(lat)) * m_per_deg
    dist2 = dx * dx + dy * dy
    return dist2 <= (radius_m * radius_m)


@app.get("/api/d_anchor_custom")
def get_d_anchor_custom(
    lon: float = Query(..., description="Longitude of custom location"),
    lat: float = Query(..., description="Latitude of custom location"),
    mode: str = Query("drive", description="Travel mode, e.g. 'drive' or 'walk'"),
    cutoff: int = Query(30, description="Primary cutoff in minutes"),
    overflow_cutoff: int = Query(90, description="Overflow cutoff in minutes"),
):
    """
    One-off D_anchor for a custom point. Returns {anchor_int_id: seconds} suitable
    for GPU composition with T_hex tiles.
    """
    print(f"[d_anchor_custom] Request: lon={lon}, lat={lat}, mode={mode}, cutoff={cutoff}min")
    try:
        G, A = _load_graph_and_anchors(mode)
        
        # Check if anchor cache is empty (e.g., walk mode data missing)
        anchor_nodes = A.get("anchor_nodes")
        if anchor_nodes is None or len(anchor_nodes) == 0:
            print(f"WARNING: No anchor data available for mode={mode}. Returning empty result.")
            return {}
        node_ids = G["node_ids"]  # type: ignore
        lats = G["lats"]  # type: ignore
        lons = G["lons"]  # type: ignore
        anchor_idx = A["anchor_idx"]  # type: ignore
        anchor_nodes = A.get("anchor_nodes")  # type: ignore
        anchor_ids = A.get("anchor_ids")  # type: ignore
        anchor_lats = A.get("anchor_lats")  # type: ignore
        anchor_lons = A.get("anchor_lons")  # type: ignore
        if anchor_nodes is None or anchor_ids is None or anchor_lats is None or anchor_lons is None:
            anchor_nodes = np.flatnonzero(anchor_idx >= 0).astype(np.int32, copy=False)
            anchor_ids = anchor_idx[anchor_nodes].astype(np.int32, copy=False)
            anchor_lats = lats[anchor_nodes].astype(np.float32, copy=False)
            anchor_lons = lons[anchor_nodes].astype(np.float32, copy=False)
            A["anchor_nodes"] = anchor_nodes
            A["anchor_ids"] = anchor_ids
            A["anchor_lats"] = anchor_lats
            A["anchor_lons"] = anchor_lons
        anchor_nodes = np.asarray(anchor_nodes, dtype=np.int32)
        anchor_ids = np.asarray(anchor_ids, dtype=np.int32)
        anchor_lats = np.asarray(anchor_lats, dtype=np.float32)
        anchor_lons = np.asarray(anchor_lons, dtype=np.float32)
        if anchor_nodes.size == 0:
            return {}

        # Find nearest node to the custom lon/lat
        j_custom = _nearest_node_index(lons, lats, lon, lat)
        print(f"[d_anchor_custom] Nearest node: {node_ids[j_custom]} at index {j_custom}")

        ch_graph = G.get("ch_rev")
        if ch_graph is None:
            raise RuntimeError("CH graph missing from graph cache")
            
        cutoff_s = int(cutoff) * 60
        overflow_s = int(overflow_cutoff) * 60
        limit_s = max(cutoff_s, overflow_s)
        print(
            f"[d_anchor_custom] Running CH+PHAST query to {len(anchor_nodes)} anchors (cutoff={cutoff}min, overflow={overflow_cutoff}min)..."
        )
        import time
        start = time.time()
        # Use query_subset to target only anchor nodes - much faster than query_all
        ts_anchor = np.asarray(ch_graph.query_subset(int(j_custom), anchor_nodes, limit_s), dtype=np.uint32)
        elapsed = time.time() - start
        print(f"[d_anchor_custom] CH query completed in {elapsed:.3f}s")

        out: Dict[str, int] = {}
        reachable_count = 0
        inf_val = np.uint32(0xFFFFFFFF)
        sentinel_u32 = np.uint32(int(UNREACH_U16))
        ts_anchor = np.where(ts_anchor == inf_val, sentinel_u32, ts_anchor)
        ts_anchor = np.minimum(ts_anchor, sentinel_u32)
        ts_anchor_u16 = ts_anchor.astype(np.uint16, copy=False)
        for aid, t_raw in zip(anchor_ids, ts_anchor_u16):
            t = int(t_raw)
            if t < int(UNREACH_U16):
                reachable_count += 1
            out[str(int(aid))] = t
        print(f"[d_anchor_custom] Computed {len(out)} anchor times, {reachable_count} reachable within cutoff")
        return out
    except Exception as e:
        import traceback
        print(f"ERROR in d_anchor_custom: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

# ---------- POI Pins (GeoJSON) ----------
_CANON_POI_CACHE: Optional[pd.DataFrame] = None
_CATEGORY_ID_TO_SLUG_CACHE: Optional[Dict[str, str]] = None


def _load_category_id_to_slug() -> Dict[str, str]:
    """Invert the label->id mapping to id->label slug."""
    global _CATEGORY_ID_TO_SLUG_CACHE
    if _CATEGORY_ID_TO_SLUG_CACHE is not None:
        return _CATEGORY_ID_TO_SLUG_CACHE
    label_to_id = _load_category_label_to_id()
    inverted = {str(v): str(k) for k, v in label_to_id.items()}
    _CATEGORY_ID_TO_SLUG_CACHE = inverted
    return inverted

def _load_canonical_pois() -> pd.DataFrame:
    global _CANON_POI_CACHE
    if _CANON_POI_CACHE is not None:
        return _CANON_POI_CACHE
    path = os.path.join("data", "poi", f"{STATE}_canonical.parquet")
    if not os.path.exists(path):
        # Empty dataframe with expected columns
        _CANON_POI_CACHE = pd.DataFrame(
            columns=["brand_id", "category", "lon", "lat", "name", "address", "approx_address"]
        )  # type: ignore
        return _CANON_POI_CACHE
    try:
        desired_columns = ["brand_id", "category", "lon", "lat", "name", "address", "approx_address"]
        try:
            df = pd.read_parquet(path, columns=desired_columns)  # type: ignore
        except (KeyError, ValueError):
            df = pd.read_parquet(path)  # type: ignore
        # Restrict to desired columns if the file had extras
        present_columns = [col for col in desired_columns if col in df.columns]
        df = df[present_columns].copy()
        # Ensure correct dtypes
        if "brand_id" in df.columns:
            df["brand_id"] = df["brand_id"].astype(str).str.strip()
            df["brand_id"] = df["brand_id"].replace(
                {"": None, "nan": None, "None": None, "NaN": None, "null": None, "NULL": None}
            )
        if "category" in df.columns:
            df["category"] = df["category"].astype(str).str.strip()
        if "lon" in df.columns:
            df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
        if "lat" in df.columns:
            df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
        # Drop invalid rows
        df = df.dropna(subset=["lon", "lat"]).copy()
        _CANON_POI_CACHE = df
        return df
    except Exception as e:
        print(f"[warn] Failed to read canonical POIs at {path}: {e}")
        _CANON_POI_CACHE = pd.DataFrame(
            columns=["brand_id", "category", "lon", "lat", "name", "address", "approx_address"]
        )  # type: ignore
        return _CANON_POI_CACHE


def _normalize_category_query(value: Optional[str]) -> Optional[str]:
    """Normalize a category query parameter to a canonical slug."""
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    id_to_slug = _load_category_id_to_slug()
    # Direct ID match
    if raw in id_to_slug:
        return id_to_slug[raw]
    lowered = raw.lower().strip()
    if lowered in id_to_slug.values():
        return lowered
    # Try human-readable label (e.g., "Fast Food")
    slug_candidate = lowered.replace(" ", "_")
    labels = _load_category_labels()
    for cid, label in labels.items():
        if isinstance(label, str) and label.lower().strip() == lowered:
            return id_to_slug.get(str(cid), slug_candidate)
    return slug_candidate


@app.get("/api/poi_points")
def poi_points(
    brands: Optional[str] = Query(None, description="Comma-separated brand_ids to include"),
    category: Optional[str] = Query(None, description="Optional category id/slug/label to include"),
    bbox: Optional[str] = Query(None, description="Optional bbox lonmin,latmin,lonmax,latmax")
):
    """Return GeoJSON FeatureCollection of POI points filtered by brand and/or category."""
    brand_list = []
    if brands:
        brand_list = [b.strip() for b in str(brands).split(",") if b.strip()]
        # Deduplicate while preserving order
        brand_list = list(dict.fromkeys(brand_list))

    category_slug = _normalize_category_query(category)

    if not brand_list and not category_slug:
        return {"type": "FeatureCollection", "features": []}

    df = _load_canonical_pois()
    if df.empty:
        return {"type": "FeatureCollection", "features": []}

    sub = df
    if brand_list:
        if "brand_id" not in sub.columns:
            sub = sub.iloc[0:0]
        else:
            sub = sub[sub["brand_id"].isin(brand_list)]
    if category_slug:
        if "category" not in sub.columns:
            sub = sub.iloc[0:0]
        else:
            sub = sub[sub["category"].astype(str).str.strip() == category_slug]

    if sub.empty or "lon" not in sub.columns or "lat" not in sub.columns:
        return {"type": "FeatureCollection", "features": []}

    # Ensure we have the columns needed for feature construction
    columns_needed = [col for col in ["lon", "lat", "brand_id", "name", "category", "address", "approx_address"] if col in sub.columns]
    sub = sub[columns_needed].copy()

    # Optional bbox filter
    if bbox and not sub.empty:
        try:
            parts = [float(x) for x in bbox.split(",")]
            if len(parts) == 4:
                x0, y0, x1, y1 = parts
                xmin, xmax = (min(x0, x1), max(x0, x1))
                ymin, ymax = (min(y0, y1), max(y0, y1))
                sub = sub[(sub["lon"] >= xmin) & (sub["lon"] <= xmax) & (sub["lat"] >= ymin) & (sub["lat"] <= ymax)]
        except Exception:
            pass

    if sub.empty:
        return {"type": "FeatureCollection", "features": []}

    feats = []
    for _, r in sub.iterrows():
        lon = float(r["lon"]) if pd.notna(r["lon"]) else None
        lat = float(r["lat"]) if pd.notna(r["lat"]) else None
        if lon is None or lat is None:
            continue
        props: Dict[str, Any] = {}
        brand_id = r.get("brand_id")
        if isinstance(brand_id, str) and brand_id.strip():
            props["brand_id"] = brand_id.strip()
        category_val = r.get("category")
        if isinstance(category_val, str) and category_val.strip():
            props["category"] = category_val.strip()
        name = r.get("name") if isinstance(r.get("name"), str) else None
        if name:
            props["name"] = name
        address = r.get("address") if isinstance(r.get("address"), str) else None
        approx = r.get("approx_address") if isinstance(r.get("approx_address"), str) else None
        if address and address.strip():
            props["address"] = address.strip()
        elif approx and approx.strip():
            props["approx_address"] = approx.strip()
        else:
            fallback = f"{lat:.4f}°, {lon:.4f}°"
            props["address"] = fallback
            props["approx_address"] = fallback
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
    Returns a JSON object mapping anchor_id to travel time in seconds
    for a given category and travel mode.
    """
    # Resolve category id (raises HTTPException 404 if invalid)
    cid = resolve_category_id(category, mode)

    try:
        # Load only the specific category instead of all categories
        D = load_D_anchor_category(mode, cid)

        if D.empty:
            # Gracefully return empty result if data is missing (e.g., walk mode not computed)
            print(f"INFO: No D_anchor data available for category={category} mode={mode}. Returning empty result.")
            return {}

        # Convert to a dictionary: { anchor_id: seconds }
        # The client will use this to map anchor IDs from the T_hex tiles
        # to the travel times for the selected category.
        anchor_ids = D["anchor_id"].to_numpy(dtype=np.uint32, na_value=0)
        seconds = D["seconds_clamped"].to_numpy(dtype=np.uint16, na_value=UNREACH_U16)
        return {str(int(a)): int(s) for a, s in zip(anchor_ids, seconds)}

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
    Returns a JSON object mapping anchor_id to travel time in seconds
    for a given brand and travel mode.
    """
    bid = _resolve_brand_id(brand)
    try:
        D = load_D_anchor_brand(mode, bid)
        if D.empty:
            # Gracefully return empty result if data is missing (e.g., walk mode not computed)
            print(f"INFO: No D_anchor data available for brand={brand} mode={mode}. Returning empty result.")
            return {}
        anchor_ids = D["anchor_id"].to_numpy(dtype=np.uint32, na_value=0)
        seconds = D["seconds_clamped"].to_numpy(dtype=np.uint16, na_value=UNREACH_U16)
        return {str(int(a)): int(s) for a, s in zip(anchor_ids, seconds)}
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
    print(f"Using STATE={STATE}")
    # Pass app directly instead of module path to avoid import issues
    uvicorn.run(app, host="0.0.0.0", port=port, reload=False)
