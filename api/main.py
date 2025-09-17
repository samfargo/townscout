#!/usr/bin/env python3
# api/app/main.py

import os
import math
import glob
import pyarrow.dataset as ds
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import h3
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from typing import Any

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

# Mount static files to serve the frontend and tiles
app.mount("/static", StaticFiles(directory="tiles/web"), name="static")
app.mount("/tiles", StaticFiles(directory="tiles"), name="tiles")

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
    """List available category_id values for current dataset/partitions."""
    return {"mode": mode, "category_id": list_available_categories(mode)}

@app.get("/api/d_anchor")
def get_d_anchor_slice(
    category: str = Query(..., description="Category id (numeric) or taxonomy key"),
    mode: str = Query("drive", description="Travel mode, e.g. 'drive' or 'walk'")
):
    """
    Returns a JSON object mapping anchor_int_id to travel time in seconds
    for a given category and travel mode.
    """
    try:
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


if __name__ == "__main__":
    # For local dev, allow overriding the port
    port = int(os.environ.get("PORT", 5174)) # Default to 5174 to avoid conflict with frontend
    print(f"Starting TownScout D_anchor server on http://0.0.0.0:{port}")
    print(f"Using data from STATE={STATE} in DATA_DIR={DATA_DIR}")
    uvicorn.run("api.main:app", host="0.0.0.0", port=port, reload=True)
