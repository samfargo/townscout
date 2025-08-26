#!/usr/bin/env python3
# api/app/main.py

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
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from src.categories import get_category

APP_NAME = "TownScout D_anchor API"

# ---------- Config ----------
# Paths
DATA_DIR = os.environ.get("TS_DATA_DIR", "data/minutes")
STATE = os.environ.get("TS_STATE", "massachusetts")

# Column & sentinel conventions
UNREACH_U16 = np.uint16(65535)

# ---------- Data loading (cached) ----------
@lru_cache(maxsize=8)
def load_D_anchor(mode: str) -> pd.DataFrame:
    """
    Load seconds-based anchor→category table produced by precompute_d_anchor.py.
    Expected columns: anchor_int_id(int32), category_id(int32), seconds_u16(uint16)
    """
    path = os.path.join(DATA_DIR, f"{STATE}_anchor_to_category_{mode}.parquet")
    if not os.path.exists(path):
        raise RuntimeError(f"D_anchor parquet missing at {path}")
    df = pd.read_parquet(path)
    need = {"anchor_int_id", "category_id", "seconds_u16"}
    missing = need - set(df.columns)
    if missing:
        raise RuntimeError(f"D_anchor missing required columns: {missing}")
    return df[["anchor_int_id", "category_id", "seconds_u16"]].copy()

# ---------- FastAPI ----------
app = FastAPI(title=APP_NAME)

# Basic CORS for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # In production, restrict this to your frontend's domain
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

@app.get("/health")
def health():
    return {"ok": True, "app": APP_NAME}

@app.get("/api/d_anchor")
def get_d_anchor_slice(
    category: str = Query(..., description="Category name, e.g. 'costco'"),
    mode: str = Query("drive", description="Travel mode, e.g. 'drive' or 'walk'")
):
    """
    Returns a JSON object mapping anchor_int_id to travel time in seconds
    for a given category and travel mode.
    """
    try:
        cat = get_category(category)
        cid = int(cat.id)
    except (ValueError, KeyError) as e:
        raise HTTPException(status_code=404, detail=f"Category '{category}' not found.")

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
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True, app_dir="api/app")