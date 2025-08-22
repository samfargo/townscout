#!/usr/bin/env python3
"""
TownScout API Server
Clean separation: /static for assets, /api for dynamic endpoints
"""

import os
import math
import json
from functools import lru_cache
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import h3
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from src.categories import get_category

APP_NAME = "TownScout API"

# ---------- Config ----------
DATA_DIR = os.environ.get("TS_DATA_DIR", "data/minutes")  # Updated to correct path
STATE = os.environ.get("TS_STATE", "massachusetts")
DEFAULT_RES = int(os.environ.get("TS_H3_RES", "8"))
UNREACH_U16 = np.uint16(65535)

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
    """Return H3 cells covering a bounding box."""
    min_lon, min_lat, max_lon, max_lat = bbox
    cells = set()
    # Sample grid points and convert to H3
    lats = np.linspace(min_lat, max_lat, max(3, int((max_lat - min_lat) * 20)))
    lons = np.linspace(min_lon, max_lon, max(3, int((max_lon - min_lon) * 20)))
    for lat in lats:
        for lon in lons:
            try:
                cell = h3.latlng_to_cell(lat, lon, res)
                cells.add(cell)
            except:
                continue
    return list(cells)

# ---------- Data Loading ----------
@lru_cache(maxsize=4)
def load_T_hex(mode: str, res: int) -> pd.DataFrame:
    """Load T_hex matrix for a given mode/resolution."""
    # Updated to match actual file naming: T_hex_drive.parquet or T_hex_walk.parquet
    fname = f"{DATA_DIR}/T_hex_{mode}.parquet"
    if not os.path.exists(fname):
        raise FileNotFoundError(f"T_hex file not found: {fname}")
    return pd.read_parquet(fname)

@lru_cache(maxsize=8) 
def load_D_anchor(mode: str, category_ids: tuple) -> Dict[int, np.ndarray]:
    """Load D_anchor arrays for given categories."""
    fname = f"{DATA_DIR}/D_anchor_{mode}.parquet"
    if not os.path.exists(fname):
        print(f"Warning: D_anchor file not found: {fname}")
        return {}
    
    df = pd.read_parquet(fname)
    out = {}
    
    # Group by category_id and create arrays indexed by anchor_int_id
    for cid in category_ids:
        category_data = df[df['category_id'] == cid]
        if not category_data.empty:
            # Create array indexed by anchor_int_id
            max_anchor_id = category_data['anchor_int_id'].max()
            anchor_array = np.full(max_anchor_id + 1, UNREACH_U16, dtype=np.uint16)
            
            # Fill in the actual values
            for _, row in category_data.iterrows():
                anchor_id = int(row['anchor_int_id'])
                seconds = int(row['seconds_u16'])
                anchor_array[anchor_id] = seconds
            
            out[int(cid)] = anchor_array
        else:
            print(f"Warning: No data found for category {cid} in {fname}")
    
    return out

# ---------- FastAPI App ----------
app = FastAPI(title=APP_NAME)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure as needed for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files for frontend assets
app.mount("/static", StaticFiles(directory="tiles"), name="static")

@app.get("/")
def root():
    """Serve the landing page."""
    from fastapi.responses import FileResponse
    return FileResponse("tiles/index.html")

@app.get("/health")
def health():
    return {"ok": True, "app": APP_NAME}

@app.get("/api/criteria")
def get_criteria_tiles(
    z: int = Query(..., description="Tile zoom level"),
    x: int = Query(..., description="Tile X coordinate"),
    y: int = Query(..., description="Tile Y coordinate"),
    res: int = Query(DEFAULT_RES, description="H3 resolution"),
    unit: str = Query("minutes", description="Threshold unit: minutes or seconds"),
    criteria: str = Query(..., description="JSON criteria array"),
):
    """
    Get H3 hexes matching criteria for a specific tile.
    Returns GeoJSON FeatureCollection.
    """
    try:
        # Parse criteria
        criteria_list = json.loads(criteria)
        parsed_criteria = []
        
        for c in criteria_list:
            if "category" not in c or "threshold" not in c:
                raise ValueError("Each criterion must have 'category' and 'threshold'")
            
            cat = get_category(c["category"])
            mode = str(c.get("mode", cat.default_mode))
            threshold = float(c["threshold"])
            threshold_seconds = int(round(threshold * 60.0)) if unit.lower().startswith("min") else int(round(threshold))
            op = str(c.get("op", "AND")).upper()
            
            if op not in ("AND", "OR"):
                raise ValueError("op must be 'AND' or 'OR'")
                
            parsed_criteria.append({
                "cid": int(cat.id),
                "mode": mode,
                "threshold_seconds": threshold_seconds,
                "op": op
            })

        # Get H3 cells for tile
        bbox = tile_to_bbox(z, x, y)
        h3_cells = bbox_h3_cells(bbox, res)
        
        if not h3_cells:
            return JSONResponse({"type": "FeatureCollection", "features": []})

        # Group criteria by mode
        by_mode = {}
        for c in parsed_criteria:
            by_mode.setdefault(c["mode"], []).append(c)

        # Initialize result mask
        result_mask = {cell: True for cell in h3_cells}

        # Process each mode
        for mode, mode_criteria in by_mode.items():
            try:
                # Load data for this mode
                T_hex = load_T_hex(mode, res)
                category_ids = tuple(c["cid"] for c in mode_criteria)
                D_anchor = load_D_anchor(mode, category_ids)
                
                if not D_anchor:
                    # No data for any category in this mode - fail all cells
                    for cell in h3_cells:
                        result_mask[cell] = False
                    continue

                # Convert H3 cells to uint64 for lookup
                cell_uint64s = [np.uint64(int(cell, 16)) for cell in h3_cells]
                
                # Filter T_hex to our cells (use h3_id column instead of h3_u64)
                T_subset = T_hex[T_hex['h3_id'].isin(cell_uint64s)].copy()
                
                if T_subset.empty:
                    # No T_hex data for these cells
                    for cell in h3_cells:
                        result_mask[cell] = False
                    continue

                # Create mapping from uint64 back to h3 string
                uint64_to_h3 = {np.uint64(int(cell, 16)): cell for cell in h3_cells}

                # Process each criterion for this mode
                for criterion in mode_criteria:
                    cid = criterion["cid"]
                    threshold_seconds = criterion["threshold_seconds"]
                    op = criterion["op"]
                    
                    if cid not in D_anchor:
                        # No data for this category - fail all cells
                        for cell in h3_cells:
                            if op == "AND":
                                result_mask[cell] = False
                        continue

                    D = D_anchor[cid]
                    
                    # Compute travel times for each hex
                    for _, row in T_subset.iterrows():
                        h3_uint64 = row['h3_id']  # Updated column name
                        h3_cell = uint64_to_h3.get(h3_uint64)
                        
                        if h3_cell is None:
                            continue
                            
                        # Get anchors and times (updated column names: a0_id, a0_s, a1_id, a1_s)
                        min_time = float('inf')
                        
                        # Check primary anchor (a0)
                        anchor_id_0 = row.get('a0_id')
                        anchor_time_0 = row.get('a0_s')
                        if pd.notna(anchor_id_0) and pd.notna(anchor_time_0) and anchor_time_0 < UNREACH_U16:
                            if int(anchor_id_0) < len(D):
                                total_time = int(anchor_time_0) + int(D[int(anchor_id_0)])
                                min_time = min(min_time, total_time)
                        
                        # Check secondary anchor (a1)
                        anchor_id_1 = row.get('a1_id')
                        anchor_time_1 = row.get('a1_s')
                        if pd.notna(anchor_id_1) and pd.notna(anchor_time_1) and anchor_time_1 < UNREACH_U16 and anchor_id_1 != -1:
                            if int(anchor_id_1) < len(D):
                                total_time = int(anchor_time_1) + int(D[int(anchor_id_1)])
                                min_time = min(min_time, total_time)
                        
                        # Check if this hex passes the criterion
                        passes = min_time <= threshold_seconds if min_time != float('inf') else False
                        
                        # Apply boolean logic
                        if op == "AND":
                            result_mask[h3_cell] = result_mask[h3_cell] and passes
                        else:  # OR
                            result_mask[h3_cell] = result_mask[h3_cell] or passes

            except Exception as e:
                print(f"Error processing mode {mode}: {e}")
                # On error, fail all cells for safety
                for cell in h3_cells:
                    result_mask[cell] = False

        # Build GeoJSON response
        features = []
        for cell, passes in result_mask.items():
            if not passes:
                continue
                
            try:
                boundary = h3.cell_to_boundary(cell)
                # H3 returns (lat, lon); GeoJSON needs [lon, lat]
                ring = [[lon, lat] for lat, lon in boundary]
                ring.append(ring[0])  # Close polygon
                
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "Polygon", "coordinates": [ring]},
                    "properties": {"h3": cell}
                })
            except Exception as e:
                print(f"Error creating geometry for cell {cell}: {e}")
                continue

        return JSONResponse({"type": "FeatureCollection", "features": features})

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/categories")
def list_categories():
    """List available POI categories."""
    from src.categories import list_categories
    cats = list_categories()
    return {
        slug: {
            "id": cat.id,
            "slug": cat.slug,
            "default_mode": cat.default_mode,
            "default_cutoff": cat.default_cutoff
        }
        for slug, cat in cats.items()
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080"))) 