# TownScout Troubleshooting Guide

## Frontend Loading Issues & Map Tile Problems

This document covers common issues when setting up the TownScout web frontend and their solutions.

## 1. JavaScript Library Loading Errors

### Problem: PMTiles.js CDN Loading Failures
```
[Error] Failed to load resource: the server responded with a status of 404 (Not Found) (pmtiles.js, line 0)
[Error] Refused to execute https://unpkg.com/pmtiles@2.11.0/dist/pmtiles.js as script because "X-Content-Type-Options: nosniff" was given and its Content-Type is not a script MIME type.
[Error] ReferenceError: Can't find variable: pmtiles
```

### Root Causes:
- CDN availability issues
- Incorrect CDN URL paths
- MIME type restrictions from CDN
- Network/firewall blocking external resources

### Solution:
**Download JavaScript libraries locally** instead of relying on CDN:

```bash
# Download pmtiles.js locally
curl -L -o tiles/web/pmtiles.js https://unpkg.com/pmtiles@2.11.0/dist/index.js

# Update HTML to use local path
# Change: <script src="https://unpkg.com/pmtiles@2.11.0/dist/pmtiles.js"></script>
# To:     <script src="/static/pmtiles.js"></script>
```

### Architecture Update:
Add static file serving in FastAPI (`api/app/main.py`):
```python
from fastapi.staticfiles import StaticFiles

# Mount static files to serve the frontend and tiles
app.mount("/static", StaticFiles(directory="tiles/web"), name="static")
app.mount("/tiles", StaticFiles(directory="tiles"), name="tiles")
```

## 2. Map Tiles Not Found (404 Errors)

### Problem: Map loads but no tiles display
- Frontend loads but map is blank
- Console shows 404 errors for `.pmtiles` files
- Map controls work but no hexagon data appears

### Root Causes:
- Missing PMTiles files
- Incorrect static file routing
- Wrong tile URLs in frontend

### Solution:
1. **Verify tiles exist**:
```bash
ls -la tiles/*.pmtiles
# Should show: t_hex_r7_drive.pmtiles, t_hex_r8_drive.pmtiles
```

2. **Add static routing for tiles**:
```python
app.mount("/tiles", StaticFiles(directory="tiles"), name="tiles")
```

3. **Test tile accessibility**:
```bash
curl -I http://localhost:5174/tiles/t_hex_r8_drive.pmtiles
# Should return: HTTP/1.1 200 OK
```

## 3. H3 Library Version Compatibility Issues

### Problem: H3 Function Errors
```
AttributeError: module 'h3' has no attribute 'h3_to_geo_boundary'
AttributeError: module 'h3' has no attribute 'int_to_string'
```

### Root Cause:
H3 library has breaking changes between v3 and v4 APIs.

### Solution:
**Use robust version compatibility code** in scripts:

```python
# Handle h3 v3 and v4 compatibility
try:
    import h3
    int_to_str = getattr(h3, "int_to_string", None) or getattr(h3, "h3_to_string", None)
    to_boundary = h3.h3_to_geo_boundary
except Exception:
    try:
        from h3.api.basic_int import h3 as h3v4
        h3 = h3v4
        int_to_str = h3v4.h3_to_string
        to_boundary = h3v4.h3_to_geo_boundary
    except Exception:
        # Direct H3 v4 API
        import h3
        int_to_str = h3.int_to_str
        to_boundary = h3.cell_to_boundary

def hex_polygon_lonlat(h3_addr: str):
    try:
        # H3 v3 API
        boundary_latlon = to_boundary(h3_addr, geo_json=True)
    except TypeError:
        # H3 v4 API - no geo_json parameter
        boundary_latlon = to_boundary(h3_addr)
    # ... rest of function
```

### Key H3 API Differences:
- **v3**: `h3.h3_to_geo_boundary(h3_addr, geo_json=True)`
- **v4**: `h3.cell_to_boundary(h3_addr)` (no geo_json param)
- **v3**: `h3.int_to_string()` or `h3.h3_to_string()`  
- **v4**: `h3.int_to_str()`

## 4. Data Type Corruption in Pandas

### Problem: Scientific Notation Corruption
```
ValueError: invalid literal for int() with base 16: '6.132317455122432e+17'
```

### Root Cause:
**`.iterrows()` converts uint64 to float64**, causing precision loss with large H3 integer IDs.

### Wrong Approach:
```python
for _, row in df.iterrows():
    h3_val = row['h3_id']  # uint64 becomes float64!
    h3_addr = int_to_str(int(h3_val))  # Precision lost
```

### Correct Solution:
**Use `.itertuples()` to preserve data types**:
```python
for i, row in enumerate(df.itertuples(index=False)):
    h3_col_idx = df.columns.get_loc('h3_id')
    h3_val = row[h3_col_idx]  # Preserves uint64
    h3_addr = int_to_str(int(h3_val))  # Safe conversion
```

### Alternative Solutions:
1. **Convert to string early**: Store H3 IDs as strings in parquet
2. **Use `.iloc[]`**: `h3_val = df.iloc[i]['h3_id']` (slower but safer)
3. **Batch processing**: Process in chunks to avoid memory pressure

## 5. Server Configuration Issues

### Problem: Routes Not Working After Code Changes
- 404s for new routes
- Static files not serving
- Old behavior persists

### Solution:
**Always restart the server** after route changes:
```bash
# Kill existing server
pkill -f uvicorn

# Restart
make serve
# or
TS_DATA_DIR=data/minutes TS_STATE=massachusetts .venv/bin/uvicorn api.app.main:app --host 0.0.0.0 --port 5174
```

## 6. Missing Map Tiles Pipeline

### Problem: Frontend loads but map is blank
The frontend expects specific tile files but the pipeline hasn't been run.

### Complete Tile Building Pipeline:
```bash
# 1. Build GeoJSON from T_hex data (with H3 compatibility fixes)
PYTHONPATH=. .venv/bin/python scripts/05_h3_to_geojson.py \
    --input data/minutes/massachusetts_hex_to_anchor_drive.parquet \
    --output tiles/t_hex_r8_drive.geojson.nd \
    --h3-col h3_id

# 2. Create r7 aggregated data (optional, for multi-resolution)
# Aggregate r8 → r7 using h3.cell_to_parent()

# 3. Build PMTiles
PYTHONPATH=. .venv/bin/python scripts/06_build_tiles.py \
    --input tiles/t_hex_r8_drive.geojson.nd \
    --output tiles/t_hex_r8_drive.pmtiles \
    --layer t_hex_r8_drive \
    --minzoom 5 --maxzoom 12
```

### Expected Output Files:
```
tiles/
├── t_hex_r7_drive.pmtiles  # Low resolution (overview)
├── t_hex_r8_drive.pmtiles  # High resolution (detail)
└── web/
    ├── index.html
    └── pmtiles.js           # Local copy
```

## 7. General Debugging Approach

### Step 1: Check Server Logs
Look for specific error patterns:
- `404 Not Found` → Missing routes or files
- `AttributeError` → Library version issues  
- `ValueError` → Data type corruption
- `TypeError` → Function signature mismatches

### Step 2: Test Individual Components
```bash
# Test API health
curl http://localhost:5174/health

# Test static files
curl -I http://localhost:5174/static/pmtiles.js
curl -I http://localhost:5174/tiles/t_hex_r8_drive.pmtiles

# Test frontend loading
curl -s http://localhost:5174/ | grep -E "(error|404|pmtiles)"
```

### Step 3: Verify Data Pipeline
```bash
# Check if T_hex data exists
ls -la data/minutes/massachusetts_hex_to_anchor_*.parquet

# Check data structure
python -c "
import pandas as pd
df = pd.read_parquet('data/minutes/massachusetts_hex_to_anchor_drive.parquet')
print('Columns:', df.columns.tolist())
print('H3 dtype:', df['h3_id'].dtype)
print('Sample:', df.head(2))
"

# Check tiles exist
ls -la tiles/*.pmtiles tiles/*.geojson.nd
```

### Step 4: Browser Developer Tools
- **Console**: JavaScript errors, network failures
- **Network tab**: 404s, failed resource loads, MIME type issues
- **Sources**: Verify local vs CDN script loading

## 8. Common File Structure Issues

### Expected Directory Structure:
```
townscout/
├── api/app/main.py              # FastAPI server
├── data/minutes/                # T_hex and D_anchor data
│   ├── massachusetts_hex_to_anchor_drive.parquet
│   └── massachusetts_anchor_to_category_drive.parquet
├── tiles/                       # Map tiles and frontend
│   ├── t_hex_r7_drive.pmtiles
│   ├── t_hex_r8_drive.pmtiles
│   └── web/
│       ├── index.html           # Frontend
│       └── pmtiles.js           # Local library copy
└── scripts/                     # Build scripts
    ├── 05_h3_to_geojson.py     # GeoJSON conversion
    └── 06_build_tiles.py       # PMTiles creation
```

## 9. Memory and Performance Issues

### Large Dataset Handling:
- **Use `.itertuples()`** instead of `.iterrows()`
- **Process in batches** for large H3 datasets
- **Add `gc.collect()`** after memory-intensive operations
- **Use appropriate data types** (uint16 for times, int32 for IDs)

### Tile Size Optimization:
- **Compress with ZSTD**: Use `compression="zstd"` in parquet output
- **Limit properties**: Only include essential fields in tiles
- **Multi-resolution**: Use r7/r8 for zoom-appropriate detail

## 10. Preventive Measures

1. **Always use robust H3 compatibility code** (don't simplify it)
2. **Download critical JS libraries locally** (don't rely on CDN)
3. **Test data type preservation** when processing large integers
4. **Restart servers** after configuration changes
5. **Verify complete pipeline** before testing frontend
6. **Check browser console** for client-side errors
7. **Use absolute paths** for static file references

## Quick Recovery Checklist

When things break:

- [ ] Check server logs for specific errors
- [ ] Restart the API server (`pkill -f uvicorn && make serve`)
- [ ] Verify all expected files exist (`ls -la tiles/*.pmtiles`)
- [ ] Test individual endpoints (`curl -I http://localhost:5174/health`)
- [ ] Check browser console for JavaScript errors
- [ ] Verify data types in pipeline scripts
- [ ] Ensure H3 library compatibility code is intact
- [ ] Confirm static file routes are properly configured

---

*This guide documents solutions to actual issues encountered during TownScout development. Keep it updated as new issues are discovered and resolved.*
