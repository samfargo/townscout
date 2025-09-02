# TownScout Debugging Guide

## Quick Diagnostic Checklist

When the map isn't working, check these in order:

### 1. Backend Services ✅
```bash
# Is the API running?
curl http://localhost:5174/health

# Are D_anchor endpoints working?
curl "http://localhost:5174/api/d_anchor?category=chipotle&mode=drive" | head -50
# Test airports category
curl "http://localhost:5174/api/d_anchor?category=airports&mode=drive" | jq 'to_entries | map(select(.value != 65535)) | length'

# Are PMTiles being served?
curl -I http://localhost:5174/tiles/t_hex_r8_drive.pmtiles
```

### 2. Browser Console (F12) ✅
- **Network tab**: Check for 404s on PMTiles or API calls
- **Console tab**: Look for JavaScript errors or warnings
- **Application tab**: Verify no CORS issues

### 3. Data Integrity ✅
```bash
# Check T_hex tile structure
head -1 tiles/t_hex_r8_drive.geojson.nd | python3 -c "
import sys, json
props = json.loads(sys.stdin.read())['properties']
print('Properties:', list(props.keys()))
print('Sample:', props)
"

# Check D_anchor data format
ls -la out/d_anchor/massachusetts_anchor_to_category_*.parquet
```

## Common Error Patterns & Solutions

### 🚨 "Expected number, found null" (MapLibre)
**Symptoms**: Map loads but sliders don't filter hexes  
**Root Cause**: Filter expressions receiving `null` values from missing data lookups

**Debug Steps**:
```javascript
// Add temporary logging to filter function in frontend
console.log('D_anchor data sample:', Object.keys(dAnchorState.chipotle).slice(0,5));
console.log('Filter criteria:', criteria);
```

**Common Fixes**:
- Ensure D_anchor data includes all anchor IDs from T_hex tiles
- Use `["coalesce", expression, 65535]` to handle missing lookups
- Verify anchor IDs are strings in D_anchor, integers in T_hex

**Verification**:
```bash
# Check anchor ID consistency
grep -o '"a0_id":[0-9]*' tiles/t_hex_r8_drive.geojson.nd | head -5
curl -s "http://localhost:5174/api/d_anchor?category=chipotle&mode=drive" | jq 'keys[0:5]'
```

### 🚨 PMTiles Loading Failures
**Symptoms**: Map shows base layer but no hex overlays

**Debug Steps**:
```bash
# Check if files exist
ls -la tiles/*.pmtiles

# Test PMTiles format
pmtiles info tiles/t_hex_r8_drive.pmtiles

# Verify layer names match frontend
pmtiles info tiles/t_hex_r8_drive.pmtiles | grep "Layer names"
grep "source-layer" tiles/web/index.html
```

**Common Fixes**:
- Rebuild tiles: `make geojson && make tiles`
- Ensure layer names match between tippecanoe `--layer` and frontend `source-layer`
- Check file permissions and static file serving configuration

### 🚨 JavaScript Library Loading (CDN Issues)
**Symptoms**: 
```
Failed to load resource: the server responded with a status of 404 (pmtiles.js)
Refused to execute script because X-Content-Type-Options: nosniff
ReferenceError: Can't find variable: pmtiles
```

**Solution**: Use local libraries instead of CDN
```bash
# Download pmtiles.js locally
curl -L -o tiles/web/pmtiles.js https://unpkg.com/pmtiles@2.11.0/dist/index.js

# Update HTML to use local path
# Change: src="https://unpkg.com/pmtiles@2.11.0/dist/pmtiles.js"
# To: src="/static/pmtiles.js"
```

### 🚨 API 500 Internal Server Error
**Symptoms**: D_anchor API calls fail with server errors

**Debug Steps**:
```bash
# Check server logs
tail -50 ~/.cache/uvicorn.log  # or terminal where uvicorn runs

# Test data file integrity
python3 -c "
import pandas as pd
df = pd.read_parquet('out/d_anchor/massachusetts_anchor_to_category_drive.parquet')
print('Columns:', df.columns.tolist())
print('Sample:', df.head(2).to_dict())
"
```

**Common Fixes**:
- Rebuild D_anchor data: `make d-anchor`
- Verify columns match API expectations (`category_id`, `seconds_u16`)
- Check for corrupted parquet files (0-byte or unreadable)

### 🚨 Memory Issues (SIGKILL during computation)
**Symptoms**: Processes killed during `make t-hex` with exit code 137/Killed

**Debug Steps**:
```bash
# Monitor memory during computation
top -p $(pgrep -f precompute_t_hex)

# Check for corrupted cache files
find data/osm/cache -name "*.graphml" -size 0

# Check available memory
free -h
```

**Solutions**:
```bash
# Use memory-efficient K-pass mode
make t-hex EXTRA_FLAGS="--k-pass-mode"

# Reduce batch size
make t-hex EXTRA_FLAGS="--batch-size 250"

# Clear corrupted cache
rm data/osm/cache/*.graphml

# Ensure sufficient disk space for temp files
df -h /tmp
```

### 🚨 H3 Compatibility Errors
**Symptoms**: 
```
AttributeError: module 'h3' has no attribute 'h3_to_geo_boundary'
ImportError: cannot import name 'h3' from 'h3.api.basic_int'
ValueError: invalid literal for int() with base 16: '6.132317455122432e+17'
```

**Debug Steps**:
```bash
# Check H3 version
python3 -c "import h3; print(h3.__version__)"

# Test available APIs
python3 -c "
import h3
print('v3 API available:', hasattr(h3, 'h3_to_geo_boundary'))
print('v4 API available:', hasattr(h3, 'cell_to_boundary'))
"
```

**Solution**: Always use robust H3 compatibility code (see `scripts/05_h3_to_geojson.py`):
```python
# Handle H3 v3/v4 compatibility
try:
    # Try H3 v4 first
    from h3 import cell_to_boundary, int_to_str
    h3_api_version = 'v4'
except ImportError:
    try:
        # Fall back to H3 v3
        from h3 import h3_to_geo_boundary as cell_to_boundary
        from h3 import h3_to_string as int_to_str
        h3_api_version = 'v3'
    except ImportError:
        raise ImportError("No compatible H3 version found")
```

**Data Type Issues**: Use `.itertuples()` instead of `.iterrows()` to preserve uint64 H3 IDs:
```python
# BAD: Converts uint64 to float64, causing precision loss
for idx, row in df.iterrows():
    h3_val = row['h3_id']  # Now float64, scientific notation

# GOOD: Preserves original dtypes
for row in df.itertuples(index=False):
    h3_val = row.h3_id  # Still uint64
    h3_addr = int_to_str(int(h3_val))  # Safe conversion
```

## Data Flow Debugging

### Trace Data Through Pipeline
```bash
# 1. Check anchor generation
head -2 out/anchors/anchors_drive.parquet  # If CSV export exists

# 2. Verify T_hex computation
python3 -c "
import pandas as pd
df = pd.read_parquet('data/minutes/massachusetts_hex_to_anchor_drive.parquet')
print('T_hex shape:', df.shape)
print('Sample anchors per hex:', df[['a0_id', 'a1_id']].head())
"

# 3. Check D_anchor computation
python3 -c "
import pandas as pd
df = pd.read_parquet('out/d_anchor/massachusetts_anchor_to_category_drive.parquet')
print('D_anchor shape:', df.shape)
print('Categories:', df['category_id'].unique())
print('Sample times:', df[['anchor_id', 'seconds_u16']].head())
"

# 4. Verify GeoJSON conversion
head -1 tiles/t_hex_r8_drive.geojson.nd | jq '.properties | keys'

# 5. Check PMTiles structure
pmtiles info tiles/t_hex_r8_drive.pmtiles
```

### End-to-End Data Validation
```bash
# Pick a specific anchor and trace it through the system
ANCHOR_ID=4585

echo "=== Tracing Anchor $ANCHOR_ID ==="

# Where does this anchor appear in T_hex tiles?
echo "T_hex references:"
grep "\"a0_id\":$ANCHOR_ID\|\"a1_id\":$ANCHOR_ID" tiles/t_hex_r8_drive.geojson.nd | head -2

# What are its category travel times in D_anchor?
echo "D_anchor travel times:"
curl -s "http://localhost:5174/api/d_anchor?category=chipotle&mode=drive" | jq ".\"$ANCHOR_ID\""
curl -s "http://localhost:5174/api/d_anchor?category=costco&mode=drive" | jq ".\"$ANCHOR_ID\""

# Is the anchor reachable?
if [[ $(curl -s "http://localhost:5174/api/d_anchor?category=chipotle&mode=drive" | jq ".\"$ANCHOR_ID\"") == "65535" ]]; then
  echo "WARNING: Anchor $ANCHOR_ID is unreachable from Chipotle"
fi
```

## Performance Debugging

### Frontend Performance
Add to browser console to monitor filter performance:
```javascript
// Monitor filter update timing
let startTime = performance.now();
// Move slider...
let endTime = performance.now();
console.log(`Filter update took ${endTime - startTime} milliseconds`);

// Monitor API response times
console.time('D_anchor_fetch');
fetch('/api/d_anchor?category=chipotle&mode=drive')
  .then(() => console.timeEnd('D_anchor_fetch'));
```

### Backend Performance
```bash
# Monitor API response times
time curl -s "http://localhost:5174/api/d_anchor?category=chipotle&mode=drive" > /dev/null

# Check file sizes
du -h tiles/*.pmtiles
du -h out/d_anchor/*.parquet

# Monitor memory during computation
watch -n 2 'ps aux | grep precompute_t_hex | grep -v grep'
```

## Recovery Procedures

### Clean Rebuild (Nuclear Option)
```bash
# Remove all generated data and rebuild from scratch
make clean
rm -rf data/osm/cache/*.graphml
rm -rf data/minutes/*
rm -rf out/d_anchor/*
rm -rf tiles/*.pmtiles tiles/*.geojson.nd
make all
```

### Partial Rebuild Procedures
```bash
# Frontend issues only (tiles corrupted)
make geojson && make tiles && make serve

# API issues only (D_anchor data corrupted)
make d-anchor && make serve

# T_hex computation issues
make t-hex && make geojson && make tiles

# Cache corruption
rm -rf data/osm/cache/*.graphml
make t-hex  # Will rebuild cache
```

### Environment Reset
```bash
# Python dependency issues
pip uninstall -y h3 pandas geopandas
pip install h3 pandas geopandas

# Clear Python cache
find . -name "__pycache__" -type d -exec rm -rf {} +
find . -name "*.pyc" -delete

# Disk space issues
du -sh data/ out/ tiles/
df -h /tmp
```

## Success Indicators

### ✅ Working System
```bash
# All these should return success
curl -f http://localhost:5174/health                              # 200 OK
curl -f "http://localhost:5174/api/d_anchor?category=chipotle&mode=drive" | jq 'keys | length'  # >1000 keys
pmtiles info tiles/t_hex_r8_drive.pmtiles | grep "Zoom levels"   # 0-14 or similar
head -1 tiles/t_hex_r8_drive.geojson.nd | jq '.properties.k'    # 2 or 4
```

### ✅ Working Frontend
- Map loads with blue hexagons visible
- Sliders move smoothly and update display in <250ms
- Browser console shows no errors
- URL sharing works (parameters persist on page reload)

### ✅ Working Data Pipeline
- `make all` completes without SIGKILL or errors
- PMTiles files exist and are non-zero size
- D_anchor API returns anchor→category mappings
- Frontend can combine both datasets for filtering

Remember: Most issues stem from data format mismatches between pipeline stages. Always verify the data contracts in `ARCHITECTURE.md` when debugging.
