# TownScout Debugging Guide

## Quick Diagnostic Checklist

When the map isn't working, check these in order:

### 1. Backend Services ✅
```bash
# Is the API running?
curl http://localhost:5174/health

# Are D_anchor endpoints working?
# Tip: you can pass a numeric category_id discovered via /api/categories
curl "http://localhost:5174/api/categories?mode=drive"
curl "http://localhost:5174/api/d_anchor?category=1&mode=drive" | head -50

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
- Verify columns match API expectations (`anchor_int_id`, `category_id`, `seconds_u16`)
- If you switch datasets or partitions, hit `/api/categories` to confirm available ids.

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

**Solution**: Always use robust H3 compatibility code (see `src/05_h3_to_geojson.py`):
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

# 3. Check D_anchor dataset presence
python3 -c "
import pandas as pd
df = pd.read_parquet('data/minutes/massachusetts_anchor_to_category_drive.parquet')
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
curl -s "http://localhost:5174/api/d_anchor?category=1&mode=drive" | jq ".\"$ANCHOR_ID\""
curl -s "http://localhost:5174/api/d_anchor?category=2&mode=drive" | jq ".\"$ANCHOR_ID\""

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

## POI Coverage Debugging (Comprehensive Guide)

### 🚨 Sparse POI Coverage (e.g., "Starbucks coverage is too light")

**Symptoms**: POI brand shows much lower coverage than expected based on real-world density (e.g., Boston should be 100% covered by Starbucks within 15 minutes, but map shows <80%).

**Root Causes** (in order of likelihood):
1. **Brand aliasing gaps** - POI name variations not in `BRAND_REGISTRY`
2. **Missing brand fallback logic** - Overture POIs with `brand = None` not captured
3. **K-best algorithm limitations** - K too small for dense urban areas
4. **Graph connectivity issues** - Unreachable hexagons due to water/parks
5. **Overlays system needed** - Requires K=1 nearest-neighbor for comprehensive coverage

### Systematic Investigation Process

#### Step 1: Initial Coverage Assessment
```bash
# Check current coverage statistics
python -c "
import pandas as pd
import h3

# Load final merged data
df = pd.read_parquet('state_tiles/us_r8.parquet')

# Check if brand column exists
print('Available columns:', [col for col in df.columns if 'starbucks' in col.lower()])

# Overall coverage
total_hexagons = len(df)
if 'starbucks_drive_min' in df.columns:
    covered = df['starbucks_drive_min'].notna().sum()
    print(f'Total hexagons: {total_hexagons:,}')
    print(f'Starbucks coverage: {covered:,} ({covered/total_hexagons*100:.1f}%)')
else:
    print('ERROR: starbucks_drive_min column missing - brand not in final data')
"
```

#### Step 2: Trace POI Count Through Pipeline
```bash
# Check canonical POI count
python -c "
import pandas as pd
canonical_df = pd.read_parquet('data/poi/massachusetts_canonical.parquet')
starbucks_count = len(canonical_df[canonical_df['brand_id'] == 'starbucks'])
print(f'Canonical Starbucks POIs: {starbucks_count}')

# Check anchor sites count
anchors_df = pd.read_parquet('data/anchors/massachusetts_drive_sites.parquet')
import numpy as np
starbucks_anchors = 0
for _, row in anchors_df.iterrows():
    brands = row['brands']
    if isinstance(brands, (list, np.ndarray)):
        if isinstance(brands, np.ndarray):
            brands = brands.tolist()
        if 'starbucks' in brands:
            starbucks_anchors += 1
print(f'Starbucks anchor sites: {starbucks_anchors}')
"
```

#### Step 3: Fix Brand Registry Gaps
```bash
# Identify missing brand aliases from raw data
python -c "
import pandas as pd

# Check Overture brand names
overture_df = pd.read_parquet('data/overture/massachusetts_places.parquet')
starbucks_variants = set()

for _, row in overture_df.iterrows():
    name = None
    if row['names'] and 'primary' in row['names']:
        name = row['names']['primary']
    elif row['brand'] and row['brand']['names'] and 'primary' in row['brand']['names']:
        name = row['brand']['names']['primary']
    
    if name and 'starbucks' in name.lower():
        starbucks_variants.add(name.lower())

print('Found Starbucks name variants:')
for variant in sorted(starbucks_variants):
    print(f'  \"{variant}\"')
"

# Add missing variants to src/taxonomy.py BRAND_REGISTRY
# Example: "starbucks": ("Starbucks", ["starbucks coffee", "starbucks reserve", "starbucks mashpee commons"])
```

#### Step 4: Fix Brand Fallback Logic
Check `src/02_normalize_pois.py` for fallback logic in `normalize_overture_pois`:

```python
# Ensure this logic exists (lines ~140-150)
# If no brand found from brand field, try the POI name as a fallback
if not brand_id:
    poi_name = row['names']['primary'] if row['names'] and 'primary' in row['names'] else None
    if poi_name:
        brand_id = _brand_alias_to_id.get(poi_name.lower())
        if brand_id:
            brand_name = BRAND_REGISTRY[brand_id][0]
```

#### Step 5: Investigate K-best Parameters
```bash
# Check current k-best setting in Makefile
grep "k-best" Makefile

# For dense urban areas with many POIs, increase k-best:
# Change from --k-best 4 to --k-best 20 or higher
# Also consider increasing cutoff from 30 to 90 minutes for broader reach
```

#### Step 6: Implement Overlays System (for 100% Coverage)

**When K-best is insufficient** (common for dense brands like Starbucks, McDonald's):

1. **Check overlays script exists**: `src/03c_compute_overlays.py`

2. **Verify numpy array handling** in overlays script:
```python
# Lines ~65-70: _collect_brand_sources_anchor_ids function
if not isinstance(brands, (list, np.ndarray)):
    continue
if isinstance(brands, np.ndarray):
    brands = brands.tolist()

# Lines ~175-185: anchor_to_brands mapping
anchor_to_brands: Dict[int, List[str]] = {}
for aint, brands in anchors_df[["anchor_int_id", "brands"]].itertuples(index=False):
    brand_list = []
    if isinstance(brands, (list, np.ndarray)):
        if isinstance(brands, np.ndarray):
            brands = brands.tolist()
        brand_list = [str(b) for b in brands]
    anchor_to_brands[int(aint)] = brand_list
```

3. **Add overlays to Makefile**:
```makefile
# Add to .PHONY
.PHONY: overlays

# Add overlays target
overlays:
	source .venv/bin/activate && PYTHONPATH=src python src/03c_compute_overlays.py \
		--pbf data/osm/massachusetts.osm.pbf \
		--pois data/poi/massachusetts_canonical.parquet \
		--mode drive \
		--cutoff 90 \
		--res 8 \
		--out-overlays data/minutes/mode=0/ \
		--anchors data/anchors/massachusetts_drive_sites.parquet

# Add overlays as dependency to merge
merge: overlays
```

#### Step 7: Complete Pipeline Rebuild
```bash
# After fixes, rebuild entire pipeline
make pois      # Re-normalize with fixed brand registry
make anchors   # Rebuild anchor sites 
make minutes   # Recompute with updated k-best parameters
make overlays  # Compute K=1 overlays for dense brands
make merge     # Merge all data including overlays
make tiles     # Rebuild tiles with complete coverage
```

#### Step 8: Verify Final Coverage
```bash
# Check final results
python -c "
import pandas as pd
df = pd.read_parquet('state_tiles/us_r8.parquet')
total = len(df)
covered = df['starbucks_drive_min'].notna().sum()
print(f'Final coverage: {covered/total*100:.1f}% ({covered:,}/{total:,} hexagons)')

# Travel time distribution
times = df['starbucks_drive_min'].dropna()
print(f'Mean travel time: {times.mean():.1f} minutes')
print(f'0-15 min coverage: {(times <= 15).sum():,} hexagons')
"
```

### Success Indicators for POI Coverage

**✅ Excellent Coverage (target)**:
- Urban areas: 95-100% coverage within 15-minute threshold
- Suburban areas: 80-95% coverage  
- Rural areas: 60-80% coverage (acceptable due to genuine sparsity)
- Mean travel time: <15 minutes in metro areas

**⚠️ Needs Investigation**:
- Urban coverage <90% for common brands (Starbucks, McDonald's, CVS)
- Large gaps in obviously dense areas
- Mean travel times >20 minutes in cities

**🚨 Serious Issues**:
- Brand column missing from final tiles
- Zero POIs making it through normalization
- All hexagons showing 65535 (unreachable)

### Brand-Specific Debugging Tips

**For Chain Restaurants/Retail**:
- Expect 95-100% urban coverage with overlays system
- Check both Overture and OSM sources
- Verify franchise name variations in brand registry

**For Civic/Government POIs**:
- Lower expected coverage is normal (genuine sparsity)
- Focus on OSM source completeness
- Check category mapping from OSM tags

**For Natural Amenities**:
- Coverage limited by geographic distribution
- Polygon vs point representation important
- Check area calculations for large features

### Prevention Checklist

Before adding new POI categories:
1. ✅ Research all known name variations and add to `BRAND_REGISTRY`
2. ✅ Test normalization with sample data from both Overture and OSM
3. ✅ Verify category mapping from source taxonomies
4. ✅ For dense brands, plan to use overlays system from start
5. ✅ Set realistic coverage expectations based on real-world distribution

Remember: Most issues stem from data format mismatches between pipeline stages. Always verify the data contracts in `ARCHITECTURE.md` when debugging.
