# Power Corridors Processing Fix Summary

## Problem
The power corridors processing (`make power_corridors`) was failing with warnings:
- "No voltage column in power lines; resulting dataset may be empty."
- "No buffered corridor geometry produced; writing all False flags."

All hexes were incorrectly marked as `near_power_corridor=False` despite 2,582 high-voltage transmission lines being present in the OSM data.

## Root Causes

### 1. Pyrosm/Shapely 2.x Incompatibility
- Pyrosm 0.6.2 has compatibility issues with Shapely 2.0.4
- `get_data_by_custom_criteria()` was failing with: `ufunc 'create_collection' not supported for the input types`
- The custom filter `{"power": ["line"]}` returned empty DataFrames

### 2. Missing OSM Tag Columns
- Even when pyrosm succeeded, requested `tags_as_columns` were not guaranteed to be present in the returned DataFrame
- Power infrastructure data is stored in OSM's `other_tags` column and requires special parsing

### 3. Geometry Dissolution Failures  
- Shapely's `union_all()` and `unary_union()` were failing with the same `create_collection` error
- The buffering and dissolving of power line geometries was never completing

### 4. DataFrame Apply Function Issue
- The `apply(is_hit, axis=1)` approach for marking flagged hexes was silently failing
- Despite logging showing hexes were identified, no flags were being set

## Solutions Implemented

### 1. Replaced Pyrosm with GeoPandas OSM Driver
**File**: `townscout/domains_overlay/power_corridors/build_corridor_overlay.py`

- Switched from `get_osm_data()` (pyrosm) to `gpd.read_file(pbf_path, layer='lines')`
- Added `_parse_osm_tag()` function to extract power and voltage from `other_tags` column
- Filters for lines with `"power"=>` tag and parses voltage values correctly

```python
def _load_power_lines(pbf_path: str) -> gpd.GeoDataFrame:
    lines_gdf = gpd.read_file(pbf_path, layer='lines')
    power_mask = lines_gdf['other_tags'].fillna('').str.contains('"power"=>')
    power_lines = lines_gdf[power_mask].copy()
    # Parse tags from other_tags column
    power_lines['power'] = power_lines['other_tags'].apply(lambda x: _parse_osm_tag(x, 'power'))
    power_lines['voltage'] = power_lines['other_tags'].apply(lambda x: _parse_osm_tag(x, 'voltage'))
    ...
```

### 2. Iterative Union for Geometry Dissolution
**File**: `townscout/domains_overlay/power_corridors/build_corridor_overlay.py`

- Replaced `union_all()` / `unary_union()` with iterative `.union()` approach
- Buffers each line individually first, then dissolves incrementally
- Uses `clean_geoms()` utility to filter problematic geometries

```python
def _dissolve_and_buffer(lines: gpd.GeoDataFrame, buffer_meters: float):
    geom_col = clean_geoms(projected, ["LineString", "MultiLineString"])
    buffered_list = [geom.buffer(buffer_meters) for geom in geom_col]
    
    # Iterative union to avoid create_collection errors
    dissolved = buffered_list[0]
    for geom in buffered_list[1:]:
        dissolved = dissolved.union(geom)
    ...
```

### 3. Vectorized Flag Assignment
**File**: `townscout/domains_overlay/power_corridors/build_corridor_overlay.py`

- Replaced `base.apply(is_hit, axis=1)` with vectorized pandas operations
- Uses `.isin()` and boolean masking for reliable flag assignment

```python
base["near_power_corridor"] = False
for res, hit_set in all_hits.items():
    if hit_set:
        mask = (base['res'] == res) & (base['h3_id'].isin(hit_set))
        base.loc[mask, "near_power_corridor"] = True
```

### 4. Enhanced pyrosm_utils.py
**File**: `townscout/osm/pyrosm_utils.py`

- Added logic to ensure all requested `tags_as_columns` are present in results
- Works across multiple pyrosm API fallbacks
- Critical for POI ingestion reliability

## Results

### Before Fix
```
[warn] No voltage column in power lines; resulting dataset may be empty.
[warn] No buffered corridor geometry produced; writing all False flags.
Near power corridor: True=0, False=84537
```

### After Fix
```
[info] Retained 2582 high-voltage ways
[info] Dissolving 2582 buffered power corridors...
[info] res=7: 225 hexes flagged
[info] res=8: 1656 hexes flagged
Near power corridor: True=1744, False=82793
Percentage flagged: 2.06%
```

## Files Modified

1. `townscout/domains_overlay/power_corridors/build_corridor_overlay.py` - Main processing logic
2. `townscout/osm/pyrosm_utils.py` - Created new shared OSM utilities module
3. `townscout/poi/ingest_osm.py` - Updated to use new pyrosm_utils module
4. `townscout/domains_poi/beaches/classify_beaches.py` - Updated to use pyrosm_utils
5. `Makefile` - No changes needed, existing targets work correctly
6. `docs/ARCHITECTURE_OVERVIEW.md` - Documented the fix and approach

## Technical Notes

- **GeoPandas OSM driver** is more reliable than pyrosm for custom OSM data extraction
- **Iterative union** is slower (~2-3 seconds for 2,582 geometries) but reliable with Shapely 2.x
- **Vectorized pandas operations** are both faster and more reliable than `apply()` for this use case
- The fix maintains backward compatibility with existing pipeline and Makefile targets

## Testing

```bash
# Clean rebuild
rm -f data/power_corridors/massachusetts_near_power_corridor.parquet
make power_corridors

# Verify output
python -c "import pandas as pd; df = pd.read_parquet('data/power_corridors/massachusetts_near_power_corridor.parquet'); print(f'Near power corridor: True={df[\"near_power_corridor\"].sum()}')"
```

Expected output: `Near power corridor: True=1744`

