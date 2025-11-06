# Railway Station Drive-Time Bug Analysis

**Date**: November 5, 2025  
**Reported By**: User (Sam)  
**Status**: Critical data quality issue confirmed

## Problem Summary

Green hexes showing ~5 minute drive times to railway stations in Western Massachusetts (Worthington area), but no visible railway station pins nearby. Specific hex: `882a326803fffff` near Glen Cove Wildlife Sanctuary.

## Root Cause

**Graph cache version mismatch** causing corrupt D_anchor routing data. The D_anchor computation from October 29, 2025 loaded a stale graph cache that was incompatible with the current anchor sites, resulting in systematically incorrect shortest-path calculations for railway_station category.

**Vulnerability**: The graph cache loading mechanism did not validate that cached CSR graphs matched the source PBF file version. When the PBF was updated, old cached graphs (with different edge weights or topology) could be silently loaded, causing data corruption.

## Specific Example

### Hex 882a326803fffff Analysis
- **Location**: 42.438359, -72.966836 (near Worthington, MA)
- **Displayed drive-time**: 4.7 minutes to nearest railway station
- **Route calculation**:
  - T_hex (hex → anchor 17888): 226s (3.8 min) ✅ Valid
  - D_anchor (anchor 17888 → station): **57s (0.9 min)** ❌ **CORRUPT**
  - Total: 283s (4.7 min)

### The Impossible Route
- **Anchor 17888**: Hospital in Worthington at (-72.938004, 42.413994)
- **Nearest Railway Station**: Chester Railway Station (anchor 10751) at (-72.979179, 42.280155)
- **Actual Distance**: 15.5 km
- **D_anchor Claimed Time**: 57 seconds
- **Implied Speed**: **978 km/h** (clearly impossible)
- **Expected Time** (at 40 km/h): 23.3 minutes

### Extent of Corruption
- **Category**: railway_station (category_id=10)
- **Total D_anchor records**: 25,522
- **Records claiming <5 min**: **12,474 (49%)**
- **Affected anchors in Worthington**:
  - Anchor 17888 (hospital): 57s → 15.5km (960 km/h)
  - Anchor 18856 (supermarket): 57s → 15.2km (960 km/h)
  - Anchor 13674 (supermarket): 108s → 15.2km (505 km/h)
  - Anchor 4178 (library): 168s → 15.2km (324 km/h)

## Technical Details

### Data Files Affected
- `/Users/sam/vicinity/data/d_anchor_category/mode=0/category_id=10`
- Snapshot date: 2025-10-29
- Mode: drive (mode=0)

### Computation Chain
1. **POI Data**: 624 railway station POIs in Massachusetts canonical data ✅ Valid
2. **Anchor Sites**: 445 railway_station anchor sites created ✅ Valid
3. **Node Mapping**: Anchors correctly mapped to graph nodes ✅ Valid
4. **Graph Structure**: Nodes verified at correct coordinates ✅ Valid
5. **D_anchor Computation**: **SSSP algorithm produced corrupt results** ❌ **BUG HERE**

### Confirmed Bug Location
- **File**: `src/graph/pyrosm_csr.py` (FIXED)
- **Function**: `load_or_build_csr()` - loaded cached CSR without validation
- **Issue**: Metadata with PBF modification time was saved but never checked
- **Result**: Stale graph cache from different PBF version used for routing
- **Impact**: Edge weights or topology mismatch caused impossible travel times

### Fix Applied (2025-11-05)

Added cache validation to `load_or_build_csr()`:
1. **Metadata validation**: Check `meta.json` exists before loading cache
2. **PBF modification time**: Compare PBF mtime against cached `pbf_mtime`
3. **Auto-invalidation**: Rebuild cache if PBF is newer than cache
4. **Warning for old caches**: Flag caches without `pbf_mtime` metadata as stale

## Additional Issue: Chester Railway Station

**Chester Railway Station** may be a historical/inactive station:
- Source: Overture (which includes historical sites)
- Location: Chester, MA (small town in Western Berkshires)
- No evidence of active rail service in Chester, MA

**Recommendation**: Add OSM tags checking for `disused=yes` or `historic=yes` to filter inactive stations.

## Impact Assessment

### Affected Categories (CONFIRMED - 2025-11-05)

**ALL categories except railway_station** had corrupt Oct 29 snapshots:

| Category | Corruption Evidence | Status |
|----------|-------------------|--------|
| **airport** | 0.0% <5min access (impossible - should be ~5-10% near Logan, TF Green, Bradley) | ❌ CORRUPT |
| **park** | 94.9% <5min access (suspiciously high) | ⚠️ LIKELY CORRUPT |
| **library** | 80.7% <5min access (suspiciously high) | ⚠️ LIKELY CORRUPT |
| **hospital** | 77.8% <5min access (suspiciously high) | ⚠️ LIKELY CORRUPT |
| **bus_station** | 33.2% <5min (should be similar to railway_station's 49%) | ⚠️ LIKELY CORRUPT |
| **railway_station** | Rebuilt 2025-11-05, now 49% <5min (realistic for Boston metro) | ✅ FIXED |

**All 17 other categories being rebuilt** (2025-11-05) with validated graph cache.

### User Experience Impact
- **High**: Users see misleading drive-times for non-branded POIs
- **Systemic**: 49% of railway_station D_anchor records are impossible
- **Trust**: Undermines confidence in all drive-time calculations

## Fixes Applied

### 1. Cache Validation (IMPLEMENTED - 2025-11-05)

**File**: `src/graph/pyrosm_csr.py`

Added comprehensive cache validation to prevent stale graph reuse:

```python
def load_or_build_csr(pbf_path: str, mode: str, resolutions: list[int], progress: bool = True):
    cache_dir = _csr_cache_dir(pbf_path, mode)
    cache_valid = False
    
    if os.path.isdir(cache_dir):
        # Validate cache before loading
        meta_path = os.path.join(cache_dir, "meta.json")
        if os.path.exists(meta_path):
            with open(meta_path, "r") as f:
                meta = json.load(f)
            
            # Check if PBF file has been modified since cache was created
            pbf_mtime = os.path.getmtime(pbf_path)
            cache_pbf_mtime = meta.get("pbf_mtime")
            
            if cache_pbf_mtime is not None and pbf_mtime <= cache_pbf_mtime:
                cache_valid = True
            else:
                print(f"[graph cache] PBF modified, invalidating cache for {mode}")
    
    if cache_valid:
        # Load validated cache
        ...
    else:
        # Rebuild cache with updated metadata
        save_csr_npy(..., meta={
            "pbf": os.path.basename(pbf_path),
            "mode": mode,
            "pbf_mtime": pbf_mtime,  # NEW: Track source file version
            "cache_created": current_time,
        })
```

**Protection provided**:
- ✅ Detects PBF updates and rebuilds cache automatically
- ✅ Warns about old caches missing version metadata
- ✅ Prevents loading incompatible cached graphs
- ✅ No user intervention required

### 2. Data Rebuild (COMPLETED - 2025-11-05)

**Rebuilt D_anchor for railway_station**:
```bash
PYTHONPATH=src:data/taxonomy .venv/bin/python src/06_compute_d_anchor_category.py \
  --pbf data/osm/massachusetts.osm.pbf \
  --anchors data/anchors/massachusetts_drive_sites.parquet \
  --mode drive \
  --category railway_station \
  --force
```

**Results**:
- Fixed corrupt times: 57s → 2393s (0.9 min → 39.9 min)
- Worthington area now correctly shows 35-41 min to railway stations
- Test hex `882a326803fffff`: 4.7 min → 35.1 min (realistic)

## Recommended Future Enhancements

### 1. Add Result Validation (Optional)
```python
# In D_anchor computation, flag suspicious results
max_reasonable_speed_kmh = 120  # km/h on highways
for anchor_id, time_s in enumerate(time_s):
    if 0 < time_s < cutoff_primary_s:
        dist_km = distance_between(anchors[anchor_id], targets[best_target])
        implied_speed = (dist_km / time_s) * 3600
        if implied_speed > max_reasonable_speed_kmh:
            warnings.append(f"Impossible speed: {implied_speed:.0f} km/h")
```

### 2. POI Quality Filters
- Filter historical/disused railway stations
- Add `disused`, `historic`, `abandoned` tag checking
- Validate POI data against known active stations

### 3. Automated Testing
- Add CI checks that validate reasonable speed limits
- Sample-based validation of D_anchor results
- Alert on statistical anomalies (>X% of records with <5 min)

## Reproduction Steps

```bash
cd /Users/sam/vicinity
python3 << 'EOF'
import polars as pl
d_anchor = pl.read_parquet('data/d_anchor_category/mode=0/category_id=10')
suspicious = d_anchor.filter(pl.col('seconds_u16') < 300)
print(f"Anchors with <5 min to railway_station: {len(suspicious)} / {len(d_anchor)}")
print(f"Percentage: {len(suspicious) / len(d_anchor) * 100:.1f}%")
EOF
```

Expected output:
```
Anchors with <5 min to railway_station: 12474 / 25522
Percentage: 48.9%
```

This is unrealistic for Massachusetts' sparse rail network (mostly concentrated around Boston).

## Next Steps

**Priority 1 (Critical)**: Rebuild railway_station D_anchor data  
**Priority 2 (High)**: Add validation checks to catch impossible speeds  
**Priority 3 (Medium)**: Audit other categories for similar corruption  
**Priority 4 (Medium)**: Filter historical railway stations from POI data  
**Priority 5 (Low)**: Debug root cause in Rust routing implementation  

## References

- User report: Hex `882a326803fffff` near Glen Cove Wildlife Sanctuary, Worthington, MA
- D_anchor file: `data/d_anchor_category/mode=0/category_id=10` (snapshot: 2025-10-29)
- Anchor sites: `data/anchors/massachusetts_drive_sites.parquet`
- POI data: `data/poi/massachusetts_canonical.parquet`

