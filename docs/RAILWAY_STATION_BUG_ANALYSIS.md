# Railway Station Drive-Time Bug Analysis

**Date**: November 5, 2025  
**Reported By**: User (Sam)  
**Status**: Critical data quality issue confirmed

## Problem Summary

Green hexes showing ~5 minute drive times to railway stations in Western Massachusetts (Worthington area), but no visible railway station pins nearby. Specific hex: `882a326803fffff` near Glen Cove Wildlife Sanctuary.

## Root Cause

**Corrupt D_anchor routing data** showing impossible travel times. The D_anchor computation from October 29, 2025 contains systematically incorrect shortest-path calculations for railway_station category.

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

### Suspected Bug Location
- File: `src/06_compute_d_anchor_category.py`
- Function: `compute_times()` calling `kbest_multisource_bucket_csr()`
- Rust implementation: `vicinity_native` crate
- Algorithm: Multi-source Dijkstra with reverse CSR

## Additional Issue: Chester Railway Station

**Chester Railway Station** may be a historical/inactive station:
- Source: Overture (which includes historical sites)
- Location: Chester, MA (small town in Western Berkshires)
- No evidence of active rail service in Chester, MA

**Recommendation**: Add OSM tags checking for `disused=yes` or `historic=yes` to filter inactive stations.

## Impact Assessment

### Affected Categories
Likely **ALL non-branded categories** have similar D_anchor corruption:
- railway_station (confirmed corrupted)
- bus_station (likely corrupted)
- airport (likely corrupted)  
- hospital (needs verification)
- library (needs verification)
- park (needs verification)

### User Experience Impact
- **High**: Users see misleading drive-times for non-branded POIs
- **Systemic**: 49% of railway_station D_anchor records are impossible
- **Trust**: Undermines confidence in all drive-time calculations

## Recommended Fixes

### Immediate Actions
1. **Rebuild D_anchor for railway_station**:
   ```bash
   PYTHONPATH=src .venv/bin/python src/06_compute_d_anchor_category.py \
     --pbf data/osm/massachusetts.osm.pbf \
     --anchors data/anchors/massachusetts_drive_sites.parquet \
     --mode drive \
     --category railway_station \
     --force
   ```

2. **Add validation checks** to D_anchor computation:
   ```python
   # After compute_times(), validate results
   max_reasonable_speed_kmh = 120  # km/h on highways
   for anchor_id, time_s in enumerate(time_s):
       if time_s > 0 and time_s < cutoff_primary_s:
           dist_km = distance_between(anchors[anchor_id], targets[best_target])
           implied_speed = (dist_km / time_s) * 3600
           if implied_speed > max_reasonable_speed_kmh:
               warnings.append(f"Impossible speed: {implied_speed:.0f} km/h")
   ```

3. **Audit other categories**:
   - Check bus_station, airport, hospital, library, park
   - Look for <5 min times to sparse POI networks

### Long-term Fixes
1. **Investigate Rust routing bug**: Debug `kbest_multisource_bucket_csr()` in `vicinity_native`
2. **Add POI quality filters**:
   - Filter historical/disused railway stations
   - Add `disused`, `historic`, `abandoned` tag checking
3. **Automated validation**: Add CI checks that flag impossible speeds
4. **Data lineage tracking**: Store provenance metadata with D_anchor files

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

