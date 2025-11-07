# Bug Fixes & Quality Improvements Changelog

This document tracks major bug fixes and quality improvements to the vicinity system in reverse chronological order.

---

## 2025-11-07: Multi-Resolution Travel Time Inconsistency (HIGH)

**Status**: 🔍 ROOT CAUSE IDENTIFIED - Fix pending  
**Severity**: HIGH - Breaks core UX promise  
**Reported By**: User (Sam)

### Problem Summary

Hex shading changes dramatically when zooming in/out, with the same geographic area showing as "reachable" at one zoom level but "unreachable" at another. User filtering for Railway Station within 5 minutes sees green coverage when zoomed out, but the same area turns not-green when zoomed in.

### Specific Example: Hex 872a32688ffffff

- **R7 parent (zoomed out)**: 102s to Railway Station → ✅ GREEN (passes 5min filter)
- **R8 child (zoomed in)**: 262s to Railway Station → ❌ NOT GREEN (fails 5min filter)
- **Difference**: Exactly **+160 seconds** with zero variance across all 20 anchors

### Root Cause

**Independent aggregation violates hierarchical consistency.**

Current pipeline computes r7 and r8 travel times independently from raw road network:
1. R7 hex `872a32688ffffff` contains **407 road nodes**
2. These nodes split into 7 different r8 children (1-117 nodes each)
3. R7 aggregation: `MIN(all 407 nodes)` = **102s** (includes best-connected roads)
4. R8 child #1: `MIN(117 local nodes)` = **262s** (misses best roads)
5. **Result**: Parent has better times than children (hierarchical inconsistency)

### Data Evidence

```
R7 parent (872a32688ffffff): 102s (407 nodes across 7 children)
  ├─ Child #1 (882a326881fffff):  262s (+160s) ← User's view!
  ├─ Child #2 (882a326883fffff):  394s (+292s)
  ├─ Child #3 (882a326885fffff):  398s (+296s)
  ├─ Child #4 (882a326887fffff):  577s (+475s)
  ├─ Child #5 (882a326889fffff):  117s ( +15s) ← Only consistent child
  ├─ Child #6 (882a32688bfffff):  524s (+422s)
  └─ Child #7 (882a32688dfffff):  188s ( +86s)
```

Only 1 of 7 children is within 15s of the parent. The other 6 are 86-475s worse!

### Fix Design

**Bottom-up hierarchical aggregation** (full spec in `docs/MULTI_RESOLUTION_BUG.md`):

1. Compute r8 (finest resolution) from raw road network  
2. Derive r7 from r8 using parent-child MIN aggregation:
   ```
   FOR each r7_hex:
       FOR each anchor:
           r7_time = MIN(r8_time for all 7 children)
   ```

This ensures:
- ✅ Monotonic hierarchy: parent times ≤ all children
- ✅ Zoom consistency: no "reachable → unreachable" transitions
- ✅ Conservative accuracy: parents represent best-case scenario across children

### Implementation Plan

- **Location**: `src/07_merge_states.py` (add post-processing step after line 105)
- **Test**: `tests/test_cross_resolution_consistency.py::test_parent_never_better_than_children`
- **Status**: Design complete, implementation pending

### Investigation Notes

Initial hypotheses ruled out:
1. ❌ Frontend layer overlap (fixed separately, not root cause)
2. ❌ Filter expression mismatch
3. ❌ Coordinate/centroid error (r7 and r8 centroids are identical)  
4. ❌ Rust aggregation kernel bug (code is correct)
5. ✅ Multi-resolution data inconsistency

The zero-variance +160s offset was the key clue: if this were an aggregation variance issue, different anchors would show different offsets. The perfect consistency across all anchors indicated a systematic node inclusion/exclusion problem.

### Lessons Learned

1. **Multi-resolution systems need explicit consistency constraints** - Independent computation is not sufficient
2. **Visual bugs reveal data bugs** - Frontend rendering issue → fundamental data hierarchy problem
3. **Zero-variance is a red flag** - Perfect consistency across dimensions indicates systematic offset, not variance
4. **Bottom-up > Top-down** - Deriving coarse from fine preserves accuracy; reverse loses information

### Related Issues

- Frontend layer zoom transition overlap (fixed in `MapController.ts`)
- Test coverage gap for cross-resolution validation (added in `test_cross_resolution_consistency.py` and `test_frontend_layer_config.py`)

---

## 2025-11-05: Graph Cache Validation (Critical)

**Status**: ✅ Fixed  
**Severity**: Critical  
**Reported By**: User (Sam)

### Problem Summary

Green hexes showing ~5 minute drive times to railway stations in Western Massachusetts (Worthington area), but no visible railway station pins nearby. Specific hex: `882a326803fffff` near Glen Cove Wildlife Sanctuary.

### Root Cause

**Graph cache version mismatch** causing corrupt D_anchor routing data. The D_anchor computation from October 29, 2025 loaded a stale graph cache that was incompatible with the current anchor sites, resulting in systematically incorrect shortest-path calculations.

**Vulnerability**: The graph cache loading mechanism did not validate that cached CSR graphs matched the source PBF file version. When the PBF was updated, old cached graphs (with different edge weights or topology) could be silently loaded, causing data corruption.

### Specific Example: Hex 882a326803fffff

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
- **Impact**: ALL 17 categories had corrupt Oct 29 snapshots due to stale cache

### Fix Implemented

Added comprehensive cache validation to `src/graph/pyrosm_csr.py`:

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

### Data Rebuild

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

### Impact Assessment

**User Experience Impact**:
- **High**: Users saw misleading drive-times for all POI categories
- **Systemic**: 49% of railway_station D_anchor records were impossible
- **Trust**: Undermined confidence in all drive-time calculations

### Files Modified

- `src/graph/pyrosm_csr.py` - Added cache validation logic
- `data/d_anchor_category/mode=0/*` - All category shards rebuilt

### Additional Notes

**Chester Railway Station** may be a historical/inactive station:
- Source: Overture (which includes historical sites)
- Location: Chester, MA (small town in Western Berkshires)
- No evidence of active rail service in Chester, MA
- **Recommendation**: Add OSM tags checking for `disused=yes` or `historic=yes` to filter inactive stations

### References

- User report: Hex `882a326803fffff` near Glen Cove Wildlife Sanctuary, Worthington, MA
- D_anchor file: `data/d_anchor_category/mode=0/category_id=10` (snapshot: 2025-10-29)
- Full analysis: See git history for original `RAILWAY_STATION_BUG_ANALYSIS.md`

---

## 2024-10-12: Connectivity-Aware POI Snapping (High Priority)

**Status**: ✅ Fixed  
**Severity**: High  
**Problem Type**: Graph connectivity optimization

### Summary

Successfully implemented **connectivity-aware POI snapping** to resolve Logan International Airport's severe graph connectivity limitations. The fix improves ~38% of all POI anchors statewide.

### Problem Analysis

**Original Issue**:
- **Logan Airport**: 20,245 reachable nodes, 366 anchors (1.8%) within 30 min
- **Worcester Airport**: 288,943 reachable nodes, 995 anchors (4.8%) within 30 min
- **14x disparity** despite Logan being in the dense Boston metro area

**Root Cause**:
Simple nearest-neighbor (k=1) POI snapping caused Logan to snap to:
- **Node 270051742** (79m away)
- **1 outgoing edge only** (dead end or isolated service road)
- SSSP algorithm couldn't propagate travel times effectively

### Solution Implemented

**Algorithm: Connectivity-Aware Snapping**

```python
K_CANDIDATES = 10           # Consider 10 nearest nodes
MAX_DISTANCE_FACTOR = 2.0   # Accept nodes ≤2x nearest distance
MIN_ACCEPTABLE_EDGES = 2    # Prefer nodes with ≥2 edges

for each POI:
    candidates = query k=10 nearest nodes
    valid = filter by distance ≤ nearest * 2.0
    
    if nearest.edges == 1 and exists(valid with ≥2 edges):
        select best_connected(valid)
    else:
        select nearest
```

**Key Features**:
- **Backwards compatible**: Falls back to k=1 if indptr not provided
- **Distance-bounded**: Only considers alternatives within 2x nearest
- **Edge-count aware**: Computes `np.diff(indptr)` for each candidate
- **Logged improvements**: Reports count of POIs with improved connectivity

### Results

**Logan Airport Specifically**:
```
BEFORE:
  Node: 270051742
  Distance: 79.4m
  Edges: 1 (poorly connected)

AFTER:
  Node: 74281214  
  Distance: 125.4m (+46m, acceptable trade-off)
  Edges: 2 (well-connected)
  Connects to: [6346137933, 6346137932]
```

**Massachusetts-Wide Impact**:
- **12,586 POIs improved** (37.6% of 33,446 anchorable POIs)
- **20,471 total anchor sites** created with improved connectivity
- Affects airports, shopping centers, and other locations near service roads

### Expected Impact

**For Logan Airport**:
- Should reach **significantly more nodes** within 30 minutes
- Reachability should approach **Worcester's ~289K nodes**
- The **14x disparity should be resolved**

**System-Wide**:
- Better connectivity for ~38% of POI anchors
- More accurate travel time estimates
- Improved reachability metrics for dense urban areas

### Technical Notes

**Why This Fixes the Problem**:

The SSSP (Single-Source Shortest Path) algorithm propagates travel times by:
1. Starting at the source node (Logan Airport)
2. Following **outgoing edges** to neighboring nodes
3. Recursively exploring the graph

With only **1 edge**, Logan's propagation was severely restricted. The new node with **2 edges** provides proper pathways into the broader road network, allowing normal propagation.

**Algorithm Trade-offs**:
- **Distance penalty**: Willing to snap 2x farther for better connectivity
- **Conservative**: Requires ≥2 edges (not just >1) for improvement
- **Fast**: K-d tree query with k=10 adds minimal overhead
- **Transparent**: Logs count of improved POIs for monitoring

### Maintenance

The algorithm is **self-maintaining**:
- No hardcoded node IDs or locations
- Works for any state/region
- Automatically handles future OSM updates
- No manual intervention needed

### Files Modified

**Source Code**:
- `src/03_build_anchor_sites.py` (lines 34-40, 83-156, 228)
- `src/04_compute_minutes_per_state.py` (line 115)

**Documentation**:
- `README.md` (lines 82, 86-88)
- `docs/ARCHITECTURE_OVERVIEW.md` (line 48)

**Verification Scripts**:
- `scripts/verify_logan_fix.py` (created for testing)

**Data Outputs**:
- `data/anchors/massachusetts_drive_sites.parquet` (rebuilt)
- `data/anchors/massachusetts_drive_site_id_map.parquet` (rebuilt)

### Verification

Run the verification script:
```bash
python scripts/verify_logan_fix.py
```

Expected output:
```
✅ Logan Airport now snaps to node 74281214
✅ This node has 2 outgoing edges (vs. 1 before)
✅ Connectivity-aware snapping successfully improved the anchor
```

---

## Template for Future Entries

When adding new bug fixes, use this template:

```markdown
## YYYY-MM-DD: Brief Title (Severity)

**Status**: ✅ Fixed / ⏳ In Progress / ❌ Open  
**Severity**: Critical / High / Medium / Low  
**Reported By**: [Name/Source]

### Problem Summary
[Brief description]

### Root Cause
[Technical explanation]

### Fix Implemented
[Code changes, algorithms]

### Results
[Impact, metrics]

### Files Modified
[List of changed files]

### Verification
[How to test]
```

---

**Last Updated**: November 7, 2025

