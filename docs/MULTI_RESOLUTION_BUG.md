# Multi-Resolution Travel Time Inconsistency

**Status**: Identified root cause, fix pending
**Severity**: High (breaks core UX promise of zoom consistency)
**Date**: 2025-11-07

## Problem

Users see dramatically different hex shading when zooming in/out, making reachability analysis unreliable.

### Example
- **Hex ID**: `872a32688ffffff` (r7) with 7 r8 children
- **Filter**: Railway Station within 5 minutes (300s)
- **Observed**:
  - Zoomed out (r7): GREEN (min time = 102s) ✅ Passes filter
  - Zoomed in (r8 child `882a326881fffff`): NOT GREEN (min time = 262s) ❌ Fails filter
  - **Difference**: +160s systematic offset

### Data Analysis

```
R7 parent (872a32688ffffff): 102s to nearest Railway Station
  └─ Aggregates from 407 road nodes across all children

R8 children (7 total):
  1. 882a326881fffff:  262s  (+160s worse) ← User's view
  2. 882a326883fffff:  394s  (+292s worse)
  3. 882a326885fffff:  398s  (+296s worse)
  4. 882a326887fffff:  577s  (+475s worse)
  5. 882a326889fffff:  117s  ( +15s worse) ← Only consistent child
  6. 882a32688bfffff:  524s  (+422s worse)
  7. 882a32688dfffff:  188s  ( +86s worse)
```

## Root Cause

### Current Approach (Broken)
1. Compute T_hex for r7: Aggregate road nodes → hexes independently
2. Compute T_hex for r8: Aggregate road nodes → hexes independently
3. **Problem**: Larger r7 hex includes MORE nodes (407) than any single r8 child (max 117)
4. **Result**: R7 benefits from nodes that r8 children don't include

### Why This Happens
- R7 hex `872a32688ffffff` contains nodes from a well-connected road segment
- These nodes split across 7 different r8 children when resolution increases
- Child #5 (`882a326889fffff`) gets the good nodes → 117s travel time
- Child #1 (`882a326881fffff`) misses them → 262s travel time
- But r7 parent aggregates ALL nodes → 102s travel time (best of all children)

This violates the **hierarchical consistency principle**: A parent hex should NEVER have better travel times than the best of its children.

## Solution

### Proposed Fix: Bottom-Up Hierarchical Aggregation

Instead of computing r7 and r8 independently:

1. **Compute r8 first** (finest resolution, most accurate)
2. **Derive r7 from r8** using parent-child relationships:
   ```python
   for each r7_hex:
       r7_travel_times = MIN(travel_times of all 7 r8 children)
   ```

This ensures:
- ✅ Parent times ≤ child times (monotonic hierarchy)
- ✅ Zooming in/out shows consistent coverage
- ✅ User never sees "reachable → unreachable" transitions
- ⚠️ Slight accuracy trade-off (r7 might be slightly more conservative)

### Implementation

**File**: `src/04_compute_minutes_per_state.py`

**Current** (lines 185-189):
```python
h3_id_arr, site_id_arr, time_arr, res_arr = aggregate_h3_topk_precached(
    node_h3_by_res, best_anchor_int, time_s, 
    np.array(res_used, dtype=np.int32), K, int(UNREACH_U16), 
    os.cpu_count(), False
)
```

**Proposed Change** (vectorized implementation):

In `07_merge_states.py` line 104-105, replace independent loading with hierarchical derivation:

```python
# Old approach: compute r7 and r8 independently
# all_times = pd.concat([pd.read_parquet(f) for f in drive_time_files])

# New approach: compute r8, derive r7 from it
r8 = pd.concat([pd.read_parquet(f) for f in r8_drive_time_files])  
# columns: h3_id (uint64), anchor_int_id (int32), time_s (uint16), res, mode, snapshot_ts

# Add parent column (vectorized, not per-hex loop)
r8['parent_h3_int'] = r8['h3_id'].apply(
    lambda x: int(h3.cell_to_parent(h3.int_to_str(int(x)), 7), 16)
)

# Aggregate: MIN time per (parent, anchor) across all r8 children
r7 = (r8.groupby(['parent_h3_int', 'anchor_int_id'], as_index=False)
         .agg(time_s=('time_s', 'min')))

# Add metadata columns
r7['res'] = 7
r7['mode'] = 'drive'  # or inherit from r8
r7['snapshot_ts'] = r8['snapshot_ts'].iloc[0]
r7 = r7.rename(columns={'parent_h3_int': 'h3_id'})

# Combine
all_times = pd.concat([r7, r8], ignore_index=True)
```

**Key advantages**:
- ✅ Vectorized (no per-hex loops)
- ✅ Single groupby operation
- ✅ Handles all anchors in one pass
- ✅ Preserves dtype consistency

### Alternative: Frontend Crossfade (Temporary Mitigation)

Already implemented in `tiles/web/lib/map/MapController.ts`:
- Fade out r7 from zoom 8-9
- Fade in r8 from zoom 8-9
- **BUT**: Doesn't fix the underlying data inconsistency

Users will still see flickering/changing coverage during the transition.

## Testing

Add test in `tests/test_cross_resolution_consistency.py`:

```python
def test_parent_never_better_than_children(self):
    """Ensure r7 parent travel times <= MIN of r8 children."""
    r7_df = pd.read_parquet('state_tiles/us_r7.parquet')
    r8_df = pd.read_parquet('state_tiles/us_r8.parquet')
    
    # Sample r7 hexes
    sample_r7 = r7_df.sample(min(100, len(r7_df)))
    
    for _, r7_row in sample_r7.iterrows():
        r7_hex = h3.int_to_str(int(r7_row['h3_id']))
        children = list(h3.cell_to_children(r7_hex, 8))
        child_ints = [int(c, 16) for c in children]
        
        # Get child data
        child_data = r8_df[r8_df['h3_id'].isin(child_ints)]
        
        # For each anchor in r7, check if its time <= MIN of children
        for i in range(20):
            r7_anchor_id = r7_row.get(f'a{i}_id')
            r7_time = r7_row.get(f'a{i}_s')
            
            if pd.notna(r7_anchor_id):
                # Find this anchor in children
                child_times = []
                for child_int in child_ints:
                    child_row = child_data[child_data['h3_id'] == child_int]
                    if len(child_row) > 0:
                        child_row = child_row.iloc[0]
                        for j in range(20):
                            if child_row.get(f'a{j}_id') == r7_anchor_id:
                                child_times.append(child_row.get(f'a{j}_s'))
                
                if child_times:
                    min_child_time = min(child_times)
                    assert r7_time <= min_child_time, (
                        f"R7 {r7_hex} anchor {r7_anchor_id}: "
                        f"parent time ({r7_time}s) > min child time ({min_child_time}s)"
                    )
```

## Priority

**HIGH** - This breaks the core user experience and makes the tool unreliable for location decisions.

## Next Steps

1. ✅ Document root cause
2. ⬜ Implement hierarchical aggregation in `07_merge_states.py`
3. ⬜ Add test for parent-child consistency
4. ⬜ Regenerate tiles with fixed data
5. ⬜ Verify frontend shows consistent coverage

