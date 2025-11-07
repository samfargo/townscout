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
1. During CSR build we call `compute_h3_for_nodes(lats, lons, [7, 8])`.
2. That helper computes each resolution **independently** from the raw coordinates instead of deriving r7 via `h3.cell_to_parent(r8, 7)`.
3. ~7% of nodes near res-7 boundaries therefore get r7 IDs whose canonical r8 parent is a *different* r7 hex.
4. When `aggregate_h3_topk_precached` runs, those boundary nodes contribute to one r7 hex but a different r8 child, so parents inherit travel times their visible children never see.

### Why This Happens
- Example hex `872a32688ffffff` has 407 road nodes; 21 of them map to r8 cells whose true parent is a neighboring r7 (`872a3268cffffff`, etc.).
- The parent’s 102 s number therefore includes nodes that belong to other r7s; its seven real children top out at 117 s or worse.
- This is a hierarchy mismatch, not just “parent has more nodes.” Our parquet analysis shows 27 754 parent/anchor pairs (3 369 unique r7 hexes) with `time_parent < min(time_children)`.

This violates the **hierarchical consistency principle**: A parent hex should NEVER have better travel times than the best of its children.

## Solution

### Proposed Fix: Hierarchical H3 Mapping at the Source

Rather than recomputing r7 from r8 tiles, fix the node→hex mapping so the hierarchy is correct before aggregation:

1. When building `node_h3_by_res` (Rust `compute_h3_for_nodes` or the Python wrapper in `src/graph/pyrosm_csr.py`), compute the **finest** requested res (currently r8) once from lat/lon.
2. Derive coarser resolutions by repeatedly calling `h3.cell_to_parent` on those fine IDs, ensuring every node’s r7 column is the parent of its r8 column.
3. Persist the stacked `[N, R]` matrix and pass it unchanged to `aggregate_h3_topk_precached`.

This keeps the existing aggregation/kernel path unchanged while guaranteeing `time_s(parent) == min(time_s(children))` because every node flows through a consistent parent-child chain. After the fix, regenerate `data/minutes/*.parquet` and rerun `src/07_merge_states.py` to refresh `state_tiles/us_r7.parquet` and `us_r8.parquet`.

### Alternative: Frontend Crossfade (Temporary Mitigation)

Already implemented in `tiles/web/lib/map/MapController.ts`:
- Fade out r7 from zoom 8-9
- Fade in r8 from zoom 8-9
- **BUT**: Doesn't fix the underlying data inconsistency

Users will still see flickering/changing coverage during the transition.

## Testing

- Expand `tests/test_cross_resolution_consistency.py` with a regression that loads the long-format parquet (or melts the tiles back to long), derives parent IDs via a vectorized `cell_to_parent`, uses `groupby(["parent_h3", "anchor_int_id"]).min()` to get child minima, merges that onto r7 rows, and asserts `time_parent <= child_min` for every `(h3_id, anchor)` with data. This fails today and will pass once the mapping fix lands.

## Priority

**HIGH** - This breaks the core user experience and makes the tool unreliable for location decisions.

## Next Steps

1. ✅ Document root cause based on parquet analysis
2. ⬜ Fix `compute_h3_for_nodes` / `src/graph/pyrosm_csr.py` to derive coarse IDs from the finest resolution
3. ⬜ Add regression test enforcing `time_parent <= min(time_children)`
4. ⬜ Regenerate tiles with fixed data
5. ⬜ Verify frontend shows consistent coverage
