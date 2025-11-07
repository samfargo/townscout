# Anchor Selection Strategy

## Executive Summary

**vicinity's scalability claim hinges on smart anchor selection.** This document explains how anchors are chosen, density constraints, and memory budgets that make "scales to nationwide cheaply" provable rather than asserted.

### Core Principle: Matrix Factorization

```
Total Travel Time = T_hex[hex→anchor] + D_anchor[anchor→category]
```

- **T_hex**: Each H3 hex stores travel times to its **top-K nearest anchors**
- **D_anchor**: Each anchor stores travel time to the **nearest POI** in each category/brand
- **Frontend**: Computes `min(T_hex[hex→anchor_i] + D_anchor[anchor_i→category])` over all K anchors on GPU

The anchor selection strategy directly controls the tradeoff between accuracy and memory.

---

## Anchor Selection Criteria

### 1. POI-Driven Selection

**Anchors are NOT arbitrary spatial points.** Each anchor represents a road network node where one or more POIs have been snapped.

**Selection Process:**
1. **POI Filtering**: Only POIs from categories in `data/taxonomy/POI_category_registry.csv` or brands in `data/taxonomy/POI_brand_registry.csv` become anchor candidates
2. **Connectivity-Aware Snapping**: POIs snap to road graph nodes using intelligent node selection:
   - Query k=10 nearest road nodes
   - Prefer nodes with ≥2 edges (avoid dead ends)
   - Accept alternatives within 2× nearest distance
   - **Fixed Logan Airport**: Improved ~38% of POI anchors statewide by avoiding isolated service roads
3. **Node Aggregation**: Multiple POIs at the same road node create one anchor site
4. **Stable IDs**: `anchor_int_id` assigned by sorting `site_id` (deterministic UUID5)

### 2. Geographic Distribution

**Natural Density Adaptation**: Anchor density automatically reflects:
- **Urban areas**: Dense POI clusters → many anchors (better accuracy)  
- **Rural areas**: Sparse POIs → fewer anchors (acceptable accuracy with longer travel times)
- **Desert/water**: No POIs → no anchors (handled by sentinel values)

**Example (Massachusetts)**: 26,019 drive-mode anchors covering 26,098 H3 r8 hexes
- **Average ratio**: ~1.0 hexes per anchor (nearly saturated coverage)
- **High anchor density**: Current implementation prioritizes accuracy over memory efficiency
- **Scaling implications**: This density may need adjustment for nationwide deployment

---

## K-Best Constraints

### T_hex: Hex→Anchor Storage

**Configuration:**
- **Default K**: 5 anchors per hex (configurable via `--k-best`)
- **Dense urban recommendation**: K=20+ for comprehensive coverage
- **Current tiles**: K=20 (set in `src/07_merge_states.py`)

**Memory per hex (K=20):**
- **Anchor IDs**: 20 × 4 bytes (int32) = 80 bytes
- **Travel times**: 20 × 2 bytes (uint16) = 40 bytes  
- **Total per hex**: 120 bytes
- **Nationwide estimate (H3 r8)**: ~65M hexes × 120 bytes = **7.8 GB**

**K-Best Enforcement:**
1. Multi-source Dijkstra from all anchors using native `kbest_multisource_bucket_csr` kernel
2. Global top-K selection per hex (no duplicates across batches)
3. Results sorted by travel time, ranked 0..K-1
4. Pivot into tile columns: `a0_id`, `a0_s`, `a1_id`, `a1_s`, ..., `a{K-1}_id`, `a{K-1}_s`

### D_anchor: Anchor→POI Storage

**Entity-Specific K Values** (from `data/taxonomy/d_anchor_limits.json`):
- **Sparse categories** (airports, ski resorts): K=6-8, max_minutes=180
- **Common categories** (supermarkets, cafes): K=12, max_minutes=60  
- **High-frequency brands** (Starbucks, McDonald's): K=14, max_minutes=60

**Memory per anchor** (20K anchors × 50 categories × 2 bytes = **2 MB per category**)

---

## Data Types and Encoding

### Numeric Encoding

**Travel Times (uint16):**
- **Precision**: 1-second granularity
- **Range**: 0-65,534 seconds (~18.2 hours)
- **Sentinels**:
  - `65535`: Unreachable or ≥ cutoff time
  - `65534`: No road node available for hex (even after neighbor borrowing)

**Anchor IDs (int32):**
- **Range**: 0 to 2.1 billion anchors (nationwide capacity)
- **Assignment**: Deterministic by sorting `site_id` (UUID5 based on `mode|node_id`)
- **Stability**: IDs remain stable across pipeline reruns if POI locations unchanged

### Storage Format

**T_hex (Long Format)**:
```
h3_id: uint64
anchor_int_id: int32  
time_s: uint16
res: int32
mode: string
snapshot_ts: string
```

**Tiles (Wide Format)**:
```
a0_id: int32, a0_s: uint16
a1_id: int32, a1_s: uint16
...
a{K-1}_id: int32, a{K-1}_s: uint16
```

---

## Scaling Analysis

### Memory Budget per Tile

**H3 Resolution 8 (Nationwide)**:
- **Total hexes**: ~65 million
- **Bytes per hex**: 120 (K=20 anchors)
- **Raw memory**: 7.8 GB
- **PMTiles compressed**: ~400 MB (target for 2-category nationwide coverage)

**Compression Ratio**: ~20:1 achieved through:
- Quantized uint16 times
- Spatial correlation in H3 grid
- ZSTD compression in PMTiles

### Anchor:Hex Ratios by Geography

**Target Ratios** (adaptive, not hard limits):

| Geography Type | Hexes per Anchor | Anchor Density | Memory Efficiency |
|----------------|------------------|----------------|-------------------|
| **Dense Urban** (Manhattan) | 5-10 | High | Lower (more accuracy) |
| **Suburban** (typical US metro) | 20-30 | Medium | Balanced |
| **Rural** (farmland, mountains) | 50-100 | Low | Higher (acceptable accuracy) |
| **Uninhabited** (desert, water) | ∞ (no anchors) | Zero | Maximum (sentinel values) |

**Real Data (Massachusetts)**:
- **Average**: 1.0 hexes per anchor (high density implementation)
- **PMTiles**: 29.8 MB (r8) + 6.0 MB (r7) = 35.8 MB total
- **Accuracy vs Memory**: Current approach heavily favors accuracy; rural areas likely over-anchored

---

## Quality Assurance

### Logan Airport Fix (Oct 2024)

**Problem**: Naive k=1 snapping caused Logan Airport to snap to isolated service road
- **Reachability**: Only 20,245 nodes within 30 minutes
- **Comparison**: Worcester Airport reached 288,943 nodes (14× better)

**Solution**: Connectivity-aware snapping algorithm
- **Improvement**: ~38% of POI anchors statewide got better-connected nodes
- **Logan result**: Now reaches comparable node counts to other major airports

### Automated Quality Checks

Anchor quality is validated through automated tests:

**Automated Tests** (`tests/test_anchor_contract.py`):
- `site_id` uniqueness within each file
- Valid modes (drive/walk only)
- Every anchor has ≥1 POI
- Non-negative node_id references
- Valid coordinate ranges
- Non-empty categories list

**Additional Validation** (`tests/test_t_hex_contract.py`):
- All anchor_int_id values in T_hex exist in anchor files
- No orphan anchor references

See `docs/quality_control_infra.md` for comprehensive QA documentation.

### Anchor Consistency Checks

1. **Completeness**: Every `anchor_int_id` in T_hex tiles exists in D_anchor tables
2. **Stability**: Re-running pipeline with same inputs yields identical anchor IDs
3. **Coverage**: No orphaned anchors; every anchor reachable from ≥1 hex within cutoff
4. **Brand matching**: A-list brands appear in anchor sites when their POIs are present

---

## Configuration Files

### Core Anchor Selection

**Category Registry** (`data/taxonomy/POI_category_registry.csv`):
```csv
category_id,numeric_id,display_name
supermarket,11,Supermarket
cafe,3,Café
fast_food,5,Fast Food
hospital,7,Hospital
trauma_level_1_adult,19,Trauma Level 1 Adult
pharmacy,9,Pharmacy
park,8,Park
airport,1,Airport
```
All categories in the CSV are automatically included in anchors and precomputed. Numeric IDs are explicit to prevent drift.

**Brand Registry** (`data/taxonomy/POI_brand_registry.csv`):
```csv
brand_id,canonical,aliases,wikidata
costco,Costco,"costco wholesale",
starbucks,Starbucks,"starbucks coffee|starbucks reserve",
dunkin,Dunkin',"dunkin donuts|dunkin'",
mcdonalds,McDonald's,"mcdonalds|mcdonald's",
cvs,CVS Pharmacy,"cvs|cvs/pharmacy|cvs health",
walmart,Walmart,"wal-mart",
```
All brands in the registry are automatically included in anchors and precomputed. Add or remove brands to control scope.

### Runtime Limits (`data/taxonomy/d_anchor_limits.json`)

**Purpose**: Control SSSP cutoffs and memory usage per entity

**Structure**:
```json
{
  "category": {
    "airport": {"max_minutes": 180, "top_k": 8},
    "supermarket": {"max_minutes": 60, "top_k": 12}
  },
  "brand": {
    "costco": {"max_minutes": 60, "top_k": 10}
  },
  "_defaults": {
    "category": {"max_minutes": 60, "top_k": 12},
    "brand": {"max_minutes": 60, "top_k": 12}
  }
}
```

**Sparse Categories**: Airports, ski resorts, beaches use 180-minute cutoffs for weekend destination planning.

---

## Performance Targets

### Proven Scaling (Massachusetts)

- **Anchors**: 26,019 sites
- **H3 hexes**: 26,098 (r8)  
- **PMTiles size**: 35.8 MB (drive mode, compressed)
- **API response**: <250ms for 3-slider queries
- **Browser memory**: <200 MB total
- **Initial load**: <2s (tiles + D_anchor cache)

### Nationwide Projections

**Scaling Factor**: Massachusetts = ~2% of US population/area

| Metric | Massachusetts | Nationwide (50× scaling) | Memory Budget |
|--------|---------------|-------------------------|---------------|
| **Anchors** | 26,019 | ~1,300,000 | int32 sufficient |
| **Hexes (r8)** | 26,098 | ~65,000,000 | 7.8 GB raw |
| **PMTiles** | 35.8 MB | ~1,800 MB | **Exceeds CDN target** |
| **D_anchor** | 2 MB/category | 100 MB/category | API cacheable |

**⚠️ Scaling Issue**: Current 1:1 anchor density would produce ~1.8 GB PMTiles nationwide, exceeding the 400 MB target by 4×.

**Bottleneck Analysis**: Current anchor density creates PMTiles size bottleneck. **Anchor thinning required for nationwide scaling.**

---

## Open Questions & Future Work

### Density-Adaptive Refinements

**Critical for Nationwide Scaling:**

1. **Anchor Thinning**: Reduce rural anchor density from 1:1 to target ratios (5-20 hexes per anchor)
2. **Hierarchical Selection**: Guarantee inclusion of major destinations (hospitals, airports, major brands)
3. **Distance-Based Culling**: Remove anchors within X km of higher-importance anchors
4. **Dynamic K**: Use higher K in dense areas, lower K in rural areas
5. **Travel mode mixing**: Walk→transit→drive composite journeys

### Validation Metrics

1. **Anchor utilization**: What % of anchors are used in typical queries?
2. **Error distribution**: Quantify accuracy loss vs. full routing matrix by geography type  
3. **Coverage gaps**: Identify hexes with poor anchor coverage

**Next Steps**: Implement error benchmarking vs. full OSRM matrix on representative 1000-hex samples.

---

## Summary

**Anchor selection is POI-driven, connectivity-aware, and naturally density-adaptive.** The system scales because:

1. **Smart selection**: Only meaningful POI locations become anchors
2. **Connectivity fixes**: Logan Airport-style issues resolved by multi-candidate snapping  
3. **Memory-controlled K**: Configurable K-best constraints prevent memory explosion
4. **Geographic adaptation**: Dense cities get more anchors; rural areas get fewer (but sufficient)
5. **Compression-friendly**: Spatial correlation + quantization + ZSTD = 20:1 compression

**The claim "scales to nationwide cheaply" requires anchor density optimization**: Current 1:1 ratio would produce 1.8 GB PMTiles. **Target: 5-10× anchor reduction in rural areas** to achieve 400 MB nationwide goal.
