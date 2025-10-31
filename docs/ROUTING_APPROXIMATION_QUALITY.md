# Routing Approximation Quality & Triangle Inequality Issue

## Problem Statement

vicinity's anchor-based routing system uses the approximation:

```
Total Travel Time ≈ T_hex[hex→anchor] + D_anchor[anchor→POI]
```

This **violates triangle inequality** when the optimal hex→POI path doesn't go through the selected anchor. The error can be substantial in:

- **Suburban/rural networks** with limited connectivity
- **Mixed road hierarchies** where anchors are on slow local roads but POIs are accessible via fast highways  
- **Directional routing** where optimal paths vary significantly by destination

## Current Mitigation

The system uses **K=20 anchors per hex** (configurable via `K_BEST` in Makefile), providing multiple routing options. However:

✅ **Partially effective** in dense urban grids where multiple anchors lie on optimal paths  
❌ **Insufficient** in sparse networks where even K=20 anchors may not cover optimal routing paths  
❌ **No error measurement** or confidence bounds provided to users

## Immediate Solutions

### 1. Increase K Anchors (Quick Fix)

For suburban/rural areas, increase anchor count:

```bash
# Default: K_BEST=20
# For better coverage in sparse networks:
make minutes K_BEST=40

# For comprehensive coverage (higher compute cost):
make minutes K_BEST=60
```

**Trade-offs:**
- ✅ Better path coverage, especially in sparse networks
- ❌ Larger tile sizes (more anchor data per hex)
- ❌ Higher compute cost during T_hex generation

### 2. Validate Current Error (Essential)

Run the validation framework to understand error magnitude:

```bash
python scripts/validate_triangle_approximation.py --state massachusetts --sample-size 1000
```

This measures the distribution of approximation errors across different network topologies.

### 3. Network-Aware Configuration

Use different K values based on network density:

```bash
# Urban areas (dense road networks)
make minutes K_BEST=20

# Suburban areas (medium density)  
make minutes K_BEST=35

# Rural areas (sparse networks)
make minutes K_BEST=50
```

## Medium-Term Solutions

### Enhanced Anchor Selection

Instead of selecting the K **nearest** anchors, select anchors that maximize **routing diversity**:

1. **Directional coverage**: Ensure anchors in all cardinal directions
2. **Road hierarchy diversity**: Include anchors on local roads, arterials, and highways
3. **Network topology awareness**: Prioritize anchors that serve as routing hubs

### Confidence Scoring

Compute per-hex confidence scores based on:

```python
confidence_score = f(
    anchor_spatial_dispersion,    # How spread out are the K anchors?
    road_network_density,         # Dense urban vs sparse rural?
    anchor_time_variance,         # Do all anchors give similar POI times?
    network_connectivity_index    # How well-connected is this area?
)
```

### Hybrid Routing

For **low-confidence predictions**:
- Compute exact shortest paths on-demand
- Cache frequently-requested routes
- Use approximation only for high-confidence cases

## Long-Term Solutions

### Approximation Quality API

Extend API responses to include error bounds:

```json
{
  "travel_time_minutes": 23,
  "confidence": 0.85,
  "range_minutes": [19, 28],
  "approximation_method": "k_anchor",
  "network_type": "suburban"
}
```

### UI Changes

Display uncertainty in the interface:
- Travel time **ranges** instead of point estimates
- Visual confidence indicators on the map
- Clear disclaimers about approximation quality

### Advanced Anchor Strategies

1. **Contraction Hierarchy Integration**: Use CH shortcuts as "super-anchors" for long-distance routing
2. **Dynamic Anchor Selection**: Choose anchors based on query destination patterns
3. **Regional Error Models**: Build error correction models based on local network topology

## Implementation Priorities

### Phase 1: Immediate (Low effort, high impact)
- [x] Document the limitation in architecture docs
- [x] Create validation framework
- [ ] Test higher K values (K_BEST=40-60) in sparse regions
- [ ] Measure error distribution on real data

### Phase 2: Medium-term (Moderate effort)
- [ ] Implement confidence scoring
- [ ] Add error bounds to API responses
- [ ] Create network topology-aware anchor selection

### Phase 3: Long-term (High effort, fundamental improvements)
- [ ] Hybrid exact/approximate routing
- [ ] Advanced anchor selection algorithms
- [ ] Real-time error measurement and correction

## Testing & Validation

### Error Metrics to Track

1. **Absolute Error**: `|approximation - ground_truth|` in minutes
2. **Relative Error**: `absolute_error / ground_truth` as percentage
3. **P95 Error**: 95th percentile of error distribution
4. **Large Error Rate**: Percentage of cases with >5min or >50% error

### Validation Scenarios

Test in different network types:
- **Dense urban**: Manhattan, downtown Boston
- **Suburban**: Residential areas with highway access
- **Rural**: Areas with limited road connectivity
- **Mixed**: Regions spanning multiple network types

### Acceptance Criteria

For production deployment:
- **P95 absolute error < 5 minutes** for 90% of hex-POI pairs
- **P95 relative error < 30%** for routes under 30 minutes
- **Large error rate < 5%** across all network types
- **User communication** about approximation quality

## Example Error Cases

### Case 1: Suburban Mall Access
- **Hex**: Residential neighborhood, connected via local roads
- **Anchor**: Shopping center, 8 minutes via residential streets
- **POI**: Hospital near highway interchange, 12 minutes direct via highway
- **Approximation**: 8 + 4 = 12 minutes ✅ (happens to be correct)
- **Reality**: 12 minutes direct ✅

### Case 2: Rural Highway Access  
- **Hex**: Rural home, connected via county road
- **Anchor**: Small town center, 15 minutes via slow county road
- **POI**: Regional hospital, accessible via nearby highway on-ramp
- **Approximation**: 15 + 8 = 23 minutes 
- **Reality**: 12 minutes direct via highway ❌ **92% error**

This is exactly the type of error your critique identified - the approximation can be dramatically wrong when optimal paths don't share routing infrastructure.
