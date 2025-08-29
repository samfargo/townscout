# TownScout Architecture

## Overview

TownScout is an interactive map that answers "Where should I live given my criteria?" using a novel matrix factorization approach for geospatial queries. The system precomputes travel networks once and stores them in a compact, factorized form for real-time client-side filtering.

## Core Concept: Matrix Factorization

```
Total Travel Time = T_hex[hex→anchor] + D_anchor[anchor→category]
```

Instead of precomputing every hex→category combination (massive storage), we factor the problem:

1. **T_hex**: Each hex knows travel times to its K nearest "anchor" points
2. **D_anchor**: Each anchor knows travel times to category locations (Chipotle, Costco, etc.)
3. **Frontend**: Combines both datasets dynamically using GPU-accelerated expressions

## Data Flow Architecture

```
OSM Data → Anchors → T_hex Computation → PMTiles → Frontend
                  ↘ D_anchor Computation → API     ↗
```

### 1. Data Sources
- **OSM PBF Files**: Road networks parsed with Pyrosm
- **POI Data**: Brand locations extracted from OSM (name/brand/operator filters)

### 2. Anchor System
- **Purpose**: Spatial sampling points that reduce computation complexity
- **Generation**: K-means clustering of POI locations + grid sampling
- **Count**: ~23K anchors for Massachusetts (drive mode)

### 3. T_hex Computation (`scripts/precompute_t_hex.py`)
**Input**: OSM graph + anchor locations  
**Algorithm**: Multi-source Dijkstra from all anchors to H3 hexagons  
**Output**: For each hex, store travel times to K=4 nearest anchors

```python
# T_hex row format
{
  "h3_id": "882a306043fffff",
  "k": 2,
  "a0_id": 4585, "a0_s": 0,      # Anchor 0: ID 4585, 0 seconds away
  "a1_id": 2066, "a1_s": 30,     # Anchor 1: ID 2066, 30 seconds away
  "a0_flags": 0, "a1_flags": 0   # Metadata flags
}
```

**Memory Optimizations**:
- K-pass single-label Dijkstra (instead of multi-label)
- Batched processing (default 500 anchors per batch)
- Float32 edge weights, uint16 output times
- ZSTD compression for parquet files

### 4. D_anchor Computation (`scripts/precompute_d_anchor.py`)
**Input**: Anchor locations + POI category locations  
**Algorithm**: Single-source Dijkstra from each POI to all anchors  
**Output**: For each anchor, store travel time to nearest POI in each category

```python
# D_anchor API format  
{
  "4585": 101,    # Anchor 4585 is 101 seconds from nearest Chipotle
  "2066": 65535,  # Anchor 2066 is unreachable (sentinel value)
  "1509": 326     # Anchor 1509 is 326 seconds from nearest Chipotle
}
```

### 5. Tile Generation
**GeoJSON Conversion** (`scripts/05_h3_to_geojson.py`):
- Converts T_hex parquet → NDJSON GeoJSON with H3 polygon geometries
- Handles H3 v3/v4 API compatibility
- Preserves uint64 H3 IDs using `.itertuples()` instead of `.iterrows()`

**PMTiles Build** (`scripts/06_build_tiles.py`):
- Uses tippecanoe to convert GeoJSON → MBTiles → PMTiles
- Multi-resolution: r7 (zoom < 8) and r8 (zoom ≥ 8)
- Layer names must match frontend source IDs

## Frontend Architecture

### Static File Serving
```python
# FastAPI configuration
app.mount("/static", StaticFiles(directory="tiles/web"), name="static")  # UI assets
app.mount("/tiles", StaticFiles(directory="tiles"), name="tiles")        # PMTiles
```

### PMTiles Integration
```javascript
// Local PMTiles library (no CDN dependency)
let protocol = new pmtiles.Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

// Multi-resolution tile sources
const T_HEX_R7_URL = "pmtiles:///tiles/t_hex_r7_drive.pmtiles";
const T_HEX_R8_URL = "pmtiles:///tiles/t_hex_r8_drive.pmtiles";
```

### Filter Expression Logic

The core filtering happens entirely on the GPU using MapLibre expressions:

```javascript
function buildFilterExpression(criteria, dAnchorData) {
  const UNREACHABLE = 65535;
  const expressions = [];

  for (const [category, thresholdSecs] of Object.entries(criteria)) {
    const categoryData = dAnchorData[category];
    
    // Calculate minimum travel time across all anchors
    const travelTimeOptions = [];
    for (let i = 0; i < K_ANCHORS; i++) {
      travelTimeOptions.push([
        "+",
        ["coalesce", ["get", `a${i}_s`], UNREACHABLE],              // hex → anchor
        ["coalesce", 
          ["get", ["to-string", ["get", `a${i}_id`]], ["literal", categoryData]], 
          UNREACHABLE
        ]                                                           // anchor → category
      ]);
    }
    
    const minTravelTime = ["min", ...travelTimeOptions];
    expressions.push(["<=", minTravelTime, thresholdSecs]);
  }
  
  // Show hex if ALL criteria are met
  return ["case", ["all", ...expressions], 0.8, 0.0];
}
```

### Performance Optimizations

**Client-Side Filtering**: No server round-trips after initial data load  
**GPU Acceleration**: Filter expressions execute on graphics card  
**Multi-Resolution**: Automatic r7/r8 switching based on zoom level  
**Debounced Updates**: 250ms delay to avoid excessive filtering  
**Cached API Data**: D_anchor data fetched once per category, reused for all slider changes

## Data Contracts

### T_hex Tiles Must Provide
- Properties: `k`, `a{i}_id`, `a{i}_s` for i = 0 to k-1
- Layer names matching PMTiles source IDs  
- H3 cell boundaries as polygon geometry
- Consistent anchor IDs across all hexes

### D_anchor API Must Return
- JSON object: `{anchor_id: seconds, ...}`
- Anchor IDs matching T_hex tile `a{i}_id` values
- Sentinel value `65535` for unreachable locations
- All anchor IDs that appear in T_hex tiles

### Frontend Assumptions
- `K_ANCHORS = 4` (configurable, must match T_hex computation)
- Categories: extendable (currently `chipotle`, `costco`)
- Mode: `drive` (walk mode uses separate tiles)
- Time units: seconds (UI converts from minutes)

## Extension Points

### Adding New Categories
1. Extract POI data for new category from OSM
2. Run D_anchor computation: `make d-anchor`
3. Add slider control to frontend HTML
4. Update `getSliderValues()` function
5. No changes needed to filter logic (automatic)

### Adding New Geographic Areas
1. Download OSM PBF for new state/region
2. Update `Makefile` targets for new area
3. Run full pipeline: `make all`
4. Deploy new PMTiles and update frontend tile sources

### Adding Walk Mode
1. Compute separate T_hex with walk routing
2. Build walk-mode PMTiles (`t_hex_r8_walk.pmtiles`)
3. Add mode toggle to frontend UI
4. Switch tile sources based on mode selection
5. Update D_anchor API calls with `mode=walk`

### Performance Scaling
- **Memory**: Use K-pass mode for large datasets
- **Computation**: Parallel processing across states
- **Storage**: ZSTD compression for all parquet files
- **Serving**: CDN deployment for PMTiles

## Technical Decisions

### Why Anchors Instead of Direct Hex→Category?
- **Storage**: O(hexes × anchors) vs O(hexes × categories × POIs)
- **Flexibility**: Adding categories doesn't require T_hex recomputation
- **Performance**: Smaller tile sizes, faster downloads

### Why Client-Side Filtering?
- **Latency**: No server round-trips for slider changes
- **Scalability**: CDN can serve static tiles globally
- **Cost**: Computation pushed to user's GPU, not server CPU

### Why PMTiles Over Traditional Tiles?
- **Single File**: Simpler deployment than tile pyramid
- **Range Requests**: Efficient partial downloads
- **No Server**: Works with static hosting/CDN

### Why MapLibre Over Other Map Libraries?
- **Vector Tiles**: Native support for client-side styling
- **GPU Acceleration**: Filter expressions run on graphics card
- **Expression Language**: Powerful enough for complex travel time calculations

## Performance Targets

- **Initial Load**: < 2 seconds including tiles and API data
- **Slider Response**: < 250ms from input to visual update  
- **Tile Rendering**: < 100ms for zoom/pan operations
- **Memory Usage**: < 200MB browser heap for full Massachusetts
- **File Sizes**: < 400MB total PMTiles for 2-category nationwide dataset

## Known Limitations

- **Real-time Traffic**: Uses free-flow speeds, no live traffic data
- **Mode Mixing**: No walking to transit station, then driving
- **POI Updates**: Requires pipeline rerun when new locations open
- **Precision**: Travel times rounded to nearest second (uint16 limit ~18 hours)

## Future Architecture

The current anchor-matrix approach provides a foundation for advanced features:

- **Friend/Family Proximity**: Treat social connections as additional categories
- **Multi-Modal Routing**: Extend anchors to include transit stations
- **Dynamic POI**: API-driven category updates without tile rebuilds
- **User Preferences**: Personalized anchor weighting based on usage patterns

The factorized architecture scales to nationwide deployment while maintaining sub-second interactive response times.
