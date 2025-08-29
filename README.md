🗺️ TownScout — Anchor-Matrix Architecture

TownScout is an interactive, stackable-filter map that answers one deceptively simple question:

“Where should I live given my criteria?”

The user sees a map of the United States. Every time they add a filter —
“≤ 10 min to Costco”, “walkability ≥ 70”, “within 2 hrs of skiing” —
the livable area visibly shrinks in real time.

Not Zillow filters. A compute engine disguised as a magical map.

⸻

🔑 Core Idea

Routing every query on the fly is prohibitively expensive. TownScout avoids it by precomputing travel networks once and storing them in a compact, factorized form.

At runtime, every filter is answered with a single algebraic lookup:

(Hex \to Anchor) \times (Anchor \to Category) = (Hex \to Category)

Two offline truth tables:
	•	T_hex (Hex → Anchors)
For each H3 hex, store its travel time to top-K nearby anchors.
Example row:

h3_id=…, a0_id=123, a0_s=540s, a1_id=456, a1_s=720s …


	•	D_anchor (Anchor → Category)
For each anchor, store its travel time to the nearest POI in a category.
Example row:

anchor_id=123, category_id=Costco, seconds=360


At runtime, the browser computes:

TT(hex, Costco) = \min_k (a_k.s + D[a_k.id, Costco])

Stacking filters is just a boolean AND over conditions — all evaluated client-side on the GPU.

⸻

📂 Repo Structure

.
├── scripts/
│   ├── precompute_t_hex.py    # Build T_hex (Hex→Anchors)
│   ├── precompute_d_anchor.py # Build D_anchor (Anchors→Categories)
│   ├── 05_h3_to_geojson.py    # Convert T_hex to GeoJSON
│   └── 06_build_tiles.py      # Build PMTiles from GeoJSON
│
├── api/
│   └── app/
│       └── main.py            # FastAPI server (frontend + D_anchor API)
│
├── tiles/
│   ├── web/
│   │   ├── index.html         # Map UI with filter panel
│   │   └── pmtiles.js         # PMTiles library (local)
│   ├── t_hex_r7_drive.pmtiles # Low-res tiles (zoom < 8)
│   └── t_hex_r8_drive.pmtiles # High-res tiles (zoom ≥ 8)
│
└── schemas/
    ├── filters.catalog.json   # Filter definitions, IDs, metadata
    └── tiles.manifest.json    # Tile/PMTiles locations per dataset version


⸻

📐 Data Contracts

T_hex.pmtiles
	•	Geometry: H3 hex boundaries (res=8)
	•	Attributes per feature:
	•	h3_id: string
	•	k: uint8 (# of anchor slots used)
	•	Repeated slots i ∈ [0..K-1]:
	•	a{i}_id: uint32 (stable anchor ID)
	•	a{i}_s: uint16 (seconds; 65535=UNREACH, 65534=NODATA)
	•	a{i}_flags: uint8 (bit 0=borrowed, bit 1=pruned, …)

Invariants
	•	a{i}_s ≤ cutoff_s or equals sentinel.
	•	Anchors in strictly increasing order for SIMD-friendly min.

D_anchor.parquet
	•	Columns:
	•	anchor_int_id: int32
	•	seconds: uint16 (65535=UNREACH, 65534=NODATA)
	•	snapshot_ts: string (YYYY-MM-DD format)

Partitioning: mode=<m>/category_id=<c>/part-*.parquet
Note: Also merged into flat files for API: massachusetts_anchor_to_category_{mode}.parquet

Invariants
	•	One row per (anchor_id, category_id, mode)
	•	No duplicates, no nulls

⸻

🏗️ How It Works
	1.	Offline Precompute
	•	precompute_t_hex.py: hex → top-K anchors (with memory-efficient batching)
	•	precompute_d_anchor.py: anchor → POI categories
	2.	Tile Build  
	•	05_h3_to_geojson.py: Convert T_hex parquet → GeoJSON (H3 v3/v4 compatible)
	•	06_build_tiles.py: GeoJSON → MBTiles → PMTiles (via tippecanoe)
	3.	Serving
	•	FastAPI serves frontend + PMTiles via static routes
	•	API serves D_anchor slices (currently returns errors - see logs)
	4.	Frontend
	•	MapLibre loads multi-resolution PMTiles (r7/r8 zoom switching)
	•	Local pmtiles.js library (no CDN dependency)
	•	Filter expressions applied as MapLibre paint properties

⸻

🧮 Runtime Math (Client-Side)

Example: “≤10 min to Costco AND ≥70 walkability AND ≤2 hr to skiing”

// Compute travel time to Costco
["min",
  ["+", ["get","a0_s"], ["literal", dAnchor.get(["get","a0_id"]) || 65535]],
  ["+", ["get","a1_s"], ["literal", dAnchor.get(["get","a1_id"]) || 65535]],
  ["+", ["get","a2_s"], ["literal", dAnchor.get(["get","a2_id"]) || 65535]],
  ["+", ["get","a3_s"], ["literal", dAnchor.get(["get","a3_id"]) || 65535]]
]

// Apply all filters
["case",
  ["all",
    ["<=", ["var","tt_costco"], 600],   // ≤ 10 min
    [">=", ["get","walkscore"], 70],    // walkability ≥ 70
    ["<=", ["var","tt_ski"], 7200]      // ≤ 2 hr
  ],
  0.9, 0.05 // visible vs masked
]


⸻

🚀 Demo Workflow

# Complete pipeline (run from repo root)
make all

# Or step by step:

# 1. Build T_hex for Massachusetts (with memory optimizations)
PYTHONPATH=. .venv/bin/python scripts/precompute_t_hex.py \
  --pbf data/osm/massachusetts.osm.pbf \
  --anchors out/anchors/anchors_drive.parquet \
  --mode drive --res 8 --cutoff 90 --batch-size 250 \
  --anchor-index-out out/anchors/anchor_index_drive.parquet \
  --out data/minutes/massachusetts_hex_to_anchor_drive.parquet

# 2. Build D_anchor 
PYTHONPATH=. .venv/bin/python scripts/precompute_d_anchor.py \
  --anchors out/anchors/anchors_drive.parquet \
  --anchor-index out/anchors/anchor_index_drive.parquet \
  --mode drive --state massachusetts \
  --out data/minutes/

# 3. Build map tiles
PYTHONPATH=. .venv/bin/python scripts/05_h3_to_geojson.py \
  --input data/minutes/massachusetts_hex_to_anchor_drive.parquet \
  --output tiles/t_hex_r8_drive.geojson.nd --h3-col h3_id

PYTHONPATH=. .venv/bin/python scripts/06_build_tiles.py \
  --input tiles/t_hex_r8_drive.geojson.nd \
  --output tiles/t_hex_r8_drive.pmtiles \
  --layer t_hex_r8_drive

# 4. Run server
make serve

# Open in browser
http://localhost:5174


⸻

## Current Status & Known Issues

### ✅ Working
- **Data Pipeline**: T_hex and D_anchor computation complete
- **Map Visualization**: Interactive hex map with multi-resolution tiles
- **Frontend**: PMTiles loading, zoom-based layer switching
- **Memory Optimizations**: Batched processing, H3 v3/v4 compatibility

### ⚠️ Known Issues  
- **D_anchor API**: Missing required columns (`category_id`, `seconds_u16`)
  ```
  ERROR: D_anchor missing required columns: {'category_id', 'seconds_u16'}
  GET /api/d_anchor?category=chipotle&mode=drive HTTP/1.1 404 Not Found
  ```
- **Filter Controls**: Frontend expects API data for dynamic filtering
- **Category Mapping**: Need to map POI names to category IDs

### 🚧 Next Steps
1. **Fix D_anchor API**: Update data schema or API expectations
2. **Category Integration**: Connect POI data to frontend categories  
3. **Filter Implementation**: Enable interactive time-based filtering
4. **Walk Mode**: Add walk mode tiles and routing

### 📊 Performance Notes
- **Memory**: Successfully handles 23K+ hexes with batched processing
- **Tiles**: ~6MB total (1MB r7 + 5MB r8) for Massachusetts
- **H3 Compatibility**: Robust fallback for different H3 versions
- **Data Types**: `.itertuples()` preserves uint64 precision

⸻

🧭 Summary

TownScout is not a filter UI.
It's a geospatial compute engine packaged as a map:
	•	Heavy math precomputed once
	•	Compact tiles served from static routes
	•	Browser evaluates livability filters with MapLibre expressions

The core matrix factorization works. The visualization works. The data pipeline is robust.

Next: Connect the pieces for dynamic filtering.