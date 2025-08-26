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
│
├── api/
│   └── app/
│       └── main.py            # FastAPI microservice serving D_anchor slices
│
├── web/
│   ├── index.html             # Map UI with filter panel
│   └── src/main.ts            # MapLibre + PMTiles frontend
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
	•	anchor_id: uint32
	•	category_id: uint16 (see catalog)
	•	seconds: uint16 (sentinels above)
	•	mode: uint8 (drive=0, bike=1, walk=2, transit=3)
	•	snapshot_ts: int64 (epoch ms)

Partitioning: mode=<m>/category_id=<c>/part-*.parquet

Invariants
	•	One row per (anchor_id, category_id, mode)
	•	No duplicates, no nulls

⸻

🏗️ How It Works
	1.	Offline Precompute
	•	precompute_t_hex.py: hex → top-K anchors
	•	precompute_d_anchor.py: anchor → POI categories
	2.	Tile Build
	•	Results written as PMTiles (T_hex) and Parquet (D_anchor)
	3.	Serving
	•	PMTiles served from CDN (immutable, cacheable)
	•	API serves tiny JSON slices of D_anchor
	4.	Frontend
	•	MapLibre loads T_hex tiles
	•	On filter add: browser fetches matching D_anchor slices, evaluates min-plus algebra as a GPU expression, and updates map mask instantly

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

# 1. Build T_hex for Massachusetts
python scripts/precompute_t_hex.py \
  --pbf data/massachusetts.osm.pbf \
  --anchors data/anchors.parquet \
  --mode drive \
  --out data/t_hex_drive.parquet \
  --k-best 4 --borrow-neighbors

# 2. Build D_anchor for Costco + Chipotle
python scripts/precompute_d_anchor.py \
  --pbf data/massachusetts.osm.pbf \
  --anchors data/anchors.parquet \
  --mode drive --state massachusetts \
  --categories costco chipotle \
  --out data/d_anchor_drive.parquet

# 3. Run API
cd api
uvicorn app.main:app --reload --port 5174

# 4. Run frontend
cd web
npm install
npm run dev

# Open in browser
http://localhost:5173


⸻

🧭 Summary

TownScout is not a filter UI.
It’s a geospatial compute engine packaged as a map:
	•	Heavy math precomputed once
	•	Compact tiles served over CDN
	•	Browser GPU evaluates livability filters instantly

That’s why it feels magic. And why it scales nationally.