# 🏘️ TownScout

**Interactive, stackable-filter map that answers: "Where should I live given my criteria?"**

TownScout uses an **anchor-based architecture** that precomputes travel networks once, then answers complex multi-POI queries in milliseconds. Instead of building every road from scratch for each trip, we lay permanent highways and just check intersections.

The application is a map of the United States where each time a filter/criteria is added, the livable land for that user visually shrinks in real-time.

Zillow tells you what's for sale. Google Maps tells you how to get somewhere. TownScout tells you where your life actually works—by stacking together your criteria and instantly shrinking the map to only the livable areas.

## 🚀 Quick Start

```bash
# Setup
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Build data pipeline  
make pbf        # Download OSM data
make pois       # Extract POI locations
make anchors    # Create travel network anchor points
make t-hex      # Precompute Hex→Anchor matrices
make d-anchor   # Precompute Anchor→Category matrices

# Start web interface
make serve      # Runs on http://localhost:8080
```

**Access your TownScout interface**:
- **Main App**: http://localhost:8080/static/web/index.html
- **Demo**: http://localhost:8080/static/web/runtime_demo.html
- **Landing**: http://localhost:8080/

## 🌐 Web Interface

### Architecture: Clean Separation
- **`/static`** → All frontend assets (HTML, CSS, JS)
- **`/api`** → Dynamic runtime endpoints
- **MapLibre GL** → Interactive map with real-time tile loading

### Features
- **Multi-criteria filtering**: Chipotle ≤ 15min, Costco ≤ 20min, Airports ≤ 120min
- **Viewport-based loading**: Fetches multiple tiles covering visible area
- **Instant updates**: Real-time filtering as you adjust sliders
- **Share functionality**: URL parameters preserve your criteria
- **Mobile responsive**: Works on all devices

### API Endpoints
```bash
# Health check
curl http://localhost:8080/health

# Available categories
curl http://localhost:8080/api/categories

# Query criteria for a specific tile
curl "http://localhost:8080/api/criteria?z=8&x=77&y=94&criteria=[{\"category\":\"chipotle\",\"threshold\":15}]"

# Multi-criteria query
curl "http://localhost:8080/api/criteria?z=8&x=77&y=94&criteria=[
  {\"category\":\"costco\",\"threshold\":15},
  {\"category\":\"chipotle\",\"threshold\":30}, 
  {\"category\":\"airports\",\"threshold\":240}
]"
```

**Response**: GeoJSON FeatureCollection of H3 hexes meeting ALL criteria.

## 💡 Our Solution: Matrix Factorization
```
(Hex→Anchor) × (Anchor→Category) = Linear Scale
```

🔑 **How It Works**
- **Anchor-based architecture**: Instead of brute-forcing every trip for every user query, TownScout builds a permanent "backbone" of anchors (bridges, intersections, key network nodes) that guarantee coverage.
- **Precomputed matrices**:
  - **T_hex** (Hex → Anchor travel times) — how long it takes from any hex tile to the network backbone.
  - **D_anchor** (Anchor → POI categories) — how long from the backbone to things like Costco, airports, ski resorts.
- **Min-plus algebra at runtime**: When a user asks for "≤15 min drive to Costco AND ≤30 min to Chipotle AND ≤2 hr to a ski resort", TownScout just does fast matrix lookups and bitset combinations. No recomputation, no waiting.

**Result**: Nationwide, stackable, live filters across multiple criteria that feel instantaneous.

## 🏗️ Architecture

```
Data → Anchors → Matrices → Runtime Queries → Web Interface
```

1. **Anchors** — Strategic points on drive/walk networks (bridges, intersections, motorway chains)
   - QA targets:
     - Drive: ≥95% of r7 hexes within 10 km
     - Walk (urban): ≥95% of r8 hexes within 600 m

2. **T_hex** — Hex→Anchor travel times (sparse, K≈24–48 anchors per hex)

3. **D_anchor** — Anchor→Category travel times (multi-source floods)

4. **Runtime API** — Queries = fast min-plus matrix ops + bitset masking

5. **Web Interface** — MapLibre GL + FastAPI for real-time visualization

## 📂 Data Structure

```
out/anchors/                               # Anchor points
  anchors_drive.parquet                    # Drive network anchors
  anchors_walk.parquet                     # Walk network anchors  
  anchors_map.html                         # QA visualization

data/minutes/                              # Precomputed matrices  
  T_hex_drive.parquet                      # Hex→Anchor drive times
  T_hex_walk.parquet                       # Hex→Anchor walk times
  D_anchor_drive.parquet                   # Anchor→Category drive times
  D_anchor_walk.parquet                    # Anchor→Category walk times

data/poi/                                  # POI locations
  {state}_{category}.parquet               # POI coordinates by category

tiles/web/                                 # Web interface
  index.html                               # Main TownScout interface
  runtime_demo.html                        # Simple demo interface
  style.css                                # Responsive styling
```

## 🔄 Pipeline Commands

### Core Pipeline
```bash
make all        # Complete anchor-based pipeline
make quick      # Skip downloads, build and serve

# Individual steps
make anchors    # Build network anchor points
make t-hex      # Precompute Hex→Anchor matrices  
make d-anchor   # Precompute Anchor→Category matrices
make serve      # Start web interface on port 8080
make test       # Validate pipeline outputs
```

### Utilities  
```bash
make pbf        # Download OSM data
make pois       # Extract POI locations
make clean      # Remove generated files
```

## 🎚️ Supported Categories

Current POI categories with optimized defaults:

| Category   | ID | Default Mode | Default Cutoff | Description        |
|------------|----|--------------|--------------| ------------------|
| `chipotle` | 1  | drive        | 30min        | Chipotle restaurants |
| `costco`   | 2  | drive        | 60min        | Costco warehouses  |
| `airports` | 3  | drive        | 240min       | Major airports     |

*Adding new categories is trivial - just update `src/categories.py` and re-run `make d-anchor`.*

## 🔧 Key Technologies

### Backend
- **Python 3.11**: Core runtime
- **FastAPI**: Web API with automatic docs
- **OSMnx**: Road network analysis
- **NetworkX**: Graph algorithms
- **H3**: Hexagonal spatial indexing
- **Pandas**: Data processing
- **NumPy**: Matrix operations

### Frontend  
- **MapLibre GL JS**: Interactive mapping
- **Vanilla JavaScript**: Lightweight UI
- **CSS Grid/Flexbox**: Responsive design

### Data
- **OpenStreetMap**: Road networks via Pyrosm
- **Parquet**: Columnar storage for matrices
- **GeoJSON**: Spatial data exchange

## 🚀 Performance

- **Query Response**: < 250ms for complex multi-criteria
- **Data Size**: ~17K anchor-to-category relationships
- **Coverage**: 705 H3 hexes in Massachusetts
- **Memory**: Matrices cached in RAM for instant access

## 🤝 Contributing

1. **Add Categories**: Update `src/categories.py` with new POI types
2. **Extend Regions**: Add states to `src/config.py`
3. **Improve UI**: Enhance `tiles/web/` interfaces
4. **Optimize Performance**: Improve anchor selection algorithms

## 📄 License

*Map data © OpenStreetMap contributors*

---

**TownScout**: Where data meets decisions. Where algorithms meet life choices. Where you discover not just where you *can* live, but where you *should* live.