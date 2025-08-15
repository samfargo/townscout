# TownScout: Interactive Drive-Time Map

**Mission**: Build an interactive, stackable-filter map that answers: "Where should I live given my criteria?" It feels instant, costs pennies, and scales nationwide.

## 🚀 Quick Start

### Prerequisites (macOS)
```bash
# Install dependencies
brew install python@3.11 proj geos gdal tippecanoe pmtiles jq node

# Setup Python environment
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Running the Map
```bash
# Activate Python environment
source .venv/bin/activate

# Start the web server (IMPORTANT: Use npx serve, not Python's http.server)
npx --yes serve . -p 5173

# Open the map
open http://localhost:5173/tiles/web/
```

## ✅ Current Status (MVP)

Your TownScout map currently shows:
- **Geographic Coverage**: Massachusetts (Boston metro area)
- **H3 Resolution**: 7 (overview) and 8 (detail) hexagonal cells
- **Metrics**: Drive times to Chipotle and Costco locations + Crime rates per 100k population
- **Interactive Filters**: 
  - Chipotle ≤ 5-45 minutes (5min steps)
  - Costco ≤ 5-60 minutes (5min steps)
  - Crime rate ≤ 0-10,000 per 100k (50 steps)
- **Real-time Filtering**: Instant visual updates as you move sliders
- **URL Sharing**: Share your current filter settings via URL
- **Zoom Transitions**: Automatic layer switching between zoom levels

### Data Coverage
- **3,116 H3 cells** at resolution 7 (overview level)
- **17,611 H3 cells** at resolution 8 (detail level)  
- **Drive time ranges**: Chipotle 0-149 min, Costco 0-240 min
- **Crime rate ranges**: 222-13,297 per 100k (2024 MA data)
- **Crime rate coverage**: 70-74% of hexes have valid crime data
- **Geographic bounds**: ~42.0-42.3°N, 72.8-72.3°W (Boston area)

## 🏛️ Crime Rate Integration

✅ **Fully Integrated** - Crime rate data is now available as a filterable layer in TownScout.

### How It Works

**User Experience:**
- **Crime Rate Slider**: Filter areas by crime rate per 100k population (0-10,000, 50-unit steps)
- **Smart Filtering**: Areas without crime data remain visible but don't interfere with filtering
- **Combined Filters**: Crime rate works alongside drive time filters for comprehensive location scoring
- **Visual Feedback**: Instant map updates as you adjust the crime rate threshold

**Data Processing:**
1. Downloads Massachusetts municipal boundaries from U.S. Census TIGER/Line 2024
2. Matches 287 valid crime records to 357 jurisdictions (76.5% match rate)
3. Assigns crime rates to 19,245+ H3 hexes via centroid-in-polygon spatial join
4. Excludes 47 towns with incomplete data (0.0 or missing values)
5. Areas without crime data get special value (-1) and are excluded from crime filtering

### Usage Instructions

```bash
# If starting fresh, run the complete pipeline with crime data:
make all

# If you already have H3 data and want to add crime rates:
make boundaries    # Download municipal boundaries (Massachusetts only)
make crime-rates   # Enrich H3 hexes with crime data  
make merge geojson tiles  # Rebuild tiles with crime data

# Test the integration:
make test         # Validate crime rate processing

# Start the map:
npx --yes serve . -p 5173
open http://localhost:5173/tiles/web/
```

### Data Quality & Coverage

**Crime Rate Data:**
- **Valid Records**: 287 towns with complete crime data
- **Data Range**: 222 to 13,297 crimes per 100k population
- **Excluded Data**: 47 towns with 0.0 or missing values (incomplete records)
- **Match Rate**: 76.5% of Massachusetts jurisdictions matched to crime data

**H3 Hex Coverage:**
- **R7 (Overview)**: 3,014/4,311 hexes (70%) have valid crime data
- **R8 (Detail)**: 19,245/25,859 hexes (74%) have valid crime data
- **No Data Areas**: Remain visible but excluded from crime rate filtering

**Example High/Low Crime Areas:**
- **Lowest**: Groveland (223 per 100k), Worthington (424 per 100k)
- **Highest**: Holyoke (13,298 per 100k), Springfield (9,271 per 100k)
- **Boston**: Cambridge (5,417), Boston (6,618), Chelsea (6,459)

## 🏗️ Architecture

```
Data → Compute → Tiles → UI
```

1. **Data**: OSM Geofabrik PBFs + brand name filters
2. **Compute**: Build drivable graph → multi-source Dijkstra → aggregate to H3
3. **Tiles**: Parquet → GeoJSON → MBTiles → PMTiles
4. **UI**: MapLibre GL loads PMTiles; filter expressions drive instant updates

## 📂 Directory Structure

```
data/osm/             # *.osm.pbf files
data/poi/             # {state}_{brand}.parquet
data/minutes/         # {state}_r{res}.parquet
state_tiles/          # us_r{res}.parquet (+ optional CSV)
tiles/                # us_r{res}.geojson, .mbtiles, .pmtiles
tiles/web/            # index.html, style.css, pmtiles.js
src/                  # Python pipeline scripts
```

## 🔄 Data Pipeline

### Full Pipeline
```bash
make all    # Run complete pipeline
```

### Individual Steps
```bash
make pbf       # Download OSM PBF files
make pois      # Extract brand POIs
make minutes   # Compute drive times per state
make merge     # Merge states into unified files
make geojson   # Export to GeoJSON
make tiles     # Build MBTiles and convert to PMTiles
```

## 🛠️ Key Technologies

- **Backend**: Python 3.11, OSMnx, NetworkX, H3, Pandas
- **Tiles**: Tippecanoe, PMTiles
- **Frontend**: MapLibre GL, vanilla JavaScript
- **Serving**: Node.js `serve` (for proper HTTP range request support)

## 🔧 Critical Fixes Applied

### 1. HTTP Server Issue ⚠️
**Problem**: Python's `http.server` doesn't support HTTP range requests properly.
**Solution**: Use `npx serve` instead for proper PMTiles support.

```bash
# ❌ DON'T USE - causes tile loading errors
python -m http.server 5173

# ✅ USE THIS - proper range request support
npx --yes serve . -p 5173
```

### 2. PMTiles Library Issue
**Problem**: CDN URLs for PMTiles were returning 404 errors.
**Solution**: Downloaded PMTiles library locally to `tiles/web/pmtiles.js`.

### 3. Layer Zoom Ranges
**Problem**: HTML was using incorrect zoom ranges for tile layers.
**Solution**: Fixed zoom ranges to match actual tile data:
- R7 tiles: zoom 0-5 (not 0-8)
- R8 tiles: zoom 5-22 (not 8-22)

### 4. Missing Assets
- Added favicon.ico to prevent 404 errors
- Added OpenStreetMap attribution
- Enhanced error handling and debugging

## 🎛️ Configuration

### Current POI Brands (src/config.py)
```python
POI_BRANDS = {
    "chipotle": {"tags": {"amenity": ["fast_food"], "name": ["Chipotle"]}},
    "costco":   {"tags": {"shop": ["supermarket", "wholesale"], "name": ["Costco"]}},
    "airports": {"tags": {"aeroway": ["aerodrome"]}},
}
```

### Adding New POI Brands

The system uses a robust multi-stage extraction pipeline that automatically ensures comprehensive coverage:

1. **Stage 1**: Pyrosm extraction with specific shop/amenity filters
2. **Stage 2**: OGR fallback across points+polygons if count is suspiciously low
3. **Stage 3**: Validation against expected minimums with warnings

To add a new POI brand:

1. **Add to `src/config.py`**:
```python
POI_BRANDS = {
    # ... existing brands ...
    "starbucks": {
        "tags": {
            "amenity": ["cafe", "fast_food"],
            "shop": ["coffee"],  # if applicable
            "name": ["Starbucks", "Starbucks Coffee"],
            "brand": ["Starbucks", "Starbucks Coffee"], 
            "operator": ["Starbucks", "Starbucks Coffee"]
        }
    },
}
```

2. **Set expected minimum count** in `src/02_extract_pois_from_pbf.py`:
```python
EXPECTED_MIN_COUNTS = {
    # ... existing counts ...
    "starbucks": 25,  # Estimate for Massachusetts
}
```

3. **Add brand-specific variants** (if needed) in `extract_and_validate_pois()`:
```python
elif brand == "starbucks":
    keywords.extend(["Starbucks #", "Starbucks Store"])
```

4. **Run extraction**: `make pois`

The system will automatically:
- Try multiple extraction methods
- Include points, polygons, and relations
- Filter out irrelevant features (gas stations, etc.)
- Validate against expected counts
- Report warnings if coverage seems incomplete

### Geographic Scope
```python
STATES = ["massachusetts"]  # Currently Massachusetts only
```

### H3 Resolutions
```python
H3_RES_LOW = 7   # overview (fewer, larger hexes)
H3_RES_HIGH = 8  # detail (more, smaller hexes)
```

## 🚀 Scaling to Nationwide

To expand beyond Massachusetts:

1. **Update config**:
   ```python
   STATES = ["massachusetts", "connecticut", "rhode-island", "new-hampshire"]
   ```

2. **Add more POI categories**:
   ```python
   POI_BRANDS = {
       "chipotle": {...},
       "costco": {...},
       "hospitals": {"tags": {"amenity": ["hospital"]}},
       "airports": {"tags": {"aeroway": ["aerodrome"]}},
   }
   ```

3. **Rebuild pipeline**:
   ```bash
   make clean
   make all
   ```

## 🐛 Troubleshooting

### Map Not Loading
1. **Check server**: Ensure using `npx serve`, not Python's http.server
2. **Check browser console**: Look for PMTiles loading errors
3. **Verify tiles**: Check if `.pmtiles` files exist in `tiles/` directory

### No Hexagons Visible
1. **Check zoom level**: Zoom in/out to trigger layer switching
2. **Adjust filters**: Try raising slider values (current data is Massachusetts only)
3. **Check data**: Verify `state_tiles/us_r*.parquet` files exist

### Server Issues
```bash
# Kill existing servers
lsof -ti:5173 | xargs kill

# Start fresh
npx --yes serve . -p 5173
```

## 📊 Performance Targets

- **Initial tile download**: ≤10-50 MB per session
- **Slider update response**: ≤250ms
- **Total tile size**: <400 MB combined (r7 + r8)
- **Single-state compute**: ≤10 minutes on 8-core laptop

## 🔒 Security & Privacy

- **Entirely static**: No user auth required for MVP
- **No PII**: Only public POI data and aggregated drive times
- **Open data**: Uses OpenStreetMap contributors' data

## 📈 Next Steps

### Immediate Improvements
- [ ] Add base map layer (state boundaries, roads)
- [ ] Expand to neighboring states
- [ ] Add hospitals and airports POI categories
- [ ] Implement CSV export for Pro users

### Advanced Features
- [ ] Friend/family proximity scoring
- [ ] User-uploaded custom POIs
- [ ] Live traffic integration
- [ ] Mobile-responsive design

## 📜 License & Attribution

- **Map data**: © OpenStreetMap contributors
- **Code**: MIT License (add your license file)
- **H3**: Uber's hexagonal hierarchical geospatial indexing system

---

**Built with ❤️ for finding the perfect place to live** 