PY=PYTHONPATH=. .venv/bin/python
STATES=massachusetts new-hampshire rhode-island connecticut maine vermont new-york
CATEGORIES=chipotle costco airports ski-areas public-transit

.PHONY: help init pbf pois anchors anchors-fresh t-hex d-anchor clean test serve clear-cache all quick fresh

help:  ## Show this help message
	@echo "TownScout Anchor-Based Pipeline - Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

init:  ## Initialize virtual environment and install dependencies
	@if command -v python3.12 >/dev/null 2>&1; then \
		python3.12 -m venv .venv; \
	else \
		python3 -m venv .venv; \
	fi && . .venv/bin/activate && pip install -r requirements.txt

# ========== ANCHOR-BASED PIPELINE ==========

pbf:  ## Download OSM PBF extracts for all states
	$(PY) scripts/download_osm.py

pois:  ## Extract POI data from PBF files (legacy format, still needed for D_anchor)
	$(PY) scripts/extract_pois.py

.PHONY: ski-pois
ski-pois:  ## Extract only ski-areas POIs with Overpass (uses cache)
	TS_ONLY_CATEGORY=ski-areas $(PY) scripts/extract_pois.py

anchors:  ## Build anchor points for drive and walk networks (uses cached networks)
	@echo "Building anchors for Massachusetts..."
	$(PY) scripts/build_anchors_modular.py --pbf data/osm/massachusetts.osm.pbf --out out/anchors
	@echo "✅ Anchors complete. Check out/anchors/anchors_map.html for QA visualization."

anchors-fresh:  ## Build anchor points from scratch (clears network cache first)
	@echo "Building anchors from scratch (clearing cache)..."
	$(PY) scripts/build_anchors_modular.py --pbf data/osm/massachusetts.osm.pbf --out out/anchors --clear-cache
	@echo "✅ Fresh anchors complete. Check out/anchors/anchors_map.html for QA visualization."

clear-cache:  ## Clear cached network files to force rebuild
	@echo "Clearing network cache..."
	rm -f out/anchors/network_*.pkl
	@echo "✅ Network cache cleared. Next 'make anchors' will rebuild networks."

t-hex: anchors ## Precompute Hex→Anchor travel times (T_hex matrix) and generate anchor indices
	@echo "Computing T_hex (Hex→Anchor) for Massachusetts..."
	@mkdir -p data/minutes
	$(PY) scripts/precompute_t_hex.py \
		--pbf data/osm/massachusetts.osm.pbf \
		--anchors out/anchors/anchors_drive.parquet \
		--mode drive \
		--res 8 \
		--cutoff 90 \
		--batch-size 250 \
		--k-pass-mode \
		--anchor-index-out out/anchors/anchor_index_drive.parquet \
		--out data/minutes/massachusetts_hex_to_anchor_drive.parquet
	$(PY) scripts/precompute_t_hex.py \
		--pbf data/osm/massachusetts.osm.pbf \
		--anchors out/anchors/anchors_walk.parquet \
		--mode walk \
		--res 8 \
		--cutoff 30 \
		--k-pass-mode \
		--anchor-index-out out/anchors/anchor_index_walk.parquet \
		--out data/minutes/massachusetts_hex_to_anchor_walk.parquet
	@echo "✅ T_hex matrices complete."

d-anchor: t-hex ## Precompute Anchor→Category travel times (D_anchor matrix)
	@echo "Computing D_anchor (Anchor→Category) for Massachusetts..."
	$(PY) scripts/precompute_d_anchor.py \
		--pbf data/osm/massachusetts.osm.pbf \
		--anchors out/anchors/anchors_drive.parquet \
		--mode drive \
		--state massachusetts \
		--categories $(CATEGORIES) \
		--anchor-index out/anchors/anchor_index_drive.parquet \
		--out data/minutes/
	$(PY) scripts/precompute_d_anchor.py \
		--pbf data/osm/massachusetts.osm.pbf \
		--anchors out/anchors/anchors_walk.parquet \
		--mode walk \
		--state massachusetts \
		--categories $(CATEGORIES) \
		--anchor-index out/anchors/anchor_index_walk.parquet \
		--out data/minutes/
	@echo "✅ D_anchor matrices complete."

geojson: t-hex ## Convert T_hex parquet matrices to GeoJSON
	@echo "Converting T_hex matrices to GeoJSON..."
	@mkdir -p tiles
	$(PY) scripts/05_h3_to_geojson.py \
		--input data/minutes/massachusetts_hex_to_anchor_drive.parquet \
		--output tiles/massachusetts_drive.geojson.nd
	$(PY) scripts/05_h3_to_geojson.py \
		--input data/minutes/massachusetts_hex_to_anchor_walk.parquet \
		--output tiles/massachusetts_walk.geojson.nd
	@echo "✅ GeoJSON conversion complete."

tiles: geojson ## Build PMTiles from GeoJSON
	@echo "Building PMTiles from GeoJSON..."
	$(PY) scripts/06_build_tiles.py \
		--input tiles/massachusetts_drive.geojson.nd \
		--output tiles/massachusetts_drive.pmtiles \
		--layer massachusetts_hex_to_anchor_drive
	$(PY) scripts/06_build_tiles.py \
		--input tiles/massachusetts_walk.geojson.nd \
		--output tiles/massachusetts_walk.pmtiles \
		--layer massachusetts_hex_to_anchor_walk
	@echo "✅ PMTiles build complete."

serve:  ## Start the FastAPI API server
	@echo "Starting TownScout API server..."
	@echo "Access frontend at: http://localhost:5174/"
	TS_DATA_DIR=data/minutes TS_STATE=massachusetts .venv/bin/uvicorn api.app.main:app --host 0.0.0.0 --port 5174

# ========== SHORTCUTS ==========

all: clean pbf pois anchors t-hex d-anchor geojson tiles serve  ## Full fresh build of the anchor-matrix pipeline and start the server

quick: anchors t-hex d-anchor geojson tiles serve  ## Skip downloads, build anchors and serve

fresh: clear-cache anchors t-hex d-anchor geojson tiles ## Clear cache and rebuild everything

clean:  ## Clean generated data files
	rm -rf data/osm/*.pbf data/poi/*.parquet data/minutes/*.parquet
	rm -rf out/anchors/*.parquet out/anchors/*.html out/anchors/*.pkl
	rm -rf state_tiles/*.parquet tiles/*.geojson tiles/*.mbtiles tiles/*.pmtiles

.PHONY: clean-graph
clean-graph:
	rm -rf data/osm/cache/*.graphml