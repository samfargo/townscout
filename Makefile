PY=PYTHONPATH=. .venv/bin/python
STATES=massachusetts new-hampshire rhode-island connecticut maine
CATEGORIES=chipotle costco airports

.PHONY: help init pbf pois anchors anchors-fresh t-hex d-anchor clean test serve clear-cache

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
	$(PY) src/01_download_osm_extracts.py

pois:  ## Extract POI data from PBF files (legacy format, still needed for D_anchor)
	$(PY) src/02_extract_pois_from_pbf.py

anchors:  ## Build anchor points for drive and walk networks (uses cached networks)
	@echo "Building anchors for Massachusetts..."
	$(PY) src/build_anchors_modular.py --pbf data/osm/massachusetts.osm.pbf --out out/anchors
	@echo "✅ Anchors complete. Check out/anchors/anchors_map.html for QA visualization."

anchors-fresh:  ## Build anchor points from scratch (clears network cache first)
	@echo "Building anchors from scratch (clearing cache)..."
	$(PY) src/build_anchors_modular.py --pbf data/osm/massachusetts.osm.pbf --out out/anchors --clear-cache
	@echo "✅ Fresh anchors complete. Check out/anchors/anchors_map.html for QA visualization."

clear-cache:  ## Clear cached network files to force rebuild
	@echo "Clearing network cache..."
	rm -f out/anchors/network_*.pkl
	@echo "✅ Network cache cleared. Next 'make anchors' will rebuild networks."

t-hex: anchors ## Precompute Hex→Anchor travel times (T_hex matrix) and generate anchor indices
	@echo "Computing T_hex (Hex→Anchor) for Massachusetts..."
	@mkdir -p data/minutes
	$(PY) src/precompute_t_hex.py \
		--pbf data/osm/massachusetts.osm.pbf \
		--anchors out/anchors/anchors_drive.parquet \
		--mode drive \
		--res 8 \
		--cutoff 90 \
		--batch 200 \
		--anchor-index-out out/anchors/anchor_index_drive.parquet \
		--out data/minutes/massachusetts_hex_to_anchor_drive.parquet
	$(PY) src/precompute_t_hex.py \
		--pbf data/osm/massachusetts.osm.pbf \
		--anchors out/anchors/anchors_walk.parquet \
		--mode walk \
		--res 8 \
		--cutoff 30 \
		--batch 200 \
		--anchor-index-out out/anchors/anchor_index_walk.parquet \
		--out data/minutes/massachusetts_hex_to_anchor_walk.parquet
	@echo "✅ T_hex matrices complete."

d-anchor: t-hex ## Precompute Anchor→Category travel times (D_anchor matrix)
	@echo "Computing D_anchor (Anchor→Category) for Massachusetts..."
	$(PY) src/precompute_d_anchor.py \
		--pbf data/osm/massachusetts.osm.pbf \
		--anchors out/anchors/anchors_drive.parquet \
		--mode drive \
		--state massachusetts \
		--categories $(CATEGORIES) \
		--anchor-index out/anchors/anchor_index_drive.parquet \
		--out data/minutes/massachusetts_anchor_to_category_drive.parquet
	$(PY) src/precompute_d_anchor.py \
		--pbf data/osm/massachusetts.osm.pbf \
		--anchors out/anchors/anchors_walk.parquet \
		--mode walk \
		--state massachusetts \
		--categories $(CATEGORIES) \
		--anchor-index out/anchors/anchor_index_walk.parquet \
		--out data/minutes/massachusetts_anchor_to_category_walk.parquet
	@echo "✅ D_anchor matrices complete."

serve:  ## Start the runtime tile server
	@echo "Starting TownScout runtime tile server..."
	@echo "Access at: http://localhost:8080/health"
	@echo "Use /tiles/criteria endpoint for dynamic queries"
	TS_DATA_DIR=data/minutes TS_ANCHOR_DIR=out/anchors TS_STATE=massachusetts $(PY) src/runtime_tiles.py

# ========== SHORTCUTS ==========

all: pbf pois anchors t-hex d-anchor  ## Run complete anchor-based pipeline

quick: anchors t-hex d-anchor serve  ## Skip downloads, build anchors and serve

fresh: clear-cache anchors t-hex d-anchor  ## Clear cache and rebuild everything

clean:  ## Clean generated data files
	rm -rf data/osm/*.pbf data/poi/*.parquet data/minutes/*.parquet
	rm -rf out/anchors/*.parquet out/anchors/*.html out/anchors/*.pkl
	rm -rf state_tiles/*.parquet tiles/*.geojson tiles/*.mbtiles tiles/*.pmtiles