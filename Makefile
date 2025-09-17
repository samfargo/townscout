PY=PYTHONPATH=src .venv/bin/python
PYTHON_BIN?=$(shell command -v python3.11 2>/dev/null || command -v python3 2>/dev/null)
STATES=massachusetts
# Add more states as needed, e.g., STATES=massachusetts new-hampshire

.PHONY: help init clean all \
	download pois minutes merge geojson tiles export-csv native

help:  ## Show this help message
	@echo "TownScout Data Pipeline - Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

init:  ## Initialize virtual environment with Python 3.11 and install dependencies
	rm -rf .venv
	$(PYTHON_BIN) -m venv .venv
	. .venv/bin/activate && pip install -r requirements.txt
	@. .venv/bin/activate && python -c 'import sys; print(f"Using Python {sys.version.split()[0]}")'
	@echo "✅ Environment initialized. Run 'source .venv/bin/activate' to use it."

# ========== Data Pipeline (New) ==========

native:  ## Build the native Rust extension (release optimized)
	.venv/bin/maturin develop --release --manifest-path townscout_native/Cargo.toml

download:  ## 1. Download OSM and Overture data extracts
	$(PY) src/01_download_extracts.py

pois: download  ## 2. Normalize and conflate POIs from all sources
	$(PY) src/02_normalize_pois.py

# Define a target for each state's minutes file
MINUTE_FILES := $(patsubst %,data/minutes/%_drive_t_hex.parquet,$(STATES))
# TODO: Add walk mode back in
# MINUTE_FILES += $(patsubst %,data/minutes/%_walk_t_hex.parquet,$(STATES))

minutes: $(MINUTE_FILES)  ## 3. Compute per-state travel time minutes from POIs to hexes

data/minutes/%_drive_t_hex.parquet: data/poi/%_canonical.parquet native
	@echo "--- Computing minutes for $* (drive) ---"
	$(PY) src/03_compute_minutes_per_state.py \
		--pbf data/osm/$*.osm.pbf \
		--pois data/poi/$*_canonical.parquet \
		--mode drive \
		--cutoff 30 \
		--overflow-cutoff 30 \
		--k-best 4 \
		--res 7 8 \
		--progress \
		--out-times $@ \
		--out-sites data/minutes/$*_drive_sites.parquet

# data/minutes/%_walk_t_hex.parquet: data/poi/%_canonical.parquet native
# 	@echo "--- Computing minutes for $* (walk) ---"
# 	$(PY) src/03_compute_minutes_per_state.py \
# 		--pbf data/osm/$*.osm.pbf \
# 		--pois data/poi/$*_canonical.parquet \
# 		--mode walk \
# 		--cutoff 20 \
# 		--res 7 8 \
# 		--out-times $@ \
# 		--out-sites data/minutes/$*_walk_sites.parquet

merge: minutes  ## 4. Merge per-state data and create summaries
	$(PY) src/04_merge_states.py

geojson: merge  ## 5. Convert summary parquet to GeoJSON for tiling
	@mkdir -p tiles
	# The input will be the summarized parquet from the merge step.
	# This needs to be updated once the merge step is implemented.
	$(PY) src/05_h3_to_geojson.py \
		--input state_tiles/us_r7.parquet \
		--output tiles/us_r7.geojson
	$(PY) src/05_h3_to_geojson.py \
		--input state_tiles/us_r8.parquet \
		--output tiles/us_r8.geojson

tiles: geojson  ## 6. Build vector tiles (PMTiles)
	@mkdir -p tiles/web
	$(PY) src/06_build_tiles.py \
		--input tiles/us_r7.geojson \
		--output tiles/t_hex_r7_drive.pmtiles \
		--layer t_hex_r7_drive \
		--minzoom 4 --maxzoom 8
	$(PY) src/06_build_tiles.py \
		--input tiles/us_r8.geojson \
		--output tiles/t_hex_r8_drive.pmtiles \
		--layer t_hex_r8_drive \
		--minzoom 8 --maxzoom 12

export-csv: merge ## 7. Export summary data to CSV
	@mkdir -p state_tiles
	$(PY) src/07_export_csv.py \
		--input state_tiles/us_r7.parquet \
		--output state_tiles/us_r7.csv
	$(PY) src/07_export_csv.py \
		--input state_tiles/us_r8.parquet \
		--output state_tiles/us_r8.csv

all: tiles export-csv  ## Run the full data pipeline
	@echo "✅ Full pipeline complete."

# ========== Housekeeping ==========

clean:  ## Clean all generated data files
	rm -rf data/osm/*.pbf data/overture/*.parquet data/poi/*.parquet data/minutes/*.parquet
	rm -rf state_tiles/*.parquet state_tiles/*.csv
	rm -rf tiles/*.geojson tiles/*.mbtiles tiles/*.pmtiles
	rm -rf data/osm/cache

serve: ## Serve the frontend locally
	@echo "Serving frontend at http://localhost:5173/tiles/web/index.html"
	python3 -m http.server 5173

.PHONY: clean-graph
clean-graph:
	rm -rf data/osm/cache/*.graphml
