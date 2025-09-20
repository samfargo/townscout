PY=PYTHONPATH=src .venv/bin/python
PYTHON_BIN?=$(shell command -v python3.11 2>/dev/null || command -v python3 2>/dev/null)
STATES=massachusetts
# Add more states as needed, e.g., STATES=massachusetts new-hampshire

.PHONY: help init clean all \
	download pois anchors minutes overlays geojson tiles export-csv native

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

# --- Native build (stamp to avoid triggering rebuilds) ---
build/native.stamp:
	@mkdir -p build
	.venv/bin/maturin develop --release --manifest-path townscout_native/Cargo.toml
	@touch $@

native: build/native.stamp ## Build the native Rust extension (release optimized)

download:  ## 1. Download OSM and Overture data extracts
	$(PY) src/01_download_extracts.py

POI_FILES := $(patsubst %,data/poi/%_canonical.parquet,$(STATES))
pois: $(POI_FILES)  ## 2. Normalize and conflate POIs from all sources

# Source data artifacts (built by the download step)
data/osm/%.osm.pbf:
	$(PY) src/01_download_extracts.py
	@test -f $@ || (echo "[error] expected $@ after download" && exit 1)

data/overture/ma_places.parquet:
	$(PY) src/01_download_extracts.py
	@test -f $@ || (echo "[error] expected $@ after download" && exit 1)

# Allow make to build canonical POI parquet on demand
data/poi/%_canonical.parquet: data/osm/%.osm.pbf data/overture/ma_places.parquet
	$(PY) src/02_normalize_pois.py

# Build anchor sites per state (deterministic, reusable)
ANCHOR_FILES := $(patsubst %,data/anchors/%_drive_sites.parquet,$(STATES))

anchors: $(ANCHOR_FILES) ## 2.5 Build anchor sites per state

data/anchors/%_drive_sites.parquet: data/poi/%_canonical.parquet data/osm/%.osm.pbf
	@mkdir -p data/anchors
	@echo "--- Building anchor sites for $* (drive) ---"
	$(PY) src/03_build_anchor_sites.py \
		--state $* \
		--mode drive \
		--pois data/poi/$*_canonical.parquet \
		--pbf data/osm/$*.osm.pbf \
		--out-sites $@ \
		--out-map data/anchors/$*_drive_site_id_map.parquet

# Define a target for each state's minutes file
MINUTE_FILES := $(patsubst %,data/minutes/%_drive_t_hex.parquet,$(STATES))
# TODO: Add walk mode back in
# MINUTE_FILES += $(patsubst %,data/minutes/%_walk_t_hex.parquet,$(STATES))

minutes: $(MINUTE_FILES)  ## 3. Compute per-state travel time minutes from POIs to hexes

data/minutes/%_drive_t_hex.parquet: data/poi/%_canonical.parquet data/anchors/%_drive_sites.parquet | build/native.stamp
	@echo "--- Computing minutes for $* (drive) ---"
	$(PY) src/03_compute_minutes_per_state.py \
		--pbf data/osm/$*.osm.pbf \
		--pois data/poi/$*_canonical.parquet \
		--mode drive \
		--cutoff 90 \
		--overflow-cutoff 240 \
		--k-best 20 \
		--res 7 8 \
		--progress \
		--out-times $@ \
		--anchors data/anchors/$*_drive_sites.parquet

ifeq ($(FORCE),1)
  OVERLAYS_FORCE := --force
else
  OVERLAYS_FORCE :=
endif

# Compute brand overlays (nearest time per hex) for brands with >=20 sites
.PHONY: overlays
overlays: | build/native.stamp ## 3.5 Compute popular brand overlays (K=1) for nearest-time guarantees
	@for S in $(STATES); do \
	  echo "--- Computing overlays for $$S (drive) ---"; \
	  $(PY) src/03c_compute_overlays.py \
	    --pbf data/osm/$$S.osm.pbf \
	    --anchors data/anchors/$$S\_drive_sites.parquet \
	    --mode drive \
	    --res 7 8 \
	    --brands-threshold 10 \
	    --cutoff 30 \
	    --overflow-cutoff 90 \
	    --out-dir data/overlays $(OVERLAYS_FORCE) ; \
	done

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

# --- Merge & Summaries ---
# Produce both outputs in one run; use a stamp to avoid duplicate execution.
MERGE_DEPS := $(MINUTE_FILES) $(ANCHOR_FILES)
state_tiles/.merge.stamp: $(MERGE_DEPS)
	$(PY) src/04_merge_states.py
	@mkdir -p state_tiles
	@touch $@

state_tiles/us_r7.parquet state_tiles/us_r8.parquet: state_tiles/.merge.stamp

.PHONY: merge
merge: state_tiles/us_r7.parquet state_tiles/us_r8.parquet ## 4. Merge per-state data and create summaries

# --- GeoJSON (build from merged summaries) ---
tiles/us_r7.geojson: state_tiles/us_r7.parquet
	@mkdir -p tiles
	$(PY) src/05_h3_to_geojson.py \
		--input state_tiles/us_r7.parquet \
		--output $@

tiles/us_r8.geojson: state_tiles/us_r8.parquet
	@mkdir -p tiles
	$(PY) src/05_h3_to_geojson.py \
		--input state_tiles/us_r8.parquet \
		--output $@

.PHONY: geojson
geojson: tiles/us_r7.geojson tiles/us_r8.geojson ## 5. Convert summaries to GeoJSON for tiling


tiles: tiles/t_hex_r7_drive.pmtiles tiles/t_hex_r8_drive.pmtiles ## 6. Build vector tiles (PMTiles)
	@mkdir -p tiles/web

tiles/t_hex_r7_drive.pmtiles: tiles/us_r7.geojson
	$(PY) src/06_build_tiles.py \
		--input $< \
		--output $@ \
		--layer t_hex_r7_drive \
		--minzoom 4 --maxzoom 8

tiles/t_hex_r8_drive.pmtiles: tiles/us_r8.geojson
	$(PY) src/06_build_tiles.py \
		--input $< \
		--output $@ \
		--layer t_hex_r8_drive \
		--minzoom 8 --maxzoom 12
	@if [ -f tiles/us_r8_walk.geojson ]; then \
		$(PY) src/06_build_tiles.py \
			--input tiles/us_r8_walk.geojson \
			--output tiles/t_hex_r8_walk.pmtiles \
			--layer t_hex_r8_walk \
			--minzoom 8 --maxzoom 12 ; \
	fi

export-csv: state_tiles/us_r7.parquet state_tiles/us_r8.parquet ## 7. Export summary data to CSV
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
	rm -rf data/osm/*.pbf data/overture/*.parquet data/poi/*.parquet data/minutes/*.parquet data/anchors/*.parquet
	rm -rf state_tiles/*.parquet state_tiles/*.csv
	rm -rf tiles/*.geojson tiles/*.mbtiles tiles/*.pmtiles
	rm -rf data/osm/cache

serve: ## Serve the frontend + tiles via FastAPI (supports HTTP Range)
	@echo "Serving frontend at http://localhost:5173/tiles/web/index.html"
	.venv/bin/uvicorn api.main:app --host 0.0.0.0 --port 5173 --reload

.PHONY: clean-graph
clean-graph:
	rm -rf data/osm/cache/*.graphml
