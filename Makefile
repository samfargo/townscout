PY=PYTHONPATH=.:src .venv/bin/python
PYTHON_BIN?=$(shell command -v python3.11 2>/dev/null || command -v python3 2>/dev/null)
STATES=massachusetts

# Tuning knobs
THREADS?=8
CUTOFF?=30
OVERFLOW?=60
K_BEST?=20
# Increase K_BEST for better routing approximation in sparse networks
# Urban: 20, Suburban: 35, Rural: 50+ recommended
# Higher values improve coverage but increase tile size and compute cost

# Fingerprint directories to track when downstream data must be recomputed
DANCHOR_BRAND_FINGERPRINT_DIR := build/d_anchor_brand_hash
DANCHOR_CATEGORY_FINGERPRINT_DIR := build/d_anchor_category_hash

.PHONY: help init clean all \
	download pois anchors minutes geojson tiles native d_anchor_category d_anchor_brand \
	d_anchor_category_force d_anchor_brand_force merge climate power_corridors

help:  ## Show this help message
	@echo "TownScout Data Pipeline - Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

init:  ## Initialize virtual environment with Python 3.11 and install dependencies
	rm -rf .venv
	$(PYTHON_BIN) -m venv .venv
	. .venv/bin/activate && pip install -r requirements.txt
	@. .venv/bin/activate && python -c 'import sys; print(f"Using Python {sys.version.split()[0]}")'
	@echo "✅ Environment initialized. Run 'source .venv/bin/activate' to use it."

# ========== Data Pipeline ==========

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
data/poi/%_canonical.parquet: data/osm/%.osm.pbf data/overture/ma_places.parquet src/02_normalize_pois.py data/taxonomy/taxonomy.py data/taxonomy/POI_brand_registry.csv
	$(PY) src/02_normalize_pois.py

# Build anchor sites per state (deterministic, reusable)
ANCHOR_FILES := $(patsubst %,data/anchors/%_drive_sites.parquet,$(STATES))

anchors: $(ANCHOR_FILES) ## 2.5 Build anchor sites per state

data/anchors/%_drive_sites.parquet: data/poi/%_canonical.parquet data/osm/%.osm.pbf src/03_build_anchor_sites.py src/graph/pyrosm_csr.py data/taxonomy/taxonomy.py src/config.py | build/native.stamp
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

data/minutes/%_drive_t_hex.parquet: data/poi/%_canonical.parquet data/anchors/%_drive_sites.parquet src/04_compute_minutes_per_state.py src/graph/pyrosm_csr.py src/config.py | build/native.stamp
	@echo "--- Computing minutes for $* (drive) ---"
	$(PY) src/04_compute_minutes_per_state.py \
		--pbf data/osm/$*.osm.pbf \
		--pois data/poi/$*_canonical.parquet \
		--mode drive \
		--cutoff $(CUTOFF) \
		--overflow-cutoff $(OVERFLOW) \
		--k-best $(K_BEST) \
		--res 7 8 \
		--out-times $@ \
		--anchors data/anchors/$*_drive_sites.parquet

POWER_CORRIDOR_FILES := $(patsubst %,data/power_corridors/%_near_power_corridor.parquet,$(STATES))

power_corridors: $(POWER_CORRIDOR_FILES) ## Build high-voltage corridor avoidance flags per hex

data/power_corridors/%_near_power_corridor.parquet: data/osm/%.osm.pbf src/config.py
	@mkdir -p $(dir $@)
	$(PY) townscout/domains_overlay/power_corridors/osm_to_hex.py \
		--state $* \
		--pbf data/osm/$*.osm.pbf \
		--out $@



# Compute D_anchor brand tables for brand-level anchor-mode filtering
# The Python script handles all incremental logic - it checks if each brand's
# parquet exists and only computes missing ones. This is fast when up-to-date.
.PHONY: d_anchor_brand
d_anchor_brand: anchors | build/native.stamp ## 3.6 Compute anchor->brand seconds (incremental, delta only)
	@set -e; mkdir -p $(DANCHOR_BRAND_FINGERPRINT_DIR); \
	for S in $(STATES); do \
	  anchor_sites="data/anchors/$${S}_drive_sites.parquet"; \
	  anchor_map="data/anchors/$${S}_drive_site_id_map.parquet"; \
	  fingerprint=$$($(PY) scripts/compute_anchor_fingerprint.py "$$anchor_sites" "$$anchor_map"); \
	  hash_file="$(DANCHOR_BRAND_FINGERPRINT_DIR)/$${S}.hash"; \
	  force_flag=""; \
	  if [ -z "$$fingerprint" ]; then \
	    echo "[d_anchor_brand] Failed to compute fingerprint for $$S"; exit 1; \
	  fi; \
	  if [ ! -f "$$hash_file" ]; then \
	    echo "[d_anchor_brand] No existing fingerprint for $$S; forcing full recompute."; \
	    force_flag="--force"; \
	  else \
	    prev_hash=$$(cat "$$hash_file"); \
	    if [ "$$prev_hash" != "$$fingerprint" ]; then \
	      echo "[d_anchor_brand] Anchor fingerprint changed for $$S; forcing full recompute."; \
	      force_flag="--force"; \
	    else \
	      echo "[d_anchor_brand] Anchor fingerprint unchanged for $$S; running incremental update."; \
	    fi; \
	  fi; \
	  $(PY) src/05_compute_d_anchor.py \
	    --pbf data/osm/$$S.osm.pbf \
	    --anchors data/anchors/$$S\_drive_sites.parquet \
	    --mode drive \
	    --threads $(THREADS) \
	    --cutoff $(CUTOFF) \
	    --overflow-cutoff $(OVERFLOW) \
	    $$force_flag \
	    --out-dir data/d_anchor_brand && \
	  echo "$$fingerprint" > "$$hash_file"; \
	done

.PHONY: d_anchor_brand_force
d_anchor_brand_force: ## 3.6 Force recompute all D_anchor brand data
	@for S in $(STATES); do \
	  echo "--- Force computing D_anchor brand for $$S (drive) ---"; \
	  $(PY) src/05_compute_d_anchor.py \
	    --pbf data/osm/$$S.osm.pbf \
	    --anchors data/anchors/$$S\_drive_sites.parquet \
	    --mode drive \
	    --threads $(THREADS) \
	    --cutoff $(CUTOFF) \
	    --overflow-cutoff $(OVERFLOW) \
	    --force \
	    --out-dir data/d_anchor_brand ; \
	done

# Compute D_anchor category tables (anchor->category seconds) for categories present in anchors
# The Python script handles all incremental logic - it checks if each category's
# parquet exists and only computes missing ones. This is fast when up-to-date.
.PHONY: d_anchor_category
d_anchor_category: anchors | build/native.stamp ## 3.6b Compute anchor->category seconds (incremental, delta only)
	@set -e; mkdir -p $(DANCHOR_CATEGORY_FINGERPRINT_DIR); \
	for S in $(STATES); do \
	  anchor_sites="data/anchors/$${S}_drive_sites.parquet"; \
	  anchor_map="data/anchors/$${S}_drive_site_id_map.parquet"; \
	  fingerprint=$$($(PY) scripts/compute_anchor_fingerprint.py "$$anchor_sites" "$$anchor_map"); \
	  hash_file="$(DANCHOR_CATEGORY_FINGERPRINT_DIR)/$${S}.hash"; \
	  force_flag=""; \
	  if [ -z "$$fingerprint" ]; then \
	    echo "[d_anchor_category] Failed to compute fingerprint for $$S"; exit 1; \
	  fi; \
	  if [ ! -f "$$hash_file" ]; then \
	    echo "[d_anchor_category] No existing fingerprint for $$S; forcing full recompute."; \
	    force_flag="--force"; \
	  else \
	    prev_hash=$$(cat "$$hash_file"); \
	    if [ "$$prev_hash" != "$$fingerprint" ]; then \
	      echo "[d_anchor_category] Anchor fingerprint changed for $$S; forcing full recompute."; \
	      force_flag="--force"; \
	    else \
	      echo "[d_anchor_category] Anchor fingerprint unchanged for $$S; running incremental update."; \
	    fi; \
	  fi; \
	  $(PY) src/06_compute_d_anchor_category.py \
	    --pbf data/osm/$$S.osm.pbf \
	    --anchors data/anchors/$$S\_drive_sites.parquet \
	    --mode drive \
	    --threads $(THREADS) \
	    --cutoff $(CUTOFF) \
	    --overflow-cutoff $(OVERFLOW) \
	    --prune \
	    $$force_flag \
	    --out-dir data/d_anchor_category && \
	  echo "$$fingerprint" > "$$hash_file"; \
	done

.PHONY: d_anchor_category_force
d_anchor_category_force: ## 3.6b Force recompute all D_anchor category data
	@for S in $(STATES); do \
	  echo "--- Force computing D_anchor category for $$S (drive) ---"; \
	  $(PY) src/06_compute_d_anchor_category.py \
	    --pbf data/osm/$$S.osm.pbf \
	    --anchors data/anchors/$$S\_drive_sites.parquet \
	    --mode drive \
	    --threads $(THREADS) \
	    --cutoff $(CUTOFF) \
	    --overflow-cutoff $(OVERFLOW) \
	    --prune \
	    --force \
	    --out-dir data/d_anchor_category ; \
	done

CLIMATE_PARQUET := out/climate/hex_climate.parquet

$(CLIMATE_PARQUET): $(MINUTE_FILES)
	@mkdir -p $(dir $@)
	$(PY) townscout/domains_overlay/climate/prism_to_hex.py

climate: $(CLIMATE_PARQUET) ## Build PRISM climate parquet for r7 + r8
	@echo "[ok] Climate parquet ready at $(CLIMATE_PARQUET)"

# --- Merge & Summaries ---
# Produce both outputs in one run; use a stamp to avoid duplicate execution.
MERGE_DEPS := $(MINUTE_FILES) $(ANCHOR_FILES) $(CLIMATE_PARQUET) $(POWER_CORRIDOR_FILES)
.PHONY: merge
merge: $(MERGE_DEPS) ## 4. Merge per-state data and create summaries
	$(PY) src/07_merge_states.py

# --- GeoJSON (build from merged summaries) ---
# Use a stamp file to avoid running merge twice (it produces both r7 and r8 files)
state_tiles/.merge.stamp: $(MERGE_DEPS)
	@mkdir -p state_tiles
	$(PY) src/07_merge_states.py
	@touch $@

state_tiles/us_r7.parquet: state_tiles/.merge.stamp
state_tiles/us_r8.parquet: state_tiles/.merge.stamp

tiles/us_r7.geojson: state_tiles/us_r7.parquet
	@mkdir -p tiles
	CLIMATE_DECODE_AT_EXPORT=false $(PY) src/08_h3_to_geojson.py \
		--input state_tiles/us_r7.parquet \
		--output $@

tiles/us_r8.geojson: state_tiles/us_r8.parquet
	@mkdir -p tiles
	CLIMATE_DECODE_AT_EXPORT=false $(PY) src/08_h3_to_geojson.py \
		--input state_tiles/us_r8.parquet \
		--output $@

.PHONY: geojson
geojson: tiles/us_r7.geojson tiles/us_r8.geojson ## 5. Convert summaries to GeoJSON for tiling


tiles: tiles/t_hex_r7_drive.pmtiles tiles/t_hex_r8_drive.pmtiles ## 6. Build vector tiles (PMTiles)
	@mkdir -p tiles/web

tiles/t_hex_r7_drive.pmtiles: tiles/us_r7.geojson
	$(PY) src/09_build_tiles.py \
		--input $< \
		--output $@ \
		--layer t_hex_r7_drive \
		--minzoom 4 --maxzoom 8

tiles/t_hex_r8_drive.pmtiles: tiles/us_r8.geojson
	$(PY) src/09_build_tiles.py \
		--input $< \
		--output $@ \
		--layer t_hex_r8_drive \
		--minzoom 8 --maxzoom 12
	@if [ -f tiles/us_r8_walk.geojson ]; then \
		$(PY) src/09_build_tiles.py \
			--input tiles/us_r8_walk.geojson \
			--output tiles/t_hex_r8_walk.pmtiles \
			--layer t_hex_r8_walk \
			--minzoom 8 --maxzoom 12 ; \
	fi

## Full pipeline now includes brand/category D_anchor so the API works out of the box
all:  ## Run the full data pipeline (tiles + D_anchor)
	@set -e; \
	START_TIME=$$(date +%s); \
	$(MAKE) tiles d_anchor_category d_anchor_brand; \
	END_TIME=$$(date +%s); \
	ELAPSED=$$(($$END_TIME - $$START_TIME)); \
	HOURS=$$(($$ELAPSED / 3600)); \
	MINUTES=$$((($$ELAPSED % 3600) / 60)); \
	SECONDS=$$(($$ELAPSED % 60)); \
	if [ $$HOURS -gt 0 ]; then \
		echo "✅ Full pipeline complete. Total time: $${HOURS}h $${MINUTES}m $${SECONDS}s"; \
	elif [ $$MINUTES -gt 0 ]; then \
		echo "✅ Full pipeline complete. Total time: $${MINUTES}m $${SECONDS}s"; \
	else \
		echo "✅ Full pipeline complete. Total time: $${SECONDS}s"; \
	fi

# ========== Housekeeping ==========

clean:  ## Clean all generated data files
	rm -rf data/osm/*.pbf data/overture/*.parquet data/poi/*.parquet data/minutes/*.parquet data/anchors/*.parquet data/d_anchor_category/*.parquet data/d_anchor_brand/*.parquet
	find data/d_anchor_category -type f -name '*.parquet' -delete 2>/dev/null || true
	find data/d_anchor_brand -type f -name '*.parquet' -delete 2>/dev/null || true
	rm -rf state_tiles/*.parquet state_tiles/*.csv state_tiles/.merge.stamp
	rm -rf tiles/*.geojson tiles/*.mbtiles tiles/*.pmtiles
	rm -rf data/osm/cache

serve: ## Serve the frontend + tiles via FastAPI (supports HTTP Range)
	@echo "Serving API + tiles at http://localhost:5173 (start Next.js separately: npm run dev in tiles/web)"
	.venv/bin/uvicorn api.main:app --host 0.0.0.0 --port 5173 --reload --env-file .env
