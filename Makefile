PY=PYTHONPATH=. .venv/bin/python
STATES=massachusetts new-hampshire rhode-island connecticut maine

.PHONY: help init pbf pois minutes minutes-full minutes-par merge geojson tiles csv all clean delta boundaries crime-rates test

help:  ## Show this help message
	@echo "TownScout MVP - Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-12s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

init:  ## Initialize virtual environment and install dependencies
	@if command -v python3.12 >/dev/null 2>&1; then \
		python3.12 -m venv .venv; \
	else \
		python3 -m venv .venv; \
	fi && . .venv/bin/activate && pip install -r requirements.txt

pbf:  ## Download OSM PBF extracts for all states
	$(PY) src/01_download_osm_extracts.py

pois:  ## Extract POI data from PBF files
	$(PY) src/02_extract_pois_from_pbf.py

boundaries:  ## Download TIGER/Line municipal boundaries
	$(PY) src/08_download_boundaries.py

minutes:  ## Compute travel times (delta by default; falls back to full if needed)
	@set -e; \
	if [ -s data/deltas/poi_delta.csv ]; then \
		echo "Delta file found. Checking for existing minutes..."; \
		ok=1; \
		for s in $(STATES); do \
			[ -f data/minutes/$$s_r7.parquet ] && [ -f data/minutes/$$s_r8.parquet ] || ok=0; \
		done; \
		if [ $$ok -eq 1 ]; then \
			echo "Applying delta recompute (default)"; \
			$(PY) src/03b_delta_recompute.py; \
			$(PY) src/04_merge_states.py; \
			$(PY) src/05_h3_to_geojson.py; \
			$(PY) src/06_build_tiles.py; \
		else \
			echo "Minutes outputs missing; running full recompute"; \
			$(PY) src/03_compute_minutes_per_state.py; \
		fi; \
	else \
		echo "No delta file; running full recompute"; \
		$(PY) src/03_compute_minutes_per_state.py; \
	fi

minutes-full:  ## Force full recompute of travel times for all states
	$(PY) src/03_compute_minutes_per_state.py

minutes-par:  ## Compute travel times (parallel processing)
	$(PY) src/03a_parallel_driver.py

crime-rates:  ## Enrich H3 hexes with crime rate data
	$(PY) src/09_enrich_crime_rates.py

merge:  ## Merge state results into national datasets
	$(PY) src/04_merge_states.py

geojson:  ## Convert H3 data to GeoJSON
	$(PY) src/05_h3_to_geojson.py

tiles:  ## Build vector tiles (requires tippecanoe & pmtiles CLI)
	$(PY) src/06_build_tiles.py

csv:  ## Export CSV files for Pro feature
	$(PY) src/07_export_csv.py

test:  ## Run validation tests for crime rate integration
	$(PY) src/test_crime_integration.py

all: pbf pois boundaries minutes-full crime-rates merge geojson tiles  ## Run complete pipeline

delta:  ## Apply POI deltas and rebuild merged, geojson, and tiles
	$(PY) src/03b_delta_recompute.py
	$(PY) src/04_merge_states.py
	$(PY) src/09_enrich_crime_rates.py
	$(PY) src/05_h3_to_geojson.py
	$(PY) src/06_build_tiles.py

clean:  ## Clean generated data files
	rm -rf data/osm/*.pbf data/poi/*.parquet data/minutes/*.parquet data/boundaries/
	rm -rf state_tiles/*.parquet tiles/*.geojson tiles/*.mbtiles tiles/*.pmtiles 