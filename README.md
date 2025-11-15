# vicinity: System Overview & LLM Implementation Spec

> Architectural map: `docs/ARCHITECTURE_OVERVIEW.md`

## 1) Purpose, Context, Goals

**Purpose.** Vicinity helps a user answer: *"Where should I live given my criteria?"* by computing travel‑time proximity to things that matter (Chipotle, Costco, airports, schools, etc.) and rendering results as a fast, interactive map.

**Context.** Instead of precomputing every hex→category path, vicinity factorizes the problem into **hex→anchor** and **anchor→category/brand** legs. The frontend combines these in real time with GPU expressions using per‑anchor seconds stored in tiles and per‑anchor category/brand seconds served by the API. This keeps costs near‑zero and makes adding thousands of POIs (brands and categories) cheap.

**Primary Goals.**

* Each time a POI from OSM or Overture is added, the livable land for that user visually shrinks based on what the filter was.
* Sub‑second (≤250ms) slider→map response with zero server round‑trips.
* Add/extend categories without recomputing hex tiles.
* Keep deployment simple: static tiles on CDN, thin API for D\_anchor.
* Scale from a single state to nationwide without blowing up storage/compute.

**Non‑Goals.** Live traffic, multimodal chaining (walk→transit→drive), and route turn‑by‑turn.

**Important Limitation.** vicinity uses a matrix factorization approach (`hex→anchor + anchor→POI`) that can violate triangle inequality in complex road networks. This may produce routing approximations that differ significantly from true shortest paths, especially in suburban/rural areas. See `docs/ARCHITECTURE_OVERVIEW.md` for details and proposed mitigation strategies.

---

## Current Implementation Status

The system is fully implemented with a modular architecture. Key components include:
- **T_hex computation** with Rust native kernels for performance
- **Anchor-based architecture** enabling thousands of POI filters without tile changes
- **Modular POI processing** with clean separation between shared logic and domain-specific handlers
- **Climate and overlay data** integrated into hex summaries

For detailed implementation status, technical specifications, and data contracts, see `docs/ARCHITECTURE_OVERVIEW.md`.

---

## Core Model: Matrix Factorization

vicinity uses a two-stage approach to compute travel times:

```
Total Travel Time = T_hex[hex→anchor] + D_anchor[anchor→category]
```

This factorization enables efficient real-time filtering by precomputing hex-to-anchor times and computing anchor-to-POI times on-demand. The frontend combines these using GPU expressions for sub-second response times.

---

## Data Sources

vicinity combines multiple data sources for comprehensive POI coverage:
- **OSM** for road networks and civic/natural POIs
- **Overture Places** for commercial brands and businesses  
- **Curated CSVs** for specialized data (airports, trauma centers)

The hybrid approach provides better coverage than any single source alone.

---

## System Architecture

The system consists of four main layers:
1. **Data pipeline** (`src/`) - Ingests data, builds road graphs, computes travel times
2. **Native kernels** (`vicinity_native/`) - High-performance Rust algorithms
3. **API service** (`api/`) - Serves D_anchor lookups and metadata
4. **Frontend** (`tiles/web/`) - Interactive map with real-time filtering

For detailed architecture information, see `docs/ARCHITECTURE_OVERVIEW.md`.

---

## Data Contracts

The system maintains strict contracts between components to ensure reliable operation. Key contracts include:
- **T_hex tiles** with anchor arrays and H3 geometry
- **D_anchor API** returning travel times with sentinel values
- **Frontend GPU expressions** for real-time filtering

For detailed data contracts and technical specifications, see `docs/ARCHITECTURE_OVERVIEW.md`.

### Quality Control

vicinity includes automated validation to catch data quality issues early:

**Automated Tests** (run with `pytest tests/`):
- `test_poi_schema.py` - Validates POI parquet schema, datatypes, and taxonomy coverage
- `test_anchor_contract.py` - Validates anchor uniqueness, modes, and POI linkage
- `test_t_hex_contract.py` - Validates travel time arrays, anchor references, and sentinel usage

**Validation Scripts** (run before releases):
- `scripts/check_d_anchor_stats.py` - Validates D_anchor shards and enforces P95 <= 7200s
- `scripts/check_tile_schema.py` - Validates PMTiles against contract in `docs/tile_contract.json`
- `scripts/validate_golden_drivetime.py` - Compares computed times against hand-verified golden dataset
- `scripts/update_source_ledger.py` - Tracks source file hashes and detects staleness/corruption

**Quick Validation:**
```bash
# Run all tests
pytest tests/

# Run validation scripts
python scripts/check_d_anchor_stats.py
python scripts/check_tile_schema.py
python scripts/validate_golden_drivetime.py

# Update source ledger
python scripts/update_source_ledger.py --auto-scan
```

For comprehensive QA documentation, see `docs/quality_control_infra.md`.

---

## POI Module Architecture

The system uses a modular architecture for POI processing:
- **Shared POI logic** in `vicinity/poi/` for ingestion, normalization, and conflation
- **Domain-specific handlers** in `vicinity/domains_poi/` for specialized POI types
- **Overlay computation** in `vicinity/domains_overlay/` for non-routable hex enrichment:
  - **Climate** - Temperature and precipitation metrics from PRISM normals
  - **Power corridors** - High-voltage transmission line proximity flags
  - **Political lean** - County-level 2024 presidential election results mapped to H3 cells

The architecture supports both category-based (e.g., "supermarket") and brand-based (e.g., "Whole Foods") queries, with configurable runtime limits and comprehensive coverage of livability-relevant categories.

## Documentation

- [Architecture Overview](docs/ARCHITECTURE_OVERVIEW.md) - Complete system design
- [Anchor Selection Strategy](docs/ANCHORS.md) - How anchors are chosen and scaled
- [Quality Control Infrastructure](docs/quality_control_infra.md) - Automated validation and testing
- [Bug Fixes Changelog](docs/BUG_FIXES_CHANGELOG.md) - History of major bug fixes and quality improvements
- [Power Corridors](docs/POWER_CORRIDORS.md) - High-voltage transmission line avoidance feature
- [Routing Approximation Quality](docs/ROUTING_APPROXIMATION_QUALITY.md) - Analysis of triangle inequality limitations

For detailed module structure and implementation, see the architecture overview.

## Known Issues & Fixes

### Graph Cache Validation (Fixed 2025-11-05)
A vulnerability in the graph cache loading mechanism allowed stale CSR graphs to be loaded when the source PBF file was updated, causing data corruption in D_anchor computations. This has been **fixed** with automatic cache validation based on PBF modification times. See `docs/BUG_FIXES_CHANGELOG.md` for details.

---

## Quick Start

- Create environment and install deps: `make init`
- Build native ext: `make native`
- Download data and normalize POIs: `make pois`
- Build anchor sites: `make anchors`
- Compute minutes (T_hex long format): `make minutes`
- Build climate parquet: `make climate`
- Compute power-corridor overlays: `make power_corridors`
- Compute D_anchor category tables: `make d_anchor_category`
- Compute D_anchor brand tables: `make d_anchor_brand`
- Merge + summarize, build tiles, and bring up the stack:
  - `make merge tiles` then `make serve` (FastAPI on `http://127.0.0.1:5173`)
  - In a new terminal: `cd tiles/web && npm install && npm run dev`
  - Visit `http://localhost:3000` (Next.js) — the app will call back to the FastAPI service on port 5173 unless you override `NEXT_PUBLIC_vicinity_API_BASE_URL`

### Full Pipeline Command
```bash
make pois anchors minutes climate power_corridors d_anchor_category d_anchor_brand merge tiles
```

### Vector Basemap (Planetiler)
- `make vector_basemap` wraps `scripts/build_vector_basemap.sh` and `planetiler-openmaptiles.jar` to produce `tiles/vicinity_basemap.pmtiles`.
- Override `PLANETILER_OSM`, `PLANETILER_AREA`, `PLANETILER_HEAP`, or `PLANETILER_OUTPUT` inline when building (e.g., `PLANETILER_OSM=data/osm/massachusetts.osm.pbf PLANETILER_AREA=us/massachusetts make vector_basemap`).
- See `docs/vector_basemap_plan.md` for prerequisites (JDK 21+, disk/memory guidance) and styling workflows once the PMTiles file is generated.

### Remote Compute Offloading (GCP)

For computationally expensive steps like `d_anchor_category`, you can offload to Google Cloud Platform:

```bash
make categories_remote
```

This orchestrator script (`scripts/run_remote.sh`):
- Packages your local repo with `git archive HEAD` and uploads to GCS
- Spins up an ephemeral c4d-highcpu-96 VM (96 cores, 500 GB disk by default)
- Runs the full setup: installs dependencies, builds Rust native extensions, downloads data
- Executes `make d_anchor_category` on the VM (~8–10 hours for Massachusetts when CPU-bound)
- Syncs results back to `data/categories_results/<timestamp>/`
- Automatically terminates the VM and cleans up

**Prerequisites:**
- GCP project with Compute Engine API enabled
- Service account with permissions for VM creation and GCS access
- GCS bucket for artifacts (e.g., `gs://vicinity-batch-<project-id>`)
- Configure environment variables in `Makefile`:
  - `PROJECT_ID`, `ZONE`, `BUCKET`, `SERVICE_ACCOUNT`

**Monitoring:**
- Serial console logs: `logs/remote_runs/<timestamp>-serial.log`
- Build logs: `data/categories_results/<timestamp>/build.log`
- Watch progress: `tail -f logs/remote_runs/<timestamp>-serial.log`
- Telemetry: the startup script runs `vmstat`/`mpstat` at `TELEMETRY_INTERVAL` seconds (default 5s) in the background, prefixing their rows with `[telemetry][vmstat]` / `[telemetry][cpu]` so CPU utilization and `%wa` are captured in the serial log for both `categories_remote` and `pipeline_remote`. Use `python scripts/analyze_telemetry.py logs/remote_runs/<run>-serial.log --after "starting make d_anchor_category"` to summarize averages.
- Phase timings: look for `[timer] ...` entries in the serial log to see how long apt, pip, Rust build, and the `make` target took end-to-end.
- Verbose apt/pip/rustup output is muted from the serial stream; check `data/categories_results/<timestamp>/startup_detail.log` (synced from `${RESULTS_PREFIX}/startup_detail.log`) for the full transcript if a dependency step fails.

The VM automatically handles all dependencies (Python packages, Rust toolchain, DuckDB CLI) and ensures reproducible builds. Results are downloaded automatically when the job completes.

**Recommended 96-vCPU config**
- `MACHINE_TYPE=c4d-highcpu-96`
- `BOOT_DISK_SIZE_GB=500` (use 1 TB if you need >300 MB/s sustained disk throughput)
- `THREADS=1` with `OMP_NUM_THREADS/MKL_NUM_THREADS/OPENBLAS_NUM_THREADS/NUMEXPR_{NUM,MAX}_THREADS=1` (already enforced by the startup script)
- `WORKERS=32` (≈ vCPU/3) to keep each ProcessPool worker single-threaded while filling the box
- `TELEMETRY_INTERVAL=5` (seconds between `vmstat`/`mpstat` samples; raise this if you want fewer log lines)
- **Worker sweep:** to quantify utilization, run a small series of jobs (e.g. `WORKERS=8/16/32 make categories_remote`) and feed each serial log through `scripts/analyze_telemetry.py`. Capture wall clock (`orchestrator` already prints it) plus the average `usr/sys/idle/wa` values before deciding how far to push concurrency.

**Worker tuning checklist**
1. Kick off `make categories_remote` (defaults above are pre-wired) and open `htop`, `vmstat 1`, and `free -h` via the serial log.
2. Expect CPU utilization ≥90 % and `wa` <5 % when the run is CPU-bound. If CPU drops below ~70 % and `wa` stays low, terminate the VM and rerun with `WORKERS` bumped by 4–8.
3. Stop increasing `WORKERS` when either `wa` spikes (disk-throttled) or free memory falls under ~25 % headroom (each worker peaks at ~2.5–3 GiB RSS while writing parquet shards).
4. Capture wall-clock runtime, average CPU%, and average `wa` from each run (the serial log already timestamps every phase) so you can compare c4d-highcpu-32 / WORKERS=12 runs (~17–18 h for MA) against the c4d-highcpu-96 / WORKERS=32 runs (~8–10 h). A 1.8–2.1× speed-up is typical when the workload stays CPU-bound.

### Category & Brand Scope (Stay Focused)
- **Categories**: edit `data/taxonomy/POI_category_registry.csv` (columns: `category_id`, `numeric_id`, `display_name`). Any category in the CSV is automatically allowlisted for anchors and precomputation. Numeric IDs are explicit to prevent drift.
- **Brands**: edit `data/taxonomy/POI_brand_registry.csv` (columns: `brand_id`, `canonical`, `aliases`, `wikidata`). Any brand in the registry is automatically allowlisted for anchors and precomputation.
- **Tip**: Use `--threads` and consider a smaller `--overflow-cutoff` (e.g., 60) for faster runs on laptops.

### Coverage Optimization
For maximum POI coverage (especially dense brands):
1. Add brands to `data/taxonomy/POI_brand_registry.csv` with comprehensive aliases for name matching
2. Compute `make d_anchor_brand` for all brands in the registry (or use threshold)
3. Increase K-best parameters (`--k-best 20+`) for dense urban areas if needed

API:
- Run: `uvicorn api.main:app --reload --host 0.0.0.0 --port 5173` (or expose the same base URL you pass via `NEXT_PUBLIC_vicinity_API_BASE_URL`)
- Categories: `GET /api/categories?mode=drive`
- D_anchor slice: `GET /api/d_anchor?category=<id>&mode=drive`
- D_anchor brand slice: `GET /api/d_anchor_brand?brand=<id or alias>&mode=drive`
- Custom point (escape hatch): `GET /api/d_anchor_custom?lon=<lon>&lat=<lat>&mode=drive`
 
Prerequisites for Overture clipping: install the DuckDB CLI (`duckdb`) and ensure it is on your PATH. The downloader (`src/01_download_extracts.py`) invokes the DuckDB command.

---

## Climate Data Fields

PRISM-derived climate metrics are stored as quantized integers to keep parquet and tile payloads compact. Any column ending with `_q` follows these rules:

- `*_f_q`: Temperatures in tenths of degrees Fahrenheit. Decode with `value / 10`.
- `*_mm_q`: Precipitation totals in tenths of millimetres. Decode with `value / 10`.
- `*_in_q`: Precipitation totals in tenths of inches. Decode with `value / 10`.

The scale factors are also recorded in the `out/climate/hex_climate.parquet` metadata under the `vicinity_prism` key for downstream analytics.

### Climate Data Validation

Before using climate data in tiles, verify these key properties:

1. **Seasonal Pattern**: Monthly temperatures should follow natural progression:
   - Winter (Dec-Feb): Coldest months
   - Spring (Mar-May): Warming trend
   - Summer (Jun-Aug): Peak temperatures
   - Fall (Sep-Nov): Cooling trend

2. **Expected Ranges** (Massachusetts example):
   - January mean: ~25-35°F (coastal warmer)
   - July mean: ~68-74°F
   - Annual mean: ~45-52°F
   - Annual precipitation: ~42-50 inches

3. **File Mapping**: Ensure PRISM files map to correct months:
   - Use exact patterns like `*_202001_*.tif` for January
   - Avoid ambiguous patterns that could match wrong months
   - Sort matches for deterministic selection

4. **Data Integrity**:
   - No null values in climate columns
   - Temperatures and precipitation within realistic ranges
   - Consistent units across all hexes
