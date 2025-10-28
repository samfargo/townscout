# TownScout Architecture Overview

This repository powers the TownScout experience: a livability map that blends precomputed travel times, on-demand POI routing, and climate enrichment. The system is split into four cooperating layers:

1. **Data engineering pipeline** in `src/` that ingests reference data, builds the road graph/anchor sites, computes travel-time products, enriches hexes with climate metrics, and produces parquet + PMTiles artefacts.
2. **Native Rust kernels** in `townscout_native/` that accelerate the heaviest graph algorithms (multi-source K-best routing, contraction hierarchy helpers).
3. **FastAPI service** in `api/main.py` that exposes D_anchor lookups, catalog metadata, Google Places proxies, and static assets.
4. **Next.js frontend** in `tiles/web/` that renders the interactive map and composes GPU expressions from tiles + API responses.

The `README.md` captures product intent and data contracts. This document focuses on how the codebase is organized and how the pieces interconnect.

---

## Top-Level Layout

| Path | Purpose |
| --- | --- |
| `src/` | Python pipeline scripts and shared utilities. Files are numbered in execution order (01\* → 06\*). |
| `api/` | FastAPI application that serves the web demo, D_anchor API, and PMTiles. |
| `tiles/web/` | Next.js App Router project (MapLibre map, sidebar UI, API proxy routes). |
| `townscout_native/` | Rust crate compiled as a Python extension containing high-performance graph kernels. |
| `state_tiles/`, `tiles/`, `out/`, `data/` | Generated artefacts (H3 parquet, PMTiles, parquet datasets). |
| `docs/` | Additional specs (e.g., `WEATHER_IMPLEMENTATION.md`) and this architectural guide. |
| `tests/` | Pytest suite (currently climate parquet schema enforcement). |

---

## Data Pipeline (`src/`)

Pipeline scripts are designed to be run sequentially; each stage emits artefacts consumed by downstream steps and the API/UI.

### 1. Ingestion & Normalization

| Script | Responsibility | Key Outputs |
| --- | --- | --- |
| `src/01_download_extracts.py` | Downloads Geofabrik OSM PBF extracts and clips Overture Places via DuckDB. | `data/osm/<state>.osm.pbf`, `data/overture/ma_places.parquet` |
| `src/02_normalize_pois.py` | Loads Overture + OSM POIs, normalizes to the TownScout taxonomy (`src/taxonomy.py`), resolves brands, and deduplicates. | `data/poi/<state>_canonical.parquet` (geometry in WKB, brand/category columns) |

Supporting modules:

- `src/taxonomy.py` defines the canonical class → category → subcategory hierarchy, brand registry, and source tag mappings. Optional CSV/YAML files in `data/` override defaults. The taxonomy includes special handling for:
  - **Places of Worship**: OSM `amenity=place_of_worship` POIs are classified by their `religion` tag into separate categories: `place_of_worship_church` (Christian), `place_of_worship_synagogue` (Jewish), `place_of_worship_temple` (Hindu/Buddhist/Jain/Sikh), and `place_of_worship_mosque` (Muslim). This allows users to filter by specific worship types while OSM only provides the generic `place_of_worship` amenity tag.
  - **Libraries**: Mapped from OSM `amenity=library` and Overture `library` category.
- `src/osm_beaches.py` provides specialized beach classification that identifies ocean vs. lake beaches using spatial analysis. Beaches are classified into separate categories (`beach_ocean`, `beach_lake`, `beach_river`, `beach_other`) based on proximity to coastlines (150m) and inland water bodies (100m for lakes, 80m for rivers). This enables distinct frontend filter options for "Any Beach (Ocean)" and "Any Beach (Lake)".
- `src/geometry_utils.py` provides geometry hygiene utilities to prevent Shapely 2.x `create_collection` errors when building GeometryCollection or Multi* objects from mixed/invalid geometries. The `clean_geoms()` function filters out null, empty, and non-geometry objects before unary_union operations.
- `src/util_osm.py` wraps Geofabrik downloads used by step 01.

### 2. Anchor Generation & Graph Preparation

| Script | Responsibility | Notes |
| --- | --- | --- |
| `src/03_build_anchor_sites.py` | Snaps canonical POIs to road graph nodes (drive/walk), groups them into anchor sites with deterministic `anchor_int_id`s. | **Connectivity-aware snapping** (as of Oct 2024): queries k=10 nearest nodes and prefers nodes with ≥2 edges over poorly-connected nodes within 2x the nearest distance. This fixes issues like Logan Airport snapping to isolated service roads. Relies on CSR graph caches built by `graph/pyrosm_csr.py`; uses config snap radii. |
| `src/03_compute_minutes_per_state.py` | Builds/loads CSR graphs, maps anchors onto nodes, runs the native K-best kernel to compute `hex → anchor` travel times (T_hex), and writes parquet for each H3 resolution. | Imports helpers from `townscout_native` via `t_hex` module; optionally emits anchor site outputs for reuse. |
| `src/03c_compute_overlays.py`, `src/03d_compute_d_anchor.py`, `src/03e_compute_d_anchor_category.py` | Compute auxiliary overlays and D_anchor tables (anchor → nearest brand/category). | Share logic via `src/d_anchor_common.py`. Runtime limits (max_minutes, top_k) are loaded from `data/taxonomy/d_anchor_limits.json` to control SSSP cutoffs and result filtering per entity. |

Core graph helpers live in `src/graph/`:

- `graph/pyrosm_csr.py` builds CSR representations of the road network (forward + cached reverse).
- `graph/csr_utils.py` offers CSR transforms (transpose, connected components, etc.).
- `graph/anchors.py` ensures stable `anchor_int_id` assignment and node mappings.
- `graph/ch_cache.py` stores/loads contraction hierarchy caches (used when available).

D_anchor computation utilities in `src/d_anchor_common.py`:

- `load_d_anchor_limits()` loads runtime configuration from `data/taxonomy/d_anchor_limits.json` with entity-specific max_minutes and top_k values.
- `get_entity_limits()` retrieves limits for a specific brand or category, falling back to defaults.
- `write_shard()` implements top_k filtering and max_seconds cutoff when writing D_anchor parquet outputs, keeping only the nearest k sources per anchor within the time threshold.

The Rust extension in `townscout_native/` exposes:

- `kbest_multisource_bucket_csr` – multi-source bucketed Dijkstra that yields the K best anchors per node under primary/overflow cutoffs.
- `aggregate_h3_topk_precached` – aggregates node-level results into per-hex top-K tables using precomputed node→H3 mappings.
- `weakly_connected_components` and CH utilities used during D_anchor builds.

### 3. Power Corridor Overlay

| Script | Function |
| --- | --- |
| `src/03f_compute_power_corridors.py` | Queries high-voltage OSM `power=line` features (via Pyrosm), buffers them by 200 m, dissolves the corridor, intersects with the H3 grid at r7/r8, and writes per-hex `near_power_corridor` flags to `data/power_corridors/<state>_near_power_corridor.parquet`. Buffer distance and minimum voltage thresholds are CLI parameters so product changes do not require code edits. |

The merge step in `src/04_merge_states.py` consumes these parquet files and defaults missing values to `False`, ensuring tiles always expose the boolean expected by the frontend toggle.

### 4. Hex Tile Assembly

| Script | Function |
| --- | --- |
| `src/05_h3_to_geojson.py` | Converts the long-format T_hex parquet into H3 polygon NDJSON, preserving `a{i}_id` + `a{i}_s` arrays per hex. |
| `src/06_build_tiles.py` | Uses tippecanoe/PMTiles tooling to build multi-resolution vector tiles (`tiles/t_hex_r{7,8}_drive.pmtiles`). Layer IDs must match frontend constants. |

Generated tiles are tracked in `state_tiles/` (raw parquet) and `tiles/` (NDJSON + PMTiles). The FastAPI service streams these via HTTP range responses.

### 5. Climate Enrichment

- `src/climate/prism_normals_fetch.py` downloads PRISM climate normals (temperature/precipitation rasters).
- `src/climate/prism_to_hex.py` mosaics raster bands, computes zonal stats for each populated H3 hex, derives seasonal metrics, classifies climate typologies (`classify_climate_expr()`), and outputs quantized parquet at `out/climate/hex_climate.parquet`.
- Tests in `tests/test_climate_parquet.py` enforce dtype expectations on quantized columns.

Climate outputs are later joined to T_hex tiles so the map can expose climate metadata alongside travel times.

---

## FastAPI Service (`api/main.py`)

The service exposes data needed by the web client and can also serve tiles/static assets for local development.

### Startup & Static Assets

- Adds `src/` to `sys.path` for direct module imports.
- Mounts `tiles/web` for static files and implements `/tiles/{name}.pmtiles` with range-aware streaming so MapLibre can request vector tiles.
- CORS is permissive by default; narrow `allow_origins` for production.

### Key Endpoints

| Route | Handler | Purpose |
| --- | --- | --- |
| `/` | Redirects to configured frontend origin or returns API status JSON. |
| `/health` | Simple readiness probe. |
| `/api/categories` | Lists available category IDs (per mode) based on parquet partitions. |
| `/api/catalog` | Consolidates category metadata, brand registry names, and category→brand relationships by inspecting canonical POIs. Uses `data/taxonomy/category_label_to_id.json` to map POI category labels (e.g., "fast_food", "cafe") to category IDs, ensuring brands are properly associated with their parent categories in the `cat_to_brands` mapping. |
| `/api/places/autocomplete`, `/api/places/details` | Proxy Google Places Autocomplete + Details using `GOOGLE_PLACES_API_KEY`, adding validation and structured responses. |

**Graceful Mode Handling**: The API gracefully handles missing mode data (e.g., walk mode parquet files not yet computed). If walk mode data is unavailable, endpoints return empty results (`{}`) with warning logs rather than raising errors, allowing the application to continue functioning with available modes (typically drive mode).
| `/api/d_anchor` | Loads parquet shards under `data/d_anchor_category/`, merges requested categories, and returns `{anchor_id: seconds}` maps. |
| `/api/d_anchor_brand` | Same for brand partitions at `data/d_anchor_brand/`. |
| `/api/d_anchor_custom` | Runs on-the-fly routing for arbitrary lon/lat points by seeding temporary anchors and invoking the native kernel. |
| `/api/poi_points` | Emits anchor site centroids + metadata for debugging or visualization. |

The API uses Arrow datasets (`pyarrow.dataset`) for efficient parquet scans and caches label lookups in-memory. Sanitization helpers guard against invalid query parameters (e.g., `locationBias` parsing for Places).

---

## Frontend (`tiles/web/`)

The web client is a Next.js 13+ App Router project that renders a full-height map with a sidebar of filters.

### Layout & Rendering

- `app/page.tsx` composes the shell: sidebar (`app/(sidebar)/Sidebar.tsx`) + map canvas (`app/(map)/MapCanvas.tsx` lazily imported on the client).
- `app/layout.tsx` wires Tailwind, fonts, and global styles.
- API routes under `app/api/` (e.g., `catalog/route.ts`, `tiles/[...path]/route.ts`) proxy backend endpoints or stream PMTiles when deploying the Next.js app separately from FastAPI.

### State & Actions

- Global state lives in `lib/state/store.ts` (Zustand) tracking active POIs, per-filter sliders, mode selections, cached D_anchor maps, and climate selections. State is ephemeral and resets on page refresh to ensure clean initialization.
- `lib/actions/index.ts` holds the imperative bridge between UI state and the map:
  - Fetches catalog metadata and ensures caches are hydrated (`ensureCatalogLoaded`).
  - Adds/removes POIs (`addCategory`, `addBrand`, `addCustom`, `removePOI`) and keeps D_anchor caches in sync.
  - Applies GPU filters by coordinating with the map worker to build GPU expressions that are then applied to the MapLibre layers through the `MapController`.

### Map Integration

- `lib/map/MapController.ts` instantiates MapLibre, registers the PMTiles protocol, tracks active mode filters, and exposes hover callbacks. The base style defines r7/r8 PMTiles sources with default visibility and 0.4 opacity, ensuring that when no filters are active, all hexes are shaded (showing the full coverage area).
- `lib/map/map.worker.ts` builds GPU expressions in a Web Worker to combine multiple POI criteria. When multiple filters are active (e.g., airport + Costco), the worker uses **intersection logic** (MapLibre `'all'` expressions) so only hexes meeting ALL criteria are shown. This ensures that adding more criteria progressively narrows the livable area, as intended. Optional overlays (climate selections and the “Avoid power lines” toggle) are AND-ed into those expressions by inspecting tile properties such as `climate_label` and `near_power_corridor`.
- The MapController maintains a singleton worker instance and applies expression updates via RAF-coalesced batches to minimize render thrashing during slider interactions.

### Sidebar & UI Components

- `app/(sidebar)/SearchBox.tsx` fetches catalog data and Google Places suggestions (using TanStack Query), dispatches add actions, and exposes the climate typology dropdown plus the “Avoid power lines” toggle.
- `app/(sidebar)/FiltersPanel.tsx` renders sliders per active POI, debouncing updates before invoking `updateSlider`.
- `app/(sidebar)/HoverBox.tsx` summarizes hover details: travel times computed client-side using the same anchor combination logic, decoded climate stats, and a callout when the hovered hex lies within a buffered power corridor.
- Shared UI primitives live in `components/ui/` (Tailwind + Radix-inspired shorthands).

Supporting services:

- `lib/services/api.ts` resolves backend URLs based on environment variables (`NEXT_PUBLIC_TOWNSCOUT_API_BASE_URL`, etc.) and enforces JSON parsing with optional Zod validation.
- `lib/services/catalog.ts`, `lib/services/dAnchor.ts`, `lib/services/places.ts` encapsulate API fetches with schema validation where appropriate.
- Utilities in `lib/utils/` (debounce, numbers, className helpers) keep UI logic concise.

### Styling & Tooling

- Tailwind CSS is configured in `tailwind.config.ts` with CSS variables for the faux-aged palette.
- Vitest config and ESLint rules live alongside the Next.js project (e.g., `vitest.config.ts`, `.eslintrc.js`).
- `pmtiles.js` ships the bundled PMTiles protocol script when the frontend needs to self-host tiles.

---

## Climate Overlay Flow

1. `src/climate/prism_normals_fetch.py` downloads PRISM raster normals by variable/month.
2. `src/climate/prism_to_hex.py` reads H3 IDs from the travel-time parquet (`data/minutes/*_drive_t_hex.parquet` by default), builds GeoJSON polygons, runs zonal statistics on the PRISM rasters, derives seasonal averages, quantizes values, classifies them into typologies, and writes `out/climate/hex_climate.parquet`.
3. Downstream tooling (not shown here) merges the climate parquet into tile generation so each hex feature includes:
   - Quantized fields (`*_f_q`, `*_in_q`) for compact storage (kernel expects scaling factors like 0.1°F or 0.1").
   - `climate_label` matching `CLIMATE_TYPOLOGY` used by the frontend for filtering.
4. The map UI uses `HoverBox` + climate actions to surface climate data and optionally mask tiles through expressions (logic currently lives server-side; see `setClimateSelections` for integration points).

---

## Native Kernels (`townscout_native/`)

The Rust crate compiles to a Python module (`t_hex`) available to scripts in `src/`. Key modules:

- `lib.rs` – entry point exposing PyO3 bindings for K-best search, H3 aggregation, and helper algorithms. It handles label insertion logic, multi-threading via Rayon, and sentinel management (`65535` as unreachable).
- `ch.rs` – contraction hierarchy utilities used by the API when computing on-demand routes or building caches.

Cargo builds drop artefacts into `townscout_native/target/`; ensure the library is built (`maturin develop` or similar) before running heavy pipeline steps.

---

## Testing & Validation

- `tests/test_climate_parquet.py` ensures that climate parquet quantized columns use the expected integer types. Extend this area with additional schema or regression tests as new datasets are introduced.
- FastAPI endpoints can be exercised with `make api` (if defined) or via `uvicorn api.main:app`. Pair with `npm run dev` in `tiles/web` for end-to-end testing.

---

## Typical Local Workflow

1. **Bootstrap data**: run `src/01_download_extracts.py` → `src/02_normalize_pois.py`.
2. **Build anchors & travel times**: `src/03_build_anchor_sites.py`, `src/03_compute_minutes_per_state.py`, and the D_anchor scripts for categories/brands.
3. **Refresh overlays**: `make power_corridors` (or invoke `src/03f_compute_power_corridors.py` directly) and `make climate` to update `data/power_corridors/*.parquet` plus `out/climate/hex_climate.parquet`.
4. **Generate tiles**: `src/05_h3_to_geojson.py` + `src/06_build_tiles.py` (or follow Makefile recipes if present).
5. **Serve backend**: `uvicorn api.main:app --reload` (expects data directories populated).
6. **Run frontend**: `cd tiles/web && npm install && npm run dev`. Set `NEXT_PUBLIC_TOWNSCOUT_API_BASE_URL` if the API runs on a non-default host/port.

Subsequent development typically touches a single layer (e.g., adjusting taxonomy, tuning D_anchor kernels, or iterating on the React UI) but the data contracts described in `README.md` keep cross-layer dependencies explicit.

---

## Extending the System

- **Adding new POI categories/brands**: extend `src/taxonomy.py` or the override files in `data/`, regenerate canonical POIs, rebuild anchors, rerun D_anchor scripts, and refresh the catalog API.
- **Supporting additional states/modes**: update `config.py` (`STATES`, snap radii, H3 resolutions), ensure download scripts clip the desired region, and regenerate all pipeline outputs. Anchors will inherit stable IDs as long as the same `site_id` hashing strategy is used.
- **New overlays (e.g., crime, schools)**: model after the climate and power-corridor flows—write a script that enriches H3 hexes, emit parquet keyed by `h3_id`/`res`, and merge outputs before tile generation so the frontend can consume the new attributes.
- **Frontend experiments**: reuse `lib/actions` to keep map expressions consistent. Any new filter that depends on D_anchor data should populate the cache structure (`dAnchorCache`) and invoke `applyCurrentFilter`.

Use this document as a map when onboarding new contributors or when tracing a data flow end-to-end; it highlights which modules own each responsibility and how artefacts move between layers.
