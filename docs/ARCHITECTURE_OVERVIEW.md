# vicinity Architecture Overview

This repository powers the vicinity experience: a livability map that blends precomputed travel times, on-demand POI routing, and climate enrichment. The system is split into four cooperating layers:

1. **Data engineering pipeline** in `src/` that ingests reference data, builds the road graph/anchor sites, computes travel-time products, enriches hexes with climate metrics, and produces parquet + PMTiles artifacts.
2. **Native Rust kernels** in `vicinity_native/` that accelerate the heaviest graph algorithms (multi-source K-best routing, contraction hierarchy helpers).
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
| `vicinity_native/` | Rust crate compiled as a Python extension containing high-performance graph kernels. |
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
| `src/02_normalize_pois.py` | Loads Overture + OSM POIs, normalizes to the vicinity taxonomy (`data/taxonomy/taxonomy.py`), resolves brands, and deduplicates. | `data/poi/<state>_canonical.parquet` (geometry in WKB, brand/category columns) |

Supporting modules:

- `data/taxonomy/taxonomy.py` defines the canonical class → category → subcategory hierarchy, brand registry, and source tag mappings. Optional CSV/YAML files in `data/taxonomy/` override defaults. The taxonomy includes special handling for:
  - **Places of Worship**: OSM `amenity=place_of_worship` POIs are classified by their `religion` tag into separate categories: `place_of_worship_church` (Christian), `place_of_worship_synagogue` (Jewish), `place_of_worship_temple` (Hindu/Buddhist/Jain/Sikh), and `place_of_worship_mosque` (Muslim). This allows users to filter by specific worship types while OSM only provides the generic `place_of_worship` amenity tag.
  - **Libraries**: Mapped from OSM `amenity=library` and Overture `library` category.
- `src/geometry_utils.py` provides geometry hygiene utilities to prevent Shapely 2.x `create_collection` errors when building GeometryCollection or Multi* objects from mixed/invalid geometries. The `clean_geoms()` function filters out null, empty, and non-geometry objects before unary_union operations. Used extensively in power corridor processing to work around pyrosm compatibility issues.
- `src/util_osm.py` wraps Geofabrik downloads used by step 01.
- `vicinity/osm/pyrosm_utils.py` provides robust OSM data extraction across pyrosm versions with automatic fallback to alternative APIs and ensures requested tag columns are always present in returned DataFrames (critical for power corridor voltage extraction).

**New Module Structure (Oct 2024 Refactoring):**

The POI processing logic has been reorganized into a clean modular architecture under `vicinity/`:

- **`vicinity/poi/`** - Shared POI ingestion, normalization, and conflation
  - `schema.py` - Canonical POI schema definition and validators
  - `ingest_osm.py` - OSM data loading via Pyrosm with taxonomy-driven filtering
  - `ingest_overture.py` - Overture data loading from parquet with WKB geometry conversion
  - `normalize.py` - Source-specific normalization to canonical schema (brand resolution, category mapping)
  - `conflate.py` - Multi-source deduplication using H3-based spatial heuristics
  - `snap.py` - Connectivity-aware snapping (placeholder for future enhancements)

- **`vicinity/domains_poi/`** - Domain-specific POI handlers with custom processing
  - `airports/` - Curated CSV loading from `Future/airports_coordinates.csv`, arterial road snapping logic
  - `beaches/` - Beach classification using Overture water spatial analysis (ocean/lake/river/other)
  - `trauma/` - ACS Level 1 trauma center ingestion via HTTP API with state-level filtering

- **`vicinity/domains_overlay/`** - Per-hex overlay computation (not route-to-able)
  - `power_corridors/` - High-voltage transmission line proximity flags from OSM power=line (uses GeoPandas OSM driver due to pyrosm/Shapely 2.x compatibility issues; implements iterative union for buffered corridor geometry)
  - `climate/` - PRISM normals quantization, seasonal aggregation, climate typology classification
  - `politics/` - County-level 2024 U.S. Presidential election results with Republican vote share and political lean bucketing (0-4: Strong Democrat to Strong Republican)

Overlay processing is organized under `vicinity/domains_overlay/` with CLI entry points for each domain:
- **`vicinity/domains_overlay/climate/`** - Climate data ingestion and processing
  - `prism_normals_fetch.py` - Downloads PRISM climate normals
  - `prism_to_hex.py` - CLI wrapper that processes rasters to per-hex climate metrics
- **`vicinity/domains_overlay/power_corridors/`** - Power corridor data processing
  - `osm_to_hex.py` - CLI wrapper that computes per-hex power corridor proximity flags
- **`vicinity/domains_overlay/politics/`** - Political lean data processing
  - `politics_to_hex.py` - CLI wrapper that processes 2024 presidential election results from MIT Election Lab and joins county polygons to H3 cells

**Power Corridor Processing Notes:**
- Extracts high-voltage transmission lines (power=line with voltage ≥100kV) from OSM PBF files
- Uses GeoPandas OSM driver instead of pyrosm to avoid Shapely 2.x compatibility issues with ufunc 'create_collection' errors
- Parses OSM `other_tags` column to extract voltage and power type information
- Buffers power lines by 200m (configurable) and dissolves using iterative union approach for Shapely 2.x compatibility
- Flags H3 hexes within buffer zone using vectorized pandas operations for performance
- **Massachusetts results**: 2,582 high-voltage transmission lines identified, 1,744 hexes flagged (~2.06% of state coverage)

The existing pipeline scripts (`src/02_normalize_pois.py`) and overlay CLI entry points (`vicinity/domains_overlay/*/`) act as thin CLI wrappers that import from the vicinity modules, maintaining backward compatibility while providing a clean separation of concerns. This modular design makes it straightforward to add new POI types or overlays without modifying core pipeline logic.

### 2. Anchor Generation & Graph Preparation

| Script | Responsibility | Notes |
| --- | --- | --- |
| `src/03_build_anchor_sites.py` | Snaps canonical POIs to road graph nodes (drive/walk), groups them into anchor sites with deterministic `anchor_int_id`s. | **Connectivity-aware snapping** (as of Oct 2024): queries k=10 nearest nodes and prefers nodes with ≥2 edges over poorly-connected nodes within 2x the nearest distance. This fixes issues like Logan Airport snapping to isolated service roads. Relies on CSR graph caches built by `graph/pyrosm_csr.py`; uses config snap radii. |
| `src/04_compute_minutes_per_state.py` | Builds/loads CSR graphs, maps anchors onto nodes, runs the native K-best kernel to compute `hex → anchor` travel times (T_hex), and writes parquet for each H3 resolution. | Imports helpers from `vicinity_native` via `t_hex` module; optionally emits anchor site outputs for reuse. |
| `src/05_compute_d_anchor.py`, `src/06_compute_d_anchor_category.py` | Compute D_anchor tables (anchor → nearest brand/category). | Share logic via `src/d_anchor_common.py`. Runtime limits (max_minutes, top_k) are loaded from `data/taxonomy/d_anchor_limits.json` to control SSSP cutoffs and result filtering per entity. |

Core graph helpers live in `src/graph/`:

- `graph/pyrosm_csr.py` builds CSR representations of the road network (forward + cached reverse).
- `graph/csr_utils.py` offers CSR transforms (transpose, connected components, etc.).
- `graph/anchors.py` ensures stable `anchor_int_id` assignment and node mappings.
- `graph/ch_cache.py` stores/loads contraction hierarchy caches (used when available).

D_anchor computation utilities in `src/d_anchor_common.py`:

- `load_d_anchor_limits()` loads runtime configuration from `data/taxonomy/d_anchor_limits.json` with entity-specific max_minutes and top_k values.
- `get_entity_limits()` retrieves limits for a specific brand or category, falling back to defaults.
- `write_shard()` implements top_k filtering and max_seconds cutoff when writing D_anchor parquet outputs, keeping only the nearest k sources per anchor within the time threshold.

The Rust extension in `vicinity_native/` exposes:

- `kbest_multisource_bucket_csr` – multi-source bucketed Dijkstra that yields the K best anchors per node under primary/overflow cutoffs.
- `aggregate_h3_topk_precached` – aggregates node-level results into per-hex top-K tables using precomputed node→H3 mappings.
- `weakly_connected_components` and CH utilities used during D_anchor builds.

### 3. Overlay Data Processing

| Script | Function |
| --- | --- |
| `vicinity/domains_overlay/power_corridors/osm_to_hex.py` | Queries high-voltage OSM `power=line` features (via Pyrosm), buffers them by 200 m, dissolves the corridor, intersects with the H3 grid at r7/r8, and writes per-hex `near_power_corridor` flags to `data/power_corridors/<state>_near_power_corridor.parquet`. Buffer distance and minimum voltage thresholds are CLI parameters so product changes do not require code edits. |

The merge step in `src/07_merge_states.py` consumes these parquet files and defaults missing values to `False`, ensuring tiles always expose the boolean expected by the frontend toggle.

**Politics overlay:**
- Loads 2024 U.S. Presidential election results from MIT Election Lab dataset (`vicinity/domains_overlay/politics/countypres_2000-2024.csv`)
- Calculates Republican vote share per county as `rep_votes / (rep_votes + dem_votes)`
- Assigns counties to political lean buckets:
  - 0: Strong Democrat (0.0-0.2 Rep share)
  - 1: Lean Democrat (0.2-0.4)
  - 2: Moderate (0.4-0.6)
  - 3: Lean Republican (0.6-0.8)
  - 4: Strong Republican (0.8-1.0)
- Downloads Census TIGER county boundaries if not present
- Joins county polygons to H3 cells at r7/r8 resolutions using H3 polyfill
- Outputs `data/politics/<state>_political_lean.parquet` with columns: `h3_id`, `res`, `political_lean` (uint8), `rep_vote_share` (float32), `county_fips`, `county_name`
- Hexes without political data (water, unpopulated areas, etc.) have null values and are excluded when the filter is active

### 4. Hex Tile Assembly

| Script | Function |
| --- | --- |
| `src/08_h3_to_geojson.py` | Converts the long-format T_hex parquet into H3 polygon NDJSON, preserving `a{i}_id` + `a{i}_s` arrays per hex. |
| `src/09_build_tiles.py` | Uses tippecanoe/PMTiles tooling to build multi-resolution vector tiles (`tiles/t_hex_r{7,8}_drive.pmtiles`). Layer IDs must match frontend constants. |

Generated tiles are tracked in `state_tiles/` (raw parquet) and `tiles/` (NDJSON + PMTiles). The FastAPI service streams these via HTTP range responses.

### 5. Overlay Enrichment (Climate & Power Corridors)

Climate and power corridor overlays are processed as auxiliary data enrichment:

- `vicinity/domains_overlay/climate/prism_normals_fetch.py` downloads PRISM climate normals (temperature/precipitation rasters).
- `vicinity/domains_overlay/climate/prism_to_hex.py` - CLI wrapper for climate overlay logic. Mosaics raster bands, computes zonal stats for each populated H3 hex, derives seasonal metrics, classifies climate typologies (via `classify_climate_expr()`), and outputs quantized parquet at `out/climate/hex_climate.parquet`.
- `vicinity/domains_overlay/power_corridors/osm_to_hex.py` - CLI wrapper for power corridor overlay logic. Processes OSM power infrastructure data to generate per-hex proximity flags.
- Tests in `tests/test_climate_parquet.py` enforce dtype expectations on quantized columns.
- Validation functions in `vicinity.domains_overlay.climate.climate_validation` check for reasonable temperature/precipitation ranges and seasonal patterns.

These overlay outputs are later joined to T_hex tiles so the map can expose climate and power corridor metadata alongside travel times.

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

- Global state lives in `lib/state/store.ts` (Zustand) tracking active POIs, per-filter sliders, mode selections, cached D_anchor maps, climate selections, and political lean range. State is ephemeral and resets on page refresh to ensure clean initialization.
- `lib/actions/index.ts` holds the imperative bridge between UI state and the map:
  - Fetches catalog metadata and ensures caches are hydrated (`ensureCatalogLoaded`).
  - Adds/removes POIs (`addCategory`, `addBrand`, `addCustom`, `removePOI`) and keeps D_anchor caches in sync.
  - Applies GPU filters by coordinating with the map worker to build GPU expressions that are then applied to the MapLibre layers through the `MapController`.

### Map Integration

- `lib/map/MapController.ts` instantiates MapLibre, registers the PMTiles protocol, tracks active mode filters, and exposes hover callbacks. The base style defines r7/r8 PMTiles sources with default visibility and 0.4 opacity, ensuring that when no filters are active, all hexes are shaded (showing the full coverage area).
- `lib/map/map.worker.ts` builds GPU expressions in a Web Worker to combine multiple POI criteria. When multiple filters are active (e.g., airport + Costco), the worker uses **intersection logic** (MapLibre `'all'` expressions) so only hexes meeting ALL criteria are shown. This ensures that adding more criteria progressively narrows the livable area, as intended. Optional overlays (climate selections, "Avoid power lines" toggle, and political lean range filter) are AND-ed into those expressions by inspecting tile properties such as `climate_label`, `near_power_corridor`, and `political_lean`.
- The MapController maintains a singleton worker instance and applies expression updates via RAF-coalesced batches to minimize render thrashing during slider interactions.

### Sidebar & UI Components

- `app/(sidebar)/SearchBox.tsx` fetches catalog data and Google Places suggestions (using TanStack Query), dispatches add actions, surfaces the livable-area summary, manages the active-filter pill list (sliders, pin toggles, and travel-mode switches), and exposes the climate typology dropdown, "Avoid power lines" toggle, and Political Views range slider (0-4: Strong Democrat to Strong Republican).
- `app/(sidebar)/HoverBox.tsx` summarizes hover details: travel times computed client-side using the same anchor combination logic, decoded climate stats, and a callout when the hovered hex lies within a buffered power corridor.
- Shared UI primitives live in `components/ui/` (Tailwind + Radix-inspired shorthands).

Supporting services:

- `lib/services/api.ts` resolves backend URLs based on environment variables (`NEXT_PUBLIC_vicinity_API_BASE_URL`, etc.) and enforces JSON parsing with optional Zod validation.
- `lib/services/catalog.ts`, `lib/services/dAnchor.ts`, `lib/services/places.ts` encapsulate API fetches with schema validation where appropriate.
- Utilities in `lib/utils/` (debounce, numbers, className helpers) keep UI logic concise.

### Styling & Tooling

- Tailwind CSS is configured in `tailwind.config.ts` with CSS variables for the faux-aged palette.
- Vitest config and ESLint rules live alongside the Next.js project (e.g., `vitest.config.ts`, `.eslintrc.js`).
- `pmtiles.js` ships the bundled PMTiles protocol script when the frontend needs to self-host tiles.

---

## Climate Overlay Flow

1. `vicinity/domains_overlay/climate/prism_normals_fetch.py` downloads PRISM raster normals by variable/month.
2. `vicinity/domains_overlay/climate/prism_to_hex.py` reads H3 IDs from the travel-time parquet (`data/minutes/*_drive_t_hex.parquet` by default), builds GeoJSON polygons, runs zonal statistics on the PRISM rasters, derives seasonal averages, quantizes values, classifies them into typologies, and writes `out/climate/hex_climate.parquet`.
3. Downstream tooling (not shown here) merges the climate parquet into tile generation so each hex feature includes:
   - Quantized fields (`*_f_q`, `*_in_q`) for compact storage (kernel expects scaling factors like 0.1°F or 0.1").
   - `climate_label` matching `CLIMATE_TYPOLOGY` used by the frontend for filtering.
4. The map UI uses `HoverBox` + climate actions to surface climate data and optionally mask tiles through expressions (logic currently lives server-side; see `setClimateSelections` for integration points).

---

## Native Kernels (`vicinity_native/`)

The Rust crate compiles to a Python module (`t_hex`) available to scripts in `src/`. Key modules:

- `lib.rs` – entry point exposing PyO3 bindings for K-best search, H3 aggregation, and helper algorithms. It handles label insertion logic, multi-threading via Rayon, and sentinel management (`65535` as unreachable).
- `ch.rs` – contraction hierarchy utilities used by the API when computing on-demand routes or building caches.

Cargo builds drop artefacts into `vicinity_native/target/`; ensure the library is built (`maturin develop` or similar) before running heavy pipeline steps.

---

## Testing & Validation

- `tests/test_climate_parquet.py` ensures that climate parquet quantized columns use the expected integer types. Extend this area with additional schema or regression tests as new datasets are introduced.
- FastAPI endpoints can be exercised with `make api` (if defined) or via `uvicorn api.main:app`. Pair with `npm run dev` in `tiles/web` for end-to-end testing.

---

## Typical Local Workflow

1. **Bootstrap data**: run `src/01_download_extracts.py` → `src/02_normalize_pois.py`.
2. **Build anchors & travel times**: `src/03_build_anchor_sites.py`, `src/04_compute_minutes_per_state.py`, and the D_anchor scripts for categories/brands.
3. **Refresh overlays**: `make climate` and `make power_corridors` (or invoke `vicinity/domains_overlay/climate/prism_to_hex.py` and `vicinity/domains_overlay/power_corridors/osm_to_hex.py` directly) to update `out/climate/hex_climate.parquet` and `data/power_corridors/*.parquet`.
4. **Generate tiles**: `src/08_h3_to_geojson.py` + `src/09_build_tiles.py` (or follow Makefile recipes if present).
5. **Serve backend**: `uvicorn api.main:app --reload` (expects data directories populated).
6. **Run frontend**: `cd tiles/web && npm install && npm run dev`. Set `NEXT_PUBLIC_vicinity_API_BASE_URL` if the API runs on a non-default host/port.

Subsequent development typically touches a single layer (e.g., adjusting taxonomy, tuning D_anchor kernels, or iterating on the React UI) but the data contracts described in `README.md` keep cross-layer dependencies explicit.

---

## Extending the System

- **Adding new POI categories/brands**: extend `data/taxonomy/taxonomy.py` or the override files in `data/taxonomy/`, regenerate canonical POIs, rebuild anchors, rerun D_anchor scripts, and refresh the catalog API.
- **Supporting additional states/modes**: update `config.py` (`STATES`, snap radii, H3 resolutions), ensure download scripts clip the desired region, and regenerate all pipeline outputs. Anchors will inherit stable IDs as long as the same `site_id` hashing strategy is used.
- **New overlays (e.g., crime, schools)**: model after the climate and power-corridor flows—write a script that enriches H3 hexes, emit parquet keyed by `h3_id`/`res`, and merge outputs before tile generation so the frontend can consume the new attributes.
- **Frontend experiments**: reuse `lib/actions` to keep map expressions consistent. Any new filter that depends on D_anchor data should populate the cache structure (`dAnchorCache`) and invoke `applyCurrentFilter`.

Use this document as a map when onboarding new contributors or when tracing a data flow end-to-end; it highlights which modules own each responsibility and how artefacts move between layers.

---

## Implementation Status & Technical Details

### Current Implementation Status

- T_hex compute is implemented in `src/04_compute_minutes_per_state.py` with a Rust native kernel.
- The compute emits a long format per-hex table: `(h3_id, anchor_int_id, time_s, res)` and tiles store top‑K per hex as `a{i}_id` + `a{i}_s`.
- The frontend exclusively uses anchor‑mode: it composes `a{i}_s` from tiles with API‑served D\_anchor per category and per brand, enabling thousands of POIs as filter options without tile changes.
- Category D\_anchor: Hive‑partitioned parquet at `data/d_anchor_category/mode={0|2}/category_id=*/part-*.parquet` loaded by the API.
- Brand D\_anchor: Hive‑partitioned parquet at `data/d_anchor_brand/mode={0|2}/brand_id=*/part-*.parquet` produced by `src/05_compute_d_anchor.py` and loaded by the API.
- Hex summaries include climate quantiles and a `near_power_corridor` boolean generated by `vicinity/domains_overlay/power_corridors/osm_to_hex.py`, powering the "Avoid power lines" toggle in the frontend.

### Core Model: Matrix Factorization

```
Total Travel Time = T_hex[hex→anchor] + D_anchor[anchor→category]
```

* **T\_hex**: For each H3 hex, store travel time to its top‑K nearest anchors.
* **D\_anchor**: For each anchor, store travel time to the *nearest* POI in each category/brand.
* **Frontend**: Calculates the min over K anchors per criterion entirely on the GPU.

Sentinel: `65535` (uint16) means unreachable / ≥ cutoff.

### Sources & Baselines (Massachusetts examples)

* OSM broad POIs ≈ 76,688 (many are low‑value street furniture).
* Overture Places (MA clip) ≈ 461,249 with strong brand normalization.
* Conclusion: **Hybrid** ≫ either alone. Overture for brands; OSM for civic/natural/tag richness. Airports are sourced from a curated CSV (`Future/airports_coordinates.csv`) and OSM/Overture airports are ignored to keep a consistent set.

### Anchors & Sites

**Anchor Sites (definition).** A site is a *road‑node–centric* aggregation of nearby POIs for a mode (drive/walk). Multiple POIs (across sources) can share one site.

**Why sites?**

* One precompute per site instead of per POI ⇒ 2–5× routing reduction in dense areas.
* Stable IDs tied to the road graph, not noisy POI centroids.

**Generation.**

1. Build routable graph from OSM.
2. Snap POIs to nearest graph node by mode (uses **connectivity-aware snapping** as of Oct 2024: considers k=10 candidates and prefers nodes with ≥2 edges to avoid isolated service roads).
3. Group by `(mode, node_id)` ⇒ `site_id`.
4. Store aggregated brand/category membership.

**Counts.** 26,019 drive‑mode anchors for Massachusetts (1:1 hex coverage; see [ANCHORS.md](ANCHORS.md) for scaling analysis).

**Quality fix (Oct 2025).** Previous naive nearest-neighbor snapping caused Logan Airport to reach only 20K nodes (vs. Worcester's 289K). Connectivity-aware snapping now selects better-connected nodes within 2× the nearest distance, improving ~38% of POI anchors statewide.

### Data Contracts (Hard Requirements)

**T\_hex tiles (contract):**

* Anchor arrays per hex: `a{i}_id` (int32, anchor_int_id) and `a{i}_s` (uint16 seconds).
* Polygon geometry = H3 cell boundary.
* Layer names = `t_hex_r7_*`, `t_hex_r8_*`.
* Additional attributes may include boolean overlays; currently `near_power_corridor` (true when a hex is within 200 m of a high-voltage transmission corridor) is required for the livability toggle.

**D\_anchor API must return:**

* JSON `{anchor_id: seconds}` with `65535` sentinel for unreachable.
* Anchor IDs that appear in T\_hex tiles.
* Mode partitioning if multiple modes are supported.

**Frontend assumptions:**

* Uses anchor fields in tiles (`a{i}_id`, `a{i}_s`) and composes with API D_anchor (categories and brands) on the GPU.
* Applies optional overlays (climate selections, "Avoid power lines") by combining tile properties such as `climate_label` and `near_power_corridor` inside GPU expressions.
* Mode: `drive` (walk supported if walk tiles + D_anchor provided). The system gracefully handles missing walk mode data by returning empty results, allowing drive mode to function normally.
* Units: minutes in the UI.

### Canonical Schemas

**POI**

* `poi_id: str` (uuid5 over `source|ext_id|rounded lon/lat`)
* `name: str`
* `brand_id: str|null` (canonical)
* `brand_name: str|null`
* `class: str` (venue|civic|transport|natural|…)
* `category: str` (supermarket|hospital|…)
* `subcat: str` (ER|preschool|mexican fast food|…)
* `lon, lat: float32`
* `geom_type: uint8` (0=point,1=centroid,2=entrance)
* `area_m2: float32`
* `source: str` (overture|osm|fdic|cms|csv\:chipotle|user)
* `ext_id: str|null`
* `h3_r9: str`
* `node_drive_id, node_walk_id: int64|null`
* `dist_drive_m, dist_walk_m: float32`
* `anchorable: bool` | `exportable: bool`
* `license, source_updated_at, ingested_at: str`
* `provenance: list[str]`

**Anchor Site**

* `site_id: str` (uuid5 of `mode|node_id`)
* `mode: str` (drive|walk)
* `node_id: int64`
* `lon, lat: float32`
* `poi_ids: list[str]`
* `brands: list[str]`
* `categories: list[str]`
* `brand_tiers: list[int]`
* `weight_hint: int`

**t\_hex (Travel)**

* `h3_id: uint64`
* `anchor_int_id: int32`
* `time_s: uint16` (`65535` sentinel)

**Summaries**

* `min_cat(hex_r9, category, min_time_drive_s, min_time_walk_s)`
* `min_brand(hex_r9, brand_id, min_time_drive_s, min_time_walk_s)`
* `near_power_corridor: bool` (per hex, copied to both r7/r8 exports; defaults to `False` when corridor data is missing)

### Pipeline (Deterministic Stages)

1. **Ingest**

* Overture → `data/overture/<state>_places.parquet` (via DuckDB clip).
* OSM → `data/osm/<state>.osm.pbf` (Pyrosm/OGR).
* Optional CSVs.

2. **Normalize**

* Lowercase/strip names.
* Brand resolution: `brand.names.primary > names.primary > alias registry`.
* Category mapping: Overture `categories.primary/alternate` + OSM `amenity/shop/cuisine` → **vicinity taxonomy**.
* **Brand fallback logic**: POIs with missing `brand` field fall back to `name` field for brand matching.

3. **Conflate & Deduplicate**

* H3 r9 proximity: walk 0.25 mi, drive 1 mi (tunable, density‑aware).
* Merge same brand+category; tie‑break: Overture wins chains; OSM wins civic/natural & polygons; record `provenance`.

4. **Build Anchor Sites**

* Snap to nearest road node by mode; group by `(mode,node_id)`.
* Aggregate `poi_ids`, `brands`, `categories`.

5. **Travel Precompute (K-best)**

* Multi‑source Dijkstra from sites using K-best algorithm.
* Store global top‑K per hex (recommended K=20+ for dense urban areas).
* Parameters: `--cutoff 90 --k-best 20` for comprehensive coverage.

6. **Overlay Data (Climate, Power Corridors & Politics)**

* Run `vicinity/domains_overlay/climate/prism_to_hex.py` to generate climate metrics from PRISM rasters.
* Run `vicinity/domains_overlay/power_corridors/osm_to_hex.py` to buffer high-voltage OSM `power=line` features, dissolve the corridor, and intersect with the H3 grid.
* Writes `data/power_corridors/<state>_near_power_corridor.parquet` with `near_power_corridor` flags at r7/r8 resolutions (defaults to `False` when no qualifying lines exist).
* Buffer distance defaults to 200 m; adjust with `--buffer-meters` if product requirements change.
* Run `vicinity/domains_overlay/politics/politics_to_hex.py` to process 2024 presidential election results and join county-level political lean to H3 cells.
* Writes `data/politics/<state>_political_lean.parquet` with `political_lean` (0-4 buckets) and `rep_vote_share` at r7/r8 resolutions (null values for hexes without county data).

7. **Summaries & Merge**

* Precompute `min_cat` & `min_brand` for exposed categories & A‑list brands.
* Long‑tail brands resolved via joins at query time.
* Merge overlay datasets (climate, `near_power_corridor`, `political_lean`, `rep_vote_share`) so downstream tiles expose the necessary attributes.

8. **Tiles**

* Convert merged data → NDJSON → PMTiles (r7/r8 layers, exact layer names).

### Query UX (Deterministic Rules)

* **Category filter:** Compute on GPU from D\_anchor category chunks.
* **Brand (A‑list):** Compute on GPU from D\_anchor brand chunks (no per‑brand tile columns).
* **Fallback:** Optional local Dijkstra for rare gaps.

UI rules:

* Choosing a brand auto‑locks its parent category.
* If no coverage, suggest category fallback.
* Clicking a hex shows nearest POI from the underlying site with provenance.

### Performance Targets (Enforced)

* Initial load (tiles + D\_anchor cache): **< 2s**.
* Slider response: **< 250ms**.
* Render on zoom/pan: **< 100ms**.
* Browser heap (MA full): **< 200MB**.
* PMTiles size (2 categories, nationwide): **< 400MB**.

### Known Limitations

* Free‑flow speeds only; no live traffic.
* No mode mixing (e.g., walk→transit→drive).
* POI freshness requires periodic pipeline runs.
* uint16 cap (≈18h) on times; rounding to seconds.
* **Triangle inequality violations**: The core approximation `hex→anchor + anchor→POI ≈ hex→POI` can be significantly wrong when optimal paths don't share common routing nodes. This is especially problematic in:
  - Suburban/rural networks with limited connectivity options
  - Areas where anchors are accessible via slow local roads but POIs are reachable via fast highways
  - Scenarios where the closest anchor by distance is not on the optimal routing path
* **No error bounding**: Current system provides no quantification of approximation error magnitude or confidence intervals.
* **Silent approximation failures**: Users receive travel time estimates without indication of uncertainty or potential error ranges.

### Implementation Tasks (LLM‑friendly, with I/O and checks)

**A. Taxonomy & Brand Registry**

* **Input:** Overture categories + OSM tags; seed CSV of brand aliases.
* **Output:** `data/taxonomy/categories.yml`, `data/taxonomy/POI_brand_registry.csv` (columns: `brand_id,canonical,aliases|;‑sep,wikidata?`).
* **Checks:** Aliases must be unique; map every exposed UI category to ≥1 source tag.

**B. Ingest + Normalize**

* **Input:** Overture parquet, OSM PBF, CSVs.
* **Output:** `data/poi/normalized.parquet` (schema above).
* **Checks:** ≥95% of A‑list brands receive `brand_id`; drop obviously wrong geoms (>200km off state bbox).

**C. Conflation**

* **Input:** `normalized.parquet`.
* **Output:** `data/poi/conflated.parquet` with `provenance` list.
* **Checks:** For chains (Dunkin, Starbucks, etc.) Overture dominates when both present; polygons preserved.

**D. Anchor Sites**

* **Input:** Conflated POIs; OSM graph.
* **Output:** `data/anchors/sites_{mode}.parquet`.
* **Checks:** No site with 0 POIs; record `brands/categories` arrays; `site_id` stable.

**E. T\_hex**

* **Input:** Sites; OSM graph.
* **Output:** `out/t_hex/{state}_{mode}.parquet` with `k` and `a{i}_*` fields.
* **Checks:** Each hex has `k≤K_ANCHORS`; flags only in {0, bitfield}; no orphan anchor IDs.

**F. D\_anchor**

* **Input:** Sites; POIs by category/brand.
* **Output:** Hive‑partitioned parquet per `(mode, category_id|brand_id)`.
* **Checks:** Every anchor present in T\_hex appears in D\_anchor (or sentinel 65535).

**G. Tiles**

* **Input:** T\_hex summary parquet from merge/summarize step.
* **Output:** `tiles/t_hex_r7_{mode}.pmtiles`, `tiles/t_hex_r8_{mode}.pmtiles`.
* **Checks:** Layer names match frontend configs; H3 boundaries valid; NDJSON line count == hex count.

**H. Frontend Wiring**

* **Input:** PMTiles, D\_anchor JSON endpoints.
* **Output:** Working sliders; GPU expressions; debounced updates.
* **Checks:** Synthetic test: hex with hand‑set `a{i}` times produces correct visibility across thresholds.

### Deterministic Config (Single Source of Truth)

* `K_ANCHORS = 20` (recommended for dense urban areas; adjustable via Makefile `--k-best` parameter).
* `UNREACHABLE = 65535`.
* Snap radii defaults: walk 0.25 mi; drive 1 mi; allow density‑adaptive overrides.
* Partitions: `mode ∈ {drive, walk}`.
* Tile layers: `t_hex_r7_*`, `t_hex_r8_*` only.

Allowlists & Sources (overhaul scope)
- `data/taxonomy/POI_category_registry.csv`: category definitions with explicit numeric IDs (anti-drift). Columns: `category_id`, `numeric_id`, `display_name`. Any category in the CSV is automatically allowlisted for anchors and precomputation.
- `data/taxonomy/POI_brand_registry.csv`: brand definitions with aliases. Columns: `brand_id`, `canonical`, `aliases`, `wikidata`. All brands in the registry are automatically allowlisted for anchors and precomputation.
- Airports: `Future/airports_coordinates.csv` only (normalization injects these; OSM/Overture airports are discarded).

Taxonomy & Config Files (minimal)
- `data/taxonomy/taxonomy.py`: built‑in taxonomy + mappings (defaults).
- `data/taxonomy/POI_brand_registry.csv`: brand canon and aliases. Any brand present is automatically allowlisted.
- `data/taxonomy/POI_category_registry.csv`: category definitions with explicit numeric IDs and display names. Single source of truth.
- `data/taxonomy/d_anchor_limits.json`: runtime limits for D_anchor computation (max_minutes, top_k per category/brand).
- Optional advanced override (disabled by default): `data/taxonomy/categories.yml`. Enable with `TS_TAXONOMY_YAML=1` if you need to extend mappings.

### Airport Handling (Target)

* Snap internal airport POIs to nearest public arterial (motorway/trunk/primary/secondary/tertiary/residential) within 5km.
* If none found, mark unreachable rather than routing through private/service ways.

### Scaling Guidelines

* Batch sizes for Dijkstra tuned to memory; parallelize across states.
* ZSTD for all parquet; prefer column pruning.
* Serve PMTiles via CDN; avoid dynamic map servers.

### Snapshots & Deltas

* Monthly snapshots: `snapshot_date=YYYY‑MM‑DD`.
* Track deltas: added/moved/removed POIs.
* Incremental recompute: only anchors whose sites changed.

### Acceptance Tests (Targets)

1. **Anchor consistency:** Every `a{i}_id` in tiles exists in D\_anchor for every exposed category (or sentinel).
2. **Determinism:** Re‑running T\_hex/D\_anchor with same inputs yields byte‑identical outputs (modulo parquet row group ordering) — verify with hash of sorted records.
3. **Performance:** On MA dataset, map responds ≤250ms for a 3‑slider scenario; memory ≤200MB.
4. **Correctness:** Known hand‑crafted cells verify expected mins across K anchors.
5. **Schema compliance:** Validate parquet schemas against canonical definitions in CI.

### Known Pitfalls (Avoid)

* Reducing polygons to points for large venues (hospitals, parks) — keep polygon for UX and snapping sanity.
* Letting brand alias chaos bleed into queries — **require** registry usage everywhere.
* Airport routing through service/private roads — always snap to arterials.
* Breaking contract between T\_hex and D\_anchor — keep IDs stable and complete.

### Glossary

* **H3 r9:** Hex resolution used for per‑cell summaries.
* **Anchor / Site:** Aggregation of POIs snapped to a road node by mode.
* **T\_hex:** Hex→anchor top‑K travel times.
* **D\_anchor:** Anchor→nearest POI per category/brand.
* **A‑list brands:** Pre‑indexed brands with precomputed `min_brand`.

### Proposed Solutions for Triangle Inequality Issues

**1. Error Distribution Analysis**
* Implement validation framework (`scripts/validate_triangle_approximation.py`) to empirically measure error distributions across different network topologies
* Generate per-region error statistics (urban vs suburban vs rural)
* Establish error thresholds for acceptable approximation quality

**2. Enhanced K-Anchor Strategy**
* **Diversified anchor selection**: Instead of K=20 nearest anchors, select anchors that maximize routing path diversity
* **Directional anchors**: Ensure anchor coverage in all cardinal directions from each hex
* **Network topology-aware selection**: Prioritize anchors on different road hierarchies (local, arterial, highway)

**3. Approximation Confidence Scoring**
* Compute per-hex "confidence" scores based on:
  - Anchor spatial distribution around the hex
  - Road network density and connectivity
  - Variance in anchor-to-POI times across the K anchors
* Expose confidence levels in API responses and UI

**4. Selective Ground Truth Validation**
* For low-confidence predictions, compute actual shortest paths on-demand
* Cache frequently-requested routes to amortize computation cost
* Hybrid approach: use approximation for high-confidence cases, exact routing for uncertain cases

**5. User Communication**
* Display travel time ranges instead of point estimates (e.g., "12-18 minutes")
* Visual indicators for approximation confidence in the UI
* Clear disclaimers about estimation methodology in dense vs sparse areas

### Open Questions (Not Blockers; Track Separately)

* Exact tiering for brands (A‑list vs long‑tail) per state.
* Walk‑mode tiles rollout order and UI toggle design.
* Density‑adaptive snap radius heuristics.
* **Routing approximation quality**: Acceptable error thresholds for different use cases (browsing vs final decisions).

### Climate Data Fields

PRISM-derived climate metrics are stored as quantized integers to keep parquet and tile payloads compact. Any column ending with `_q` follows these rules:

- `*_f_q`: Temperatures in tenths of degrees Fahrenheit. Decode with `value / 10`.
- `*_mm_q`: Precipitation totals in tenths of millimetres. Decode with `value / 10`.
- `*_in_q`: Precipitation totals in tenths of inches. Decode with `value / 10`.

The scale factors are also recorded in the `out/climate/hex_climate.parquet` metadata under the `vicinity_prism` key for downstream analytics.

#### Climate Data Validation

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
