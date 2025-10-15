# TownScout: System Overview & LLM Implementation Spec

> Architectural map: `docs/ARCHITECTURE_OVERVIEW.md`

## 1) Purpose, Context, Goals

**Purpose.** TownScout helps a user answer: *“Where should I live given my criteria?”* by computing travel‑time proximity to things that matter (Chipotle, Costco, airports, schools, etc.) and rendering results as a fast, interactive map.

**Context.** Instead of precomputing every hex→category path, TownScout factorizes the problem into **hex→anchor** and **anchor→category/brand** legs. The frontend combines these in real time with GPU expressions using per‑anchor seconds stored in tiles and per‑anchor category/brand seconds served by the API. This keeps costs near‑zero and makes adding thousands of POIs (brands and categories) cheap.

**Primary Goals.**

* Each time a POI from OSM or Overture is added, the livable land for that user visually shrinks based on what the filter was.
* Sub‑second (≤250ms) slider→map response with zero server round‑trips.
* Add/extend categories without recomputing hex tiles.
* Keep deployment simple: static tiles on CDN, thin API for D\_anchor.
* Scale from a single state to nationwide without blowing up storage/compute.

**Non‑Goals.** Live traffic, multimodal chaining (walk→transit→drive), and route turn‑by‑turn.

---

## Current Implementation Status (Practical)

- T_hex compute is implemented in `src/03_compute_minutes_per_state.py` with a Rust native kernel.
- The compute emits a long format per-hex table: `(h3_id, anchor_int_id, time_s, res)` and tiles store top‑K per hex as `a{i}_id` + `a{i}_s`.
- The frontend exclusively uses anchor‑mode: it composes `a{i}_s` from tiles with API‑served D\_anchor per category and per brand, enabling thousands of POIs as filter options without tile changes.
- Category D\_anchor: Hive‑partitioned parquet at `data/d_anchor_category/mode={0|2}/category_id=*/part-*.parquet` loaded by the API.
 - Brand D\_anchor: Hive‑partitioned parquet at `data/d_anchor_brand/mode={0|2}/brand_id=*/part-*.parquet` produced by `src/03d_compute_d_anchor.py` and loaded by the API.
 

---

## 2) Core Model: Matrix Factorization

```
Total Travel Time = T_hex[hex→anchor] + D_anchor[anchor→category]
```

* **T\_hex**: For each H3 hex, store travel time to its top‑K nearest anchors.
* **D\_anchor**: For each anchor, store travel time to the *nearest* POI in each category/brand.
* **Frontend**: Calculates the min over K anchors per criterion entirely on the GPU.

Sentinel: `65535` (uint16) means unreachable / ≥ cutoff.

---

## 3) End‑to‑End Dataflow (Concise)

```
OSM PBF  ─┐                 ┌─>  T_hex parquet  ──> GeoJSON NDJSON ──> PMTiles (r7/r8) ─┐
          ├─> Anchor Sites ─┤                                                         ├─> Frontend (MapLibre)
Overture ─┘                 └─>  D_anchor parquet (per category/brand)  ──────────────┘
```

* **OSM (Pyrosm/OGR)** for road graph + civic/natural POIs.
* **Overture Places (GeoParquet)** for brand/commercial spine.
* **Optional CSVs** (e.g., airports).

---

## 4) Sources & Baselines (Massachusetts examples)

* OSM broad POIs ≈ 76,688 (many are low‑value street furniture).
* Overture Places (MA clip) ≈ 461,249 with strong brand normalization.
* Conclusion: **Hybrid** ≫ either alone. Overture for brands; OSM for civic/natural/tag richness. Airports are sourced from a curated CSV (`Future/airports_coordinates.csv`) and OSM/Overture airports are ignored to keep a consistent set.

---

## 5) Anchors & Sites

**Anchor Sites (definition).** A site is a *road‑node–centric* aggregation of nearby POIs for a mode (drive/walk). Multiple POIs (across sources) can share one site.

**Why sites?**

* One precompute per site instead of per POI ⇒ 2–5× routing reduction in dense areas.
* Stable IDs tied to the road graph, not noisy POI centroids.

**Generation.**

1. Build routable graph from OSM.
2. Snap POIs to nearest graph node by mode (uses **connectivity-aware snapping** as of Oct 2024: considers k=10 candidates and prefers nodes with ≥2 edges to avoid isolated service roads).
3. Group by `(mode, node_id)` ⇒ `site_id`.
4. Store aggregated brand/category membership.

**Counts.** \~20k drive‑mode anchors for Massachusetts (target range; not a hard promise).

**Quality fix (Oct 2024).** Previous naive nearest-neighbor snapping caused Logan Airport to reach only 20K nodes (vs. Worcester's 289K). Connectivity-aware snapping now selects better-connected nodes within 2× the nearest distance, improving ~38% of POI anchors statewide.

---

## 6) Computations, Tiles & Frontend

Implementation details for T_hex/D_anchor generation, tile assembly, and the Next.js client now live in `docs/ARCHITECTURE_OVERVIEW.md`. Use that document for module-level references and file paths; this README treats those systems as black boxes and focuses on contracts, governance, and operator guidance.

---
## 9) Data Contracts (hard requirements)

**T\_hex tiles (contract):**

* Anchor arrays per hex: `a{i}_id` (int32, anchor_int_id) and `a{i}_s` (uint16 seconds).
* Polygon geometry = H3 cell boundary.
* Layer names = `t_hex_r7_*`, `t_hex_r8_*`.

**D\_anchor API must return:**

* JSON `{anchor_id: seconds}` with `65535` sentinel for unreachable.
* Anchor IDs that appear in T\_hex tiles.
* Mode partitioning if multiple modes are supported.

**Frontend assumptions:**

* Uses anchor fields in tiles (`a{i}_id`, `a{i}_s`) and composes with API D_anchor (categories and brands) on the GPU.
* Mode: `drive` (walk supported if walk tiles + D_anchor provided). The system gracefully handles missing walk mode data by returning empty results, allowing drive mode to function normally.
* Units: minutes in the UI.

---

## 10) POI Overhaul (In Progress)

**Problem.** Current coverage is too narrow; brand aliasing and taxonomy drift will wreck filters; OSM alone undercounts chains; Overture lacks some civic richness.

**Goals.**

* Cover all livability‑relevant categories broadly: food, retail, education, health, recreation, civic, transport, natural amenities.
* Support both *category* (e.g., supermarket) and *brand* (e.g., Whole Foods) queries.
* Use **Anchor Sites** for co‑located POIs to cut routing cost 2–5×.
* Make additions **config‑driven**, not code‑driven.
* Each time a POI from OSM or Overture is added, the livable land for that user visually shrinks based on what the filter was.

**Key insight.** Use Overture for the brand/commercial spine and OSM for civic/natural/local detail. Normalize both into a TownScout taxonomy with a brand registry. Airports are handled via a curated CSV to avoid noisy/missing source tags.

**Beach Classification.** Beaches receive specialized treatment via `src/osm_beaches.py`, which uses spatial analysis to classify them into separate categories (`beach_ocean`, `beach_lake`, `beach_river`, `beach_other`) based on proximity to coastlines and water bodies. This enables distinct frontend filter options for "Any Beach (Ocean)" and "Any Beach (Lake)" rather than a single generic beach filter. The beach processing pipeline includes geometry hygiene checks (via `src/geometry_utils.py`) to prevent Shapely 2.x `create_collection` errors when building spatial buffers and unions.

**Places of Worship Classification.** OSM provides `amenity=place_of_worship` with a `religion` tag but no building-type classification. The normalization pipeline (`src/02_normalize_pois.py`) maps religion types to user-friendly worship place categories: Christian → Church, Muslim → Mosque, Jewish → Synagogue, and Hindu/Buddhist/Jain/Sikh → Temple. This allows frontend users to filter by specific worship types ("Church", "Mosque", etc.) while maintaining compatibility with OSM's generic tagging scheme. Only the seven major religions (Christian, Muslim, Jewish, Hindu, Buddhist, Jain, Sikh) are included; other religions are excluded from the canonical POI set.

**Library Support.** Libraries are mapped from both OSM (`amenity=library`) and Overture sources, appearing in the civic category class. The frontend displays this as "Any Library" in the Places of Interest filter dropdown.

**D_anchor Runtime Limits.** Category and brand D_anchor computations respect configurable runtime limits defined in `data/taxonomy/d_anchor_limits.json`. This file specifies `max_minutes` (travel time cutoff) and `top_k` (maximum nearest sources to retain per anchor) for each category and brand. Default limits: 60 minutes / top-12 for local amenities (groceries, pharmacies, cafes); 180 minutes / top-8 to top-12 for weekend destinations (airports, ski resorts, beaches). These limits significantly reduce computation time for sparse categories (e.g., airports, ferry terminals) while maintaining realistic UX thresholds.

---

## 11) Canonical Schemas

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

---

## 12) Pipeline (Deterministic Stages)

1. **Ingest**

* Overture → `data/overture/<state>_places.parquet` (via DuckDB clip).
* OSM → `data/osm/<state>.osm.pbf` (Pyrosm/OGR).
* Optional CSVs.

2. **Normalize**

* Lowercase/strip names.
* Brand resolution: `brand.names.primary > names.primary > alias registry`.
* Category mapping: Overture `categories.primary/alternate` + OSM `amenity/shop/cuisine` → **TownScout taxonomy**.
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

6. **Summaries**

* Precompute `min_cat` & `min_brand` for exposed categories & A‑list brands.
* Long‑tail brands resolved via joins at query time.

7. **Tiles**

* Convert merged data → NDJSON → PMTiles (r7/r8 layers, exact layer names).

---

## 13) Query UX (Deterministic Rules)

* **Category filter:** Compute on GPU from D\_anchor category chunks.
* **Brand (A‑list):** Compute on GPU from D\_anchor brand chunks (no per‑brand tile columns).
* **Fallback:** Optional local Dijkstra for rare gaps.

UI rules:

* Choosing a brand auto‑locks its parent category.
* If no coverage, suggest category fallback.
* Clicking a hex shows nearest POI from the underlying site with provenance.

---

## 14) Performance Targets (Enforced)

* Initial load (tiles + D\_anchor cache): **< 2s**.
* Slider response: **< 250ms**.
* Render on zoom/pan: **< 100ms**.
* Browser heap (MA full): **< 200MB**.
* PMTiles size (2 categories, nationwide): **< 400MB**.

---

## 15) Known Limitations

* Free‑flow speeds only; no live traffic.
* No mode mixing (e.g., walk→transit→drive).
* POI freshness requires periodic pipeline runs.
* uint16 cap (≈18h) on times; rounding to seconds.

---

## 16) Implementation Tasks (LLM‑friendly, with I/O and checks)

**A. Taxonomy & Brand Registry**

* **Input:** Overture categories + OSM tags; seed CSV of brand aliases.
* **Output:** `data/taxonomy/categories.yml`, `data/brands/registry.csv` (columns: `brand_id,canonical,aliases|;‑sep,wikidata?`).
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

---

## 17) Deterministic Config (single source of truth)

* `K_ANCHORS = 20` (recommended for dense urban areas; adjustable via Makefile `--k-best` parameter).
* `UNREACHABLE = 65535`.
* Snap radii defaults: walk 0.25 mi; drive 1 mi; allow density‑adaptive overrides.
* Partitions: `mode ∈ {drive, walk}`.
* Tile layers: `t_hex_r7_*`, `t_hex_r8_*` only.

Allowlists & Sources (overhaul scope)
- `data/taxonomy/category_allowlist.txt`: category labels to precompute. The category D_anchor step reads this by default; use `--prune` to remove stale categories.
- `data/brands/allowlist.txt`: A‑list canonical brand_ids to precompute for brand queries and to include in anchors when their categories aren’t allowlisted.
- Airports: `Future/airports_coordinates.csv` only (normalization injects these; OSM/Overture airports are discarded).

Taxonomy & Config Files (minimal)
- `src/taxonomy.py`: built‑in taxonomy + mappings (defaults).
- `data/brands/registry.csv`: brand canon + aliases you edit regularly.
- `data/taxonomy/category_labels.json`: generated by category D_anchor; served by the API.
- `data/taxonomy/d_anchor_limits.json`: runtime limits for D_anchor computation (max_minutes, top_k per category/brand).
- Optional advanced override (disabled by default): `data/taxonomy/categories.yml`. Enable with `TS_TAXONOMY_YAML=1` if you need to extend mappings.

---

## 18) Airport Handling (Target)

* Snap internal airport POIs to nearest public arterial (motorway/trunk/primary/secondary/tertiary/residential) within 5km.
* If none found, mark unreachable rather than routing through private/service ways.

---

## 19) Scaling Guidelines

* Batch sizes for Dijkstra tuned to memory; parallelize across states.
* ZSTD for all parquet; prefer column pruning.
* Serve PMTiles via CDN; avoid dynamic map servers.

---

## 20) Snapshots & Deltas

* Monthly snapshots: `snapshot_date=YYYY‑MM‑DD`.
* Track deltas: added/moved/removed POIs.
* Incremental recompute: only anchors whose sites changed.

---

## 21) Acceptance Tests (targets)

1. **Anchor consistency:** Every `a{i}_id` in tiles exists in D\_anchor for every exposed category (or sentinel).
2. **Determinism:** Re‑running T\_hex/D\_anchor with same inputs yields byte‑identical outputs (modulo parquet row group ordering) — verify with hash of sorted records.
3. **Performance:** On MA dataset, map responds ≤250ms for a 3‑slider scenario; memory ≤200MB.
4. **Correctness:** Known hand‑crafted cells verify expected mins across K anchors.
5. **Schema compliance:** Validate parquet schemas against canonical definitions in CI.

---

## 22) Known Pitfalls (avoid)

* Reducing polygons to points for large venues (hospitals, parks) — keep polygon for UX and snapping sanity.
* Letting brand alias chaos bleed into queries — **require** registry usage everywhere.
* Airport routing through service/private roads — always snap to arterials.
* Breaking contract between T\_hex and D\_anchor — keep IDs stable and complete.

---

## 23) Glossary

* **H3 r9:** Hex resolution used for per‑cell summaries.
* **Anchor / Site:** Aggregation of POIs snapped to a road node by mode.
* **T\_hex:** Hex→anchor top‑K travel times.
* **D\_anchor:** Anchor→nearest POI per category/brand.
* **A‑list brands:** Pre‑indexed brands with precomputed `min_brand`.

---

## 24) Open Questions (not blockers; track separately)

* Exact tiering for brands (A‑list vs long‑tail) per state.
* Walk‑mode tiles rollout order and UI toggle design.
* Density‑adaptive snap radius heuristics.
---

**Status:** POI overhaul complete. T_hex compute + D_anchor + demo anchor‑mode tiles are implemented. GPU composition is the core design; overlays have been removed to simplify the system.

---

## Quick Start

- Create environment and install deps: `make init`
- Build native ext: `make native`
- Download data and normalize POIs: `make pois`
- Build anchor sites: `make anchors`
- Compute minutes (T_hex long format): `make minutes`
- Compute D_anchor category tables: `make d_anchor_category`
- Compute D_anchor brand tables: `make d_anchor_brand`
- Merge + summarize, build tiles, and bring up the stack:
  - `make merge tiles` then `make serve` (FastAPI on `http://127.0.0.1:5173`)
  - In a new terminal: `cd tiles/web && npm install && npm run dev`
  - Visit `http://localhost:3000` (Next.js) — the app will call back to the FastAPI service on port 5173 unless you override `NEXT_PUBLIC_TOWNSCOUT_API_BASE_URL`

### Full Pipeline Command
```bash
make pois anchors minutes d_anchor_category d_anchor_brand merge tiles
```

### Category & Brand Scope (Stay Focused)
- Categories: edit `data/taxonomy/category_allowlist.txt`. The category step uses it by default; pass `--prune` when running the script directly to remove old categories.
- Brands (A‑list): edit `data/brands/allowlist.txt`. The brand step reads it by default if you don’t pass `--brand`.
- Tip: Use `--threads` and consider a smaller `--overflow-cutoff` (e.g., 60) for faster runs on laptops.

### Coverage Optimization
For maximum POI coverage (especially dense brands):
1. Expand brand aliases in `data/brands/registry.csv` for comprehensive name matching
2. Compute `make d_anchor_brand` for all desired brands (or threshold)
3. Increase K-best parameters (`--k-best 20+`) for dense urban areas if needed

API:
- Run: `uvicorn api.main:app --reload --host 0.0.0.0 --port 5173` (or expose the same base URL you pass via `NEXT_PUBLIC_TOWNSCOUT_API_BASE_URL`)
- Categories: `GET /api/categories?mode=drive`
- D_anchor slice: `GET /api/d_anchor?category=<id>&mode=drive`
- D_anchor brand slice: `GET /api/d_anchor_brand?brand=<id or alias>&mode=drive`
- Custom point (escape hatch): `GET /api/d_anchor_custom?lon=<lon>&lat=<lat>&mode=drive`
 
Prerequisites for Overture clipping: install the DuckDB CLI (`duckdb`) and ensure it is on your PATH. The downloader (`src/01_download_extracts.py`) invokes the DuckDB command.


# Townscout Data Contracts

## Climate Data Fields

PRISM-derived climate metrics are stored as quantized integers to keep parquet and tile payloads compact. Any column ending with `_q` follows these rules:

- `*_f_q`: Temperatures in tenths of degrees Fahrenheit. Decode with `value / 10`.
- `*_mm_q`: Precipitation totals in tenths of millimetres. Decode with `value / 10`.
- `*_in_q`: Precipitation totals in tenths of inches. Decode with `value / 10`.

The scale factors are also recorded in the `out/climate/hex_climate.parquet` metadata under the `townscout_prism` key for downstream analytics.

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
