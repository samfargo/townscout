# TownScout: System Overview & LLM Implementation Spec

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
- Brand overlays (`src/03c_compute_overlays.py`) are optional for analysis and validation; the UI no longer relies on per‑brand tile columns.

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
2. Snap POIs to nearest graph node by mode.
3. Group by `(mode, node_id)` ⇒ `site_id`.
4. Store aggregated brand/category membership.

**Counts.** \~23k drive‑mode anchors for Massachusetts (target range; not a hard promise).

---

## 6) Computations

### 6.1 T\_hex (`src/03_compute_minutes_per_state.py`)

* **Inputs:** OSM graph, anchor sites.
* **Algorithm:** Multi‑source bucketed SSSP composed into K‑best per node (Rust native), aggregated to H3 hexes at requested resolutions.
* **Output (long per hex row):** `(h3_id:uint64, anchor_int_id:int32, time_s:uint16, res:int32)`.
* **Memory tactics:** CSR graph, uint16 edge weights, ZSTD parquet.

Example long row schema: `h3_id, anchor_int_id, time_s, res`

### 6.2 D\_anchor (required for categories; brand variant for brands)

* **Inputs:** Anchor sites + POIs (category/brand aware). Airports are injected from CSV during normalization.
* **Algorithm:** Multi‑source search composed per class:
  - Category: compute anchor→nearest POI in the category, store seconds per anchor.
  - Brand: compute anchor→nearest site containing the brand, store seconds per anchor.
* **Layout:**
  - Categories: `data/d_anchor_category/mode={0,2}/category_id={...}/part-*.parquet`
  - Brands: `data/d_anchor_brand/mode={0,2}/brand_id={...}/part-*.parquet`
* **Status:** Loaded by the API. Category ids listed by `/api/categories`; full catalog at `/api/catalog`.

### 6.3 Overlays System (optional)

Overlays remain useful for QA and analysis, but the UI no longer depends on per‑brand columns in tiles.

**API shape:**

```json
{
  "4585": 101,
  "2066": 65535,
  "1509": 326
}
```

---

## 7) Tiles & Serving

**GeoJSON Conversion** (`src/05_h3_to_geojson.py`): T\_hex summary parquet → NDJSON (H3 polygons). Use `.itertuples()` to preserve uint64 H3 IDs.

**PMTiles Build** (`src/06_build_tiles.py`): tippecanoe → MBTiles → PMTiles. Two layers: r7 (\<z8) and r8 (≥z8). Layer names must match frontend source IDs.

**Static Serving (FastAPI):**

```python
# Web assets (HTML/JS/CSS)
app.mount("/static", StaticFiles(directory="tiles/web"), name="static")
app.mount("/tiles/web", StaticFiles(directory="tiles/web"), name="tiles-web")

# PMTiles are served via a custom route with HTTP Range support at /tiles/{name}.pmtiles
```

---

## 8) Frontend (MapLibre)

The dropdow menun on the cleint side should be a list of every POI, and there should be just the name of the category at the start of each category's POI which the user can select to encompass the whole category. Or the user can select individual POI.

**PMTiles protocol:** local import, no CDN. Demo UI lives at `tiles/web/index.html` and composes anchor arrays with API‑served D_anchor (categories, brands, and custom points).

```js
let protocol = new pmtiles.Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);
const T_HEX_R7_URL = "pmtiles:///tiles/t_hex_r7_drive.pmtiles";
const T_HEX_R8_URL = "pmtiles:///tiles/t_hex_r8_drive.pmtiles";
```

**GPU Filter Expression (anchor‑mode):**

```js
function buildFilterExpression(criteria, dAnchorData) {
  const UNREACHABLE = 65535;
  const expressions = [];
  for (const [category, thresholdSecs] of Object.entries(criteria)) {
    const categoryData = dAnchorData[category];
    const travelTimeOptions = [];
    for (let i = 0; i < K_ANCHORS; i++) {
      travelTimeOptions.push([
        "+",
        ["coalesce", ["get", `a${i}_s`], UNREACHABLE],
        ["coalesce",
          ["get", ["to-string", ["get", `a${i}_id`]], ["literal", categoryData]],
          UNREACHABLE
        ]
      ]);
    }
    const minTravelTime = ["min", ...travelTimeOptions];
    expressions.push(["<=", minTravelTime, thresholdSecs]);
  }
  return ["case", ["all", ...expressions], 0.8, 0.0];
}
```

**Performance levers:** client‑side only after initial load; r7/r8 swap; 250ms debounce; cache D\_anchor per criterion; reuse one‑off custom D_anchor.

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
* Mode: `drive` (walk supported if walk tiles + D_anchor provided).
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

6. **Overlays Computation**

* **Dense brand detection**: Brands with ≥50 anchor sites qualify for overlay computation.
* **K=1 nearest-neighbor**: Compute shortest path from every hex to nearest brand anchor.
* **Output**: Per-brand overlay parquet files in `data/minutes/mode=0/` directory.

7. **Merge & Integration**

* Combine K-best travel times with overlay data.
* **Overlay priority**: Take minimum between K-best result and overlay result for each brand.
* Ensures 100% coverage for dense brands while maintaining efficiency.

8. **Summaries**

* Precompute `min_cat` & `min_brand` for exposed categories & A‑list brands.
* Long‑tail brands resolved via joins at query time.

9. **Tiles**

* Convert merged data → NDJSON → PMTiles (r7/r8 layers, exact layer names).

---

## 13) Query UX (Deterministic Rules)

* **Category filter:** Read `min_cat` (instant) or compute on GPU from D\_anchor chunks.
* **Brand (A‑list):** Read `min_brand` (instant); otherwise join `t_hex→sites→brands` and take min.
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

**Status:** POI overhaul complete with overlays system. T_hex compute + overlays + demo min‑based tiles are fully implemented and achieve 100% coverage for dense brands. D_anchor + full GPU composition remain the target design; this doc reflects the current reality and the end‑state architecture.

---

## Quick Start

- Create environment and install deps: `make init`
- Build native ext: `make native`
- Download data and normalize POIs: `make pois`
- Build anchor sites: `make anchors`
- Compute minutes (T_hex long format): `make minutes`
- Compute D_anchor category tables: `make d_anchor_category`
- Compute D_anchor brand tables: `make d_anchor_brand`
- Merge + summarize, build tiles, and serve demo: `make merge tiles` then `make serve` and open `http://localhost:5173/tiles/web/index.html`

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
4. Optional: compute overlays for analysis/QA (`make overlays`)

API:
- Run: `uvicorn api.main:app --reload`
- Categories: `GET /api/categories?mode=drive`
- D_anchor slice: `GET /api/d_anchor?category=<id>&mode=drive`
- D_anchor brand slice: `GET /api/d_anchor_brand?brand=<id or alias>&mode=drive`
- Custom point (escape hatch): `GET /api/d_anchor_custom?lon=<lon>&lat=<lat>&mode=drive`
