# TownScout: System Overview & LLM Implementation Spec

> Architectural map: `docs/ARCHITECTURE_OVERVIEW.md`

## 1) Purpose, Context, Goals

**Purpose.** TownScout helps a user answer: *"Where should I live given my criteria?"* by computing travel‑time proximity to things that matter (Chipotle, Costco, airports, schools, etc.) and rendering results as a fast, interactive map.

**Context.** Instead of precomputing every hex→category path, TownScout factorizes the problem into **hex→anchor** and **anchor→category/brand** legs. The frontend combines these in real time with GPU expressions using per‑anchor seconds stored in tiles and per‑anchor category/brand seconds served by the API. This keeps costs near‑zero and makes adding thousands of POIs (brands and categories) cheap.

**Primary Goals.**

* Each time a POI from OSM or Overture is added, the livable land for that user visually shrinks based on what the filter was.
* Sub‑second (≤250ms) slider→map response with zero server round‑trips.
* Add/extend categories without recomputing hex tiles.
* Keep deployment simple: static tiles on CDN, thin API for D\_anchor.
* Scale from a single state to nationwide without blowing up storage/compute.

**Non‑Goals.** Live traffic, multimodal chaining (walk→transit→drive), and route turn‑by‑turn.

**Important Limitation.** TownScout uses a matrix factorization approach (`hex→anchor + anchor→POI`) that can violate triangle inequality in complex road networks. This may produce routing approximations that differ significantly from true shortest paths, especially in suburban/rural areas. See `docs/ARCHITECTURE_OVERVIEW.md` for details and proposed mitigation strategies.

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

TownScout uses a two-stage approach to compute travel times:

```
Total Travel Time = T_hex[hex→anchor] + D_anchor[anchor→category]
```

This factorization enables efficient real-time filtering by precomputing hex-to-anchor times and computing anchor-to-POI times on-demand. The frontend combines these using GPU expressions for sub-second response times.

---

## Data Sources

TownScout combines multiple data sources for comprehensive POI coverage:
- **OSM** for road networks and civic/natural POIs
- **Overture Places** for commercial brands and businesses  
- **Curated CSVs** for specialized data (airports, trauma centers)

The hybrid approach provides better coverage than any single source alone.

---

## System Architecture

The system consists of four main layers:
1. **Data pipeline** (`src/`) - Ingests data, builds road graphs, computes travel times
2. **Native kernels** (`townscout_native/`) - High-performance Rust algorithms
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

---

## POI Module Architecture

The system uses a modular architecture for POI processing:
- **Shared POI logic** in `townscout/poi/` for ingestion, normalization, and conflation
- **Domain-specific handlers** in `townscout/domains_poi/` for specialized POI types
- **Overlay computation** in `townscout/domains_overlay/` for non-routable hex enrichment:
  - **Climate** - Temperature and precipitation metrics from PRISM normals
  - **Power corridors** - High-voltage transmission line proximity flags
  - **Political lean** - County-level 2024 presidential election results mapped to H3 cells

The architecture supports both category-based (e.g., "supermarket") and brand-based (e.g., "Whole Foods") queries, with configurable runtime limits and comprehensive coverage of livability-relevant categories.

## Documentation

- [Architecture Overview](docs/ARCHITECTURE_OVERVIEW.md) - Complete system design
- [Anchor Selection Strategy](docs/ANCHORS.md) - How anchors are chosen and scaled

For detailed module structure and implementation, see the architecture overview.

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
  - Visit `http://localhost:3000` (Next.js) — the app will call back to the FastAPI service on port 5173 unless you override `NEXT_PUBLIC_TOWNSCOUT_API_BASE_URL`

### Full Pipeline Command
```bash
make pois anchors minutes climate power_corridors d_anchor_category d_anchor_brand merge tiles
```

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
- Run: `uvicorn api.main:app --reload --host 0.0.0.0 --port 5173` (or expose the same base URL you pass via `NEXT_PUBLIC_TOWNSCOUT_API_BASE_URL`)
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
