# Quality Control Infrastructure

Vicinity relies on a factorized travel time model (`hex → anchor + anchor → POI`), GPU-side filtering, and quantized overlays to deliver trustworthy livability insights. Quality control must therefore protect three things: the integrity of upstream data, the mathematical guarantees of the factorization, and user-facing correctness in the map and API. This document lays out a pragmatic QA program tailored to the current architecture described in `README.md` and `docs/ARCHITECTURE_OVERVIEW.md`.

---

## 1. Guiding Principles
- **Accuracy is the moat.** Every release should document why T_hex tiles, D_anchor tables, and overlays are more reliable than the previous iteration.
- **Contracts over ad hoc checks.** Encode schema and value expectations close to where artifacts are produced so failures are caught before downstream steps.
- **End-to-end traceability.** Any hex value surfaced in the map must be explainable in terms of source data versions, pipeline code revision, and QA status.

---

## 2. Coverage Across the Pipeline

| Stage (Make target / module) | Primary Artefacts | QA Focus | Automated Checks | Manual/Observability |
| --- | --- | --- | --- | --- |
| Source acquisition (`make pois`, `src/01_download_extracts.py`) | OSM PBF, Overture parquet, curated CSVs | Source freshness, completeness | Checksums, version ledger, file size deltas | Source dashboard highlighting lagging feeds |
| Taxonomy / POI normalization (`src/02_normalize_pois.py`, `vicinity/poi/`) | Canonical POI parquet | Schema integrity, taxonomy mapping, deduplication | Pandera schema validation, brand/category coverage counts, duplicate H3 detection | QA map layer for randomized POI spot checks |
| Domain POI handlers (`vicinity/domains_poi/`) | Airports, trauma centers, beaches | Special-case logic, licensing constraints | Great Expectations suites per domain, HTTP response validation for external APIs | Operator checklist for curated CSV updates |
| Anchor construction (`make anchors`, `vicinity/domains_anchor/`) | `site` parquet | Connectivity-aware snapping, k-best coverage | Distribution checks for node degree, expected anchor counts per region, anchor vs POI join sanity | Visual diff of anchor density heatmap |
| Travel-time kernels (`make minutes`, `vicinity_native/`) | T_hex parquet | Sentinel handling, runtime regression | Rust unit tests, Python smoke tests comparing against golden hex samples, runtime guardrails | Alert on >10% runtime change or >1% unreachable anchors |
| Overlay enrichment (`make climate`, `make power_corridors`, `vicinity/domains_overlay/`) | Climate parquet, power corridor flags | Value ranges, quantization, spatial joins | Existing pytest (`tests/test_climate_parquet.py`), additional range tests, H3 coverage completeness | Spot-check via climate overlay dashboard |
| D_anchor tables (`make d_anchor_category`, `make d_anchor_brand`) | Parquet shards served by API | Contract with T_hex, latency budgets | Join check ensuring all anchor IDs exist in tiles, percentile stats on seconds, sentinel rate monitoring | API synthetic probe comparing historical distributions |
| Tile build (`make tiles`, `src/09_build_tiles.py`) | PMTiles, NDJSON | Attribute preservation, layer IDs | Schema diff against previous release, tippecanoe log parsing for dropped features | Map QA session on staging dataset |
| FastAPI (`api/`) and frontend (`tiles/web/`) | JSON responses, GPU expressions | API schema stability, GPU expression correctness | Contract tests for `/api/d_anchor*`, recorded browser-based assertions verifying slider → tile response, automated smoke tests hitting map worker | Synthetic user journey monitors (add Costco + airport, validate map shrink) |

---

### 2.1 Tile Regeneration Checklist
- When overlay data (politics, climate, power corridors, etc.) changes, rerun the core pipeline so vector tiles inherit the new attributes: `make state_tiles geojson tiles` (or simply `make tiles` if upstream artefacts are already current).
- Skip ad hoc patch scripts—`src/07_merge_states.py` and `src/08_h3_to_geojson.py` already merge overlay columns into the tile exports as long as the refreshed parquet files are present.
- As a quick QA spot check, inspect the NDJSON output after rebuilding (e.g., `rg "political_lean" tiles/us_r8.geojson`) to confirm the overlay columns made it through before running `src/09_build_tiles.py`.

## 3. Automated Validation Suites

### 3.1 Schema and Contract Enforcement
- Maintain Pandera models for canonical POI, anchor site, climate metrics, and D_anchor outputs. Fail builds when columns are missing, types drift, or required enumerations expand unexpectedly.
- Extend existing pytest coverage beyond climate parquet to include:
  - `tests/test_poi_schema.py` validating the canonical POI parquet.
  - `tests/test_anchor_contract.py` verifying `site_id` uniqueness, `mode` enumerations, and minimum POI membership.
  - `tests/test_t_hex_contract.py` ensuring `a{i}_id`/`a{i}_s` arrays exist with monotonic seconds and `65535` sentinel usage < 0.5%.
- Enforce tile schema by comparing freshly built PMTiles metadata with a checked-in contract (`docs/tile_contract.json`).

### 3.2 Value, Range, and Drift Checks
- Define quantized climate bounds (e.g., `temp_mean_ann_f_q ∈ [-300, 1300]`) and precipitation bounds with automatic flagging.
- Monitor D_anchor seconds distributions per category/brand; flag if P95 exceeds historical baselines by >15%.
- Run windowed drift detection (whylogs or Evidently) on canonical POI counts per category, anchor counts per state, and T_hex travel time percentiles.
- Validate reciprocity between T_hex and D_anchor by sampling hexes, recomputing total travel minutes for a common category set, and comparing against previous release medians.

### 3.3 Spatial Integrity
- Use H3 polyfill comparisons to confirm POIs, anchors, and travel-time hexes align within the intended geography (e.g., Massachusetts pilot vs nationwide rollout).
- Enforce `point-in-polygon` checks against state boundaries; flag POIs outside coverage unless `anchorable=false`.
- Run nearest-neighbour deduplication audits (<10 m duplicates) and ensure conflation collapses duplicates rather than exploding counts.
- Reverse-geocode samples to validate county/state alignment and compare with `political_lean` overlays when present.

### 3.4 Performance and Regression Guardrails
- Benchmark Rust kernels during CI (sample dataset) and alert on >10% runtime or memory regressions.
- Track API latency for core endpoints (`/api/d_anchor*`, `/api/catalog`, `/api/d_anchor_custom`); fail CI if synthetic load tests exceed 300 ms P95.
- Record GPU expression evaluation snapshots from the web worker, ensuring the number of tiles filtered matches expectations for canonical scenarios (e.g., Costco + airport + climate filter).

---

## 4. Monitoring, Alerting, and Dashboards
- **Data freshness:** Grafana tiles showing source ingest timestamps, `make` pipeline runs, and delta vs expected cadence.
- **Quality scorecards:** Percentage of POIs passing validation, anchors with valid connectivity, climate hexes within bounds, and drift statistics. Publish alongside each release.
- **Runtime observability:** Prometheus metrics from FastAPI covering error rates, latency, cache hit ratios, and sentinel usage. Include anomaly detectors that ping Slack when sentinel rate surges or catalog size drops.
- **Pipeline visibility:** Emit structured logs per step with artefact hashes, record counts, and validation outcomes. Push summaries to a QA run log in `out/logs/qa_runs/`.

---

## 5. Human-in-the-Loop Review
- Run a weekly QA map review: randomly sample hexes, inspect hover details (travel time breakdown, climate stats, political lean) and compare with ground truth sources (Google Maps travel time, NOAA climate normals).
- Provide an internal feedback tool inside the Next.js app (feature flag) allowing annotators to tag suspicious POIs, travel times, or overlays. Store annotations in `data/qa/annotations.parquet`.
- Integrate user-facing flagging in production; triage flagged items into JIRA with artifact metadata for reproducibility.

---

## 6. Data Provenance and Reproducibility
- For every parquet or PMTiles artefact, attach metadata including:
  - `source_versions`: map of dataset → timestamp/hash.
  - `etl_revision`: git SHA of pipeline code.
  - `qa_suite_revision`: git SHA/tag of validation scripts.
  - `qa_pass`: boolean, plus list of failing check identifiers if false.
- Store provenance manifests in `out/provenance/<artefact>.json` and publish aggregates in a lightweight ledger (`docs/provenance_index.md`).
- Embed provenance snippets into API responses (e.g., `/api/catalog` includes `data_timestamp`, `/api/d_anchor` exposes `tiles_build_id`). The frontend can surface this in a “data last updated” badge for transparency.

---

## 7. Release Gating Workflow
1. Run the full `make` pipeline in staging with QA mode enabled (toggles extra logging and sample exports).
2. Execute automated validations (pytest, Pandera, drift detection).
3. Publish QA report summarizing pass/fail, drift metrics, and manual review notes.
4. Require sign-off from data engineering and product before promoting artefacts to production CDN/API buckets.
5. Post-release, monitor real-time alerts for 24 hours; roll back tiles or D_anchor shards if sentinel usage or error rate spikes.

---

## 8. Implementation Backlog
1. Finish Pandera schemas for canonical POI, anchor, and D_anchor outputs.
2. Add regression harness comparing T_hex + D_anchor combinations across releases (golden hex suite).
3. Build QA dashboard (Grafana) with freshness, pass rate, and drift visualizations.
4. Instrument FastAPI and web worker with Prometheus exporters and ship baseline alerts.
5. Implement internal annotation tool and wire it to QA triage workflow.
6. Prototype user-facing “Report an issue” UX in the Next.js app with review queue integration.

---

## 9. Philosophical Rule
Automation, explainability, and measurement trump manual heroics. Codify every defect as a test, keep provenance visible, and treat QA as a first-class feature of Vicinity’s map experience.
