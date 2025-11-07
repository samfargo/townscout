# Quality Control Infrastructure

Vicinity’s factorized travel-time model (`hex → anchor + anchor → POI`), GPU filtering, and quantized overlays only pay off if the most basic data invariants hold. While the system described in `README.md` can support a heavy QA program, we are still in the earliest product stage. This document therefore lists only the non-negotiable checks that keep the pipeline trustworthy without slowing feature delivery.

---

## 1. Operating Principles
- **Focus on blockers, not polish.** Every check below exists because the product breaks without it.
- **Automate once, observe manually elsewhere.** Lightweight scripts guard contracts; everything else is a human spot-check.
- **Traceability over volume.** If a tile looks wrong, we must be able to trace it back to source files, code revision, and the small set of checks that ran.

---

## 2. Essential Checks by Stage

| Stage / Target | Must-Have QA Focus | Lightweight Automation | Manual Spot Check |
| --- | --- | --- | --- |
| **Source acquisition** (`make pois`) | Confirm files are fresh and uncorrupted before ingestion. | Record file hashes + download timestamps in a CSV ledger; fail if unchanged for >7 days or size delta >25%. | Skim ledger before each run; verify curated CSV edits in git diff. |
| **Taxonomy & normalization** (`vicinity/poi/`, `data/taxonomy/*.csv`) | Canonical POI parquet has expected columns and category/brand coverage. | Pandera schema for canonical POI + assert every category/brand listed in the registries shows up at least once. | Load a random POI sample in a notebook; confirm display names/categories look reasonable. |
| **Anchor + travel-time kernels** (`make anchors`, `make minutes`) | Anchors remain unique and travel-time arrays stay in sync with anchor IDs. | Small pytest (`tests/test_anchor_contract.py`) verifying `site_id` uniqueness, allowed modes, and >0 POIs per anchor. Separate check that `a{i}_id` entries in T_hex exist in the anchor parquet and seconds are non-decreasing with `65535` sentinel capped at 1%. | Plot anchor density (existing notebook) for the latest state and confirm no obvious gaps. |
| **D_anchor tables** (`make d_anchor_category`, `make d_anchor_brand`) | D_anchor shards reference valid anchors and seconds fall within sane bounds. | Script that joins each shard against anchors, fails if any anchor missing, and reports P50/P95 seconds; fail if P95 > 7200 s. | Hit `/api/d_anchor?category=<popular>` locally and confirm slider shrinks map as expected. |
| **Tiles + API** (`make tiles`, `api/`) | Tile schema matches what the web worker expects and API contracts stay stable. | JSON schema diff between latest PMTiles metadata and a checked-in minimal contract; FastAPI test that `/api/d_anchor*` returns required keys with HTTP 200. | Load `tiles/web` dev server, run “Costco + airport” preset, eyeball for obviously wrong regions. |

These five checkpoints are the only stages we actively gate on today. Everything else (overlays, climate extras, advanced drift metrics) is deferred until we have broader coverage.

---

## 3. Minimal Automated Suite
Keep automation scoped to scripts that run in under a minute:
- `tests/test_poi_schema.py` (Pandera) — canonical POI parquet columns, dtypes, and required enumerations.
- `tests/test_anchor_contract.py` — uniqueness, allowed transport modes, anchors tied to ≥1 POI.
- `tests/test_t_hex_contract.py` — ensures each `a{i}_id` exists, travel seconds monotonic, sentinel usage <1%.
- `scripts/check_d_anchor_stats.py` — joins shards to anchors and enforces the `P95 ≤ 7200 s` bound.
- `scripts/check_tile_schema.py` — compares PMTiles metadata to `docs/tile_contract.json`.

If any of these fail, stop the release; otherwise proceed without additional dashboards or notebooks.

---

## 4. Manual QA Rituals
- **Ledger glance:** Before each pipeline run, confirm the source ledger shows fresh timestamps and intentional CSV edits.
- **Sample sanity notebook:** After normalization, pull 20 random POIs and anchors to ensure taxonomy mapping looks human-readable.
- **Map smoke run:** Open the dev map, apply two canonical filters (e.g., Costco + airport), and confirm livable area shrinks logically in Boston, Denver, and Phoenix test hexes.
- **API curl check:** `curl '/api/d_anchor?category=grocery&mode=drive'` and verify non-empty payload plus `data_timestamp` metadata.

Each ritual takes <5 minutes and catches issues automation misses at this stage.

---

## 5. Lightweight Release Gate
1. Run the relevant `make` targets end-to-end on staging data.
2. Execute the minimal automated suite above (`pytest tests/test_* && python scripts/check_*.py`).
3. Capture a short QA note in the source ledger (hashes, git SHA, pass/fail per check).
4. Perform the manual rituals (ledger glance, notebook sample, map smoke run, API curl).
5. If all pass, publish artefacts to the CDN/API buckets; otherwise fix or roll back before retrying.

This keeps us honest on the fundamentals without building a heavy QA platform before product-market fit.
