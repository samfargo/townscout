Townscout: Data Hygiene + D_anchor Runtime Spec

This document defines fixes and parameter defaults for geometry hygiene, water/beach sourcing, and D_anchor runtime configuration.
Applies to all future state builds.

1. Geometry Hygiene: Shapely “create_collection” Errors

Issue
In Shapely 2.x, ufunc 'create_collection' not supported for the input types occurs when building GeometryCollection or Multi* objects from mixed or invalid entries (e.g., None, empty, or non-geometry types).

Fix Pattern

import shapely
from shapely.geometry.base import BaseGeometry

def clean_geoms(gdf, types=None):
    g = gdf.geometry
    g = g[g.notna()]
    g = g[~g.is_empty]
    g = g[g.apply(lambda x: isinstance(x, BaseGeometry))]

    if types is not None:
        g = g[g.apply(lambda x: x.geom_type in types)]

    # Optionally make valid for polygons
    if "Polygon" in (types or []) or "MultiPolygon" in (types or []):
        g = g.apply(lambda x: shapely.make_valid(x))

    return g

# Examples
polys = clean_geoms(gdf, ["Polygon", "MultiPolygon"])
lines = clean_geoms(gdf, ["LineString", "MultiLineString"])

Notes
	•	Always clean before any unary_union, MultiPolygon, or collection construction.
	•	Guard against numpy arrays of coordinates instead of geometry objects.
	•	Run per geometry type (polygon vs. line) rather than on mixed sets.


2. OSM “Could not find any data for given area”

Root Cause
State-level OSM extracts often omit global coastlines and offshore water polygons, leading to missing geometries when fetching natural=coastline, natural=water, or beach features.

Fix Options
	•	Preferred: Use Overture water polygons for lakes/shorelines in state builds; skip OSM water entirely.
	•	Alternative:
	•	Fetch a larger OSM region (multi-state) for coastlines and clip to the target state post-hoc.
	•	Or switch to waterway=* + natural=water|bay|beach filters, but expect sparse coverage inland.

Rationale: Simplifies pipeline and eliminates missing-geometry warnings. Overture’s water data is clean, global, and consistently typed.

3. Category/Brand Runtime Configuration

These limits define D_anchor compute and storage behavior.

Goals
	•	Cap unnecessary long-distance exploration.
	•	Keep per-label results compact.
	•	Maintain realistic UX thresholds (60 min for local amenities, up to 3 h for weekend destinations).

Defaults (data/taxonomy/d_anchor_limits.json)

{
  "category": {
    "airport":        {"max_minutes": 180, "top_k": 8},
    "ski_resort":     {"max_minutes": 180, "top_k": 12},
    "beach":          {"max_minutes": 180, "top_k": 12},
    "park":           {"max_minutes": 60,  "top_k": 12},
    "hospital":       {"max_minutes": 60,  "top_k": 10},
    "supermarket":    {"max_minutes": 60,  "top_k": 12},
    "fast_food":      {"max_minutes": 60,  "top_k": 12},
    "cafe":           {"max_minutes": 60,  "top_k": 12},
    "pharmacy":       {"max_minutes": 60,  "top_k": 12},
    "railway_station":{"max_minutes": 60,  "top_k": 10},
    "bus_station":    {"max_minutes": 60,  "top_k": 10}
  },
  "brand": {
    "costco":        {"max_minutes": 60, "top_k": 10},
    "trader_joes":   {"max_minutes": 60, "top_k": 12},
    "whole_foods":   {"max_minutes": 60, "top_k": 12},
    "target":        {"max_minutes": 60, "top_k": 12},
    "walmart":       {"max_minutes": 60, "top_k": 12},
    "starbucks":     {"max_minutes": 60, "top_k": 14},
    "dunkin":        {"max_minutes": 60, "top_k": 14},
    "mcdonalds":     {"max_minutes": 60, "top_k": 14},
    "cvs":           {"max_minutes": 60, "top_k": 12},
    "walgreens":     {"max_minutes": 60, "top_k": 12}
  }
}

Removed categories: ferry_terminal, clinic — no longer tracked as POI layers or runtime targets.


4. Implementation Notes

Runtime parameters in d_anchor_common.py
	•	Load JSON limits once at startup.
	•	For each label:

max_seconds = cfg["max_minutes"] * 60
top_k = cfg["top_k"]


	•	Pass max_seconds to the multi-source SSSP kernel.
	•	After computing distances, keep only the top_k nearest sources per target anchor (min-heap or partial sort).
	•	Omit distances beyond max_seconds.

Two-Pass Strategy (for sparse labels)

To control long tails for sparse sources (airport, ski_resort, beach):
	1.	Pass A: Run with tighter compute cap (150 min) and top_k as above.
	2.	Pass B: For anchors with 0 results, rerun with the full cap (180 min).
This preserves realism while cutting runtime by ~70–80 %.

Null Handling
	•	Distances > cap are not stored.
	•	Represent missing values as null in Parquet.
	•	In client shaders or GPU expressions: treat null as INF (1e9) during T_hex + D_anchor summation.

5. Summary

Type	Cap	top_k	Notes
Local brands/businesses	60 min	10–14	Groceries, coffee, pharmacy
Weekend destinations	180 min	8–12	Airport, ski_resort, beach
Healthcare/Parks	60 min	10–12	Keep modest cutoffs
Removed	–	–	ferry_terminal, clinic

Expected Impact
	•	3–10× speedup for sparse categories.
	•	Smaller Parquet outputs and faster joins.
	•	Cleaner, more intuitive time sliders for users.
	•	Zero “create_collection” and “no OSM data” warnings in state builds.

This is in response to the below runtime terminal output:

sam@Sams-MacBook-Pro townscout % make all
PYTHONPATH=src .venv/bin/python src/02_normalize_pois.py
--- Loading Overture POIs for massachusetts from data/overture/ma_places.parquet ---
[ok] Loaded 535885 POIs from Overture for massachusetts
--- Loading OSM POIs for massachusetts from data/osm/massachusetts.osm.pbf ---
[ok] Loaded 53630 POIs from OSM for massachusetts
--- Normalizing Overture POIs ---
[COSTCO] Overture input: 8 POIs
[COSTCO] Overture output: 20 POIs
[ok] Normalized 535585 POIs from Overture.
--- Normalizing OSM POIs ---
[COSTCO] OSM input: 3 POIs
[COSTCO] OSM output: 3 POIs
[ok] Normalized 53630 POIs from OSM.
[warn] Failed to load beaches: ufunc 'create_collection' not supported for the input types, and the inputs could not be safely coerced to any supported types according to the casting rule ''safe''
[warn] Failed to load coastlines: ufunc 'create_collection' not supported for the input types, and the inputs could not be safely coerced to any supported types according to the casting rule ''safe''
[warn] Failed to load water bodies: ufunc 'create_collection' not supported for the input types, and the inputs could not be safely coerced to any supported types according to the casting rule ''safe''
/Users/sam/townscout/.venv/lib/python3.11/site-packages/pyrosm/pyrosm.py:767: UserWarning: Could not find any OSM data for given area.
  gdf = get_user_defined_data(
--- Conflating POIs from all sources ---
[ok] Combined POIs: 589215 total
[COSTCO] Before deduplication: 23 POIs
[COSTCO] After deduplication: 23 POIs
[ok] Deduplicated POIs: 589093 remaining
[ok] Loaded 341 airports from CSV.
[ok] Saved 589434 canonical POIs to data/poi/massachusetts_canonical.parquet
--- Building anchor sites for massachusetts (drive) ---
PYTHONPATH=src .venv/bin/python src/03_build_anchor_sites.py \
                --state massachusetts \
                --mode drive \
                --pois data/poi/massachusetts_canonical.parquet \
                --pbf data/osm/massachusetts.osm.pbf \
                --out-sites data/anchors/massachusetts_drive_sites.parquet \
                --out-map data/anchors/massachusetts_drive_site_id_map.parquet
[COSTCO] Canonical POIs: 23 POIs
--- Building anchor sites for drive mode ---
[info] Anchorable POIs (overhaul scope): 33446 / 589434
[info] Building KD-tree from 5550698 graph nodes...
[info] Snapping 33446 POIs to nearest graph nodes (connectivity-aware)...
[info] Improved connectivity for 12586 POIs by selecting better-connected nodes
[info] 24245 POIs snapped within 1609m of the graph.
[info] Grouping POIs into anchor sites...
[COSTCO] Anchor sites: 13 sites with Costco
[ok] Built 20471 anchor sites from 24245 POIs.
[ok] Wrote 20471 sites to data/anchors/massachusetts_drive_sites.parquet
[ok] Wrote id map to data/anchors/massachusetts_drive_site_id_map.parquet
--- Computing minutes for massachusetts (drive) ---
PYTHONPATH=src .venv/bin/python src/03_compute_minutes_per_state.py \
                --pbf data/osm/massachusetts.osm.pbf \
                --pois data/poi/massachusetts_canonical.parquet \
                --mode drive \
                --cutoff 30 \
                --overflow-cutoff 60 \
                --k-best 20 \
                --res 7 8 \
                --out-times data/minutes/massachusetts_drive_t_hex.parquet \
                --anchors data/anchors/massachusetts_drive_sites.parquet
[info] Loading canonical POI data...
[info] Loading/building CSR graph from Pyrosm cache...
[info] Loading prebuilt anchors from data/anchors/massachusetts_drive_sites.parquet ...
[info] Mapping anchor sites to CSR node indices...
[info] Found 20471 valid anchor source nodes in the graph.
[info] Preparing adjacency (transpose for node→anchor times)...
[info] Calling native kernel (bucket K-pass) for k-best search (k=20, cutoff=30 min, overflow=60 min, threads=1)...
[info] Mapping results back to anchor IDs...
[info] Aggregating results into H3 hexes (precomputed H3) using native kernel...
[info] Aggregation complete. Total rows: 607773
[info] Writing final output to data/minutes/massachusetts_drive_t_hex.parquet...
[ok] wrote data/minutes/massachusetts_drive_t_hex.parquet  rows=607773  (long format)
PYTHONPATH=src .venv/bin/python src/climate/prism_to_hex.py
PYTHONPATH=src .venv/bin/python src/04_merge_states.py
--- Merging per-state data and creating summaries ---
Found 1 travel time files and 1 sites files.
[info] Using computed hexes as base coverage...
[info] Base coverage: 30461 hexes across all resolutions
[info] Attaching climate data...
[ok] Saved 4361 rows to state_tiles/us_r7.parquet
[ok] Saved 26100 rows to state_tiles/us_r8.parquet
--- Pipeline step 04 finished ---
CLIMATE_DECODE_AT_EXPORT=false PYTHONPATH=src .venv/bin/python src/05_h3_to_geojson.py \
                --input state_tiles/us_r8.parquet \
                --output tiles/us_r8.geojson
Debug: h3_val=613231726434451455 (<class 'int'>) -> h3_addr=882a301931fffff
Debug: h3_val=613231964924674047 (<class 'int'>) -> h3_addr=882a3391a3fffff
Debug: h3_val=613231808900759551 (<class 'int'>) -> h3_addr=882a314c67fffff
[ok] Wrote NDJSON features.
PYTHONPATH=src .venv/bin/python src/06_build_tiles.py \
                --input tiles/us_r8.geojson \
                --output tiles/t_hex_r8_drive.pmtiles \
                --layer t_hex_r8_drive \
                --minzoom 8 --maxzoom 12
26100 features, 16686792 bytes of geometry and attributes, 1385959 bytes of string pool, 0 bytes of vertices, 0 bytes of nodes
  99.9%  12/1240/1519  
2025/10/14 03:06:49 convert.go:159: Pass 1: Assembling TileID set
2025/10/14 03:06:49 convert.go:190: Pass 2: writing tiles
 100% |██████████████████████████████████████████████████████████████████████████████████| (797/797, 5329 it/s)        
2025/10/14 03:06:49 convert.go:244: # of addressed tiles:  797
2025/10/14 03:06:49 convert.go:245: # of tile entries (after RLE):  797
2025/10/14 03:06:49 convert.go:246: # of tile contents:  797
2025/10/14 03:06:49 convert.go:269: Total dir bytes:  2138
2025/10/14 03:06:49 convert.go:270: Average bytes per addressed tile: 2.68
2025/10/14 03:06:49 convert.go:239: Finished in  196.658917ms
[ok] tiles/t_hex_r8_drive.pmtiles
--- Computing D_anchor category for massachusetts (drive) ---
[debug] Reducing kernel threads from 8 to 1 to avoid oversubscription with 8 workers
[debug] Loaded anchors: rows=20471 took=0.03s
[info] Loaded 14 categories from allowlist data/taxonomy/category_allowlist.txt
[ok] Wrote stable label→id map to data/taxonomy/category_label_to_id.json
[ok] Wrote labels to data/taxonomy/category_labels.json
[debug] Loaded CSR + anchor mappings: nodes=5550698 anchors=20471 components=62 took=22.95s
[debug] Built category→source map for 16 categories in 0.02s
[info] Category 'airport': id=1, anchors=5, source_nodes=5
[debug] Category 'airport' comps=3 target_nodes=20169 build=0.00s fallback=False
[info] Category 'bus_station': id=2, anchors=155, source_nodes=155
[debug] Category 'bus_station' comps=5 target_nodes=20373 build=0.00s fallback=False
[info] Category 'cafe': id=3, anchors=1981, source_nodes=1981
[debug] Category 'cafe' comps=10 target_nodes=20391 build=0.00s fallback=False
[info] Category 'clinic': id=4, anchors=391, source_nodes=391
[debug] Category 'clinic' comps=1 target_nodes=20167 build=0.00s fallback=False
[info] Category 'fast_food': id=5, anchors=3179, source_nodes=3179
[debug] Category 'fast_food' comps=12 target_nodes=20400 build=0.00s fallback=False
[info] Category 'ferry_terminal': id=6, anchors=54, source_nodes=54
[debug] Category 'ferry_terminal' comps=8 target_nodes=20382 build=0.00s fallback=False
[info] Category 'hospital': id=7, anchors=2158, source_nodes=2158
[debug] Category 'hospital' comps=8 target_nodes=20389 build=0.00s fallback=False
[info] Category 'park': id=8, anchors=7320, source_nodes=7320
[debug] Category 'park' comps=39 target_nodes=20439 build=0.00s fallback=False
[info] Category 'pharmacy': id=9, anchors=1188, source_nodes=1188
[debug] Category 'pharmacy' comps=4 target_nodes=20371 build=0.00s fallback=False
[info] Category 'railway_station': id=10, anchors=446, source_nodes=446
[debug] Category 'railway_station' comps=4 target_nodes=20184 build=0.00s fallback=False
[info] Category 'supermarket': id=11, anchors=2472, source_nodes=2472
[debug] Category 'supermarket' comps=9 target_nodes=20390 build=0.00s fallback=False
[debug] Launching ProcessPool with max_workers=8 pending_tasks=11
[debug] Materialized memmap views for workers in 0.04s
[ok] Wrote D_anchor category id=8: data/d_anchor_category/mode=0/category_id=8/part-000.parquet rows=20471 sssp=20.78s write=0.04s total=20.81s
[debug] Category id=8 label='park' finished in 21.59s
[ok] Wrote D_anchor category id=5: data/d_anchor_category/mode=0/category_id=5/part-000.parquet rows=20471 sssp=60.87s write=0.02s total=60.89s
[debug] Category id=5 label='fast_food' finished in 61.67s
[ok] Wrote D_anchor category id=3: data/d_anchor_category/mode=0/category_id=3/part-000.parquet rows=20471 sssp=65.12s write=0.01s total=65.13s
[debug] Category id=3 label='cafe' finished in 65.93s
[ok] Wrote D_anchor category id=7: data/d_anchor_category/mode=0/category_id=7/part-000.parquet rows=20471 sssp=89.00s write=0.01s total=89.01s
[debug] Category id=7 label='hospital' finished in 89.79s
[ok] Wrote D_anchor category id=11: data/d_anchor_category/mode=0/category_id=11/part-000.parquet rows=20471 sssp=48.03s write=0.01s total=48.04s
[debug] Category id=11 label='supermarket' finished in 113.94s
[ok] Wrote D_anchor category id=9: data/d_anchor_category/mode=0/category_id=9/part-000.parquet rows=20471 sssp=104.58s write=0.01s total=104.59s
[debug] Category id=9 label='pharmacy' finished in 126.18s
[ok] Wrote D_anchor category id=4: data/d_anchor_category/mode=0/category_id=4/part-000.parquet rows=20471 sssp=303.45s write=0.01s total=303.47s
[debug] Category id=4 label='clinic' finished in 304.26s
[ok] Wrote D_anchor category id=10: data/d_anchor_category/mode=0/category_id=10/part-000.parquet rows=20471 sssp=459.92s write=0.01s total=459.93s
[debug] Category id=10 label='railway_station' finished in 521.59s
[ok] Wrote D_anchor category id=2: data/d_anchor_category/mode=0/category_id=2/part-000.parquet rows=20471 sssp=726.08s write=0.01s total=726.10s
[debug] Category id=2 label='bus_station' finished in 726.89s
[ok] Wrote D_anchor category id=1: data/d_anchor_category/mode=0/category_id=1/part-000.parquet rows=20471 sssp=2890.89s write=0.01s total=2890.90s
[debug] Category id=1 label='airport' finished in 2891.70s
[ok] Wrote D_anchor category id=6: data/d_anchor_category/mode=0/category_id=6/part-000.parquet rows=20471 sssp=3256.44s write=0.01s total=3256.45s
[debug] Category id=6 label='ferry_terminal' finished in 3257.24s
--- Computing D_anchor brand for massachusetts (drive) ---
[debug] Reducing kernel threads from 8 to 1 to avoid oversubscription with 8 workers
[debug] Loaded anchors: rows=20471 took=0.04s
[debug] Loaded CSR + anchor mappings: nodes=5550698 anchors=20471 components=62 took=22.88s
[info] Loaded 11 brands from allowlist data/brands/allowlist.txt
[debug] Built brand→source map for 20 brands in 22.90s total CSR pipeline
[info] Brand 'chipotle' → 'chipotle': anchors=134, source_nodes=134
[debug] Brand 'chipotle' comps=1 target_nodes=20167 build=0.00s fallback=False
[info] Brand 'costco' → 'costco': anchors=13, source_nodes=13
[debug] Brand 'costco' comps=2 target_nodes=20171 build=0.00s fallback=False
[info] Brand 'cvs' → 'cvs': anchors=648, source_nodes=648
[debug] Brand 'cvs' comps=2 target_nodes=20180 build=0.00s fallback=False
[info] Brand 'dunkin' → 'dunkin': anchors=1696, source_nodes=1696
[debug] Brand 'dunkin' comps=7 target_nodes=20193 build=0.00s fallback=False
[info] Brand 'mcdonalds' → 'mcdonalds': anchors=402, source_nodes=402
[debug] Brand 'mcdonalds' comps=5 target_nodes=20186 build=0.00s fallback=False
[info] Brand 'starbucks' → 'starbucks': anchors=565, source_nodes=565
[debug] Brand 'starbucks' comps=3 target_nodes=20304 build=0.00s fallback=False
[info] Brand 'target' → 'target': anchors=107, source_nodes=107
[debug] Brand 'target' comps=1 target_nodes=20167 build=0.00s fallback=False
[info] Brand 'trader_joes' → 'trader_joes': anchors=45, source_nodes=45
[debug] Brand 'trader_joes' comps=1 target_nodes=20167 build=0.00s fallback=False
[info] Brand 'walgreens' → 'walgreens': anchors=341, source_nodes=341
[debug] Brand 'walgreens' comps=1 target_nodes=20167 build=0.00s fallback=False
[info] Brand 'walmart' → 'walmart': anchors=114, source_nodes=114
[debug] Brand 'walmart' comps=2 target_nodes=20168 build=0.00s fallback=False
[info] Brand 'whole_foods' → 'whole_foods': anchors=59, source_nodes=59
[debug] Brand 'whole_foods' comps=1 target_nodes=20167 build=0.00s fallback=False
[debug] Launching ProcessPool with max_workers=8 pending_tasks=11
[debug] Materialized memmap views for workers in 0.02s
[ok] Wrote D_anchor brand 'dunkin': data/d_anchor_brand/mode=0/brand_id=dunkin/part-000.parquet rows=20471 sssp=65.65s write=0.02s total=65.68s
[debug] Brand 'dunkin' finished in 66.25s
[ok] Wrote D_anchor brand 'cvs': data/d_anchor_brand/mode=0/brand_id=cvs/part-000.parquet rows=20471 sssp=202.15s write=0.02s total=202.17s
[debug] Brand 'cvs' finished in 202.75s
[ok] Wrote D_anchor brand 'mcdonalds': data/d_anchor_brand/mode=0/brand_id=mcdonalds/part-000.parquet rows=20471 sssp=242.65s write=0.02s total=242.67s
[debug] Brand 'mcdonalds' finished in 243.25s
[ok] Wrote D_anchor brand 'starbucks': data/d_anchor_brand/mode=0/brand_id=starbucks/part-000.parquet rows=20471 sssp=330.24s write=0.02s total=330.26s
[debug] Brand 'starbucks' finished in 330.83s
[ok] Wrote D_anchor brand 'walgreens': data/d_anchor_brand/mode=0/brand_id=walgreens/part-000.parquet rows=20471 sssp=280.97s write=0.02s total=280.99s
[debug] Brand 'walgreens' finished in 347.23s
[ok] Wrote D_anchor brand 'walmart': data/d_anchor_brand/mode=0/brand_id=walmart/part-000.parquet rows=20471 sssp=1067.09s write=0.02s total=1067.11s
[debug] Brand 'walmart' finished in 1269.83s
[ok] Wrote D_anchor brand 'chipotle': data/d_anchor_brand/mode=0/brand_id=chipotle/part-000.parquet rows=20471 sssp=1327.71s write=0.02s total=1327.73s
[debug] Brand 'chipotle' finished in 1328.31s
[ok] Wrote D_anchor brand 'target': data/d_anchor_brand/mode=0/brand_id=target/part-000.parquet rows=20471 sssp=1723.75s write=0.02s total=1723.77s
[debug] Brand 'target' finished in 1724.34s
[ok] Wrote D_anchor brand 'trader_joes': data/d_anchor_brand/mode=0/brand_id=trader_joes/part-000.parquet rows=20471 sssp=3396.73s write=0.02s total=3396.75s
[debug] Brand 'trader_joes' finished in 3397.32s
[ok] Wrote D_anchor brand 'whole_foods': data/d_anchor_brand/mode=0/brand_id=whole_foods/part-000.parquet rows=20471 sssp=3399.86s write=0.01s total=3399.88s
[debug] Brand 'whole_foods' finished in 3643.11s
[ok] Wrote D_anchor brand 'costco': data/d_anchor_brand/mode=0/brand_id=costco/part-000.parquet rows=20471 sssp=4953.04s write=0.02s total=4953.05s
[debug] Brand 'costco' finished in 4953.63s
✅ Full pipeline complete. Total time: 4h 15m 29s