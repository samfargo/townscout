
We need to add beaches to our Points of Interest categories. They must be separated by lake beach and ocean beach.

Beach classification spec (ocean vs lake)

Inputs (DuckDB Spatial)

Create/expect views with these columns:
	•	beaches(id TEXT, name TEXT, tags JSON, geom_wkb BLOB)
OSM features with natural=beach (polygons or multipolygons; include nodes/lines converted to tiny buffers if you keep them).
	•	coastline(id TEXT, geom_wkb BLOB)
OSM natural=coastline lines.
	•	inland_water(id TEXT, tags JSON, geom_wkb BLOB)
OSM natural=water polygons with water subtag present.
	•	(optional but recommended) riverbanks(id TEXT, tags JSON, geom_wkb BLOB)
OSM waterway=riverbank or natural=water & water=river polygons (lets you label “river” vs dump into “other”).

Output
	•	classified_beaches(id, name, beach_type, geom_wkb) where beach_type ∈ {'ocean','lake','river','other'}.

CRS & tolerances
	•	Transform to EPSG:3857 (fast, good enough for ≤200 m tests).
	•	Tolerances (tune if needed):
	•	D_COAST = 150m (buffer around coastline lines)
	•	D_LAKE  = 100m (buffer around inland water polygons)
	•	D_RIVER = 80m

Geometry normalization
	1.	ST_MakeValid on all polygons.
	2.	Convert to metric CRS (ST_Transform(..., 3857)).
	3.	Build a representative point per beach: ST_PointOnSurface(beach_poly) (use for distance checks).
	4.	Optional: ST_Subdivide very long coastline lines for performance.

Priority rules (deterministic)

Evaluate in order; first match wins.
	1.	Ocean if beach intersects ST_Buffer(coastline, D_COAST) or beach’s point is ST_DWithin to any coastline segment ≤ D_COAST.
(This captures beaches mapped slightly inland from the tide line and avoids gaps.)
	2.	Lake if not ocean and beach (poly or point) is within D_LAKE of any inland_water where
lower(coalesce(tags->>'water','')) IN ('lake','reservoir','lagoon').
	3.	River (optional) if not ocean/lake and within D_RIVER of riverbanks (waterway='riverbank' or water='river').
	4.	Other otherwise (ponds, canals, tagging oddities).

Tie-breakers (rare but covered)
	•	If both “ocean” and “lake” fire (e.g., lagoon barrier): ocean wins (saltwater beach UX expectation).
	•	If both “lake” and “river” fire: choose the larger overlap area; if using point-distance, prefer lake.

Integration points (where this lives)
	•	src/02_normalize_pois.py
	•	After you assemble canonical POIs, materialize beaches, coastline, inland_water, riverbanks views (they can all come from the same raw OSM parquet filtered by tags).
	•	Run the SQL above via DuckDB, persist to data/poi/<state>_canonical.parquet with category='beach' and beach_type column.
	•	Ensure src/taxonomy.py maps natural=beach → category='beach' and does not attempt to infer ocean/lake; the SQL adds beach_type.
	•	src/03_build_anchor_sites.py
	•	No change; anchors are built from canonical POIs. Keep beach_type in metadata for the frontend (or partition D_anchor by category+beach_type if you want separate sliders).
	•	API/Frontend
	•	/api/catalog: expose subcategory values for beach as ['ocean','lake'].
	•	UI: show a “Beaches” parent with checkboxes for Ocean / Lake (intersection semantics already handled by your map worker).

Validation (minimal & automated)
	•	Assert 0 coastal false negatives in a coastal bbox (sample 50 known ocean beaches).
	•	Precision ≥95% for lake beaches in a lake-rich bbox (e.g., Minneapolis metro).
	•	Cap unclassified (other) ≤10% per state; log top offenders by tag combo for review.

Performance notes
	•	Pre-clip all water & coastline by your state bbox before buffering.
	•	ST_Subdivide long coastline lines to ~1–2 km segments before buffering to cut join time.
	•	Use polygon OR point tests (poly OR pt) as above; it eliminates near-misses without exploding geometry complexity.

Things not to do
	•	Don’t use name heuristics (“Beach”, “Ocean”) — brittle.
	•	Don’t rely on centroids only — narrow spits/barrier beaches will fail.
	•	Don’t set tolerances <50 m — tagging misalignments are common.