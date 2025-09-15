Townscout POI Overhaul

Townscout’s livability analysis is only as good as its POI (Point of Interest) backbone. Until now, we’ve relied on a handful of categories (Chipotle, Costco, Airports). That’s too narrow: users want to ask nuanced questions like “Am I within 10 minutes of a Whole Foods?” or “Can I find a pizza place near here?”

This overhaul needs to maintain the cost-free architecture of Townscout. Whether that’s database, on-device storage, compute etc.

⸻

Goals
	•	Expand coverage beyond a few hand-picked POIs to essentially all livability-relevant categories: food, retail, education, health, recreation, civic, transport, natural amenities.
	•	Filter flexibility: support queries by category (“Any supermarket within 10 minutes”) and brand (“Whole Foods within 10 minutes”).
	•	Anchor Sites: co-located POIs share one precompute, cutting routing load 2–5× in dense clusters.
	•	Extensible: new categories, brands, or user-supplied data can be added by config, not code.

⸻

Data Analysis (Context & Motivation)

OSM baseline (Sept 2025, Massachusetts)
	•	Total POIs (broad filter): 76,688
	•	Distinct tag values: 639 (amenity/shop/leisure/etc.)
→ Much of OSM is “street furniture” (benches, waste baskets, platforms) rather than livability anchors.
	•	Category counts (examples):
	•	Bench: 10,632
	•	Restaurant: 3,468
	•	Fast food: 1,501
	•	Supermarkets/groceries: 661
	•	Banks: 586
	•	Brand counts: Dunkin’ 657, McDonald’s 235, Starbucks 216, Subway 127, Domino’s 65, Chipotle 53
	•	Coverage gap: Dunkin’ official = ~1,029 → OSM misses ~35%

Overture baseline (Sept 2025, Massachusetts clip via DuckDB)

duckdb -c "
INSTALL spatial; LOAD spatial;
INSTALL httpfs; LOAD httpfs;
SET s3_region='us-west-2'; SET s3_use_ssl=true;

COPY (
  SELECT *
  FROM read_parquet(
    's3://overturemaps-us-west-2/release/2025-08-20.0/theme=places/type=place/*',
    hive_partitioning=1
  )
  WHERE
    bbox.xmin BETWEEN -73.508142 AND -69.928393
    AND bbox.ymin BETWEEN 41.186328 AND 42.886589
) TO 'data/overture/ma_places.parquet' (FORMAT PARQUET);
"
“
WITH ma AS (SELECT * FROM read_parquet('data/overture/ma_places.parquet'))
SELECT
  COALESCE(LOWER(brand.names.primary), LOWER(names.primary)) AS label,
  COUNT(*) AS n
FROM ma
WHERE COALESCE(brand.names.primary, names.primary) IS NOT NULL
GROUP BY 1
ORDER BY n DESC
LIMIT 30;
"
“
WITH ma AS (SELECT * FROM read_parquet('data/overture/ma_places.parquet')),
cats AS (
  SELECT LOWER(categories.primary) AS cat
  FROM ma
  WHERE categories.primary IS NOT NULL

  UNION ALL

  SELECT LOWER(c.unnest) AS cat
  FROM ma, UNNEST(categories.alternate) AS c
)
SELECT cat, COUNT(*) AS n
FROM cats
WHERE cat IS NOT NULL AND cat <> ''
GROUP BY 1
ORDER BY n DESC
LIMIT 30;
"

	•	Total POIs: 461,249 (from ma_places.parquet)
	•	Schema (simplified):
	•	categories: STRUCT(primary VARCHAR, alternate VARCHAR[])
	•	names: STRUCT(primary VARCHAR, common MAP(...))
	•	brand: STRUCT(names STRUCT(primary VARCHAR,...), wikidata VARCHAR)
	•	Top categories (primary + alternates):
	•	professional_services (41k), health_and_medical (30k), restaurants (26k), shopping (20k), hospitals (12.5k), schools (10.3k).
	•	Supermarkets/grocery: 2,057
	•	Coffee chains: Dunkin’ 1,827; Starbucks 619; Cumberland Farms 201; Honey Dew 98; Caffè Nero 30; Marylou’s 28.
	•	Pizza chains: Domino’s 275; Papa Gino’s 172; Pizza Hut 53; Regina 14; Frank Pepe’s 13.
	•	Banks: Bank of America ATM 676; Santander Bank US 649; Citizens Bank 209; Eastern Bank 107; Wells Fargo 32.
	•	Brand spine is complete: Overture solved the Dunkin’ undercount immediately.
	•	Limitation: brand.wikidata was empty in this release → rely on brand.names.primary.

Key Insights
	•	OSM coverage is uneven. It overrepresents low-value categories, underrepresents major chains.
	•	Overture coverage is strong. It gives normalized categories and robust brand presence.
	•	Hybrid is best. Overture provides the brand/corporate spine; OSM provides civic, natural, local, and geometry detail.

⸻

Core Concepts

POIs (Points of Interest)
	•	Sources:
	•	Overture Places (GeoParquet) → brand/commercial coverage.
	•	OSM extract (Pyrosm/OGR) → civic + community POIs.
	•	Optional CSVs.
	•	Normalized schema: name, brand, category, subcat, geometry, source, license, provenance.

 Both Overture and OSM have messy, evolving tags. “supermarket” in OSM may overlap with “grocery store” in Overture; “restaurant” covers everything from fine dining to a pizza joint.
	•	Without a canonical taxonomy + synonyms, your filter flexibility (“Whole Foods within 10 minutes” vs “any supermarket”) breaks down fast.
	•	Suggestion: Define a Townscout Taxonomy upfront, and maintain a mapping layer (OSM tags, Overture categories → Townscout taxonomy).
	•	Right now you’re relying on brand.names.primary (Overture) and names.primary (OSM). That will collapse quickly under alias chaos: “CVS,” “CVS Pharmacy,” “CVS Health,” “CVS/pharmacy.”
	•	You need a brand registry with canonical IDs, aliases, and possibly Wikidata/QIDs. Otherwise, your A-list brands vs long-tail approach will misfire.

Anchor Sites
	•	Definition: group of POIs snapped to the same road network node for a mode (drive/walk).
	•	Purpose: one precompute per site instead of per POI, reducing compute 2–5×.
	•	Provenance: site can contain both Overture + OSM POIs.

Travel Precompute
	•	Multi-source Dijkstra from sites, not raw POIs.
	•	Store top-K per hex with category and brand quotas.
You’ll need to quantify the upper bound of precompute size. Otherwise, you risk over-committing compute/storage. A “top-K per hex” sounds fine, but what’s K? 10? 100? Too small = user misses results, too big = storage blowup.

⸻

Data Schema

POI Schema
	•	poi_id: str (uuid5 over source|ext_id|rounded lon/lat)
	•	name: str
	•	brand_id: str|null (canonical registry ID)
	•	brand_name: str|null
	•	class: str (venue, civic, transport, natural, etc.)
	•	category: str (supermarket, hospital, etc.)
	•	subcat: str (ER, preschool, mexican fast food, etc.)
	•	lon, lat: float32
	•	geom_type: uint8 (0=point, 1=centroid, 2=entrance)
	•	area_m2: float32
	•	source: str (overture, osm, fdic, snap, cms, csv:chipotle, user)
	•	ext_id: str|null
	•	h3_r9: str
	•	node_drive_id, node_walk_id: int64|null
	•	dist_drive_m, dist_walk_m: float32
	•	anchorable: bool
	•	exportable: bool
	•	license, source_updated_at, ingested_at: str
	•	provenance: list[str]

Anchor Site Schema
	•	site_id: str (uuid5 of mode|node_id)
	•	mode: str (drive, walk)
	•	node_id: int64
	•	lon, lat: float32 (node coords)
	•	poi_ids: list[str]
	•	brands: list[str]
	•	categories: list[str]
	•	brand_tiers: list[int]
	•	weight_hint: int (major chain vs local)

t_hex (Travel Precompute)
	•	hex_r9: str
	•	site_id: str
	•	time_s: uint16 (seconds, sentinel 65535 = ≥cutoff)

Summaries
	•	min_cat: (hex_r9, category, min_time_drive_s, min_time_walk_s)
	•	min_brand: (hex_r9, brand_id, min_time_drive_s, min_time_walk_s)

⸻

Pipeline
	1.	Ingest
	•	Overture Places → data/overture/ma_places.parquet (DuckDB to clip + filter).
	•	OSM extract → data/osm/massachusetts.osm.pbf (Pyrosm/OGR).
	•	Optional CSVs.
	2.	Normalize
	•	Lowercase, strip names.
	•	Brand resolution (brand.names.primary > names.primary > alias registry).
	•	Map categories.primary + alternates (Overture) and amenity/shop/cuisine (OSM) to taxonomy.
	3.	Conflate & Deduplicate
	•	H3 r9 proximity (Walking 0.25 mile, driving 1 mile.)
	•	Same brand + category → merge.
	•	Tie-breaks: Overture wins for chains; OSM wins for civic/natural + polygons.
	•	Merge provenance (["overture","osm"]).
	4.	Build Anchor Sites
	•	Snap POIs to nearest road network node.
	•	Group by node_id + mode → one site.
	•	Aggregate POIs, brands, categories.
	5.	Travel Precompute
	•	Multi-source Dijkstra from anchor sites.
	•	Store top-K per hex (global, per-category, per-brand quotas).
	6.	Summaries
	•	Precompute min_cat and min_brand for exposed categories + A-list brands.
	•	Long-tail brands handled via joins.

⸻

Query UX
	•	Category filter: check min_cat → instant.
	•	Brand filter (A-list): check min_brand → instant.
	•	Brand filter (long-tail): join t_hex→sites→brands, filter, take min.
	•	Fallback: optional local Dijkstra.

UI behavior:
	•	Brand selection auto-locks to underlying category.
	•	If no brand coverage, suggest category fallback.
	•	Clicking a hex shows nearest POI from the site.

⸻

Snapshots & Deltas
	•	Monthly snapshots (snapshot_date=YYYY-MM-DD).
	•	Track deltas (added/moved/removed POIs).
	•	Incremental precompute for changed anchors only.

⸻

In practice: Overture provides the brand/commercial spine (Dunkin, Starbucks, Stop & Shop, CVS). OSM provides civic, natural, and tag-rich POIs (schools, hospitals, playgrounds, parks). The two are normalized and conflated into a single canonical POI layer before building Anchor Sites and running travel precompute.

Anchor Sites & Graph Optimization
	•	Dynamic anchor radius: Instead of a fixed proximity (0.25 mi walk / 1 mi drive), adapt radius by density. In Manhattan, thousands of POIs may snap to a single node; in rural areas, a 1-mile radius might miss the nearest store entirely. Dynamic clustering balances load and coverage.
	•	Hierarchical anchors: Treat mega-sites (malls, hospital campuses, airports) as multi-level anchors → reduces over-merging while still keeping compute savings.
	•	Pre-bucket road network nodes: Instead of snapping POIs individually, maintain a node index (H3 partitioned) for faster assignment.

Data Quality & Conflation
	•	Confidence scoring: Not just Overture wins chains, OSM wins civic—add confidence weights. E.g., if both sources disagree on location, favor the one with fresher timestamp or polygon geometry.
	
•	Polygon awareness: Supermarkets, parks, schools often mapped as polygons in OSM. Don’t reduce to centroids only—retain area for better UX (bounds checks, proximity logic).