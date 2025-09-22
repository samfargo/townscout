Townscout POI Overhaul

Townscout is not Yelp. Its purpose is livability: the handful of things that truly shape where people want to live. Everything else is just noise. If you try to cover every pizza shop and nail salon, you burn compute/storage and confuse users. The right backbone is a tight core set of precomputed POIs + an escape hatch for edge cases.

Townscout’s livability analysis is only as good as its POI (Point of Interest) backbone. Until now, we’ve relied on a handful of categories (Chipotle, Costco, Airports). That’s too narrow: users want to ask nuanced questions like “Am I within 10 minutes of a Whole Foods?” or “Can I find a grocery store near here?”

This overhaul needs to maintain the cost-free architecture of Townscout. Whether that’s database, on-device storage, compute etc.

⸻

Goals
	•	Expand coverage beyond a few hand-picked POIs to the following:
	•	Filter flexibility: support queries by category (“Any supermarket within 10 minutes”) and brand (“Whole Foods within 10 minutes”).
	•	Anchor Sites: co-located POIs share one precompute, cutting routing load 2–5× in dense clusters. TBD if these will be necessary.
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


We don’t want to dilute “livability” into “every possible POI.” People don’t choose where to live based on Joe’s Pizza.

1. Tighten the Core Set (always precomputed)

These are universal drivers that people consistently weigh:
	•	Major retail & grocery
Costco, Walmart, Target, Whole Foods, Trader Joe’s, Kroger, H-E-B, Stop & Shop, Albertsons (swap in regionals per market).
Category layer: “Any supermarket” (catch-all for local chains).
	•	Coffee & fast casual anchors
Starbucks, Dunkin’, McDonald’s, Chipotle.
Category layer: “Any café / coffee shop.”
	•	Critical services
Airports, hospitals, urgent care, transit stations (rail/subway/ferry/bus terminals).
	•	Recreation / nature
Beaches, ski resorts, mountains/trailheads, regional parks.

That set of POI is small enough to precompute fully, big enough to feel comprehensive.

2. Deprioritize the Long Tail
	•	By tightening the core set, we are excluding random local restaurants, nail salons, mom and pop shops.
	•	The long tail isn’t about livability — it’s about convenience once you’re already there.
	•	Nobody moves house to shave 5 minutes off a drive to their local dry cleaner.

3. Escape Hatch for Edge Cases

Instead of precomputing the tail:
	•	Provide a custom input field:
	•	“Add custom location”
	•	Type in an address, business name, or drop a pin on the map.
	•	The system does a one-off drive-time calculation for that one location and factors it into the livable area.
	•	This scratches the “but what about my yoga studio?” itch without bloating your model.

Think of it as the Google Maps fallback: core livability is fast and pre-baked; custom locations are just ad hoc routing.

4. Why This Is Better
	•	Signal vs. noise: keeps TownScout about major life drivers, not local trivia.
	•	Performance: tiny, predictable precompute set (dozens, not thousands).
	•	UX clarity: the app looks purposeful (“these are the things that matter for deciding where to live”).
	•	Scalability: easier to add regions and POI without drowning in pointless brand data.

That’s your livability spine. Everything else is a custom input (one-off calculation).